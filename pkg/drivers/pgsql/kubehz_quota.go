package pgsql

// KUBEHZ-PATCH P2 (pgsql) — this whole file is ours; see kubehz/MERGE-GUIDE.md#p2.
// The live-bytes figure --quota-bytes compares on PostgreSQL.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
)

// kubehzStatementTimeout bounds every startup statement the fork adds (the
// statistics check, ANALYZE, the reloptions read and ALTER). An
// anti-wraparound autovacuum can hold the lock an ALTER waits on; kine must
// start anyway, so these fail into a warning after this long.
const kubehzStatementTimeout = 10 * time.Second

// toastChunkBytes is TOAST_MAX_CHUNK_SIZE on 8 KB pages: the payload of one
// row in a TOAST table. A value counts as whole chunks, so the TOAST term is
// an upper bound within one chunk per value.
const toastChunkBytes = 1996

// liveSizeSQL estimates the on-disk bytes of live rows in the kine table, in
// two terms that together are the compressed size of the live data:
//
//  1. heap: n_live_tup × Σ avg_width. n_live_tup is the cumulative-statistics
//     live row count, kept current by the stats collector and autovacuum;
//     avg_width is what ANALYZE recorded in pg_stats per column. ANALYZE
//     measures the datum as stored inline, so an out-of-line TOAST value
//     (a value or old_value still above about 2 KB after compression: most
//     Pods, Secrets and CRs) counts as its 18-byte pointer here.
//  2. TOAST: the toast relation's n_live_tup × toastChunkBytes. That is where
//     those values live, one row per chunk. The inline widths are compressed
//     sizes and the chunks hold compressed data, so the whole figure is the
//     compressed size of live rows. etcd's in-use figure is uncompressed; a
//     kine cluster reads smaller for the same objects.
//
// Both row counts are GREATEST(n_live_tup, pg_class.reltuples). PostgreSQL
// resets the cumulative statistics after a crash and on a promoted standby
// (a CNPG failover), and n_live_tup then counts only the rows written since:
// alone it would lift the limit (B481). reltuples is in the catalog, so it
// survives both; VACUUM and ANALYZE set it, and it reads -1 before the
// first one. After a large delete it stays high until the next VACUUM, which
// only refuses early; the quota's relief runs one. But reltuples misses the
// rows written after the last VACUUM or ANALYZE, so the source also watches
// for a reset (statsWatch): it then reads the table size until a VACUUM ran,
// and reports the reset so that the quota schedules one.
//
// A few catalog lookups and no table scan. Indexes and page overhead are not
// counted. Before the first ANALYZE pg_stats has no rows and the heap term
// would be 0, so liveSize analyzes first when that is the case.
// pg_total_relation_size was rejected for this role: it is the file, which
// plateaus under autovacuum and stays large after a write-then-delete spike.
var liveSizeSQL = fmt.Sprintf(`
	SELECT
		COALESCE((
			SELECT GREATEST(s.n_live_tup, c.reltuples::bigint) * SUM(p.avg_width)
			FROM pg_stat_user_tables AS s
			JOIN pg_class AS c ON c.oid = s.relid
			JOIN pg_stats AS p ON p.schemaname = s.schemaname AND p.tablename = s.relname
			WHERE s.schemaname = current_schema() AND s.relname = 'kine'
			GROUP BY s.n_live_tup, c.reltuples
		), 0)::bigint
		+ COALESCE((
			SELECT GREATEST(t.n_live_tup, tc.reltuples::bigint) * %d
			FROM pg_class AS c
			JOIN pg_class AS tc ON tc.oid = c.reltoastrelid
			JOIN pg_stat_all_tables AS t ON t.relid = c.reltoastrelid
			WHERE c.oid = 'kine'::regclass
		), 0)::bigint`, toastChunkBytes)

// statsExistSQL tells whether ANALYZE has ever recorded the kine table.
const statsExistSQL = `
	SELECT EXISTS (
		SELECT 1 FROM pg_stats
		WHERE schemaname = current_schema() AND tablename = 'kine' AND attname = 'value'
	)`

// compile-time check: the dialect handed to sqllog.New offers a live figure.
var _ server.LiveSizer = (*notifyingDialect)(nil)

// statsCountersSQL reads, for the kine table and its TOAST relation, the
// cumulative insert counters, the catalog row counts and the vacuum counts
// (manual and autovacuum). The counters only grow, except when the
// statistics are reset, which also sets the vacuum counts to 0.
const statsCountersSQL = `
	SELECT
		COALESCE(s.n_tup_ins, 0),
		COALESCE(t.n_tup_ins, 0),
		GREATEST(c.reltuples, 0)::bigint,
		COALESCE(GREATEST(tc.reltuples, 0), 0)::bigint,
		COALESCE(s.vacuum_count + s.autovacuum_count, 0),
		COALESCE(t.vacuum_count + t.autovacuum_count, 0),
		c.reltoastrelid <> 0
	FROM pg_class AS c
	LEFT JOIN pg_stat_all_tables AS s ON s.relid = c.oid
	LEFT JOIN pg_class AS tc ON tc.oid = c.reltoastrelid
	LEFT JOIN pg_stat_all_tables AS t ON t.relid = c.reltoastrelid
	WHERE c.oid = 'kine'::regclass`

// vacuumCountsSQL reads the manual vacuum counts of the kine table and of its
// TOAST relation, and whether it has one.
const vacuumCountsSQL = `
	SELECT COALESCE(s.vacuum_count, 0), COALESCE(t.vacuum_count, 0), c.reltoastrelid <> 0
	FROM pg_class AS c
	LEFT JOIN pg_stat_all_tables AS s ON s.relid = c.oid
	LEFT JOIN pg_stat_all_tables AS t ON t.relid = c.reltoastrelid
	WHERE c.oid = 'kine'::regclass`

// errVacuumSkipped is RefreshStats' error when the VACUUM did not process
// the table: SKIP_LOCKED skips a relation that another process holds
// (autovacuum, a manual VACUUM, DDL) and reports no error for it.
var errVacuumSkipped = errors.New("the VACUUM skipped the kine table or its TOAST table, which another process holds")

// statsWatches holds the newest reset watch of each dialect, for StatsOwed.
// A map here keeps the dialect's struct (pgsql.go) as upstream has it.
var statsWatches sync.Map // *notifyingDialect -> *statsWatch

// LiveSize implements server.LiveSizer. Each call returns a source with its
// own reset watch; the quota's sampler is its only caller.
func (d *notifyingDialect) LiveSize() server.SizeSource {
	w := &statsWatch{d: d, heapIns: -1}
	statsWatches.Store(d, w)
	return w.liveSize
}

// StatsOwed implements server.StatsRefresher: the newest source saw a reset
// that no VACUUM settled yet.
func (d *notifyingDialect) StatsOwed() bool {
	w, ok := statsWatches.Load(d)
	return ok && w.(*statsWatch).owed.Load()
}

// statsWatch remembers the counters of the last sample. An insert or vacuum
// count that went down means the statistics were reset since. The vacuum
// counts show a reset that the insert counts miss: one that comes while they
// still read 0 from the reset before, with rows written since the last
// sample. At the first sample there is
// no last value: there a counter below its table's catalog row count, with
// no vacuum since the counters began, means rows exist that the counters
// never saw, which also means a reset. A vacuum count of 0 on the table or
// its TOAST relation is the other half: a reset sets them to 0, and any
// VACUUM after it (the quota's refresh, autovacuum) sets the live row counts
// again, so the reset is settled once both counts are above 0. The watch
// runs no VACUUM itself: it reports the reset (StatsOwed), and the quota
// schedules the refresh with the rest of its maintenance, so that no VACUUM
// overlaps or closely follows the relief's deletes.
type statsWatch struct {
	d                 *notifyingDialect
	heapIns, toastIns int64       // -1 before the first sample; sampler goroutine only
	heapVac, toastVac int64       // sampler goroutine only
	owed              atomic.Bool // a reset was seen and no VACUUM ran since
}

// liveSize reads the figure. A table without statistics is analyzed first; if
// that fails the sample fails, so the quota keeps its last figure and the
// bytes written since instead of reading a heap term of 0. Until a VACUUM
// settles a reset, the figure is the table's size on disk, an upper bound of
// its live data.
func (w *statsWatch) liveSize(ctx context.Context) (int64, error) {
	db := w.d.DB
	if err := analyzeIfNoStats(ctx, db); err != nil {
		return 0, err
	}
	var heapIns, toastIns, heapRel, toastRel, heapVac, toastVac int64
	var hasToast bool
	if err := db.QueryRowContext(ctx, statsCountersSQL).Scan(&heapIns, &toastIns, &heapRel, &toastRel, &heapVac, &toastVac, &hasToast); err != nil {
		return 0, fmt.Errorf("reading the table counters: %w", err)
	}
	vacuumed := heapVac > 0 && (!hasToast || toastVac > 0)
	owed := w.owed.Load()
	if w.heapIns < 0 {
		owed = (heapIns < heapRel || toastIns < toastRel) && !vacuumed
	} else if heapIns < w.heapIns || toastIns < w.toastIns || heapVac < w.heapVac || toastVac < w.toastVac {
		owed = true
	}
	w.heapIns, w.toastIns, w.heapVac, w.toastVac = heapIns, toastIns, heapVac, toastVac
	if owed && vacuumed {
		owed = false
		logrus.Infof("quota: a VACUUM settled the statistics reset; the live-data figure is the estimate again")
	}
	if owed && !w.owed.Load() {
		logrus.Warnf("quota: the table statistics were reset (a crash, a failover or pg_stat_reset). The table size is the figure until a VACUUM ran; the quota schedules one.")
	}
	w.owed.Store(owed)
	if owed {
		var size int64
		if err := db.QueryRowContext(ctx, "SELECT pg_table_size('kine')").Scan(&size); err != nil {
			return 0, fmt.Errorf("the statistics were reset and no VACUUM ran since; reading the table size: %w", err)
		}
		return size, nil
	}
	var size int64
	if err := db.QueryRowContext(ctx, liveSizeSQL).Scan(&size); err != nil {
		return 0, err
	}
	return size, nil
}

// compile-time check: the dialect refreshes the statistics when the quota
// schedules it (after the relief's deletes settled, or after a reset).
var _ server.StatsRefresher = (*notifyingDialect)(nil)

// RefreshStats runs VACUUM (ANALYZE) on the kine table when the quota asks
// (after its relief deleted rows, or after a statistics reset): ANALYZE for
// the widths and the heap's row count,
// VACUUM for the TOAST relation's reltuples (ANALYZE does not visit it) and
// to make the freed space reusable. SKIP_LOCKED, so a running autovacuum is
// not waited for. A skipped relation is no error to PostgreSQL, so the
// manual vacuum counts must go up on both, or this returns errVacuumSkipped
// and the caller tries again later. VACUUM cannot run in a transaction, so
// this is a plain statement under ctx.
func (d *notifyingDialect) RefreshStats(ctx context.Context) error {
	heapBefore, toastBefore, _, err := vacuumCounts(ctx, d.DB)
	if err != nil {
		return err
	}
	if _, err := d.DB.ExecContext(ctx, "VACUUM (ANALYZE, SKIP_LOCKED) kine"); err != nil {
		return fmt.Errorf("vacuuming the kine table: %w", err)
	}
	// PostgreSQL 15 and later write the counts at once; older ones send
	// them to the statistics collector, so give them a moment
	for range 10 {
		heap, toast, hasToast, err := vacuumCounts(ctx, d.DB)
		if err != nil {
			return err
		}
		if heap > heapBefore && (!hasToast || toast > toastBefore) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return errVacuumSkipped
}

// vacuumCounts reads the manual vacuum counts of the kine table and its TOAST
// relation.
func vacuumCounts(ctx context.Context, db *sql.DB) (heap, toast int64, hasToast bool, err error) {
	if err = db.QueryRowContext(ctx, vacuumCountsSQL).Scan(&heap, &toast, &hasToast); err != nil {
		err = fmt.Errorf("reading the vacuum counts: %w", err)
	}
	return heap, toast, hasToast, err
}

// analyzeIfNoStats runs ANALYZE once on a table pg_stats does not know yet
// (a new table, or one created before autovacuum ran), so the live figure is
// not 0 until the first autoanalyze. Bounded by kubehzStatementTimeout.
func analyzeIfNoStats(ctx context.Context, db *sql.DB) error {
	ctx, cancel := context.WithTimeout(ctx, kubehzStatementTimeout)
	defer cancel()
	var exists bool
	if err := db.QueryRowContext(ctx, statsExistSQL).Scan(&exists); err != nil {
		return fmt.Errorf("reading the kine table's statistics: %w", err)
	}
	if exists {
		return nil
	}
	if _, err := db.ExecContext(ctx, "ANALYZE kine"); err != nil {
		return fmt.Errorf("analyzing the kine table: %w", err)
	}
	logrus.Infof("Analyzed the kine table once: it had no statistics yet for the live-data figure")
	return nil
}
