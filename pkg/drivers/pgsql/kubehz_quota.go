package pgsql

// KUBEHZ-PATCH P2 (pgsql) — this whole file is ours; see kubehz/MERGE-GUIDE.md#p2.
// The live-bytes figure --quota-bytes compares on PostgreSQL.

import (
	"context"
	"database/sql"
	"fmt"
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
// for a reset (statsWatch) and refreshes the statistics when it sees one.
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

// statsCountersSQL reads the cumulative insert counters of the kine table and
// of its TOAST relation, and their catalog row counts. The counters only
// grow, except when the statistics are reset.
const statsCountersSQL = `
	SELECT
		COALESCE((SELECT s.n_tup_ins FROM pg_stat_user_tables AS s
			WHERE s.schemaname = current_schema() AND s.relname = 'kine'), 0),
		COALESCE((SELECT t.n_tup_ins FROM pg_class AS c
			JOIN pg_stat_all_tables AS t ON t.relid = c.reltoastrelid
			WHERE c.oid = 'kine'::regclass), 0),
		(SELECT GREATEST(c.reltuples, 0)::bigint FROM pg_class AS c WHERE c.oid = 'kine'::regclass),
		COALESCE((SELECT GREATEST(tc.reltuples, 0)::bigint FROM pg_class AS c
			JOIN pg_class AS tc ON tc.oid = c.reltoastrelid
			WHERE c.oid = 'kine'::regclass), 0)`

// statsRefreshTimeout bounds the refresh after a reset, so the size read that
// follows still has time inside the sample's own deadline.
const statsRefreshTimeout = 5 * time.Second

// LiveSize implements server.LiveSizer. Each call returns a source with its
// own reset watch; the quota's sampler is its only caller.
func (d *notifyingDialect) LiveSize() server.SizeSource {
	w := &statsWatch{d: d, heapIns: -1}
	return w.liveSize
}

// statsWatch remembers the counters of the last sample. A counter that went
// down means the statistics were reset since. At the first sample there is
// no last value: there a counter below its table's catalog row count means
// rows exist that the counters never saw, which also means a reset.
type statsWatch struct {
	d                 *notifyingDialect
	heapIns, toastIns int64
	owed              bool // a reset was seen and no refresh succeeded yet
}

// liveSize reads the figure. A table without statistics is analyzed first; if
// that fails the sample fails, so the quota keeps its last figure and the
// bytes written since instead of reading a heap term of 0. After a reset it
// refreshes the statistics first. If that fails, the figure is the table's
// size on disk, an upper bound of its live data, until a refresh succeeds.
func (w *statsWatch) liveSize(ctx context.Context) (int64, error) {
	db := w.d.DB
	if err := analyzeIfNoStats(ctx, db); err != nil {
		return 0, err
	}
	var heapIns, toastIns, heapRel, toastRel int64
	if err := db.QueryRowContext(ctx, statsCountersSQL).Scan(&heapIns, &toastIns, &heapRel, &toastRel); err != nil {
		return 0, fmt.Errorf("reading the table counters: %w", err)
	}
	if w.heapIns < 0 {
		w.owed = heapIns < heapRel || toastIns < toastRel
	} else if heapIns < w.heapIns || toastIns < w.toastIns {
		w.owed = true
	}
	w.heapIns, w.toastIns = heapIns, toastIns
	if w.owed {
		rctx, cancel := context.WithTimeout(ctx, statsRefreshTimeout)
		err := w.d.RefreshStats(rctx)
		cancel()
		if err != nil {
			var size int64
			if serr := db.QueryRowContext(ctx, "SELECT pg_table_size('kine')").Scan(&size); serr != nil {
				return 0, fmt.Errorf("the statistics were reset and refreshing them failed (%v); reading the table size: %w", err, serr)
			}
			logrus.Warnf("quota: the table statistics were reset, and refreshing them failed: %v. The table size, %d bytes, is the figure until a refresh succeeds.", err, size)
			return size, nil
		}
		w.owed = false
		logrus.Infof("quota: the table statistics were reset (a crash, a failover or pg_stat_reset); refreshed them for the live-data figure")
	}
	var size int64
	if err := db.QueryRowContext(ctx, liveSizeSQL).Scan(&size); err != nil {
		return 0, err
	}
	return size, nil
}

// compile-time check: the dialect refreshes the statistics after the quota's
// relief compaction.
var _ server.StatsRefresher = (*notifyingDialect)(nil)

// RefreshStats runs VACUUM (ANALYZE) on the kine table after the quota's
// relief deleted rows: ANALYZE for the widths and the heap's row count,
// VACUUM for the TOAST relation's reltuples (ANALYZE does not visit it) and
// to make the freed space reusable. SKIP_LOCKED, so a running autovacuum is
// not waited for; that one updates the same figures. VACUUM cannot run in a
// transaction, so this is a plain statement under ctx.
func (d *notifyingDialect) RefreshStats(ctx context.Context) error {
	if _, err := d.DB.ExecContext(ctx, "VACUUM (ANALYZE, SKIP_LOCKED) kine"); err != nil {
		return fmt.Errorf("vacuuming the kine table: %w", err)
	}
	return nil
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
