package pgsql

// KUBEHZ-PATCH P2+P3 integration test against a real PostgreSQL. Gated on
// KINE_TEST_PGSQL_DSN (unit.yml provides a postgres service); skipped when it
// is unset. It opens the driver with New() so setup, the reloptions ALTER and
// the one-time ANALYZE all run as in production, then checks that the kine
// table carries the storage parameters (P3) and that the live-size estimate
// tracks the real compressed size of 200 rows with 64 KB incompressible
// values, which live in TOAST (P2, the TOAST term). Then the B481 cases: the
// estimate's floor after a statistics reset, the refresh after a relief, rows
// written after the last VACUUM, and the source's reset watch, which reports
// a reset (StatsOwed) and the table size until the quota's refresh pass ran.

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/drivers/generic"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
)

func TestKubehzP2P3Postgres(t *testing.T) {
	ctx, backend, db := kubehzPostgres(t)

	// P3: the table carries both storage parameters after one start
	var reloptions string
	if err := db.QueryRowContext(ctx,
		`SELECT COALESCE(array_to_string(reloptions, ','), '') FROM pg_class WHERE oid = 'kine'::regclass`,
	).Scan(&reloptions); err != nil {
		t.Fatal(err)
	}
	if stmt := relOptionsPlan(false, reloptions); stmt != "" {
		t.Fatalf("kine reloptions %q are incomplete after New(): the plan still wants %q", reloptions, stmt)
	}

	// P2: 200 rows, 64 KB of random bytes each, so every value is stored
	// out of line in TOAST and compresses to nothing
	if _, err := db.ExecContext(ctx, "TRUNCATE kine"); err != nil {
		t.Fatal(err)
	}
	const rows, valueBytes = 200, 64 << 10
	for i := range rows {
		value := make([]byte, valueBytes)
		if _, err := rand.Read(value); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx,
			`INSERT INTO kine (name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			 VALUES ($1, 1, 0, 0, 0, 0, $2, $3)`,
			fmt.Sprintf("/kubehz/p2/%d", i), value, []byte{},
		); err != nil {
			t.Fatal(err)
		}
	}
	// VACUUM reports live tuples for the heap and its TOAST table at once;
	// ANALYZE records the inline widths. This is what autovacuum does later.
	if _, err := db.ExecContext(ctx, "VACUUM ANALYZE kine"); err != nil {
		t.Fatal(err)
	}
	var want int64
	if err := db.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(pg_column_size(value) + pg_column_size(old_value)), 0) FROM kine`,
	).Scan(&want); err != nil {
		t.Fatal(err)
	}

	live := server.LiveSizeOf(backend)
	if live == nil {
		t.Fatal("the pgsql backend offers no live-size source")
	}
	inBand(t, "200 TOAST rows", live, func() (int64, error) { return want, nil })
	relief, ok := backend.(server.QuotaRelief)
	if !ok {
		t.Fatal("the pgsql backend offers no quota relief")
	}
	// owedAtTableSize: after a reset the source reports it (StatsOwed) and its
	// figure is the table size, until a VACUUM ran.
	owedAtTableSize := func(what string, src server.SizeSource) {
		t.Helper()
		got, err := src(ctx)
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		var onDisk int64
		if err := db.QueryRowContext(ctx, "SELECT pg_table_size('kine')").Scan(&onDisk); err != nil {
			t.Fatal(err)
		}
		if got != onDisk || !relief.StatsOwed() {
			t.Fatalf("%s: the figure is %d and StatsOwed %v; want the table size %d and true", what, got, relief.StatsOwed(), onDisk)
		}
	}
	// refreshPass is the quota's refresh pass after a reset.
	refreshPass := func(what string) {
		t.Helper()
		if res, err := relief.RelieveQuota(ctx, 0, true); err != nil || !res.Refreshed || res.Rows != 0 {
			t.Fatalf("%s: the refresh pass: refreshed %v, rows %d, err %v; want a refresh and no rows", what, res.Refreshed, res.Rows, err)
		}
		// the refresh itself settles the reset, before any sample: samples
		// that fail after it must not keep the refresh owed
		if relief.StatsOwed() {
			t.Fatalf("%s: StatsOwed after a refresh that ran, before the next sample", what)
		}
	}

	// B481: a crash or a promoted standby resets the cumulative statistics, so
	// n_live_tup reads 0. The estimate must not drop with it (that would lift
	// the limit); reltuples in the catalog carries it. The query alone.
	// The source sees the reset only when its last sample had the inserts, so
	// wait for their counters first.
	waitInserts(t, db, rows)
	if _, err := live(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "SELECT pg_stat_reset()"); err != nil {
		t.Fatal(err)
	}
	inBand(t, "the estimate after a statistics reset", estimateOf(db), func() (int64, error) { return want, nil })
	owedAtTableSize("the source after a statistics reset", live)
	refreshPass("after a statistics reset")
	inBand(t, "the source after a statistics reset and the refresh", live, func() (int64, error) { return want, nil })
	if relief.StatsOwed() {
		t.Fatal("StatsOwed after the refresh settled the reset")
	}

	// B481: after the quota's relief deletes rows, RefreshStats brings the
	// estimate down to what is left (reltuples alone would keep it high). The
	// relief refreshes one tick after its delete, when the deleting backend's
	// counters are in the shared statistics; a VACUUM before that subtracts
	// the deleted rows twice. Here: wait for the counters, then refresh.
	heapDel, toastDel := deleteCounters(t, db)
	if _, err := db.ExecContext(ctx, `DELETE FROM kine WHERE name LIKE '/kubehz/p2/%' AND substr(name, 12)::int % 2 = 0`); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	deadline := start.Add(statsWait)
	for h, tt := deleteCounters(t, db); h < heapDel+rows/2 || tt < toastDel+rows/2*toastChunks(valueBytes); h, tt = deleteCounters(t, db) {
		if time.Now().After(deadline) {
			t.Fatalf("the delete's counters did not reach the statistics in %s: heap %d, toast %d deletes", statsWait, h-heapDel, tt-toastDel)
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Logf("the delete's counters landed after %s", time.Since(start).Truncate(time.Millisecond))
	refresher := &quotaDialect{Generic: &generic.Generic{DB: db}}
	if err := refresher.RefreshStats(ctx); err != nil {
		t.Fatalf("RefreshStats: %v", err)
	}
	inBand(t, "after deleting half and RefreshStats", live, actualOf(db))

	// B481: rows written after the last VACUUM or ANALYZE are not in
	// reltuples; n_live_tup counts them. Autovacuum is off for these phases,
	// so only the source can bring reltuples up to date.
	if _, err := db.ExecContext(ctx, "ALTER TABLE kine SET (autovacuum_enabled = off, toast.autovacuum_enabled = off)"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := db.ExecContext(context.Background(), "ALTER TABLE kine RESET (autovacuum_enabled, toast.autovacuum_enabled)"); err != nil {
			t.Errorf("restoring autovacuum on the kine table: %v", err)
		}
	})
	insertRandom(t, db, "/kubehz/p2/after-vacuum/", 0, 100, valueBytes)
	inBand(t, "100 rows written after the last VACUUM", live, actualOf(db))

	// B481: a reset after those writes. reltuples alone misses them (the
	// estimate would read low), so the source must see the reset (its counter
	// went down since the last sample) and report it until the refresh.
	if _, err := db.ExecContext(ctx, "SELECT pg_stat_reset()"); err != nil {
		t.Fatal(err)
	}
	owedAtTableSize("the source after a reset that follows new rows", live)
	// the insert counters that sample read were 0 (they only go up until the
	// next reset), so only the vacuum counts can show the second reset below
	var heapIns, toastIns, heapRel, toastRel, heapVac, toastVac int64
	var hasToast bool
	if err := db.QueryRowContext(ctx, statsCountersSQL).Scan(&heapIns, &toastIns, &heapRel, &toastRel, &heapVac, &toastVac, &hasToast); err != nil {
		t.Fatal(err)
	}
	if heapIns != 0 || toastIns != 0 {
		t.Fatalf("insert counters %d and %d after the reset; the second reset needs them at 0 to test the vacuum baseline", heapIns, toastIns)
	}
	refreshPass("after a reset that follows new rows")
	// another reset before the source sampled again: the refresh's vacuum
	// counts are the baseline, so it still shows as a decrease
	if _, err := db.ExecContext(ctx, "SELECT pg_stat_reset()"); err != nil {
		t.Fatal(err)
	}
	owedAtTableSize("the source after a second reset right after the refresh", live)
	refreshPass("after the second reset")
	inBand(t, "the source after a reset that follows new rows and the refresh", live, actualOf(db))

	// The same for a source that starts after the reset (a kine restart after
	// a failover): with no last sample, a counter below the catalog row count
	// is the reset.
	insertRandom(t, db, "/kubehz/p2/after-vacuum/", 100, 100, valueBytes)
	inBand(t, "100 more rows written after the last VACUUM", live, actualOf(db))
	if _, err := db.ExecContext(ctx, "SELECT pg_stat_reset()"); err != nil {
		t.Fatal(err)
	}
	fresh := server.LiveSizeOf(backend)
	owedAtTableSize("a new source after a reset", fresh)
	refreshPass("a new source after a reset")
	inBand(t, "a new source after a reset and the refresh", fresh, actualOf(db))

	// B481: no false reset at a start. After a reset and a VACUUM the insert
	// counters stay below the catalog row count, as they do for a moment on
	// a busy table. A VACUUM ran since the counters began, so a new source
	// sees no reset, owes no refresh and logs no reset.
	if _, err := db.ExecContext(ctx, "SELECT pg_stat_reset()"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "VACUUM kine"); err != nil {
		t.Fatal(err)
	}
	logs := &logtest.Hook{}
	hooks := logrus.StandardLogger().ReplaceHooks(logrus.LevelHooks{})
	logrus.AddHook(logs)
	src := server.LiveSizeOf(backend) // the newest source from here on, as the quota has one
	inBand(t, "a new source after a reset and a VACUUM", src, actualOf(db))
	logrus.StandardLogger().ReplaceHooks(hooks)
	if relief.StatsOwed() {
		t.Fatal("a new source owes a refresh for a reset that a VACUUM had settled")
	}
	for _, e := range logs.AllEntries() {
		if strings.Contains(e.Message, "were reset") || strings.Contains(e.Message, "settled the statistics reset") {
			t.Fatalf("the new source logged %q, but it saw no reset: a VACUUM had settled it", e.Message)
		}
	}

	// B481: VACUUM (SKIP_LOCKED) skips a table another process holds and
	// reports no error; RefreshStats must report that it did not run.
	lock, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lock.Rollback() }()
	if _, err := lock.ExecContext(ctx, "LOCK TABLE kine IN SHARE UPDATE EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	if err := refresher.RefreshStats(ctx); !errors.Is(err, errVacuumSkipped) {
		t.Fatalf("RefreshStats with the table held: %v, want %v", err, errVacuumSkipped)
	}

	// B481: a reset while no VACUUM can run. The figure is the table's size
	// on disk, an upper bound, until a VACUUM runs; then the estimate again.
	if _, err := db.ExecContext(ctx, "SELECT pg_stat_reset()"); err != nil {
		t.Fatal(err)
	}
	var onDisk int64
	if err := db.QueryRowContext(ctx, "SELECT pg_table_size('kine')").Scan(&onDisk); err != nil {
		t.Fatal(err)
	}
	if got, err := src(ctx); err != nil || got != onDisk || !relief.StatsOwed() {
		t.Fatalf("the figure after a reset with the table held: %d (err %v, StatsOwed %v), want the table size %d and owed", got, err, relief.StatsOwed(), onDisk)
	}
	// The quota's refresh pass reports that the VACUUM skipped the table, and
	// the refresh stays owed.
	if res, err := relief.RelieveQuota(ctx, 0, true); !errors.Is(err, errVacuumSkipped) || res.Refreshed {
		t.Fatalf("the refresh pass with the table held: refreshed %v, err %v; want %v", res.Refreshed, err, errVacuumSkipped)
	}
	if _, err := src(ctx); err != nil || !relief.StatsOwed() {
		t.Fatalf("after a skipped refresh: err %v, StatsOwed %v; want still owed", err, relief.StatsOwed())
	}
	// Any VACUUM settles the reset, also one from elsewhere (autovacuum, the
	// relief's): run one with the table free, then hold the table again, so
	// the source cannot refresh by itself. The figure is the estimate.
	if err := lock.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "VACUUM kine"); err != nil {
		t.Fatal(err)
	}
	again, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = again.Rollback() }()
	if _, err := again.ExecContext(ctx, "LOCK TABLE kine IN SHARE UPDATE EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	got, err := src(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if relief.StatsOwed() {
		t.Fatal("StatsOwed after a VACUUM from elsewhere")
	}
	est, err := estimateOf(db)(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got != est || got == onDisk {
		t.Fatalf("after a VACUUM from elsewhere the figure is %d, want the estimate %d (the table size is %d)", got, est, onDisk)
	}
	t.Logf("after a VACUUM from elsewhere: the estimate %d (the table size was %d)", got, onDisk)
	if err := again.Rollback(); err != nil {
		t.Fatal(err)
	}

	// The same statistics reset with values small enough to stay inline, so
	// the heap term carries the figure and its reltuples floor is the one
	// under test.
	if _, err := db.ExecContext(ctx, "TRUNCATE kine"); err != nil {
		t.Fatal(err)
	}
	// 4000 rows of 1 KiB random bytes (64 md5 digests each), in one statement
	if _, err := db.ExecContext(ctx, `
		INSERT INTO kine (name, created, deleted, create_revision, prev_revision, lease, value, old_value)
		SELECT '/kubehz/p2/inline/' || g, 1, 0, 0, 0, 0,
			decode((SELECT string_agg(md5(random()::text || g || k), '') FROM generate_series(1, 64) AS k), 'hex'),
			''::bytea
		FROM generate_series(1, 4000) AS g`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "VACUUM ANALYZE kine"); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(pg_column_size(value) + pg_column_size(old_value)), 0) FROM kine`,
	).Scan(&want); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "SELECT pg_stat_reset()"); err != nil {
		t.Fatal(err)
	}
	inBand(t, "the estimate for inline rows after a statistics reset", estimateOf(db), func() (int64, error) { return want, nil })
}

// B481: the quota's refresh VACUUM does not truncate the table. The
// truncation phase takes an ACCESS EXCLUSIVE lock, which SKIP_LOCKED does
// not avoid, so it would block reads and writes behind it (and replay on a
// standby). Autovacuum still truncates, with its own back-off.
func TestKubehzP2RefreshKeepsTrailingPages(t *testing.T) {
	ctx, _, db := kubehzPostgres(t)
	if _, err := db.ExecContext(ctx, "TRUNCATE kine"); err != nil {
		t.Fatal(err)
	}
	// no autovacuum may truncate behind the test's back
	if _, err := db.ExecContext(ctx, "ALTER TABLE kine SET (autovacuum_enabled = off, toast.autovacuum_enabled = off)"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := db.ExecContext(context.Background(), "ALTER TABLE kine RESET (autovacuum_enabled, toast.autovacuum_enabled)"); err != nil {
			t.Errorf("restoring autovacuum on the kine table: %v", err)
		}
	})
	// 4000 inline rows of 1 KiB, then the newest 3000 deleted: they fill the
	// pages at the end of the heap, so those pages are empty after a VACUUM
	if _, err := db.ExecContext(ctx, `
		INSERT INTO kine (name, created, deleted, create_revision, prev_revision, lease, value, old_value)
		SELECT '/kubehz/p2/tail/' || g, 1, 0, 0, 0, 0,
			decode((SELECT string_agg(md5(random()::text || g || k), '') FROM generate_series(1, 64) AS k), 'hex'),
			''::bytea
		FROM generate_series(1, 4000) AS g`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM kine WHERE id > (SELECT MIN(id) + 999 FROM kine)`); err != nil {
		t.Fatal(err)
	}
	heapBytes := func() int64 {
		t.Helper()
		var size int64
		if err := db.QueryRowContext(ctx, `SELECT pg_relation_size('kine')`).Scan(&size); err != nil {
			t.Fatal(err)
		}
		return size
	}
	before := heapBytes()
	refresher := &quotaDialect{Generic: &generic.Generic{DB: db}}
	if err := refresher.RefreshStats(ctx); err != nil {
		t.Fatalf("RefreshStats: %v", err)
	}
	if after := heapBytes(); after != before {
		t.Fatalf("RefreshStats truncated the heap from %d to %d bytes; its VACUUM must not take the truncation lock", before, after)
	}
	// The control: a plain VACUUM truncates the same empty pages, so the
	// pages were there to truncate
	if _, err := db.ExecContext(ctx, "VACUUM kine"); err != nil {
		t.Fatal(err)
	}
	if after := heapBytes(); after >= before {
		t.Fatalf("the control VACUUM left the heap at %d bytes (it was %d): the test made no empty trailing pages", after, before)
	}
}

// kubehzPostgres opens the PostgreSQL in KINE_TEST_PGSQL_DSN through the
// driver (which creates the table and sets its storage parameters) and as a
// plain connection. It skips the test when the variable is unset.
func kubehzPostgres(t *testing.T) (context.Context, server.Backend, *sql.DB) {
	t.Helper()
	dsn := os.Getenv("KINE_TEST_PGSQL_DSN")
	if dsn == "" {
		t.Skip("KINE_TEST_PGSQL_DSN is not set; this test needs a PostgreSQL")
	}
	// New() takes the DSN without its scheme, as the driver registry hands it
	// over (prepareConfig prepends "postgres://" again)
	_, source, found := strings.Cut(dsn, "://")
	if !found {
		t.Fatalf("KINE_TEST_PGSQL_DSN %q has no scheme; want postgres://user:pass@host:port/db", dsn)
	}

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})
	_, backend, err := New(ctx, wg, &drivers.Config{
		DataSourceName:       source,
		ConnectionPoolConfig: generic.ConnectionPoolConfig{MaxIdle: 2, MaxOpen: 4, MaxIdleTime: time.Minute},
		CompactTimeout:       5 * time.Second,
		CompactMinRetain:     1000,
		CompactBatchSize:     1000,
		PollBatchSize:        500,
		PollInterval:         time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return ctx, backend, db
}

// estimateOf is the live-size query alone, without the source's reset watch.
func estimateOf(db *sql.DB) server.SizeSource {
	return func(ctx context.Context) (int64, error) {
		var size int64
		err := db.QueryRowContext(ctx, liveSizeSQL).Scan(&size)
		return size, err
	}
}

// actualOf is the compressed size of the stored values, which the estimate
// tracks.
func actualOf(db *sql.DB) func() (int64, error) {
	return func() (int64, error) {
		var size int64
		err := db.QueryRow(`SELECT COALESCE(SUM(pg_column_size(value) + pg_column_size(old_value)), 0) FROM kine`).Scan(&size)
		return size, err
	}
}

// statsWait is how long the test waits for counters to reach the shared
// statistics. PostgreSQL flushes an idle backend's counters every 10 s.
const statsWait = 45 * time.Second

// inBand reads src until its figure is within 0.9 and 1.3 of the actual size
// or statsWait passes: the cumulative statistics land with a lag.
func inBand(t *testing.T, what string, src server.SizeSource, actual func() (int64, error)) {
	t.Helper()
	want, err := actual()
	if err != nil {
		t.Fatal(err)
	}
	var got int64
	var ratio float64
	start := time.Now()
	deadline := start.Add(statsWait)
	for {
		if got, err = src(context.Background()); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		ratio = float64(got) / float64(want)
		if ratio >= 0.9 && ratio <= 1.3 || time.Now().After(deadline) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	// unit.yml greps "live-size ratio" to prove the test ran
	t.Logf("live-size ratio, %s: estimate %d / actual %d = %.3f after %s", what, got, want, ratio, time.Since(start).Truncate(time.Millisecond))
	if ratio < 0.9 || ratio > 1.3 {
		t.Fatalf("%s: the estimate is %d, %.3f of the actual %d; want within 0.9 and 1.3", what, got, ratio, want)
	}
}

// deleteCounters reads the cumulative delete counters of the kine table and
// of its TOAST relation.
func deleteCounters(t *testing.T, db *sql.DB) (heap, toast int64) {
	t.Helper()
	if err := db.QueryRow(`
		SELECT s.n_tup_del, t.n_tup_del
		FROM pg_class AS c
		JOIN pg_stat_all_tables AS s ON s.relid = c.oid
		JOIN pg_stat_all_tables AS t ON t.relid = c.reltoastrelid
		WHERE c.oid = 'kine'::regclass`).Scan(&heap, &toast); err != nil {
		t.Fatal(err)
	}
	return heap, toast
}

// waitInserts waits until the kine table's insert counter shows at least n
// rows: a backend sends its counters to the shared statistics with a lag.
func waitInserts(t *testing.T, db *sql.DB, n int64) {
	t.Helper()
	deadline := time.Now().Add(statsWait)
	for {
		var ins int64
		if err := db.QueryRow(`SELECT n_tup_ins FROM pg_stat_all_tables WHERE relid = 'kine'::regclass`).Scan(&ins); err != nil {
			t.Fatal(err)
		}
		if ins >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the insert counter shows %d rows after %s, want %d", ins, statsWait, n)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// toastChunks is the number of TOAST rows of one incompressible value.
func toastChunks(valueBytes int) int64 {
	return int64((valueBytes + toastChunkBytes - 1) / toastChunkBytes)
}

// insertRandom inserts n rows of size random bytes, named prefix + from..from+n-1.
func insertRandom(t *testing.T, db *sql.DB, prefix string, from, n, size int) {
	t.Helper()
	for i := from; i < from+n; i++ {
		value := make([]byte, size)
		if _, err := rand.Read(value); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(
			`INSERT INTO kine (name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			 VALUES ($1, 1, 0, 0, 0, 0, $2, $3)`,
			fmt.Sprintf("%s%d", prefix, i), value, []byte{},
		); err != nil {
			t.Fatal(err)
		}
	}
}
