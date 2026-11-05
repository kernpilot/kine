package pgsql

// KUBEHZ-PATCH P2+P3 integration test against a real PostgreSQL. Gated on
// KINE_TEST_PGSQL_DSN (unit.yml provides a postgres service); skipped when it
// is unset. It opens the driver with New() so setup, the reloptions ALTER and
// the one-time ANALYZE all run as in production, then checks that the kine
// table carries the storage parameters (P3) and that the live-size estimate
// tracks the real compressed size of 200 rows with 64 KB incompressible
// values, which live in TOAST (P2, the TOAST term). Then the B481 cases: the
// estimate's floor after a statistics reset, the refresh after a relief, rows
// written after the last VACUUM, and the source's own reset watch.

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/drivers/generic"
	"github.com/k3s-io/kine/pkg/server"
)

func TestKubehzP2P3Postgres(t *testing.T) {
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
	})
	if err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

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

	// B481: a crash or a promoted standby resets the cumulative statistics, so
	// n_live_tup reads 0. The estimate must not drop with it (that would lift
	// the limit); reltuples in the catalog carries it. The query alone, before
	// the source's reset watch refreshes the statistics.
	if _, err := db.ExecContext(ctx, "SELECT pg_stat_reset()"); err != nil {
		t.Fatal(err)
	}
	inBand(t, "the estimate after a statistics reset", estimateOf(db), func() (int64, error) { return want, nil })
	inBand(t, "the source after a statistics reset", live, func() (int64, error) { return want, nil })

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
	refresher := &notifyingDialect{Generic: &generic.Generic{DB: db}}
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

	// B481: a reset after those writes. reltuples alone misses them, so the
	// source must see the reset (its counter went down since the last sample)
	// and refresh the statistics.
	if _, err := db.ExecContext(ctx, "SELECT pg_stat_reset()"); err != nil {
		t.Fatal(err)
	}
	inBand(t, "the source after a reset that follows new rows", live, actualOf(db))

	// The same for a source that starts after the reset (a kine restart after
	// a failover): with no last sample, a counter below the catalog row count
	// is the reset.
	insertRandom(t, db, "/kubehz/p2/after-vacuum/", 100, 100, valueBytes)
	inBand(t, "100 more rows written after the last VACUUM", live, actualOf(db))
	if _, err := db.ExecContext(ctx, "SELECT pg_stat_reset()"); err != nil {
		t.Fatal(err)
	}
	inBand(t, "a new source after a reset", server.LiveSizeOf(backend), actualOf(db))

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
