package pgsql

// KUBEHZ-PATCH P2+P3 integration test against a real PostgreSQL. Gated on
// KINE_TEST_PGSQL_DSN (unit.yml provides a postgres service); skipped when it
// is unset. It opens the driver with New() so setup, the reloptions ALTER and
// the one-time ANALYZE all run as in production, then checks that the kine
// table carries the storage parameters (P3) and that the live-size estimate
// tracks the real compressed size of 200 rows with 64 KB incompressible
// values, which live in TOAST (P2, the TOAST term).

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
	// cumulative statistics land with a small lag; poll instead of sleeping
	var got int64
	var ratio float64
	deadline := time.Now().Add(15 * time.Second)
	for {
		if got, err = live(ctx); err != nil {
			t.Fatal(err)
		}
		ratio = float64(got) / float64(want)
		if ratio >= 0.9 && ratio <= 1.3 || time.Now().After(deadline) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Logf("live-size ratio: estimate %d / actual %d = %.3f (%d rows of %d bytes)", got, want, ratio, rows, valueBytes)
	if ratio < 0.9 || ratio > 1.3 {
		t.Fatalf("live-size estimate %d is %.3f of the actual %d; want within 0.9 and 1.3", got, ratio, want)
	}
}
