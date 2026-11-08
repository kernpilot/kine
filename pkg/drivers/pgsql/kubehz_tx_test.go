package pgsql

// KUBEHZ-PATCH P4 test against a real PostgreSQL (upstream issue
// k3s-io/kine#824). Gated on KINE_TEST_PGSQL_DSN, like kubehz_pgsql_test.go.
// It uses its own database, kine_kubehz_p4_test, so the P2 measurements do
// not see its rows. The hermetic tests are in
// pkg/drivers/generic/kubehz_tx_test.go.

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/drivers/generic"
	"github.com/k3s-io/kine/pkg/server"
)

// TestKubehzP4Postgres: on a pool of one connection a create, an update and a
// delete each complete; a create whose id a gap fill took first is retried in
// its transaction and gets the next id; and 32 creates on a pool of 4 all
// complete. Upstream's insert took a second pool connection, so with one
// connection every write waited until its context ended.
func TestKubehzP4Postgres(t *testing.T) {
	dsn := os.Getenv("KINE_TEST_PGSQL_DSN")
	if dsn == "" {
		t.Skip("KINE_TEST_PGSQL_DSN is not set; this test needs a PostgreSQL")
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("KINE_TEST_PGSQL_DSN: %v", err)
	}
	u.Path = "/kine_kubehz_p4_test"
	// New() takes the DSN without its scheme (see kubehzPostgres)
	source := strings.TrimPrefix(u.String(), u.Scheme+"://")

	open := func(t *testing.T, maxOpen int) (server.Backend, *sql.DB) {
		t.Helper()
		ctx, cancel := context.WithCancel(context.Background())
		wg := &sync.WaitGroup{}
		t.Cleanup(func() {
			cancel()
			wg.Wait()
		})
		_, backend, err := New(ctx, wg, &drivers.Config{
			DataSourceName:       source,
			ConnectionPoolConfig: generic.ConnectionPoolConfig{MaxIdle: maxOpen, MaxOpen: maxOpen},
			CompactTimeout:       5 * time.Second,
			CompactMinRetain:     1000,
			CompactBatchSize:     1000,
			PollBatchSize:        500,
			PollInterval:         time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		db, err := sql.Open("pgx", u.String())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		return backend, db
	}
	within := func(d time.Duration) context.Context {
		ctx, cancel := context.WithTimeout(context.Background(), d)
		t.Cleanup(cancel)
		return ctx
	}
	// liveAt fails unless key is live at a revision above zero, and returns it.
	// LogStructured.Create reports a failed insert of a new key as revision
	// 0 with no error, so the test reads the key back.
	liveAt := func(t *testing.T, backend server.Backend, key string) int64 {
		t.Helper()
		_, kv, err := backend.Get(within(5*time.Second), key, 0, false)
		if err != nil || kv == nil || kv.ModRevision == 0 {
			t.Fatalf("%s after the write: %+v, %v; want a live key", key, kv, err)
		}
		return kv.ModRevision
	}
	run := fmt.Sprint(time.Now().UnixNano())

	t.Run("max-open 1", func(t *testing.T) {
		backend, _ := open(t, 1)
		key := "/kubehz/p4/one/" + run
		if _, err := backend.Create(within(5*time.Second), key, []byte("v"), 0); err != nil {
			t.Fatalf("create: %v", err)
		}
		rev := liveAt(t, backend, key)
		_, _, ok, err := backend.Update(within(5*time.Second), key, []byte("w"), rev, 0)
		if err != nil || !ok {
			t.Fatalf("update at revision %d: ok %v, %v", rev, ok, err)
		}
		rev = liveAt(t, backend, key)
		if _, _, deleted, err := backend.Delete(within(5*time.Second), key, rev); err != nil || !deleted {
			t.Fatalf("delete at revision %d: deleted %v, %v", rev, deleted, err)
		}
	})

	t.Run("kine_pkey retry in the transaction", func(t *testing.T) {
		backend, db := open(t, 1)
		ctx := within(10 * time.Second)
		// the next id the sequence gives; a gap fill takes it first
		var last int64
		var called bool
		if err := db.QueryRowContext(ctx, `SELECT last_value, is_called FROM kine_id_seq`).Scan(&last, &called); err != nil {
			t.Fatal(err)
		}
		next := last
		if called {
			next++
		}
		if _, err := db.ExecContext(ctx,
			`INSERT INTO kine(id, name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			 VALUES ($1, $2, 0, 1, 0, 0, 0, NULL, NULL)`,
			next, fmt.Sprintf("gap-%d", next),
		); err != nil {
			t.Fatal(err)
		}
		key := "/kubehz/p4/retry/" + run
		if _, err := backend.Create(within(5*time.Second), key, []byte("v"), 0); err != nil {
			t.Fatalf("create: %v", err)
		}
		if rev := liveAt(t, backend, key); rev != next+1 {
			t.Fatalf("the create got revision %d, want %d (the id after the taken one)", rev, next+1)
		}
	})

	t.Run("32 writes on a pool of 4", func(t *testing.T) {
		backend, _ := open(t, 4)
		const writers = 32
		start := time.Now()
		errs := make(chan error, writers)
		var wg sync.WaitGroup
		for i := range writers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				key := fmt.Sprintf("/kubehz/p4/many/%s/%d", run, i)
				if _, err := backend.Create(within(10*time.Second), key, []byte("v"), 0); err != nil {
					errs <- fmt.Errorf("create %s: %w", key, err)
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}
		for i := range writers {
			liveAt(t, backend, fmt.Sprintf("/kubehz/p4/many/%s/%d", run, i))
		}
		t.Logf("%d creates on a pool of 4 took %s", writers, time.Since(start).Round(time.Millisecond))
	})
}
