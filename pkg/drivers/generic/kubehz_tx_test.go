package generic_test

// KUBEHZ-PATCH P6 tests: a write holds one pool connection, and the kine_pkey
// retry works inside the write's transaction (upstream issue
// k3s-io/kine#824). Hermetic: SQLLog.Append on generic's dialect over a fake
// database/sql driver that keeps the PostgreSQL rules the fix rests on.
// unit.yml lists these tests by name before running them, so a sync that
// drops this file fails CI. pkg/drivers/pgsql/kubehz_tx_test.go runs the same
// writes against a real PostgreSQL.

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/drivers/generic"
	"github.com/k3s-io/kine/pkg/logstructured/sqllog"
	"github.com/k3s-io/kine/pkg/server"
)

// pgFake is a database/sql driver with the PostgreSQL rules P6 rests on:
//   - a failed statement aborts its transaction: every later statement fails
//     with 25P02 until ROLLBACK or ROLLBACK TO SAVEPOINT, and COMMIT then
//     rolls back and fails;
//   - the id sequence moves for every insert, also for one that fails;
//   - an insert whose id exists fails with a unique violation of kine_pkey
//     (a gap fill took that id);
//   - the insert writes only when the newest row of its key has the compare
//     revision, as upstream's insert statement;
//   - the rows of a transaction show to other connections after its commit.
//
// gate, when set, holds the first gate BEGINs until gate BEGINs arrived, so
// that many transactions hold their connections at the same time.
type pgFake struct {
	mu      sync.Mutex
	seq     int64
	rows    map[int64]fakeRow
	opened  int
	inserts []bool // per insert: did it run inside a transaction
	saves   []string
	gate    int
	begins  int
	gateCh  chan struct{}
}

type fakeRow struct {
	name    string
	deleted bool
}

func newPgFake(gate int) *pgFake {
	return &pgFake{rows: map[int64]fakeRow{}, gate: gate, gateCh: make(chan struct{})}
}

func (f *pgFake) Connect(context.Context) (driver.Conn, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.opened++
	return &fakeConn{f: f}, nil
}

func (f *pgFake) Driver() driver.Driver { return fakeDriver{f} }

type fakeDriver struct{ f *pgFake }

func (d fakeDriver) Open(string) (driver.Conn, error) { return d.f.Connect(context.Background()) }

type fakeConn struct {
	f  *pgFake
	tx *fakeTx
}

type fakeTx struct {
	c       *fakeConn
	aborted bool
	pending []pendingRow
	saved   map[string]int
}

type pendingRow struct {
	id  int64
	row fakeRow
}

func pgError(code, constraint, msg string) error {
	return &pgconn.PgError{Severity: "ERROR", Code: code, ConstraintName: constraint, Message: msg}
}

var errAborted = pgError(pgerrcode.InFailedSQLTransaction, "", "current transaction is aborted, commands ignored until end of transaction block")

func (c *fakeConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("pgFake: prepared statements are not used by these tests")
}

func (c *fakeConn) Close() error { return nil }

func (c *fakeConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *fakeConn) BeginTx(ctx context.Context, _ driver.TxOptions) (driver.Tx, error) {
	f := c.f
	f.mu.Lock()
	f.begins++
	n := f.begins
	if f.gate > 0 && n == f.gate {
		close(f.gateCh)
	}
	f.mu.Unlock()
	if f.gate > 0 && n <= f.gate {
		select {
		case <-f.gateCh:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	c.tx = &fakeTx{c: c, saved: map[string]int{}}
	return c.tx, nil
}

func (t *fakeTx) Commit() error {
	f := t.c.f
	f.mu.Lock()
	defer f.mu.Unlock()
	t.c.tx = nil
	if t.aborted {
		return errors.New("commit unexpectedly resulted in rollback")
	}
	for _, p := range t.pending {
		f.rows[p.id] = p.row
	}
	return nil
}

func (t *fakeTx) Rollback() error {
	t.c.f.mu.Lock()
	defer t.c.f.mu.Unlock()
	t.c.tx = nil
	return nil
}

func (c *fakeConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	f := c.f
	f.mu.Lock()
	defer f.mu.Unlock()
	query = strings.TrimSpace(query)
	tx := c.tx
	if tx != nil && tx.aborted && !strings.HasPrefix(query, "ROLLBACK TO SAVEPOINT ") {
		return nil, errAborted
	}
	if name, ok := strings.CutPrefix(query, "SAVEPOINT "); ok {
		if tx == nil {
			return nil, pgError(pgerrcode.NoActiveSQLTransaction, "", "SAVEPOINT can only be used in transaction blocks")
		}
		tx.saved[name] = len(tx.pending)
		f.saves = append(f.saves, query)
		return driver.ResultNoRows, nil
	}
	if name, ok := strings.CutPrefix(query, "ROLLBACK TO SAVEPOINT "); ok {
		if tx == nil {
			return nil, pgError(pgerrcode.NoActiveSQLTransaction, "", "ROLLBACK TO SAVEPOINT can only be used in transaction blocks")
		}
		mark, ok := tx.saved[name]
		if !ok {
			return nil, pgError(pgerrcode.InvalidSavepointSpecification, "", "savepoint does not exist")
		}
		tx.pending, tx.aborted = tx.pending[:mark], false
		f.saves = append(f.saves, query)
		return driver.ResultNoRows, nil
	}
	if isInsert(query) && !strings.Contains(query, "RETURNING") {
		id, wrote, err := c.insert(args)
		if err != nil {
			return nil, err
		}
		if !wrote {
			return fakeResult{}, nil
		}
		return fakeResult{id: id, rows: 1}, nil
	}
	return nil, fmt.Errorf("pgFake: unexpected statement %q", query)
}

func (c *fakeConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	f := c.f
	f.mu.Lock()
	defer f.mu.Unlock()
	if c.tx != nil && c.tx.aborted {
		return nil, errAborted
	}
	if !isInsert(query) || !strings.Contains(query, "RETURNING id") {
		return nil, fmt.Errorf("pgFake: unexpected query %q", query)
	}
	id, wrote, err := c.insert(args)
	if err != nil {
		return nil, err
	}
	if !wrote {
		return &fakeRows{}, nil
	}
	return &fakeRows{ids: []int64{id}}, nil
}

func isInsert(query string) bool { return strings.Contains(query, "INSERT INTO kine(") }

// insert runs upstream's insert statement. Its arguments are Tx.Insert's
// cols: compare revision, key, created, deleted, ... , key, compare revision.
// The caller holds f.mu.
func (c *fakeConn) insert(args []driver.NamedValue) (int64, bool, error) {
	f, tx := c.f, c.tx
	if len(args) != 9 {
		return 0, false, fmt.Errorf("pgFake: the insert has %d arguments, want 9", len(args))
	}
	key, okKey := args[1].Value.(string)
	deleted, okDeleted := args[3].Value.(int64)
	compare, okCompare := args[8].Value.(int64)
	if !okKey || !okDeleted || !okCompare {
		return 0, false, fmt.Errorf("pgFake: unexpected insert arguments %v", args)
	}

	f.seq++
	id := f.seq
	f.inserts = append(f.inserts, tx != nil)

	visible := func(id int64) (fakeRow, bool) {
		if r, ok := f.rows[id]; ok {
			return r, true
		}
		if tx != nil {
			for _, p := range tx.pending {
				if p.id == id {
					return p.row, true
				}
			}
		}
		return fakeRow{}, false
	}
	if _, taken := visible(id); taken {
		if tx != nil {
			tx.aborted = true
		}
		return 0, false, pgError(pgerrcode.UniqueViolation, "kine_pkey", `duplicate key value violates unique constraint "kine_pkey"`)
	}
	var newest int64
	for i := int64(1); i < id; i++ {
		if r, ok := visible(i); ok && r.name == key {
			newest = i
		}
	}
	if newest != compare {
		return 0, false, nil
	}
	row := fakeRow{name: key, deleted: deleted != 0}
	if tx != nil {
		tx.pending = append(tx.pending, pendingRow{id: id, row: row})
	} else {
		f.rows[id] = row
	}
	return id, true, nil
}

type fakeResult struct{ id, rows int64 }

func (r fakeResult) LastInsertId() (int64, error) { return r.id, nil }
func (r fakeResult) RowsAffected() (int64, error) { return r.rows, nil }

type fakeRows struct{ ids []int64 }

func (r *fakeRows) Columns() []string { return []string{"id"} }
func (r *fakeRows) Close() error      { return nil }
func (r *fakeRows) Next(dest []driver.Value) error {
	if len(r.ids) == 0 {
		return io.EOF
	}
	dest[0], r.ids = r.ids[0], r.ids[1:]
	return nil
}

// fakeLog is SQLLog on generic's dialect over f, with a pool of maxOpen
// connections and the PostgreSQL driver's error rules
// (pkg/drivers/pgsql/pgsql.go).
func fakeLog(t *testing.T, f *pgFake, maxOpen int, lastInsertID bool) *sqllog.SQLLog {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})
	pool := generic.ConnectionPoolConfig{MaxIdle: maxOpen, MaxOpen: maxOpen}
	d, err := generic.OpenDB(ctx, wg, "pgfake", f, pool, "$", true, nil)
	if err != nil {
		t.Fatal(err)
	}
	d.LastInsertID = lastInsertID
	d.InsertRetry = func(err error) bool {
		if err, ok := err.(*pgconn.PgError); ok && err.Code == pgerrcode.UniqueViolation && err.ConstraintName == "kine_pkey" {
			return true
		}
		return false
	}
	d.TranslateErr = func(err error) error {
		if err, ok := err.(*pgconn.PgError); ok && err.Code == pgerrcode.UniqueViolation {
			return server.ErrKeyExists
		}
		return err
	}
	d.ErrCode = func(err error) string {
		if err == nil {
			return ""
		}
		return err.Error()
	}
	return sqllog.New(d, wg, &drivers.Config{})
}

func create(key string) *server.Event {
	return &server.Event{Create: true, KV: &server.KeyValue{Key: key, Value: []byte("v")}}
}

func update(key string, rev int64) *server.Event {
	return &server.Event{KV: &server.KeyValue{Key: key, Value: []byte("w")}, PrevKV: &server.KeyValue{ModRevision: rev}}
}

// appendWithin is one write that must end within d.
func appendWithin(t *testing.T, log *sqllog.SQLLog, d time.Duration, e *server.Event) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	rev, err := log.Append(ctx, e)
	if err != nil {
		t.Fatalf("write of %s: %v", e.KV.Key, err)
	}
	if rev == 0 {
		t.Fatalf("write of %s: no row (lost compare)", e.KV.Key)
	}
	return rev
}

// inTransaction fails when an insert ran outside a transaction (on a second
// connection, autocommitted).
func inTransaction(t *testing.T, f *pgFake) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, inTx := range f.inserts {
		if !inTx {
			t.Fatalf("insert %d ran outside the write's transaction, on another connection", i+1)
		}
	}
}

// TestKubehzP6OneConnection: a create and an update each complete on a pool
// of one connection, and on a pool of two they use one connection: the
// insert runs inside the write's transaction. Upstream's insert went to the
// pool, so with one connection every write waited until its context ended.
func TestKubehzP6OneConnection(t *testing.T) {
	for _, maxOpen := range []int{1, 2} {
		t.Run(fmt.Sprintf("max-open %d", maxOpen), func(t *testing.T) {
			f := newPgFake(0)
			log := fakeLog(t, f, maxOpen, false)
			rev := appendWithin(t, log, 2*time.Second, create("/a"))
			appendWithin(t, log, 2*time.Second, update("/a", rev))
			inTransaction(t, f)
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.opened != 1 {
				t.Fatalf("the pool opened %d connections for two writes in a row, want 1", f.opened)
			}
		})
	}
}

// TestKubehzP6ConcurrentWrites: 16 writes on a pool of 4. The fake holds the
// first 4 BEGINs until all 4 arrived, so 4 transactions hold every
// connection at once. Each write must still complete: it needs no second
// connection.
func TestKubehzP6ConcurrentWrites(t *testing.T) {
	const pool, writers = 4, 16
	f := newPgFake(pool)
	log := fakeLog(t, f, pool, false)
	errs := make(chan error, writers)
	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			key := fmt.Sprintf("/c/%d", i)
			if rev, err := log.Append(ctx, create(key)); err != nil || rev == 0 {
				errs <- fmt.Errorf("write of %s: rev %d, %w", key, rev, err)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	inTransaction(t, f)
}

// TestKubehzP6RetryInTransaction: the insert's id is taken (a gap fill got it
// first), so the first attempt fails with a unique violation of kine_pkey
// and aborts the transaction. The retry rolls back to the savepoint, gets the
// next id and writes, in the same transaction. A retry without the savepoint
// fails with 25P02.
func TestKubehzP6RetryInTransaction(t *testing.T) {
	f := newPgFake(0)
	f.rows[1] = fakeRow{name: "gap-1", deleted: true}
	log := fakeLog(t, f, 1, false)
	if rev := appendWithin(t, log, 2*time.Second, create("/r")); rev != 2 {
		t.Fatalf("the write got revision %d, want 2 (the id after the taken one)", rev)
	}
	inTransaction(t, f)
	f.mu.Lock()
	defer f.mu.Unlock()
	want := []string{"SAVEPOINT kine_insert", "ROLLBACK TO SAVEPOINT kine_insert"}
	if strings.Join(f.saves, "; ") != strings.Join(want, "; ") {
		t.Fatalf("savepoint statements %q, want %q", f.saves, want)
	}
	if _, ok := f.rows[2]; !ok || f.rows[2].name != "/r" {
		t.Fatalf("row 2 is %+v after the commit, want /r", f.rows[2])
	}
}

// TestKubehzP6LastInsertIDInTransaction: the LastInsertID drivers (MySQL,
// SQLite) keep upstream's path. Their insert ran in the transaction before P6
// too; this pins that it still does, on a pool of one connection, with no
// savepoint.
func TestKubehzP6LastInsertIDInTransaction(t *testing.T) {
	f := newPgFake(0)
	log := fakeLog(t, f, 1, true)
	rev := appendWithin(t, log, 2*time.Second, create("/l"))
	appendWithin(t, log, 2*time.Second, update("/l", rev))
	inTransaction(t, f)
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.saves) != 0 {
		t.Fatalf("savepoint statements on the LastInsertID path: %q", f.saves)
	}
}
