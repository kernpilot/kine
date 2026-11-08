package pgsql

// KUBEHZ-PATCH P1 tests. Hermetic: no PostgreSQL, no network. unit.yml lists
// these by name before running them, so a rebase that drops the file fails CI
// instead of passing on an empty -run.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/drivers/generic"
	"github.com/k3s-io/kine/pkg/server"
)

func TestKubehzP1RecordIsMonotonic(t *testing.T) {
	var n revisionNotifier
	for _, step := range []struct {
		rev, want int64
	}{
		{5, 5},
		{3, 5},  // older revision never moves latest backwards
		{5, 5},  // same revision is a no-op
		{9, 9},  // newer wins
		{-1, 9}, // garbage never wins
		{10, 10},
	} {
		n.record(step.rev)
		if got := n.latest.Load(); got != step.want {
			t.Fatalf("after record(%d): latest = %d, want %d", step.rev, got, step.want)
		}
	}
}

func TestKubehzP1RecordConcurrent(t *testing.T) {
	var n revisionNotifier
	var wg sync.WaitGroup
	const writers, perWriter = 16, 1000
	for w := range writers {
		wg.Go(func() {
			for i := range perWriter {
				// Interleaved so every writer sees higher and lower values
				// than the current one.
				n.record(int64(i*writers + w + 1))
			}
		})
	}
	wg.Wait()
	if got, want := n.latest.Load(), int64(writers*perWriter); got != want {
		t.Fatalf("latest = %d, want the maximum %d", got, want)
	}
}

func TestKubehzP1ParseRevision(t *testing.T) {
	for payload, want := range map[string]int64{
		"1":                   1,
		"42":                  42,
		"9223372036854775807": 9223372036854775807,
		"0":                   0, // a revision is positive
		"-1":                  0,
		"":                    0,
		" 7":                  0,
		"7\n":                 0,
		"abc":                 0,
		"7abc":                0,
		"9223372036854775808": 0, // overflow
		"{\"rev\":7}":         0, // not our channel's shape
	} {
		rev, ok := parseRevision(payload)
		if ok != (want > 0) || rev != want {
			t.Errorf("parseRevision(%q) = (%d, %v), want (%d, %v)", payload, rev, ok, want, want > 0)
		}
	}
}

func TestKubehzP1ForwardRevisionNeverBlocks(t *testing.T) {
	revs := make(chan int64, 2)
	if !forwardRevision("11", revs) || !forwardRevision("12", revs) {
		t.Fatal("valid payloads must be queued while there is room")
	}
	if forwardRevision("bad", revs) {
		t.Fatal("a malformed payload must not be queued")
	}
	done := make(chan bool, 1)
	go func() { done <- forwardRevision("13", revs) }()
	select {
	case queued := <-done:
		if queued {
			t.Fatal("a full channel must drop, not queue")
		}
	case <-time.After(time.Second):
		t.Fatal("forwardRevision blocked on a full channel")
	}
	if got := []int64{<-revs, <-revs}; got[0] != 11 || got[1] != 12 {
		t.Fatalf("queued revisions = %v, want [11 12]", got)
	}
}

func TestKubehzP1ReconnectBackoff(t *testing.T) {
	var b reconnectBackoff
	want := []time.Duration{1, 2, 4, 8, 16, 30, 30}
	for i, w := range want {
		if got := b.delay(); got != w*time.Second {
			t.Fatalf("delay #%d = %s, want %s", i+1, got, w*time.Second)
		}
	}
	// A session that failed at once keeps the outage's delay.
	b.settle(50 * time.Millisecond)
	if got := b.delay(); got != reconnectCap {
		t.Fatalf("after a short session: delay = %s, want %s", got, reconnectCap)
	}
	// A session that stayed healthy for the cap starts over.
	b.settle(reconnectCap)
	if got := b.delay(); got != reconnectFloor {
		t.Fatalf("after a healthy session: delay = %s, want %s", got, reconnectFloor)
	}
}

// TestKubehzP1BeginTxWraps: since upstream ca3c2d5 (v0.17.5) every write
// inserts in a transaction (SQLLog.Append), so the dialect's BeginTx must hand
// out the recording transaction. A rebase that loses it keeps the other P1
// tests green and turns P1 off. A connector with no SQL is enough: BeginTx
// only begins.
func TestKubehzP1BeginTxWraps(t *testing.T) {
	db := sql.OpenDB(beginOnlyConnector{})
	t.Cleanup(func() { _ = db.Close() })
	d := &notifyingDialect{Generic: &generic.Generic{DB: db}}
	tx, err := d.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.MustRollback()
	nt, ok := tx.(*notifyingTx)
	if !ok {
		t.Fatalf("BeginTx returned %T, want *notifyingTx: committed writes would never reach the notifier", tx)
	}
	if nt.notifier != &d.notifier {
		t.Fatal("the transaction records into another notifier than the dialect's")
	}
}

// TestKubehzP1RecordsOnCommit: a transaction records its highest inserted
// revision after a successful commit, and nothing when the row is not there
// for another instance to poll.
func TestKubehzP1RecordsOnCommit(t *testing.T) {
	errInsert, errCommit := errors.New("insert failed"), errors.New("commit failed")
	cases := []struct {
		name    string
		tx      *revTx
		inserts int
		end     func(*notifyingTx) error
		want    int64
	}{
		{"commit records the highest insert", &revTx{next: 10}, 2, (*notifyingTx).Commit, 12},
		{"MustCommit records", &revTx{next: 10}, 1, func(t *notifyingTx) error { t.MustCommit(); return nil }, 11},
		{"rollback records nothing", &revTx{next: 10}, 1, (*notifyingTx).Rollback, 0},
		{"a failed commit records nothing", &revTx{next: 10, commitErr: errCommit}, 1, (*notifyingTx).Commit, 0},
		{"a failed insert records nothing", &revTx{next: 10, insertErr: errInsert}, 1, (*notifyingTx).Commit, 0},
		{"a failed compare (revision 0) records nothing", &revTx{noRow: true}, 1, (*notifyingTx).Commit, 0},
		{"no insert records nothing", &revTx{next: 10}, 0, (*notifyingTx).Commit, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var n revisionNotifier
			nt := &notifyingTx{Transaction: tc.tx, notifier: &n}
			for range tc.inserts {
				_, _ = nt.Insert(context.Background(), "/k", false, false, 0, 0, nil)
			}
			_ = tc.end(nt)
			if got := n.latest.Load(); got != tc.want {
				t.Fatalf("latest = %d, want %d", got, tc.want)
			}
		})
	}
}

// revTx is a server.Transaction whose Insert hands out the next revision.
// Only Insert and the ends of a transaction are used.
type revTx struct {
	server.Transaction
	next      int64
	noRow     bool
	insertErr error
	commitErr error
}

func (r *revTx) Insert(context.Context, string, bool, bool, int64, int64, []byte) (int64, error) {
	if r.insertErr != nil {
		return 0, r.insertErr
	}
	if r.noRow {
		return 0, nil
	}
	r.next++
	return r.next, nil
}
func (r *revTx) Commit() error   { return r.commitErr }
func (r *revTx) Rollback() error { return nil }
func (r *revTx) MustCommit() {
	if r.commitErr != nil {
		panic(r.commitErr) // upstream's Tx ends the process here
	}
}

// beginOnlyConnector opens connections that can begin, commit and roll back,
// and nothing else.
type beginOnlyConnector struct{}

func (beginOnlyConnector) Connect(context.Context) (driver.Conn, error) { return beginOnlyConn{}, nil }
func (beginOnlyConnector) Driver() driver.Driver                        { return beginOnlyDriver{} }

type beginOnlyDriver struct{}

func (beginOnlyDriver) Open(string) (driver.Conn, error) { return beginOnlyConn{}, nil }

type beginOnlyConn struct{}

func (beginOnlyConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("beginOnlyConn: no SQL")
}
func (beginOnlyConn) Close() error              { return nil }
func (beginOnlyConn) Begin() (driver.Tx, error) { return beginOnlyTx{}, nil }
func (beginOnlyConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return beginOnlyTx{}, nil
}

type beginOnlyTx struct{}

func (beginOnlyTx) Commit() error   { return nil }
func (beginOnlyTx) Rollback() error { return nil }
