package sqllog

// KUBEHZ-PATCH P1 test: the `case <-external:` arm of SQLLog.poll wakes the
// poll on a revision announced through the optional RevisionNotify interface.
// Hermetic: a fake dialect, no database. unit.yml lists this test by name
// before running it, so a rebase that drops the arm or the file fails CI.

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/query"
	"github.com/k3s-io/kine/pkg/server"
)

var errNotUsed = errors.New("not used by this test")

// newTestLog is New with a 1 s poll interval (startWatch refuses none) and
// compact-batch-size batch.
func newTestLog(d server.Dialect, batch int64) *SQLLog {
	return New(d, &sync.WaitGroup{}, &drivers.Config{CompactBatchSize: batch, PollInterval: time.Second})
}

// wakeDialect records every poll step and offers RevisionNotify. The poll
// loop prepares its statement on the first wake-up and queries on the next
// ones (upstream 31eb826). PrepareAfter reports "prepare" and succeeds;
// QueryAfter reports "query" and fails with context.Canceled on purpose:
// poll logs nothing for it, drops the statement and returns to its select,
// which is all the test needs.
type wakeDialect struct {
	revs  chan int64
	after chan string
}

func (d *wakeDialect) RevisionNotify() <-chan int64 { return d.revs }

func (d *wakeDialect) step(name string) {
	select {
	case d.after <- name:
	default:
	}
}

func (d *wakeDialect) PrepareAfter(context.Context, int64) (*query.Stmt, error) {
	d.step("prepare")
	return &query.Stmt{}, nil
}

func (d *wakeDialect) QueryAfter(context.Context, *query.Stmt, int64) (*sql.Rows, error) {
	d.step("query")
	return nil, context.Canceled
}

func (d *wakeDialect) After(context.Context, string, string, int64, int64) (*sql.Rows, error) {
	return nil, errNotUsed
}

func (d *wakeDialect) CurrentRevision(context.Context) (int64, error) { return 1, nil }

func (d *wakeDialect) ListCurrent(context.Context, string, string, int64, bool, bool) (*sql.Rows, error) {
	return nil, errNotUsed
}

func (d *wakeDialect) List(context.Context, string, string, int64, int64, bool, bool) (*sql.Rows, error) {
	return nil, errNotUsed
}
func (d *wakeDialect) CountCurrent(context.Context, string, string) (int64, int64, error) {
	return 0, 0, errNotUsed
}

func (d *wakeDialect) Count(context.Context, string, string, int64) (int64, int64, int64, error) {
	return 0, 0, 0, errNotUsed
}

func (d *wakeDialect) Insert(context.Context, string, bool, bool, int64, int64, []byte) (int64, error) {
	return 0, errNotUsed
}
func (d *wakeDialect) DeleteRevision(context.Context, int64) error          { return errNotUsed }
func (d *wakeDialect) GetCompactRevision(context.Context) (int64, error)    { return 0, errNotUsed }
func (d *wakeDialect) SetCompactRevision(context.Context, int64) error      { return errNotUsed }
func (d *wakeDialect) Compact(context.Context, int64, int64) (int64, error) { return 0, errNotUsed }
func (d *wakeDialect) PostCompact(context.Context) error                    { return errNotUsed }
func (d *wakeDialect) Fill(context.Context, int64) error                    { return errNotUsed }
func (d *wakeDialect) IsFill(string) bool                                   { return false }
func (d *wakeDialect) IsTxRetry(error) bool                                 { return false }
func (d *wakeDialect) BeginTx(context.Context, *sql.TxOptions) (server.Transaction, error) {
	return nil, errNotUsed
}
func (d *wakeDialect) GetSize(context.Context) (int64, error) { return 0, errNotUsed }
func (d *wakeDialect) FillRetryDelay(context.Context)         {}
func (d *wakeDialect) TranslateStartKey(k string) string      { return k }

func TestKubehzP1ExternalRevisionWakesPoll(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// revs has room for every send below, so a poll that never reads the
	// channel (the arm removed) fails the wake assertion instead of hanging
	// the test on a send.
	d := &wakeDialect{revs: make(chan int64, 4), after: make(chan string, 16)}
	wg := &sync.WaitGroup{}
	s := New(d, wg, &drivers.Config{CompactBatchSize: 1000, PollBatchSize: 500, PollInterval: time.Second})
	s.ctx = ctx

	result := make(chan server.EventBatch)
	go func() {
		for range result { //nolint:revive // drain until poll closes it
		}
	}()
	start := time.Now()
	wg.Add(1) // poll calls Done, as startWatch adds it
	go s.poll(result, 1)

	// Every window below is well under the 1 s fallback ticker, so a poll
	// inside one can only come from the external channel. A wake takes
	// microseconds; the negative windows are short so the whole run stays
	// far from the ticker even on a slow runner.
	const quiet, wake = 50 * time.Millisecond, 300 * time.Millisecond

	select {
	case step := <-d.after:
		t.Fatalf("poll ran (%s) before any wake-up", step)
	case <-time.After(quiet):
	}

	// A revision at or below the polled one is not worth a poll.
	d.revs <- 1
	select {
	case step := <-d.after:
		t.Fatalf("a revision at the polled position woke poll (%s)", step)
	case <-time.After(quiet):
	}

	// The first wake-up prepares the statement, the next one queries: both
	// must come from the external channel.
	for _, w := range []struct {
		rev  int64
		step string
	}{{2, "prepare"}, {3, "query"}} {
		d.revs <- w.rev
		select {
		case step := <-d.after:
			if step != w.step {
				t.Fatalf("external revision %d: poll did %s, want %s", w.rev, step, w.step)
			}
		case <-time.After(wake):
			t.Fatalf("external revision %d did not wake poll within %s; only the 1 s ticker would", w.rev, wake)
		}
	}
	// Guard the assertion itself: past 1 s the ticker could have produced
	// that poll, and a pass would say nothing.
	if elapsed := time.Since(start); elapsed >= time.Second {
		t.Fatalf("run took %s; the 1 s ticker could have produced the poll, rerun on a quieter machine", elapsed)
	}
}
