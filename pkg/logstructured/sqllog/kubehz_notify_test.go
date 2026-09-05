package sqllog

// KUBEHZ-PATCH P1 test: the `case <-external:` arm of SQLLog.poll wakes the
// poll on a revision announced through the optional RevisionNotify interface.
// Hermetic: a fake dialect, no database. unit.yml lists this test by name
// before running it, so a rebase that drops the arm or the file fails CI.

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/server"
)

var errNotUsed = errors.New("not used by this test")

// wakeDialect records every After call and offers RevisionNotify. After fails
// with context.Canceled on purpose: poll logs nothing for it and returns to
// its select, which is all the test needs.
type wakeDialect struct {
	revs  chan int64
	after chan struct{}
}

func (d *wakeDialect) RevisionNotify() <-chan int64 { return d.revs }

func (d *wakeDialect) After(context.Context, string, string, int64, int64) (*sql.Rows, error) {
	select {
	case d.after <- struct{}{}:
	default:
	}
	return nil, context.Canceled
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

func (d *wakeDialect) Insert(context.Context, string, bool, bool, int64, int64, int64, []byte) (int64, error) {
	return 0, errNotUsed
}
func (d *wakeDialect) DeleteRevision(context.Context, int64) error       { return errNotUsed }
func (d *wakeDialect) GetCompactRevision(context.Context) (int64, error) { return 0, errNotUsed }
func (d *wakeDialect) SetCompactRevision(context.Context, int64) error   { return errNotUsed }
func (d *wakeDialect) Compact(context.Context, int64) (int64, error)     { return 0, errNotUsed }
func (d *wakeDialect) PostCompact(context.Context) error                 { return errNotUsed }
func (d *wakeDialect) Fill(context.Context, int64) error                 { return errNotUsed }
func (d *wakeDialect) IsFill(string) bool                                { return false }
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
	d := &wakeDialect{revs: make(chan int64, 4), after: make(chan struct{}, 16)}
	s := New(d, 0, 0, 0, 0, 1000, 500)
	s.ctx = ctx

	result := make(chan server.Events)
	go func() {
		for range result { //nolint:revive // drain until poll closes it
		}
	}()
	go s.poll(result, 1)

	// Every window below is well under the 1 s fallback ticker, so a poll
	// inside one can only come from the external channel.
	const quiet, wake = 150 * time.Millisecond, 300 * time.Millisecond

	select {
	case <-d.after:
		t.Fatal("poll ran before any wake-up")
	case <-time.After(quiet):
	}

	// A revision at or below the polled one is not worth a poll.
	d.revs <- 1
	select {
	case <-d.after:
		t.Fatal("a revision at the polled position woke poll")
	case <-time.After(quiet):
	}

	d.revs <- 2
	select {
	case <-d.after:
	case <-time.After(wake):
		t.Fatalf("an external revision did not wake poll within %s; only the 1 s ticker would", wake)
	}
}
