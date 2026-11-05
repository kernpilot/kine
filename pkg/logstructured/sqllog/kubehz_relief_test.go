package sqllog

// KUBEHZ-PATCH P2 test: the bounds of the quota's relief compaction (B481).
// Hermetic: a fake dialect and transaction, no database. The relief must never
// pass the poller's revision or reach the newest row, must work in batches of
// compact-batch-size, each committed on its own, and must end the pass (not
// the process) on an error. A refresh runs only when asked, before the
// compaction, so it never follows this pass's own deletes.

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/server"
)

var (
	errCompact = errors.New("compact failed")
	errCommit  = errors.New("commit failed")
	errRefresh = errors.New("refresh failed")
)

// reliefCtxKey marks the caller's context, so the fake can tell that every
// query runs under it.
type reliefCtxKey struct{}

// reliefDialect hands out reliefTx transactions over one shared state.
type reliefDialect struct {
	wakeDialect
	current      int64 // MAX(id)
	compactRev   int64 // the committed compact revision
	failAt       int64 // Compact to this revision fails; 0 = never
	commitAt     int64 // the commit of the batch to this revision fails; 0 = never
	foreignCtx   bool  // a query ran under a context that is not the caller's
	refreshFails bool
	calls        []string // "refresh" and "compact", in order
	batches      []int64
	rollbacks    int
	postCompacts int
	refreshes    int
}

func (d *reliefDialect) BeginTx(ctx context.Context, _ *sql.TxOptions) (server.Transaction, error) {
	d.checkCtx(ctx)
	return &reliefTx{d: d, compactRev: d.compactRev}, nil
}
func (d *reliefDialect) checkCtx(ctx context.Context) {
	if ctx.Value(reliefCtxKey{}) == nil {
		d.foreignCtx = true
	}
}
func (d *reliefDialect) CurrentRevision(context.Context) (int64, error) { return d.current, nil }
func (d *reliefDialect) PostCompact(context.Context) error              { d.postCompacts++; return nil }
func (d *reliefDialect) RefreshStats(ctx context.Context) error {
	d.checkCtx(ctx)
	d.refreshes++
	d.calls = append(d.calls, "refresh")
	if d.refreshFails {
		return errRefresh
	}
	return nil
}

// reliefTx stages SetCompactRevision until Commit, like a real transaction.
// The Must* methods fail the test: the relief must never end the process.
type reliefTx struct {
	d          *reliefDialect
	compactRev int64
	done       bool
}

func (t *reliefTx) Commit() error {
	if t.done {
		return sql.ErrTxDone
	}
	if t.d.commitAt != 0 && t.compactRev == t.d.commitAt {
		return errCommit // the deferred Rollback then ends the transaction
	}
	t.done = true
	t.d.compactRev = t.compactRev
	return nil
}
func (t *reliefTx) Rollback() error {
	if t.done {
		return sql.ErrTxDone
	}
	t.done = true
	t.d.rollbacks++
	return nil
}
func (t *reliefTx) MustCommit()   { panic("relief called MustCommit") }
func (t *reliefTx) MustRollback() { panic("relief called MustRollback") }
func (t *reliefTx) GetCompactRevision(context.Context) (int64, error) {
	return t.compactRev, nil
}
func (t *reliefTx) SetCompactRevision(_ context.Context, rev int64) error {
	t.compactRev = rev
	return nil
}
func (t *reliefTx) Compact(ctx context.Context, rev int64) (int64, error) {
	t.d.checkCtx(ctx)
	if t.d.failAt != 0 && rev == t.d.failAt {
		return 0, errCompact
	}
	t.d.batches = append(t.d.batches, rev)
	t.d.calls = append(t.d.calls, "compact")
	return rev - t.compactRev, nil
}
func (t *reliefTx) DeleteRevision(context.Context, int64) error { return errNotUsed }
func (t *reliefTx) CurrentRevision(context.Context) (int64, error) {
	return t.d.current, nil
}

func TestKubehzP2ReliefBounds(t *testing.T) {
	cases := []struct {
		name               string
		current, polled    int64
		target, batch      int64
		failAt, commitAt   int64
		refresh, refFails  bool
		wantBatches        []int64
		wantTo, wantRows   int64
		wantErr            []error
		wantPost, wantRoll int
	}{
		{name: "the poller's revision bounds the target",
			current: 100, polled: 40, target: 90, batch: 1000,
			wantBatches: []int64{40}, wantTo: 40, wantRows: 40, wantPost: 1},
		{name: "the newest row is never compacted",
			current: 50, polled: 50, target: 50, batch: 1000,
			wantBatches: []int64{49}, wantTo: 49, wantRows: 49, wantPost: 1},
		{name: "batches of compact-batch-size, each committed",
			current: 100, polled: 100, target: 25, batch: 10,
			wantBatches: []int64{10, 20, 25}, wantTo: 25, wantRows: 25, wantPost: 1},
		{name: "nothing to compact and no refresh owed: no post-compaction step",
			current: 1, polled: 1, target: 1, batch: 10,
			wantBatches: nil, wantTo: 0, wantRows: 0, wantRoll: 1},
		{name: "an error ends the pass and keeps the committed batches",
			current: 100, polled: 100, target: 30, batch: 10, failAt: 20,
			wantBatches: []int64{10}, wantTo: 10, wantRows: 10, wantErr: []error{errCompact}, wantPost: 1, wantRoll: 1},
		{name: "a failed commit ends the pass and records nothing",
			current: 100, polled: 100, target: 30, batch: 10, commitAt: 10,
			wantBatches: []int64{10}, wantTo: 0, wantRows: 0, wantErr: []error{errCommit}, wantRoll: 1},
		{name: "an owed refresh runs with nothing to compact",
			current: 1, polled: 1, target: 1, batch: 10, refresh: true,
			wantBatches: nil, wantTo: 0, wantRows: 0, wantRoll: 1},
		{name: "an owed refresh runs before this pass's compaction",
			current: 100, polled: 100, target: 20, batch: 10, refresh: true,
			wantBatches: []int64{10, 20}, wantTo: 20, wantRows: 20, wantPost: 1},
		{name: "a failed refresh does not stop the compaction",
			current: 100, polled: 100, target: 20, batch: 10, refresh: true, refFails: true,
			wantBatches: []int64{10, 20}, wantTo: 20, wantRows: 20, wantErr: []error{errRefresh}, wantPost: 1},
		{name: "a failed refresh and a failed compaction both report",
			current: 100, polled: 100, target: 30, batch: 10, failAt: 20, refresh: true, refFails: true,
			wantBatches: []int64{10}, wantTo: 10, wantRows: 10, wantErr: []error{errRefresh, errCompact}, wantPost: 1, wantRoll: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := &reliefDialect{current: tc.current, failAt: tc.failAt, commitAt: tc.commitAt, refreshFails: tc.refFails}
			s := New(d, 0, 0, 0, 0, tc.batch, 0)
			s.polledRev.Store(tc.polled)

			ctx := context.WithValue(context.Background(), reliefCtxKey{}, true)
			res, err := s.RelieveQuota(ctx, tc.target, tc.refresh)
			if (len(tc.wantErr) == 0) != (err == nil) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			for _, want := range tc.wantErr {
				if !errors.Is(err, want) {
					t.Errorf("err = %v, want it to wrap %v", err, want)
				}
			}
			if d.foreignCtx {
				t.Error("a query ran under another context than the caller's")
			}
			wantRefresh := 0
			if tc.refresh {
				wantRefresh = 1
			}
			if d.refreshes != wantRefresh {
				t.Errorf("RefreshStats ran %d times, want %d (only when asked)", d.refreshes, wantRefresh)
			}
			if res.Refreshed != (tc.refresh && !tc.refFails) {
				t.Errorf("Refreshed = %v, want %v", res.Refreshed, tc.refresh && !tc.refFails)
			}
			if tc.refresh && len(d.calls) > 0 && d.calls[0] != "refresh" {
				t.Errorf("calls %v: the refresh must run before this pass's compaction", d.calls)
			}
			if !slices.Equal(d.batches, tc.wantBatches) {
				t.Errorf("batches = %v, want %v", d.batches, tc.wantBatches)
			}
			if res.CompactedTo != tc.wantTo || d.compactRev != tc.wantTo {
				t.Errorf("compacted to %d (committed %d), want %d", res.CompactedTo, d.compactRev, tc.wantTo)
			}
			if res.Rows != tc.wantRows {
				t.Errorf("rows = %d, want %d", res.Rows, tc.wantRows)
			}
			if d.postCompacts != tc.wantPost {
				t.Errorf("PostCompact ran %d times, want %d (once after rows were deleted)", d.postCompacts, tc.wantPost)
			}
			if d.rollbacks != tc.wantRoll {
				t.Errorf("rollbacks = %d, want %d (a committed batch is not rolled back; a pass with nothing to do rolls back its read)", d.rollbacks, tc.wantRoll)
			}
		})
	}
}

// TestKubehzP2ReliefStartsPoller: with no watch yet the poller has not run,
// and the relief, which never compacts past it, would never compact. The
// pass starts the poller instead and compacts nothing; the poller then runs.
func TestKubehzP2ReliefStartsPoller(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	d := &reliefDialect{current: 50}
	s := New(d, 0, 0, 0, 0, 10, 0)
	s.ctx = ctx

	res, err := s.RelieveQuota(context.WithValue(ctx, reliefCtxKey{}, true), 40, false)
	if err != nil || res.Rows != 0 || len(d.batches) != 0 {
		t.Fatalf("first pass: rows %d, batches %v, err %v; want nothing compacted and no error", res.Rows, d.batches, err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for s.polledRev.Load() != 50 {
		if time.Now().After(deadline) {
			t.Fatalf("the poller did not start: polled revision %d, want 50", s.polledRev.Load())
		}
		time.Sleep(20 * time.Millisecond)
	}
}
