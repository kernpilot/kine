package sqllog

// KUBEHZ-PATCH P2 (relief) — this whole file is ours; see kubehz/MERGE-GUIDE.md#p2.
// The compaction the quota runs while full (B481). It does not go through
// compact() on purpose: that keeps --compact-min-retain revisions (1000 by
// default), and while puts are refused no new revisions push the deleted
// rows out of that window, so space never comes back.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
)

// compile-time check: the log offers the quota its relief.
var _ server.QuotaRelief = (*SQLLog)(nil)

// pollerStarts holds the logs whose poller start is running, so a pass that
// comes while one runs does not start another.
var pollerStarts sync.Map // *SQLLog -> struct{}

// errCommitUnknown marks a batch whose commit returned an error. The commit
// can have taken effect all the same (a lost acknowledgement), so the batch's
// rows may be gone.
var errCommitUnknown = errors.New("the commit can have taken effect")

// RelieveQuota compacts to target, lowered so that it never passes the
// revision this log's poller has sent to its watchers (a row it has not sent
// would be a gap, sql.go's poll loop) and never reaches the newest row (the
// current revision is MAX(id)). It works in batches of compact-batch-size,
// each in its own transaction that commits or rolls back: an error ends the
// pass, it never ends the process. After rows were deleted, also before an
// error, it runs the dialect's post-compaction step, as compact() does.
// Every query runs under ctx. res.Head is MAX(id) as the last batch read it.
// res.Stale is set when rows were or may have been deleted (a failed commit)
// and the dialect keeps statistics the quota must refresh.
//
// With refresh it only asks the dialect to refresh its statistics and
// compacts nothing. A pass never does both: on PostgreSQL a VACUUM sets the
// live row count to an absolute value, and the deleting backend's counter
// changes reach the shared statistics up to 10 s later, so a VACUUM that
// overlaps or follows a delete too closely subtracts the deleted rows twice
// and the live figure then reads low. The quota runs the refresh in a later
// pass, after the deletes settled.
func (s *SQLLog) RelieveQuota(ctx context.Context, target int64, refresh bool) (server.ReliefResult, error) {
	var res server.ReliefResult
	if refresh {
		if r, ok := s.d.(server.StatsRefresher); ok {
			if err := r.RefreshStats(ctx); err != nil {
				return res, fmt.Errorf("refreshing the statistics: %w", err)
			}
		}
		res.Refreshed = true
		return res, nil
	}
	polled := s.polledRev.Load()
	if polled <= 0 {
		s.startPolling()
		return res, nil
	}
	target = min(target, polled)
	var err error
	for {
		var done bool
		var rows, to, head int64
		done, rows, to, head, err = s.reliefBatch(ctx, target)
		res.Rows += rows
		if to > 0 {
			res.CompactedTo = to
		}
		if head > 0 {
			res.Head = head
		}
		if err != nil || done {
			break
		}
	}
	if res.Rows > 0 || errors.Is(err, errCommitUnknown) {
		// a commit that returned an error can have deleted its rows too, so
		// the refresh must wait for them to settle as well
		_, res.Stale = s.d.(server.StatsRefresher)
	}
	if res.Rows > 0 {
		if perr := s.d.PostCompact(ctx); perr != nil {
			err = joinRelief(err, fmt.Errorf("after compacting: %w", perr))
		}
	}
	return res, err
}

// StatsOwed reports a statistics reset in the dialect that no VACUUM settled
// yet (server.QuotaRelief).
func (s *SQLLog) StatsOwed() bool {
	r, ok := s.d.(server.StatsRefresher)
	return ok && r.StatsOwed()
}

func joinRelief(first, then error) error {
	if first == nil {
		return then
	}
	return fmt.Errorf("%w; %w", first, then)
}

// startPolling starts the poll loop when no watch has started it yet: the
// relief never compacts past the poller, so without one it would never
// compact. The poller starts at the current revision and runs until the log
// stops (pkg/broadcaster), so this pass compacts nothing and a later one can.
// The start runs in its own goroutine, one at a time per log: its revision
// read runs under the log's context with no deadline and holds the
// broadcaster's lock, and the sampler must not wait for it. The subscription
// is drained and never read; a slow reader is dropped by the broadcaster,
// which leaves the poller running.
func (s *SQLLog) startPolling() {
	if _, running := pollerStarts.LoadOrStore(s, struct{}{}); running {
		return
	}
	go func() {
		defer pollerStarts.Delete(s)
		values, err := s.broadcaster.Subscribe(s.ctx, s.startWatch)
		if err != nil {
			if s.ctx.Err() == nil {
				logrus.Warnf("quota: the relief needs the poller, and starting it failed: %v", err)
			}
			return
		}
		logrus.Infof("quota: started the poller for the relief, which needs it")
		for range values { //nolint:revive // drain: the relief needs the poller running, not its events
		}
	}()
}

// reliefBatch compacts one batch toward target. done is true when there is
// nothing left to compact below target; head is MAX(id) as the batch read it,
// 0 when it read none.
func (s *SQLLog) reliefBatch(ctx context.Context, target int64) (done bool, rows, to, head int64, err error) {
	t, err := s.d.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return true, 0, 0, 0, fmt.Errorf("beginning the transaction: %w", err)
	}
	defer func() { _ = t.Rollback() }() // after Commit this is a no-op that returns sql.ErrTxDone

	current, err := t.CurrentRevision(ctx)
	if err != nil {
		return true, 0, 0, 0, fmt.Errorf("reading the current revision: %w", err)
	}
	compactRev, err := t.GetCompactRevision(ctx)
	if err != nil {
		return true, 0, 0, current, fmt.Errorf("reading the compact revision: %w", err)
	}
	target = min(target, current-1)
	if target <= compactRev {
		return true, 0, compactRev, current, nil
	}
	batch := target
	if s.compactBatchSize > 0 {
		batch = min(batch, compactRev+s.compactBatchSize)
	}
	if rows, err = t.Compact(ctx, batch); err != nil {
		return true, 0, 0, current, fmt.Errorf("compacting to revision %d: %w", batch, err)
	}
	if err := t.SetCompactRevision(ctx, batch); err != nil {
		return true, 0, 0, current, fmt.Errorf("recording compact revision %d: %w", batch, err)
	}
	if err := t.Commit(); err != nil {
		return true, 0, 0, current, fmt.Errorf("committing the compaction to revision %d: %w (%w)", batch, err, errCommitUnknown)
	}
	return batch >= target, rows, batch, current, nil
}
