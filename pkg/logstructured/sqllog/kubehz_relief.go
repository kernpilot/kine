package sqllog

// KUBEHZ-PATCH P2 (relief) — this whole file is ours; see kubehz/MERGE-GUIDE.md#p2.
// The compaction the quota runs while full (B481). It does not go through
// compact() on purpose: that keeps --compact-min-retain revisions (1000 by
// default), and while puts are refused no new revisions push the deleted
// rows out of that window, so space never comes back.

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
)

// compile-time check: the log offers the quota its relief.
var _ server.QuotaRelief = (*SQLLog)(nil)

// reliefRefreshBudget is the part of a pass's budget the statistics refresh
// may use, so a slow VACUUM leaves the compaction its time. A var for tests.
var reliefRefreshBudget = 10 * time.Second

// pollerStarts holds the logs whose poller start is running, so a pass that
// comes while one runs does not start another.
var pollerStarts sync.Map // *SQLLog -> struct{}

// RelieveQuota compacts to target, lowered so that it never passes the
// revision this log's poller has sent to its watchers (a row it has not sent
// would be a gap, sql.go's poll loop) and never reaches the newest row (the
// current revision is MAX(id)). It works in batches of compact-batch-size,
// each in its own transaction that commits or rolls back: an error ends the
// pass, it never ends the process. After rows were deleted, also before an
// error, it runs the dialect's post-compaction step, as compact() does.
//
// When refresh is true it first asks the dialect to refresh its statistics,
// for the rows an EARLIER pass deleted. Not for this pass's rows: on
// PostgreSQL a VACUUM sets the live row count to an absolute value, and the
// deleting backend's counter changes reach the shared statistics up to 10 s
// later, so a VACUUM right after the delete subtracts the deleted rows twice
// and the live figure then reads low. The refresh has reliefRefreshBudget of
// the pass, and its failure does not stop the compaction. Every query runs
// under ctx.
func (s *SQLLog) RelieveQuota(ctx context.Context, target int64, refresh bool) (server.ReliefResult, error) {
	var res server.ReliefResult
	polled := s.polledRev.Load()
	if polled <= 0 {
		s.startPolling()
		return res, nil
	}
	var errs error
	if refresh {
		if r, ok := s.d.(server.StatsRefresher); ok {
			rctx, cancel := context.WithTimeout(ctx, reliefRefreshBudget)
			if err := r.RefreshStats(rctx); err != nil {
				errs = fmt.Errorf("refreshing the statistics: %w", err)
			}
			cancel()
		}
		res.Refreshed = errs == nil
	}
	target = min(target, polled)
	var err error
	for {
		var done bool
		var rows, to int64
		done, rows, to, err = s.reliefBatch(ctx, target)
		res.Rows += rows
		if to > 0 {
			res.CompactedTo = to
		}
		if err != nil || done {
			break
		}
	}
	if err != nil {
		errs = joinRelief(errs, err)
	}
	if res.Rows > 0 {
		if err := s.d.PostCompact(ctx); err != nil {
			errs = joinRelief(errs, fmt.Errorf("after compacting: %w", err))
		}
	}
	return res, errs
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
		for range values {
		}
	}()
}

// reliefBatch compacts one batch toward target. done is true when there is
// nothing left to compact below target.
func (s *SQLLog) reliefBatch(ctx context.Context, target int64) (done bool, rows, to int64, err error) {
	t, err := s.d.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return true, 0, 0, fmt.Errorf("beginning the transaction: %w", err)
	}
	defer func() { _ = t.Rollback() }() // after Commit this is a no-op that returns sql.ErrTxDone

	current, err := t.CurrentRevision(ctx)
	if err != nil {
		return true, 0, 0, fmt.Errorf("reading the current revision: %w", err)
	}
	compactRev, err := t.GetCompactRevision(ctx)
	if err != nil {
		return true, 0, 0, fmt.Errorf("reading the compact revision: %w", err)
	}
	target = min(target, current-1)
	if target <= compactRev {
		return true, 0, compactRev, nil
	}
	batch := target
	if s.compactBatchSize > 0 {
		batch = min(batch, compactRev+s.compactBatchSize)
	}
	if rows, err = t.Compact(ctx, batch); err != nil {
		return true, 0, 0, fmt.Errorf("compacting to revision %d: %w", batch, err)
	}
	if err := t.SetCompactRevision(ctx, batch); err != nil {
		return true, 0, 0, fmt.Errorf("recording compact revision %d: %w", batch, err)
	}
	if err := t.Commit(); err != nil {
		return true, 0, 0, fmt.Errorf("committing the compaction to revision %d: %w", batch, err)
	}
	return batch >= target, rows, batch, nil
}
