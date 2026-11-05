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

	"github.com/k3s-io/kine/pkg/server"
)

// compile-time check: the log offers the quota its relief.
var _ server.QuotaRelief = (*SQLLog)(nil)

// RelieveQuota compacts to target, lowered so that it never passes the
// revision this log's poller has sent to its watchers (a row it has not sent
// would be a gap, sql.go's poll loop) and never reaches the newest row (the
// current revision is MAX(id)). It works in batches of compact-batch-size,
// each in its own transaction that commits or rolls back: an error ends the
// pass, it never ends the process. When rows were deleted it asks the
// dialect to refresh its statistics. Every query runs under ctx.
func (s *SQLLog) RelieveQuota(ctx context.Context, target int64) (server.ReliefResult, error) {
	var res server.ReliefResult
	if polled := s.polledRev.Load(); target > polled {
		target = polled
	}
	for {
		done, rows, to, err := s.reliefBatch(ctx, target)
		res.Rows += rows
		if to > 0 {
			res.CompactedTo = to
		}
		if err != nil {
			return res, err
		}
		if done {
			break
		}
	}
	if res.Rows == 0 {
		return res, nil
	}
	if err := s.d.PostCompact(ctx); err != nil {
		return res, fmt.Errorf("after compacting: %w", err)
	}
	if r, ok := s.d.(server.StatsRefresher); ok {
		if err := r.RefreshStats(ctx); err != nil {
			return res, fmt.Errorf("refreshing the statistics: %w", err)
		}
	}
	return res, nil
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
