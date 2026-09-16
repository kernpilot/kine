package server

// KUBEHZ-PATCH P2 — per-database size limit. This whole file is ours; see
// kubehz/MERGE-GUIDE.md#p2.
//
// INTENT: with --quota-bytes > 0, a database at or above the limit refuses
//   new keys and new values with etcd's no-space error, and keeps serving
//   reads, deletes, watches and compaction, so the client can free space.
//   That is what etcd's capped applier does once its NOSPACE alarm is armed,
//   and the apiserver already handles it.
//
// INVARIANT: the size is sampled, never queried on the write path. A write
//   costs one atomic load. The last good sample stays in use when a sample
//   fails, so a flaky size query never lifts the limit by itself.
//
// One deliberate difference from etcd: the compaction bookkeeping key stays
// writable. On kine a delete is an insert (a tombstone row), so above the
// limit only compaction can make the table smaller, and the apiserver only
// compacts after it has written that key. Refusing it would leave a full
// database with no way out except a bigger limit.

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/k3s-io/kine/pkg/metrics"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

// ErrNoSpace is the error a write gets above the limit. It is etcd's own
// value: same gRPC code (ResourceExhausted), same message.
var ErrNoSpace = rpctypes.ErrGRPCNoSpace

// QuotaSampleInterval is how often the database size is read for the quota
// check. Every sample is one cheap catalog query (on PostgreSQL,
// pg_total_relation_size), so 30 s costs nothing measurable and bounds how
// long a database can grow past the limit before writes stop.
const QuotaSampleInterval = 30 * time.Second

// Quota holds a size limit and the last sampled size. A zero limit means no
// limit; WithQuota then returns the backend unchanged.
type Quota struct {
	limit int64
	size  atomic.Int64
	full  atomic.Bool
}

// NewQuota returns a quota for limit bytes. limit <= 0 means no limit.
func NewQuota(limit int64) *Quota {
	q := &Quota{limit: max(limit, 0)}
	metrics.QuotaBytes.Set(float64(q.limit))
	return q
}

// Limit is the configured limit in bytes; 0 when there is none.
func (q *Quota) Limit() int64 { return q.limit }

// Size is the last sampled database size in bytes.
func (q *Quota) Size() int64 { return q.size.Load() }

// Full reports whether the last sample was at or above the limit. etcd
// refuses a write when size + cost would pass the quota, and every put has a
// cost, so "at the limit" already refuses.
func (q *Quota) Full() bool { return q.limit > 0 && q.full.Load() }

// Observe records a sampled size and logs the two transitions: the first
// sample at or above the limit, and the first sample below it again.
func (q *Quota) Observe(size int64) {
	q.size.Store(size)
	metrics.DBSizeBytes.Set(float64(size))
	if q.limit <= 0 {
		return
	}
	full := size >= q.limit
	if q.full.Swap(full) == full {
		return
	}
	if full {
		logrus.Infof("quota: the database is %d bytes, the limit is %d bytes. Puts are refused until the database is smaller than the limit.", size, q.limit)
	} else {
		logrus.Infof("quota: the database is %d bytes, below the limit of %d bytes. Puts are accepted again.", size, q.limit)
	}
}

// Sample reads the size once through source and records it. A failed read
// keeps the last value and is logged; it never clears the limit.
func (q *Quota) Sample(ctx context.Context, source func(context.Context) (int64, error)) error {
	size, err := source(ctx)
	if err != nil {
		logrus.Warnf("quota: cannot read the database size: %v. The last value, %d bytes, stays in use.", err, q.Size())
		return err
	}
	q.Observe(size)
	return nil
}

// Run samples the size every interval until ctx ends. Call Sample once
// before serving so a database that is already over the limit refuses from
// the first write, then run this on its own goroutine.
func (q *Quota) Run(ctx context.Context, interval time.Duration, source func(context.Context) (int64, error)) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = q.Sample(ctx, source)
		}
	}
}

// WithQuota wraps backend so Create and Update return ErrNoSpace while the
// quota is full. Everything else passes through. A nil or zero quota returns
// backend itself, so a kine without --quota-bytes runs upstream's code path.
func WithQuota(backend Backend, quota *Quota) Backend {
	if quota == nil || quota.limit <= 0 {
		return backend
	}
	return &quotaBackend{Backend: backend, quota: quota}
}

type quotaBackend struct {
	Backend
	quota *Quota
}

// refuses reports whether a write to key must fail now. The compaction
// bookkeeping key is exempt (see the file comment).
func (b *quotaBackend) refuses(key string) bool {
	return b.quota.Full() && key != string(compactRevAPI)
}

func (b *quotaBackend) Create(ctx context.Context, key string, value []byte, lease int64) (int64, error) {
	if b.refuses(key) {
		return 0, ErrNoSpace
	}
	return b.Backend.Create(ctx, key, value, lease)
}

func (b *quotaBackend) Update(ctx context.Context, key string, value []byte, revision, lease int64) (int64, *KeyValue, bool, error) {
	if b.refuses(key) {
		return 0, nil, false, ErrNoSpace
	}
	return b.Backend.Update(ctx, key, value, revision, lease)
}
