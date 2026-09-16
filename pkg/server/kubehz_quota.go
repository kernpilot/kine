package server

// KUBEHZ-PATCH P2 — per-database size limit. This whole file is ours; see
// kubehz/MERGE-GUIDE.md#p2.
//
// INTENT: with --quota-bytes > 0, a database whose live data is at or above
//   the limit refuses new keys and new values with etcd's no-space error,
//   and keeps serving reads, deletes, watches and compaction. That is what
//   etcd's capped applier does once its NOSPACE alarm is armed, and the
//   apiserver already handles it.
//
// INVARIANT: the size is sampled, never queried on the write path. A write
//   costs one atomic load. The last good sample stays in use when a sample
//   fails, so a flaky size query never lifts the limit by itself.
//
// INVARIANT: the compared figure is LIVE data where the driver can report
//   it, not the file. On PostgreSQL the relation plateaus under autovacuum:
//   kine's compaction deletes old revisions and autovacuum makes the space
//   reusable, so after a write-then-delete spike the file stays large while
//   live data is small. A limit on the file would then refuse a customer
//   who holds almost nothing. The file size is a capacity figure for
//   whoever runs the PostgreSQL; it stays in Status and in the
//   kine_db_size_bytes gauge. A driver without a live figure (sqlite) falls
//   back to its DbSize, which is pages minus the free list.
//
// Every put is refused above the limit, the apiserver's compaction
// bookkeeping key included, exactly as in etcd.

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

// QuotaSampleInterval is how often the sizes are read for the quota check.
// Every sample is one or two cheap catalog queries, so 30 s costs nothing
// measurable and bounds how long live data can grow past the limit before
// writes stop.
const QuotaSampleInterval = 30 * time.Second

// SizeSource reads one size figure in bytes.
type SizeSource func(context.Context) (int64, error)

// LiveSizer is an optional interface a backend, a log or a dialect can offer
// when it can report live data bytes apart from the file size. It returns
// nil when the layer underneath has no such figure, so a caller can fall
// back without a probe query. It is asserted at the call site on purpose,
// like RevisionNotify in P1, so the server.Backend interface stays upstream's.
type LiveSizer interface {
	LiveSize() SizeSource
}

// LiveSizeOf returns the live-bytes source of backend, or nil when the
// backend offers none.
func LiveSizeOf(backend Backend) SizeSource {
	if l, ok := backend.(LiveSizer); ok {
		return l.LiveSize()
	}
	return nil
}

// Quota holds a limit on live bytes and the last sampled figures. A zero
// limit means no limit; WithQuota then returns the backend unchanged.
type Quota struct {
	limit    int64
	live     atomic.Int64
	physical atomic.Int64
	full     atomic.Bool
}

// NewQuota returns a quota for limit bytes of live data. limit <= 0 means no
// limit.
func NewQuota(limit int64) *Quota {
	q := &Quota{limit: max(limit, 0)}
	metrics.QuotaBytes.Set(float64(q.limit))
	return q
}

// Limit is the configured limit in bytes; 0 when there is none.
func (q *Quota) Limit() int64 { return q.limit }

// Live is the last sampled live data size in bytes, the figure the limit
// compares.
func (q *Quota) Live() int64 { return q.live.Load() }

// Physical is the last sampled database size in bytes, the figure Status
// reports. It is not compared.
func (q *Quota) Physical() int64 { return q.physical.Load() }

// Full reports whether the last live sample was at or above the limit. etcd
// refuses a write when size + cost would pass the quota, and every put has a
// cost, so "at the limit" already refuses.
func (q *Quota) Full() bool { return q.limit > 0 && q.full.Load() }

// Observe records a sampled live size and logs the two transitions: the
// first sample at or above the limit, and the first sample below it again.
func (q *Quota) Observe(live int64) {
	q.live.Store(live)
	metrics.LiveBytes.Set(float64(live))
	if q.limit <= 0 {
		return
	}
	full := live >= q.limit
	if q.full.Swap(full) == full {
		return
	}
	if full {
		logrus.Infof("quota: live data is %d bytes, the limit is %d bytes. Puts are refused until live data is smaller than the limit.", live, q.limit)
	} else {
		logrus.Infof("quota: live data is %d bytes, below the limit of %d bytes. Puts are accepted again.", live, q.limit)
	}
}

// ObservePhysical records a sampled database size. It only feeds the gauge.
func (q *Quota) ObservePhysical(size int64) {
	q.physical.Store(size)
	metrics.DBSizeBytes.Set(float64(size))
}

// Sample reads both figures once and records them. physical may be nil. A
// failed read keeps the last value and is logged; it never clears the limit.
// The returned error is the live read's.
func (q *Quota) Sample(ctx context.Context, live, physical SizeSource) error {
	if physical != nil {
		if size, err := physical(ctx); err != nil {
			logrus.Warnf("quota: cannot read the database size: %v. The last value, %d bytes, stays in use.", err, q.Physical())
		} else {
			q.ObservePhysical(size)
		}
	}
	size, err := live(ctx)
	if err != nil {
		logrus.Warnf("quota: cannot read the live data size: %v. The last value, %d bytes, stays in use.", err, q.Live())
		return err
	}
	q.Observe(size)
	return nil
}

// Run samples both figures every interval until ctx ends. Call Sample once
// before serving so a database that is already over the limit refuses from
// the first write, then run this on its own goroutine.
func (q *Quota) Run(ctx context.Context, interval time.Duration, live, physical SizeSource) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = q.Sample(ctx, live, physical)
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

func (b *quotaBackend) Create(ctx context.Context, key string, value []byte, lease int64) (int64, error) {
	if b.quota.Full() {
		return 0, ErrNoSpace
	}
	return b.Backend.Create(ctx, key, value, lease)
}

func (b *quotaBackend) Update(ctx context.Context, key string, value []byte, revision, lease int64) (int64, *KeyValue, bool, error) {
	if b.quota.Full() {
		return 0, nil, false, ErrNoSpace
	}
	return b.Backend.Update(ctx, key, value, revision, lease)
}
