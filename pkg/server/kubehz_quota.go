package server

// KUBEHZ-PATCH P2 — per-database size limit. This whole file is ours; see
// kubehz/MERGE-GUIDE.md#p2.
//
// INTENT: with --quota-bytes L > 0, a database whose figure is at or above L
//   ("full") refuses new keys with etcd's no-space error and keeps serving
//   reads, deletes, watches and compaction. Unlike etcd's capped applier it
//   also keeps the cluster able to run and to clean up while full (B481):
//   - an update of an EXISTING key that does not grow the value passes
//     inside a rate limit. Leader-election Leases, kine's stand-in for the
//     apiserver's compaction key, a deletionTimestamp and a finalizer
//     removal are such updates. Without them k3s exits on its first lost
//     lease and the tenant can never delete its way back;
//   - a small create or update of a control key (Leases, master leases, the
//     compaction key) passes in its own rate limit;
//   - the sampler compacts history by itself while full (QuotaRelief), to
//     revisions at least QuotaReliefLag old, so a delete frees space with
//     no staff step. When every pass succeeds, that takes the lag plus about
//     three intervals (about 3.5 min): one to reach the next tick, one
//     compaction pass, a quiet tick, and a refresh pass at least
//     quotaRefreshSettle after the compaction ended. That is no bound: a
//     refresh that fails or does not fit its budget delays it.
//
// INVARIANT: the size is sampled, never queried on the write path. A write
//   below the limit costs atomic loads and two atomic adds. The
//   figure is the last good live sample plus the bytes this process
//   reserved, less the bytes that were committed at the start of the good
//   sample TWO samples back (60 to 90 s). PostgreSQL flushes a busy
//   backend's counters within 1 s, an idle one within 10 s, and forces a
//   flush after 60 s, so a write the statistics do not show yet is still in
//   the figure. The overlap counts some writes twice, which only refuses
//   early. The marks read committed bytes, so a write that is still running
//   at a mark stays in the figure until it is done. A sample stores the live
//   figure before it moves the base, and a reader loads the base before the
//   live figure: a mixed read is an old base with a new sample, which reads
//   high. A failed sample changes neither term, so the written bytes keep
//   adding up: a sampler that keeps failing refuses writes after (L - live)
//   bytes, it fails closed.
//
// INVARIANT: every put reserves its bytes BEFORE the check, so concurrent
//   writes see each other: the limit is passed by one write at most. A
//   refused write gives its bytes back, and so does a write that provably
//   did not happen: a create whose key exists, an update that lost its
//   compare-and-swap. Any other error counts as written, because the row can
//   exist (a lost connection after the commit). A create counts key +
//   value; an update counts key + 2 x value below the limit (the new row
//   also carries the old value; over a key's life the charge is at least the
//   bytes written, less the key's first value) and key + new + old value
//   exactly when full, because the check reads the key anyway. When full, an
//   update passes only when its compare revision is the one the check read
//   (0 when it found no live key). The insert copies the old value from the
//   row at the compare revision, and rows never change, so that row is the
//   one the check read and the charge is exact. With no live key the update
//   is a create (key + value, the create rules). The quota answers an update
//   at any other revision itself, as a lost compare-and-swap with the key as
//   read (LogStructured.Update's answer), and the backend never sees it. At
//   an older revision, compaction can make an older row of a key the newest
//   again, so the backend could write it with no charge. At a newer revision,
//   another write can land there before the insert (after a delete the
//   update writes the key again, upstream issue k3s-io/kine#825), and the
//   charge would come from a value the check did not read. An update
//   below the limit that shrinks a value is charged less than it writes, by
//   at most the old value: for that gap alone, the stored data can pass the
//   figure by up to L while no good sample shows it, with no time bound
//   (accepted, see the guide's "Bounds"). A delete is
//   never refused and not counted; its row keeps the old value until
//   compaction, so the next sample shows it.
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
// INVARIANT (the bounds a hostile tenant keeps):
//   - Above L only the two lanes put. The control lane writes values of at
//     most quotaControlMaxValue bytes at quotaControlRate; the cleanup lane
//     writes no-growth updates at quotaCleanupOps and quotaCleanupBytes
//     (charged key + new + old).
//   - The ceiling 2L + quotaCeilingMargin is a put-admission threshold: a
//     lane write passes only while figure + its cost stays at or below it.
//     It is that high so that a tenant who deletes all its data at L (each
//     delete row keeps the old value until compaction) does not close the
//     lease lane in the middle of its cleanup. Each lane has its own cap
//     below it: control creates stop at L + quotaCreateMargin, cleanup
//     updates stop quotaControlReserve below the ceiling, and only control
//     updates (the renewals) go up to the ceiling. Deletes are never
//     refused, so the database can hold up to the ceiling plus the data
//     deleted since the last relief pass.
//   - The relief compacts to the newest tick revision that is at least
//     QuotaReliefLag old, never past this process's poller and never the
//     newest row. If no revision was written during the lag, and the pass
//     compacted up to the newest row with no error, it writes kine's health
//     key, so the deleted rows fall behind the newest row (the one the pass
//     read, not the cached revision). A pass that failed writes no marker,
//     so markers cannot pile up. The statistics refresh is a pass of its
//     own, at least quotaRefreshSettle after the end of the last deleting
//     pass, with no pass in between. It also runs while not full, and it
//     pauses after quotaRefreshTries failures in a row.
//   - Each kine process counts only its own writes, and nothing fences a
//     second writer. Two processes on one database (a rolling update) see
//     each other's writes when a good sample shows them, with no time bound.
//     The relief stays behind its own poller only: a second poller that
//     falls behind by more than the lag can miss events (see the guide).
//   - kine's own TTL deletes run with no request deadline. One that waits
//     between its read and its insert for longer than the lag can pass its
//     compare-and-swap after a relief pass and delete a newer value of its
//     key. Only a key queued from a leased revision takes that path, and the
//     effect stays in the tenant's own data.
//   - A tenant that spends the lanes' rate on its own keys can starve its own
//     controllers' renewals. That harms only its own cluster.
//   None of this bounds the physical file or the WAL; those are the
//   PostgreSQL operator's (CNPG volume and WAL limits).

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"time"

	"github.com/k3s-io/kine/pkg/metrics"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"golang.org/x/time/rate"
)

// ErrNoSpace is the error a write gets above the limit. It is etcd's own
// value: same gRPC code (ResourceExhausted), same message.
var ErrNoSpace = rpctypes.ErrGRPCNoSpace

// QuotaSampleInterval is how often the sizes are read for the quota check.
// Every sample is a few cheap catalog queries, so 30 s costs nothing
// measurable. The written-bytes term covers the time between samples.
const QuotaSampleInterval = 30 * time.Second

// QuotaReliefLag is how old a revision must be before the relief compacts up
// to it. It is longer than the apiserver's request timeout (60 s) and than
// QuotaTTLDeleteTimeout, and far behind this process's poller. Compaction
// that is too recent lets a write that paused between its read and its
// insert pass its compare-and-swap, because the row it conflicted with is
// gone. The lag does not protect a second kine process on the same database:
// a poller of that process can stall for longer. Run one kine process per
// database, also during a rollout (kubehz/MERGE-GUIDE.md#p2, "Bounds").
const QuotaReliefLag = 2 * time.Minute

const (
	// quotaQueryTimeout bounds one size query, so a hung query cannot stop
	// the sampler; a timed-out sample counts as a failed one.
	quotaQueryTimeout = 10 * time.Second
	// quotaReliefBudget bounds one compaction pass, so it ends before the
	// next sample is due.
	quotaReliefBudget = 20 * time.Second
	// quotaRefreshBudget bounds one refresh pass. A pass refreshes or
	// compacts, never both, so a VACUUM of a large table does not take the
	// compaction's time. The next sample waits for it (no 30 s cadence
	// then); the bytes written meanwhile are reserved and stay in the
	// figure.
	quotaRefreshBudget = 2 * time.Minute
	// quotaRefreshSettle is how long after a pass that deleted rows the
	// statistics refresh waits, with no compaction in between. PostgreSQL
	// flushes an idle backend's counters within 10 s; a VACUUM before that
	// subtracts the deleted rows twice (sqllog's RelieveQuota). It is a
	// margin over that interval, not a proof: a backend under lock contention
	// can flush later. A slow tick can come right after a pass, so the wait is
	// measured from the end of the pass, not assumed from the interval.
	quotaRefreshSettle = 15 * time.Second
	// quotaRefreshTries failed refreshes in a row pause the refresh for
	// quotaRefreshPause. A table whose VACUUM never fits the budget then
	// costs a few attempts an hour, and autovacuum, which the relief's
	// deletes start, brings the statistics up to date.
	quotaRefreshTries = 3
	quotaRefreshPause = 10 * time.Minute
	// quotaGrowthSlack is how much larger a value may get in an update while
	// full. A Lease renewal changes renewTime, whose encoding can grow by a
	// byte or two; deletionTimestamp and a finalizer removal add a few bytes
	// or none.
	quotaGrowthSlack = 64
	// quotaControlMaxValue is the largest value the control lane writes. A
	// Lease is a few hundred bytes; a larger one goes to the cleanup lane.
	quotaControlMaxValue = 4 << 10
	// quotaCeilingMargin is added to twice the limit to make the ceiling, so a
	// small limit still has room for the deletes of its own cleanup.
	quotaCeilingMargin = 64 << 20
	// quotaCreateMargin is how far above the limit the control lane creates
	// new keys. New Leases while full are few (a node joins, a controller
	// starts), so many of them are a fault or an attack, and they must not
	// fill the room that the renewals need.
	quotaCreateMargin = 16 << 20
	// quotaControlReserve is the part of the ceiling only control updates
	// (the renewals) may use, so cleanup churn cannot close the lease lane.
	quotaControlReserve = 32 << 20
)

// The lanes' rates. A 100-node k3s cluster renews about 15 Leases a second.
var (
	quotaControlRate       = rate.Limit(100)
	quotaControlBurst      = 200
	quotaCleanupOps        = rate.Limit(20)
	quotaCleanupBurst      = 50
	quotaCleanupBytes      = rate.Limit(4 << 20)
	quotaCleanupBytesBurst = 16 << 20
)

// quotaControlPrefixes are the keys of the control lane: coordination Leases
// (leader election, node heartbeats, apiserver identity), the endpoint
// reconciler's master leases, and kine's stand-in for the apiserver's
// compaction key (compact.go). The names say which bucket a write uses; they
// do not make a caller trusted.
var quotaControlPrefixes = []string{"/registry/leases/", "/registry/masterleases/", string(compactRevAPI)}

// SizeSource reads one size figure in bytes.
type SizeSource func(context.Context) (int64, error)

// LiveSizer is an optional interface a backend, a log or a dialect can offer
// when it can report live data bytes apart from the file size. It returns
// nil when the layer underneath has no such figure, so a caller can fall
// back without a probe query. It is asserted at the call site on purpose, so
// the server.Backend interface stays upstream's.
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

// ReliefResult is what one relief pass did.
type ReliefResult struct {
	// Rows is the number of rows the pass deleted.
	Rows int64
	// CompactedTo is the compact revision after the pass.
	CompactedTo int64
	// Head is the newest revision the pass read in its transactions, 0 when
	// it read none. It is read from the database, not from a cache.
	Head int64
	// Stale is true when the pass deleted rows that the statistics behind
	// the live figure do not show yet, so a refresh is owed.
	Stale bool
	// Refreshed is true when the pass refreshed the statistics.
	Refreshed bool
}

// QuotaRelief is an optional interface a backend or a log can offer.
// RelieveQuota compacts history up to target, never past what its own
// watchers were sent and never the newest row; with refresh it only
// refreshes the statistics the live figure reads and compacts nothing.
// StatsOwed reports a statistics reset that no VACUUM settled yet. Asserted
// at the call site like LiveSizer.
type QuotaRelief interface {
	RelieveQuota(ctx context.Context, target int64, refresh bool) (ReliefResult, error)
	StatsOwed() bool
}

// QuotaReliefOf returns the relief of backend, or nil when it offers none.
func QuotaReliefOf(backend Backend) QuotaRelief {
	if r, ok := backend.(QuotaRelief); ok {
		return r
	}
	return nil
}

// QuotaTTLDeleteTimeout bounds one delete of the TTL loop (the read and the
// insert together) while the quota is on. The relief compacts with no
// --compact-min-retain floor, so a TTL delete that paused between its read
// and its insert for longer than QuotaReliefLag could pass its
// compare-and-swap and remove a newer value. The apiserver bounds its own
// requests the same way (60 s). On a timeout the TTL loop tries the key
// again.
const QuotaTTLDeleteTimeout = 60 * time.Second

// TTLDeleteDeadline is an optional interface a backend can offer: a timeout
// on every delete its TTL loop makes. It must be set before Start.
type TTLDeleteDeadline interface {
	SetTTLDeleteTimeout(d time.Duration)
}

// SetTTLDeleteTimeout sets d on backend when it offers TTLDeleteDeadline and
// reports whether it did.
func SetTTLDeleteTimeout(backend Backend, d time.Duration) bool {
	t, ok := backend.(TTLDeleteDeadline)
	if ok {
		t.SetTTLDeleteTimeout(d)
	}
	return ok
}

// StatsRefresher is an optional interface a dialect can offer to bring the
// statistics behind its live figure up to date after a compaction or a reset.
// StatsOwed reports a reset that no VACUUM settled yet; the dialect itself
// never runs a VACUUM for it, the quota schedules the refresh.
type StatsRefresher interface {
	RefreshStats(ctx context.Context) error
	StatsOwed() bool
}

// Quota holds a limit on live bytes, the last sampled figures and the
// written-bytes term. A zero limit means no limit; WithQuota then returns the
// backend unchanged.
type Quota struct {
	limit   int64
	ceiling int64
	// createCap and cleanupCap are the lower thresholds of the control
	// lane's creates and of the cleanup lane (see quotaCreateMargin and
	// quotaControlReserve). Control updates go up to the ceiling.
	createCap  int64
	cleanupCap int64
	live       atomic.Int64
	physical   atomic.Int64
	// written is every byte this process reserved through the wrapper since
	// it started, less what writes that did not happen gave back. committed
	// is the part whose writes are done (or may be: an error that does not
	// prove the write failed counts as written). base is committed at the
	// start of the good sample two samples back. The figure is
	// live + written - base. The marks read committed, not written, so a
	// reservation given back after a mark never lowers the figure.
	written   atomic.Int64
	committed atomic.Int64
	base      atomic.Int64
	full      atomic.Bool
	// marks are committed at the start of the previous two good samples, the
	// older first. Sampler goroutine only.
	marks [2]int64

	control      *rate.Limiter
	cleanupOps   *rate.Limiter
	cleanupBytes *rate.Limiter

	// The relief's state, set by EnableRelief before Run starts and used by
	// the sampler goroutine only. backend is the unwrapped backend (the
	// current revision and the head marker). revs is the current revision
	// at recent ticks, the oldest first. refreshOwed is set while the
	// statistics miss deleted rows or a reset; a refresh pass at least
	// quotaRefreshSettle after lastDelete (the end of the last pass that
	// deleted rows) clears it. refreshFails counts failed refreshes in a row;
	// refreshPaused is when they may start again.
	relief        QuotaRelief
	backend       Backend
	lag           time.Duration
	revs          []tickRev
	refreshOwed   bool
	lastDelete    time.Time
	refreshFails  int
	refreshPaused time.Time
	now           func() time.Time
}

// tickRev is the current revision at one tick.
type tickRev struct {
	at  time.Time
	rev int64
}

// NewQuota returns a quota for limit bytes of live data. limit <= 0 means no
// limit.
func NewQuota(limit int64) *Quota {
	q := &Quota{
		limit:        max(limit, 0),
		control:      rate.NewLimiter(quotaControlRate, quotaControlBurst),
		cleanupOps:   rate.NewLimiter(quotaCleanupOps, quotaCleanupBurst),
		cleanupBytes: rate.NewLimiter(quotaCleanupBytes, quotaCleanupBytesBurst),
		lag:          QuotaReliefLag,
		now:          time.Now,
	}
	if q.limit > 0 {
		q.ceiling = 2*q.limit + quotaCeilingMargin
		q.createCap = q.limit + quotaCreateMargin
		q.cleanupCap = q.ceiling - quotaControlReserve
	}
	metrics.QuotaBytes.Set(float64(q.limit))
	return q
}

// Limit is the configured limit in bytes; 0 when there is none.
func (q *Quota) Limit() int64 { return q.limit }

// Ceiling is the put-admission threshold of the lanes.
func (q *Quota) Ceiling() int64 { return q.ceiling }

// Live is the last sampled live data size in bytes.
func (q *Quota) Live() int64 { return q.live.Load() }

// Physical is the last sampled database size in bytes, the figure Status
// reports. It is not compared.
func (q *Quota) Physical() int64 { return q.physical.Load() }

// Pending is the written-bytes term of the figure.
func (q *Quota) Pending() int64 {
	base := q.base.Load()
	return q.written.Load() - base
}

// Figure is what the limit compares: the last live sample plus Pending.
func (q *Quota) Figure() int64 { return q.figure(0) }

// figure returns the figure after reserving add more bytes. It loads the base
// before the live sample, and sample stores them in the other order, so a
// figure can mix a new sample with an old base (it reads high, which only
// refuses early) but never a new base with an old sample (it would read low).
func (q *Quota) figure(add int64) int64 {
	base := q.base.Load()
	live := q.live.Load()
	if add == 0 {
		return live + q.written.Load() - base
	}
	return live + q.written.Add(add) - base
}

// Full reports whether the figure is at or above the limit. etcd refuses a
// write when size + cost would pass the quota, and every put has a cost, so
// "at the limit" already refuses.
func (q *Quota) Full() bool { return q.limit > 0 && q.Figure() >= q.limit }

// reserveBelow reserves cost and keeps it when the figure without this write
// (other writes' reservations included) is below the limit.
func (q *Quota) reserveBelow(cost int64) bool {
	if q.figure(cost)-cost < q.limit {
		return true
	}
	q.written.Add(-cost)
	return false
}

// reserveWithin reserves cost and keeps it when the figure with this write
// stays at or below limit.
func (q *Quota) reserveWithin(cost, limit int64) bool {
	if q.figure(cost) <= limit {
		return true
	}
	q.written.Add(-cost)
	return false
}

// release gives back the reservation of a write that did not happen.
func (q *Quota) release(cost int64) { q.written.Add(-cost) }

// settle ends a reservation: a write that happened, or may have, commits its
// cost; one that certainly did not gives it back.
func (q *Quota) settle(cost int64, wrote bool) {
	if wrote {
		q.committed.Add(cost)
	} else {
		q.release(cost)
	}
}

// Observe records a sampled live size and logs the two transitions: the
// first figure at or above the limit, and the first one below it again.
func (q *Quota) Observe(live int64) {
	q.live.Store(live)
	metrics.LiveBytes.Set(float64(live))
	if q.limit <= 0 {
		return
	}
	q.publish()
}

// publish sets the full and pending gauges from the figure and logs the two
// transitions. The sampler also calls it after a failed sample, because the
// written bytes alone can fill the quota.
func (q *Quota) publish() {
	figure := q.Figure()
	full := figure >= q.limit
	metrics.QuotaFull.Set(boolGauge(full))
	metrics.QuotaPendingBytes.Set(float64(q.Pending()))
	if q.full.Swap(full) == full {
		return
	}
	if full {
		logrus.Infof("quota: live data is %d bytes, the limit is %d bytes. New keys are refused until live data is smaller than the limit; reads, deletes and updates that do not grow a value continue.", figure, q.limit)
	} else {
		logrus.Infof("quota: live data is %d bytes, below the limit of %d bytes. Puts are accepted again.", figure, q.limit)
	}
}

func boolGauge(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// ObservePhysical records a sampled database size. It only feeds the gauge.
func (q *Quota) ObservePhysical(size int64) {
	q.physical.Store(size)
	metrics.DBSizeBytes.Set(float64(size))
}

// Sample reads both figures once and records them. physical may be nil. A
// failed read keeps the last value and is logged; it never clears the limit
// and it does not move the written-bytes base. The returned error is the
// live read's. A read that failed because ctx ended (shutdown) is returned
// without a warning.
func (q *Quota) Sample(ctx context.Context, live, physical SizeSource) error {
	return q.sample(ctx, live, physical, true)
}

// sample is Sample; advance=false records the live figure without moving the
// written-bytes base (the re-read right after a relief pass, which is too
// soon after the last mark for the statistics to have every write).
func (q *Quota) sample(ctx context.Context, live, physical SizeSource, advance bool) error {
	mark := q.committed.Load()
	if physical != nil {
		if size, err := querySize(ctx, physical); err != nil {
			if ctx.Err() != nil {
				return err
			}
			metrics.QuotaSampleErrors.Inc()
			logrus.Warnf("quota: cannot read the database size: %v. The last value, %d bytes, stays in use.", err, q.Physical())
		} else {
			q.ObservePhysical(size)
		}
	}
	size, err := querySize(ctx, live)
	if err != nil {
		if ctx.Err() == nil {
			metrics.QuotaSampleErrors.Inc()
			logrus.Warnf("quota: cannot read the live data size: %v. The last value, %d bytes, and the bytes written since stay in use.", err, q.Live())
		}
		return err
	}
	// The sample before the base: see figure.
	q.live.Store(size)
	if quotaSampleHook != nil {
		quotaSampleHook()
	}
	if advance {
		q.base.Store(q.marks[0])
		q.marks[0], q.marks[1] = q.marks[1], mark
		metrics.QuotaLastSample.SetToCurrentTime()
	}
	q.Observe(size)
	return nil
}

// quotaSampleHook runs between the two stores of a sample. A test seam for
// the store order; nil outside tests.
var quotaSampleHook func()

// querySize reads one size under quotaQueryTimeout.
func querySize(ctx context.Context, src SizeSource) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, quotaQueryTimeout)
	defer cancel()
	return src(ctx)
}

// EnableRelief makes Run compact history while full. backend is the
// unwrapped backend: Run reads its current revision and writes the head
// marker through it. lag is QuotaReliefLag outside tests. Call before Run.
func (q *Quota) EnableRelief(relief QuotaRelief, backend Backend, lag time.Duration) {
	q.relief, q.backend, q.lag = relief, backend, lag
}

// Run samples both figures every interval until ctx ends, and runs a relief
// pass after each sample that finds the quota full. Call Sample once before
// serving so a database that is already over the limit refuses from the
// first write, then run this on its own goroutine.
func (q *Quota) Run(ctx context.Context, interval time.Duration, live, physical SizeSource) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			q.tick(ctx, live, physical)
		}
	}
}

// tick is one sample and, while full, one relief pass. A failed sample does
// not stop the relief: the written bytes alone can fill the quota, and the
// relief needs no figure to compact.
func (q *Quota) tick(ctx context.Context, live, physical SizeSource) {
	if err := q.Sample(ctx, live, physical); err != nil {
		if ctx.Err() != nil {
			return
		}
		q.publish()
	}
	if q.relief == nil {
		return
	}
	current, err := q.backend.CurrentRevision(ctx)
	if err != nil {
		if ctx.Err() == nil {
			logrus.Warnf("quota: cannot read the current revision, no relief this pass: %v", err)
		}
		return
	}
	// after the read: a read that stalled must not date the revision early,
	// or a later tick would compact it before it is QuotaReliefLag old
	now := q.now()
	target := q.reliefTarget(now)
	q.revs = append(q.revs, tickRev{at: now, rev: current})
	if q.relief.StatsOwed() {
		// a statistics reset: the figure is an upper bound until a refresh
		q.refreshOwed = true
	}
	// One pass per tick, a refresh or a compaction. Every VACUUM the quota
	// asks for runs here, on the sampler goroutine, so none overlaps the
	// relief's deletes; also while not full, so the figure does not keep a
	// stale floor until the quota is full again.
	switch {
	case q.refreshDue(now):
		q.refresh(ctx, live, physical)
	case q.settling(now):
		// An owed refresh waits for the last deletes to settle, with no
		// compaction meanwhile: a pass on every tick would move lastDelete
		// each time and put the refresh off for as long as there is history
		// to delete.
	case q.Full() && target > 0:
		q.relieve(ctx, live, physical, target)
	}
}

// refreshDue reports whether this tick refreshes the statistics: a refresh
// is owed, the last deletes are quotaRefreshSettle old, and the refresh is
// not paused after failures.
func (q *Quota) refreshDue(now time.Time) bool {
	return q.refreshOwed && now.Sub(q.lastDelete) >= quotaRefreshSettle && !now.Before(q.refreshPaused)
}

// settling reports whether an owed refresh waits for the last deletes to
// settle. While the refresh is paused the compaction goes on.
func (q *Quota) settling(now time.Time) bool {
	return q.refreshOwed && now.Sub(q.lastDelete) < quotaRefreshSettle && !now.Before(q.refreshPaused)
}

// reliefTarget is the newest revision recorded at least q.lag before now, or
// 0 when there is none yet. Older entries are dropped.
func (q *Quota) reliefTarget(now time.Time) int64 {
	i := -1
	for j, r := range q.revs {
		if now.Sub(r.at) >= q.lag {
			i = j
		}
	}
	if i < 0 {
		return 0
	}
	q.revs = q.revs[i:]
	return q.revs[0].rev
}

// refresh is a pass that only refreshes the statistics, under
// quotaRefreshBudget, and reads the live figure again when it succeeded.
// After quotaRefreshTries failures in a row it pauses for quotaRefreshPause.
func (q *Quota) refresh(ctx context.Context, live, physical SizeSource) {
	rctx, cancel := context.WithTimeout(ctx, quotaRefreshBudget)
	defer cancel()
	start := time.Now()
	before := q.Figure()
	res, err := q.relief.RelieveQuota(rctx, 0, true)
	metrics.QuotaReliefPasses.Inc()
	took := time.Since(start).Truncate(time.Millisecond)
	if res.Refreshed {
		q.refreshOwed, q.refreshFails = false, 0
		_ = q.sample(ctx, live, physical, false)
		logrus.Infof("quota: refreshed the statistics in %s. Live data was %d bytes and is %d bytes, the limit is %d bytes.", took, before, q.Figure(), q.limit)
		return
	}
	if ctx.Err() != nil {
		return
	}
	q.refreshFails++
	logrus.Warnf("quota: the statistics refresh failed after %s: %v", took, err)
	if q.refreshFails >= quotaRefreshTries {
		q.refreshFails = 0
		q.refreshPaused = q.now().Add(quotaRefreshPause)
		logrus.Warnf("quota: the statistics refresh failed %d times in a row; it pauses for %s, and autovacuum brings the statistics up to date meanwhile", quotaRefreshTries, quotaRefreshPause)
	}
}

// relieve compacts the history up to target and reads the live figure again
// when that deleted rows. Rows the statistics do not show yet make a refresh
// owed, measured from the end of this pass. The relief never compacts the
// newest row. When no revision came during the whole lag (target is the
// head), that row is often the delete that freed the space, and it still
// carries the old value: one small write to the health key moves the head,
// so the next pass after the lag can compact it.
func (q *Quota) relieve(ctx context.Context, live, physical SizeSource, target int64) {
	rctx, cancel := context.WithTimeout(ctx, quotaReliefBudget)
	defer cancel()
	start := time.Now()
	before := q.Figure()
	res, err := q.relief.RelieveQuota(rctx, target, false)
	if res.Stale {
		q.refreshOwed, q.lastDelete = true, q.now()
	}
	metrics.QuotaReliefPasses.Inc()
	metrics.QuotaReliefRows.Add(float64(res.Rows))
	if res.Rows > 0 {
		_ = q.sample(ctx, live, physical, false)
	}
	took := time.Since(start).Truncate(time.Millisecond)
	if err != nil {
		if ctx.Err() == nil {
			logrus.Warnf("quota: relief compacted %d rows to revision %d in %s, then failed: %v. Live data is %d bytes.", res.Rows, res.CompactedTo, took, err, q.Figure())
		}
	} else {
		metrics.QuotaLastRelief.SetToCurrentTime()
		logrus.Infof("quota: relief compacted %d rows to revision %d in %s. Live data was %d bytes and is %d bytes, the limit is %d bytes.", res.Rows, res.CompactedTo, took, before, q.Figure(), q.limit)
	}
	// The head marker, only when it is what keeps rows back: no revision came
	// during the lag, and this pass compacted all but the newest row. The
	// head is the one the pass read from the database: a marker whose write
	// lost its acknowledgement moved it, and the cached current revision
	// may not show that. A pass that failed or stopped short gets no marker,
	// so markers cannot pile up.
	if res.Head > 0 && target >= res.Head && err == nil && res.CompactedTo >= res.Head-1 && q.Full() && ctx.Err() == nil {
		if err := q.markHead(ctx); err != nil && ctx.Err() == nil {
			logrus.Warnf("quota: cannot write the head marker: %v", err)
		}
	}
}

// markHead writes the health key once, through the unwrapped backend, so the
// current revision moves past the newest row. It counts as written, like any
// write; it is a few bytes, once per lag at most.
func (q *Quota) markHead(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, quotaQueryTimeout)
	defer cancel()
	_, kv, err := q.backend.Get(ctx, HealthKey, 0, false)
	if err != nil {
		return err
	}
	cost := int64(len(HealthKey) + 2*len(HealthVal))
	q.written.Add(cost)
	defer q.committed.Add(cost)
	if kv == nil {
		_, err = q.backend.Create(ctx, HealthKey, []byte(HealthVal), 0)
		return err
	}
	_, _, _, err = q.backend.Update(ctx, HealthKey, []byte(HealthVal), kv.ModRevision, 0)
	return err
}

// WithQuota wraps backend so puts follow the quota (see the INTENT above).
// Everything else passes through. A nil or zero quota returns backend
// itself, so a kine without --quota-bytes runs upstream's code path.
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
	cost := int64(len(key) + len(value))
	if !b.quota.reserveBelow(cost) {
		if reason := b.admitControlCreate(key, value, cost); reason != "" {
			metrics.QuotaRefused.WithLabelValues(reason).Inc()
			return 0, ErrNoSpace
		}
	}
	rev, err := b.Backend.Create(ctx, key, value, lease)
	// Only an existing key proves that nothing was written. Any other error
	// (a lost connection after the commit, a cancelled request) may follow a
	// write, so its cost stays counted.
	b.quota.settle(cost, !errors.Is(err, ErrKeyExists))
	return rev, err
}

// admitControlCreate decides a create while full: only a small control key,
// within the create cap and the control bucket. It returns the refusal
// reason, or "" with cost reserved.
func (b *quotaBackend) admitControlCreate(key string, value []byte, cost int64) string {
	if !isControlKey(key) || len(value) > quotaControlMaxValue {
		return "create"
	}
	if !b.quota.reserveWithin(cost, b.quota.createCap) {
		return "ceiling"
	}
	if !b.quota.control.Allow() {
		b.quota.release(cost)
		return "rate"
	}
	metrics.QuotaLaneWrites.WithLabelValues("control").Inc()
	return ""
}

func (b *quotaBackend) Update(ctx context.Context, key string, value []byte, revision, lease int64) (int64, *KeyValue, bool, error) {
	cost := int64(len(key) + 2*len(value))
	if !b.quota.reserveBelow(cost) {
		var reason string
		var lost *lostCompare
		if cost, reason, lost = b.admitUpdate(ctx, key, value, revision); reason != "" {
			metrics.QuotaRefused.WithLabelValues(reason).Inc()
			return 0, nil, false, ErrNoSpace
		}
		if lost != nil {
			// the answer LogStructured.Update gives a lost compare-and-swap:
			// the revision, the live key or nil, not updated, no error
			return lost.rev, lost.kv, false, nil
		}
	}
	rev, kv, ok, err := b.Backend.Update(ctx, key, value, revision, lease)
	// ok == false with no error is a lost compare-and-swap: no row. An error
	// may follow a write (see Create), so its cost stays counted.
	// LogStructured reports a failed insert as a lost compare-and-swap too;
	// such a row, if it exists, shows in the next samples.
	b.quota.settle(cost, ok || err != nil)
	return rev, kv, ok, err
}

// admitUpdate decides an update while full. It reads the key once, and the
// observed revision is the key's mod revision, or 0 when it has no live key.
// The backend's insert writes only when the newest row of the key has the
// compare revision, and it copies its old value from the row with that id.
// Rows never change after the insert. So at the observed revision the row
// written holds exactly the value read, or the compare fails. That is the
// only update that passes with a charge. An update at any other revision
// gets lost: the current revision and the key as read (nil: no live key).
// Update returns them as LogStructured.Update returns a lost
// compare-and-swap, and the server then answers the transaction with
// succeeded = false and the key, if any, so the apiserver retries as after
// any conflict (or, with no key, reports it not found). The backend never
// sees that update. At an older revision, compaction can make an older row
// of a key the newest again: an insert that started before a compaction can
// add a row whose prev_revision the compaction removed, and when a later
// compaction removes that row and its delete, the older row is the newest.
// At that revision the backend would write with no charge. At a newer
// revision, another write can land there before the insert. The update then
// writes over a value the check did not read, and after a delete it writes
// the key again (since upstream ca3c2d5; upstream issue k3s-io/kine#825).
// That includes an update at the revision of a deleted key's delete row (the
// observed revision is 0 then). With no live key, the update at revision 0 is
// a create: the create rules (admitControlCreate) at a create's cost, key +
// value. With a live key, the update passes when the
// value does not grow by more than quotaGrowthSlack, the figure with its
// exact cost (key + new + old) stays within its lane's cap (control: the
// ceiling; cleanup: the ceiling less quotaControlReserve), and its lane's
// bucket has room. The reservation settles like any update's. It returns the
// reserved cost and "" when it passes, the lost answer, or else the refusal
// reason.
func (b *quotaBackend) admitUpdate(ctx context.Context, key string, value []byte, revision int64) (int64, string, *lostCompare) {
	rev, cur, err := b.Backend.Get(ctx, key, 0, false)
	if err != nil {
		return 0, "read", nil
	}
	observed := int64(0)
	if cur != nil {
		observed = cur.ModRevision
	}
	if revision != observed {
		return 0, "", &lostCompare{rev: rev, kv: cur}
	}
	if cur == nil {
		cost := int64(len(key) + len(value))
		return cost, b.admitControlCreate(key, value, cost), nil
	}
	if len(value)-len(cur.Value) > quotaGrowthSlack {
		return 0, "growth", nil
	}
	cost := int64(len(key) + len(value) + len(cur.Value))
	control := isControlKey(key) && len(value) <= quotaControlMaxValue
	limit := b.quota.cleanupCap
	if control {
		limit = b.quota.ceiling
	}
	if !b.quota.reserveWithin(cost, limit) {
		return 0, "ceiling", nil
	}
	now := time.Now()
	if control {
		if !b.quota.control.AllowN(now, 1) {
			b.quota.release(cost)
			return 0, "rate", nil
		}
		metrics.QuotaLaneWrites.WithLabelValues("control").Inc()
		return cost, "", nil
	}
	if !b.quota.cleanupOps.AllowN(now, 1) {
		b.quota.release(cost)
		return 0, "rate", nil
	}
	if !b.quota.cleanupBytes.AllowN(now, int(min(cost, int64(quotaCleanupBytesBurst)))) {
		b.quota.release(cost)
		return 0, "rate", nil
	}
	metrics.QuotaLaneWrites.WithLabelValues("cleanup").Inc()
	return cost, "", nil
}

// lostCompare is admitUpdate's answer to an update at a compare revision
// other than the one it read: the revision and the key (nil: no live key) as
// the quota read them.
type lostCompare struct {
	rev int64
	kv  *KeyValue
}

func isControlKey(key string) bool {
	for _, p := range quotaControlPrefixes {
		if strings.HasPrefix(key, p) {
			return true
		}
	}
	return false
}
