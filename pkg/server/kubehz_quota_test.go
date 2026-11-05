package server

// KUBEHZ-PATCH P2 tests: the size limit on a fake backend, no database.
// unit.yml lists these tests by name before running them, so a rebase that
// drops the wrapper or this file fails CI.

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errNotUsed = errors.New("not used by this test")

// quotaFake records which writes reached the backend. Every write succeeds,
// so a refused write can only have been refused by the quota wrapper.
type quotaFake struct {
	mu        sync.Mutex
	creates   []string
	updates   []string
	deletes   []string
	size      atomic.Int64
	sizeErr   atomic.Pointer[error]
	sizeReads atomic.Int64
	// current is the value Get returns at revision 1; missing makes Get find
	// no key; createErr fails every Create.
	current   []byte
	missing   bool
	createErr error
	// hold, when set, keeps the first Create inside the backend until it is
	// closed; held is signalled when that Create arrives.
	hold   chan struct{}
	held   chan struct{}
	holder atomic.Bool
}

func (f *quotaFake) Start(context.Context) error { return nil }

func (f *quotaFake) Get(context.Context, string, int64, bool) (int64, *KeyValue, error) {
	if f.missing {
		return 1, nil, nil
	}
	return 1, &KeyValue{ModRevision: 1, Value: f.current}, nil
}

func (f *quotaFake) Create(_ context.Context, key string, _ []byte, _ int64) (int64, error) {
	if f.createErr != nil {
		return 0, f.createErr
	}
	if f.hold != nil && f.holder.CompareAndSwap(false, true) {
		f.held <- struct{}{}
		<-f.hold
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.creates = append(f.creates, key)
	return 2, nil
}

func (f *quotaFake) Delete(_ context.Context, key string, _ int64) (int64, *KeyValue, bool, error) {
	f.deletes = append(f.deletes, key)
	return 3, &KeyValue{Key: key}, true, nil
}

func (f *quotaFake) List(context.Context, string, string, int64, int64, bool) (int64, []*KeyValue, error) {
	return 1, nil, nil
}

func (f *quotaFake) Count(context.Context, string, string, int64) (int64, int64, error) {
	return 1, 0, nil
}

func (f *quotaFake) Update(_ context.Context, key string, _ []byte, revision, _ int64) (int64, *KeyValue, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updates = append(f.updates, key)
	// the compare-and-swap of the real backend: no row unless revision is current
	if f.missing || revision != 1 {
		return 1, nil, false, nil
	}
	return 4, &KeyValue{Key: key}, true, nil
}

func (f *quotaFake) Watch(context.Context, string, string, int64) WatchResult { return WatchResult{} }

func (f *quotaFake) DbSize(context.Context) (int64, error) {
	f.sizeReads.Add(1)
	if err := f.sizeErr.Load(); err != nil {
		return 0, *err
	}
	return f.size.Load(), nil
}

func (f *quotaFake) CurrentRevision(context.Context) (int64, error) { return 1, nil }
func (f *quotaFake) Compact(context.Context, int64) (int64, error)  { return 1, nil }
func (f *quotaFake) WaitForSyncTo(int64)                            {}

// compile-time check: the fake is a full Backend, so the wrapper's embedding
// forwards every method it does not override.
var _ Backend = (*quotaFake)(nil)

// liveFake is a backend that reports live bytes apart from its size.
type liveFake struct {
	quotaFake
	live atomic.Int64
}

func (f *liveFake) LiveSize() SizeSource {
	return func(context.Context) (int64, error) { return f.live.Load(), nil }
}

// TestKubehzP2LiveSizeOf: a backend that offers LiveSizer yields its live
// source; one that does not yields nil, so Listen falls back to DbSize.
func TestKubehzP2LiveSizeOf(t *testing.T) {
	if src := LiveSizeOf(&quotaFake{}); src != nil {
		t.Fatal("a backend without LiveSizer yielded a source")
	}
	f := &liveFake{}
	f.live.Store(42)
	f.size.Store(4096)
	src := LiveSizeOf(f)
	if src == nil {
		t.Fatal("a backend with LiveSizer yielded no source")
	}
	if n, err := src(context.Background()); err != nil || n != 42 {
		t.Fatalf("live source: %d, %v; want 42", n, err)
	}

	// the limit compares the live figure, the physical one only feeds the gauge
	quota := NewQuota(100)
	if err := quota.Sample(context.Background(), src, f.DbSize); err != nil {
		t.Fatal(err)
	}
	if quota.Full() || quota.Live() != 42 || quota.Physical() != 4096 {
		t.Fatalf("live %d, physical %d, full %v; want 42, 4096, false", quota.Live(), quota.Physical(), quota.Full())
	}
}

func putTxn(key string, modRev int64) *etcdserverpb.TxnRequest {
	// the apiserver's create (modRev 0) and update (modRev > 0) transactions
	return &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         []byte(key),
			Target:      etcdserverpb.Compare_MOD,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: modRev},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: []byte(key), Value: []byte("v")}},
		}},
		Failure: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{Key: []byte(key)}},
		}},
	}
}

func createTxn(key string) *etcdserverpb.TxnRequest {
	txn := putTxn(key, 0)
	txn.Failure = nil
	return txn
}

func deleteTxn(key string) *etcdserverpb.TxnRequest {
	return &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         []byte(key),
			Target:      etcdserverpb.Compare_MOD,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: 1},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestDeleteRange{RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: []byte(key)}},
		}},
		Failure: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{Key: []byte(key)}},
		}},
	}
}

func compactTxn() *etcdserverpb.TxnRequest {
	// the apiserver's compaction bookkeeping transaction (server/compact.go)
	return &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         compactRevKey,
			Target:      etcdserverpb.Compare_VERSION,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_Version{Version: 0},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: compactRevKey, Value: []byte("10")}},
		}},
		Failure: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{Key: compactRevKey}},
		}},
	}
}

func isNoSpace(err error) bool {
	return errors.Is(err, ErrNoSpace) &&
		status.Code(err) == codes.ResourceExhausted &&
		status.Convert(err).Message() == "etcdserver: mvcc: database space exceeded"
}

// TestKubehzP2Limit: below the limit every write reaches the backend. At and
// above it, a new key (Put, the create transaction) gets ErrNoSpace and never
// reaches the backend, while an update that does not grow its value, the
// compaction-bookkeeping transaction (a small control key), deletes, Compact
// and Range still do (B481: etcd's capped applier refuses those puts too, and
// a k3s at its limit then exits and can never be cleaned up).
func TestKubehzP2Limit(t *testing.T) {
	const limit = 1000
	ctx := context.Background()

	for _, tc := range []struct {
		name string
		size int64
		full bool
	}{
		{"below", limit - 200, false},
		{"at", limit, true},
		{"above", limit + 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &quotaFake{}
			quota := NewQuota(limit)
			quota.Observe(tc.size)
			if quota.Full() != tc.full {
				t.Fatalf("Full() = %v at size %d, limit %d", quota.Full(), tc.size, limit)
			}
			l := &LimitedServer{backend: WithQuota(fake, quota)}

			_, err := l.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("/a"), Value: []byte("v")})
			checkWrite(t, "Put", err, tc.full)
			_, err = l.Txn(ctx, createTxn("/b"))
			checkWrite(t, "Txn create", err, tc.full)
			if _, err := l.Txn(ctx, putTxn("/c", 1)); err != nil {
				t.Fatalf("Txn update that does not grow the value: %v", err)
			}
			compact, err := l.Txn(ctx, compactTxn())
			if err != nil || !compact.Succeeded {
				t.Fatalf("Txn compact bookkeeping: succeeded=%v, %v", compact.GetSucceeded(), err)
			}

			if _, err := l.Txn(ctx, deleteTxn("/a")); err != nil {
				t.Fatalf("Txn delete: %v", err)
			}
			if _, err := l.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: 1}); err != nil {
				t.Fatalf("Compact: %v", err)
			}
			if _, err := l.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("/a")}); err != nil {
				t.Fatalf("Range: %v", err)
			}

			wantCreates := []string{"/a", "/b", string(compactRevAPI)}
			if tc.full {
				wantCreates = []string{string(compactRevAPI)}
			}
			if strings.Join(fake.creates, ",") != strings.Join(wantCreates, ",") {
				t.Fatalf("creates that reached the backend: %v, want %v", fake.creates, wantCreates)
			}
			if strings.Join(fake.updates, ",") != "/c" {
				t.Fatalf("updates that reached the backend: %v, want [/c]", fake.updates)
			}
			if len(fake.deletes) != 1 {
				t.Fatalf("deletes that reached the backend: %v, want one", fake.deletes)
			}
		})
	}
}

func checkWrite(t *testing.T, op string, err error, wantRefused bool) {
	t.Helper()
	if wantRefused && !isNoSpace(err) {
		t.Fatalf("%s above the limit: got %v, want %v", op, err, ErrNoSpace)
	}
	if !wantRefused && err != nil {
		t.Fatalf("%s below the limit: %v", op, err)
	}
}

// TestKubehzP2ZeroIsUnlimited: a zero limit hands back the backend itself, so
// writes take upstream's path, and Full() is never true whatever the size.
func TestKubehzP2ZeroIsUnlimited(t *testing.T) {
	fake := &quotaFake{}
	for _, limit := range []int64{0, -1} {
		quota := NewQuota(limit)
		quota.Observe(1 << 40)
		if quota.Full() {
			t.Fatalf("limit %d: Full() is true", limit)
		}
		if got := WithQuota(fake, quota); got != Backend(fake) {
			t.Fatalf("limit %d: WithQuota wrapped the backend", limit)
		}
	}
	if got := WithQuota(fake, nil); got != Backend(fake) {
		t.Fatal("nil quota wrapped the backend")
	}
}

// TestKubehzP2LogLines: one INFO line when the limit is first reached, none
// while it stays reached, one when the size drops below it again.
func TestKubehzP2LogLines(t *testing.T) {
	hook := logtest.NewGlobal()
	defer hook.Reset()
	level := logrus.GetLevel()
	t.Cleanup(func() { logrus.SetLevel(level) })
	logrus.SetLevel(logrus.InfoLevel)

	quota := NewQuota(100)
	quota.Observe(50)
	quota.Observe(100)
	quota.Observe(150)
	quota.Observe(99)
	quota.Observe(10)

	var lines []string
	for _, e := range hook.AllEntries() {
		if e.Level != logrus.InfoLevel {
			t.Fatalf("unexpected %s line: %s", e.Level, e.Message)
		}
		lines = append(lines, e.Message)
	}
	if len(lines) != 2 {
		t.Fatalf("got %d log lines, want 2:\n%s", len(lines), strings.Join(lines, "\n"))
	}
	if !strings.Contains(lines[0], "live data is 100 bytes, the limit is 100 bytes") || !strings.Contains(lines[0], "refused") {
		t.Fatalf("first line: %q", lines[0])
	}
	if !strings.Contains(lines[1], "live data is 99 bytes, below the limit of 100 bytes") || !strings.Contains(lines[1], "accepted again") {
		t.Fatalf("second line: %q", lines[1])
	}
}

// TestKubehzP2Sampler: Run picks up a changed size on the next tick, and a
// failed sample keeps the last value instead of clearing the limit.
func TestKubehzP2Sampler(t *testing.T) {
	// the 1 ms interval below would print a warning per failed sample
	out := logrus.StandardLogger().Out
	logrus.SetOutput(io.Discard)
	t.Cleanup(func() { logrus.SetOutput(out) })

	// the driver has no live figure here, so DbSize serves as both sources,
	// as Listen does on sqlite
	fake := &quotaFake{}
	fake.size.Store(10)
	quota := NewQuota(100)
	if err := quota.Sample(context.Background(), fake.DbSize, fake.DbSize); err != nil {
		t.Fatal(err)
	}
	if quota.Live() != 10 || quota.Physical() != 10 || quota.Full() {
		t.Fatalf("after the first sample: live %d, physical %d, full %v", quota.Live(), quota.Physical(), quota.Full())
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		quota.Run(ctx, time.Millisecond, fake.DbSize, fake.DbSize)
	}()

	fake.size.Store(200)
	waitFor(t, "size 200 sampled", func() bool { return quota.Live() == 200 && quota.Full() })

	sizeErr := errors.New("connection refused")
	fake.sizeErr.Store(&sizeErr)
	fake.size.Store(1) // would clear the limit if a failed sample were recorded
	reads := fake.sizeReads.Load()
	waitFor(t, "three failed samples", func() bool { return fake.sizeReads.Load() >= reads+3 })
	if quota.Live() != 200 || !quota.Full() {
		t.Fatalf("after failed samples: live %d, full %v; want the last good value 200, full", quota.Live(), quota.Full())
	}

	fake.sizeErr.Store(nil)
	waitFor(t, "size 1 sampled", func() bool { return quota.Live() == 1 && !quota.Full() })

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// fullQuota is a quota at its limit, with fresh buckets.
func fullQuota(t *testing.T) *Quota {
	t.Helper()
	quota := NewQuota(1000)
	quota.Observe(1000)
	if !quota.Full() {
		t.Fatal("quota not full")
	}
	return quota
}

func bytesOf(n int) []byte { return []byte(strings.Repeat("x", n)) }

// TestKubehzP2Lanes: what passes while full (B481). A control key (a Lease,
// the compaction key) may be created small or updated without growth; any
// other existing key may be updated without growth; a new key, a growing
// value and a large control create are refused. An update that cannot write
// (missing key, stale compare revision) reaches the backend and reserves
// nothing.
func TestKubehzP2Lanes(t *testing.T) {
	ctx := context.Background()
	const lease = "/registry/leases/kube-system/kube-controller-manager"
	for _, tc := range []struct {
		name     string
		create   bool
		key      string
		current  int
		value    int
		revision int64
		missing  bool
		pass     bool
		reserved bool
	}{
		{"lease renewal", false, lease, 300, 302, 1, false, true, true},
		{"lease grows past the slack", false, lease, 300, 300 + quotaGrowthSlack + 1, 1, false, false, false},
		{"small lease create", true, "/registry/leases/default/new", 0, 200, 0, false, true, true},
		{"large lease create", true, "/registry/leases/default/big", 0, quotaControlMaxValue + 1, 0, false, false, false},
		{"other create", true, "/registry/configmaps/default/a", 0, 10, 0, false, false, false},
		{"finalizer removal", false, "/registry/pods/default/a", 2000, 1990, 1, false, true, true},
		{"other update grows", false, "/registry/pods/default/a", 10, 100, 1, false, false, false},
		{"stale compare revision", false, "/registry/pods/default/a", 10, 1000, 2, false, true, false},
		{"missing key", false, "/registry/pods/default/b", 0, 1000, 1, true, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &quotaFake{current: bytesOf(tc.current), missing: tc.missing}
			quota := fullQuota(t)
			b := WithQuota(fake, quota)
			before := quota.Pending()
			var err error
			if tc.create {
				_, err = b.Create(ctx, tc.key, bytesOf(tc.value), 0)
			} else {
				_, _, _, err = b.Update(ctx, tc.key, bytesOf(tc.value), tc.revision, 0)
			}
			if tc.pass && err != nil {
				t.Fatalf("refused: %v", err)
			}
			if !tc.pass && !isNoSpace(err) {
				t.Fatalf("got %v, want %v", err, ErrNoSpace)
			}
			reached := len(fake.creates)+len(fake.updates) > 0
			if reached != tc.pass {
				t.Fatalf("reached the backend: %v, want %v", reached, tc.pass)
			}
			if got := quota.Pending() - before; (got > 0) != tc.reserved {
				t.Fatalf("pending grew by %d, want a reservation: %v", got, tc.reserved)
			}
			if !tc.create && tc.reserved {
				// while full an update counts key + new + old exactly
				if want := int64(len(tc.key) + tc.value + tc.current); quota.Pending()-before != want {
					t.Fatalf("pending grew by %d, want %d", quota.Pending()-before, want)
				}
			}
		})
	}
}

// TestKubehzP2Ceiling: at the ceiling the lanes close too; only deletes go on.
// A lane write that would cross the ceiling is refused, one that ends on it
// passes.
func TestKubehzP2Ceiling(t *testing.T) {
	ctx := context.Background()
	const lease = "/registry/leases/kube-system/k3s"
	fake := &quotaFake{current: bytesOf(100)}
	quota := NewQuota(1000)
	b := WithQuota(fake, quota)
	cost := int64(len(lease) + 100 + 100)

	quota.Observe(quota.Ceiling() - cost)
	if _, _, _, err := b.Update(ctx, lease, bytesOf(100), 1, 0); err != nil {
		t.Fatalf("a lane write that ends on the ceiling: %v", err)
	}
	if _, _, _, err := b.Update(ctx, lease, bytesOf(100), 1, 0); !isNoSpace(err) {
		t.Fatalf("a lane write past the ceiling: got %v, want %v", err, ErrNoSpace)
	}
	if _, _, _, err := b.Delete(ctx, lease, 1); err != nil {
		t.Fatalf("a delete at the ceiling: %v", err)
	}
	if len(fake.updates) != 1 || len(fake.deletes) != 1 {
		t.Fatalf("reached the backend: updates %v, deletes %v; want one each", fake.updates, fake.deletes)
	}
}

// TestKubehzP2LaneRate: each lane has its own bucket; an exhausted control
// bucket does not stop cleanup writes, and the other way round.
func TestKubehzP2LaneRate(t *testing.T) {
	saved := []any{quotaControlRate, quotaControlBurst, quotaCleanupOps, quotaCleanupBurst}
	t.Cleanup(func() {
		quotaControlRate, quotaControlBurst = saved[0].(rate.Limit), saved[1].(int)
		quotaCleanupOps, quotaCleanupBurst = saved[2].(rate.Limit), saved[3].(int)
	})
	quotaControlRate, quotaControlBurst = 0, 2
	quotaCleanupOps, quotaCleanupBurst = 0, 1

	ctx := context.Background()
	fake := &quotaFake{current: bytesOf(10)}
	b := WithQuota(fake, fullQuota(t))
	update := func(key string) error {
		_, _, _, err := b.Update(ctx, key, bytesOf(10), 1, 0)
		return err
	}
	for i := 0; i < 2; i++ {
		if err := update("/registry/leases/a"); err != nil {
			t.Fatalf("control write %d: %v", i, err)
		}
	}
	if err := update("/registry/leases/a"); !isNoSpace(err) {
		t.Fatalf("control write past the burst: got %v, want %v", err, ErrNoSpace)
	}
	if err := update("/registry/pods/a"); err != nil {
		t.Fatalf("cleanup write with the control bucket empty: %v", err)
	}
	if err := update("/registry/pods/a"); !isNoSpace(err) {
		t.Fatalf("cleanup write past the burst: got %v, want %v", err, ErrNoSpace)
	}
}

// TestKubehzP2Reservation: a write reserves its bytes before it reaches the
// backend, so a second create just below the limit sees the first one while
// it is still in flight and is refused; a create the backend fails gives its
// bytes back.
func TestKubehzP2Reservation(t *testing.T) {
	ctx := context.Background()
	fake := &quotaFake{hold: make(chan struct{}), held: make(chan struct{}, 1)}
	quota := NewQuota(1000)
	quota.Observe(990)
	b := WithQuota(fake, quota)

	first := make(chan error, 1)
	go func() {
		_, err := b.Create(ctx, "/k", bytesOf(98), 0) // 100 bytes with the key
		first <- err
	}()
	<-fake.held
	if _, err := b.Create(ctx, "/j", bytesOf(98), 0); !isNoSpace(err) {
		close(fake.hold)
		t.Fatalf("a create while 990 + 100 in flight of 1000: got %v, want %v", err, ErrNoSpace)
	}
	close(fake.hold)
	if err := <-first; err != nil {
		t.Fatalf("the first create: %v", err)
	}
	if quota.Pending() != 100 || len(fake.creates) != 1 {
		t.Fatalf("pending %d and creates %v, want 100 and one", quota.Pending(), fake.creates)
	}

	failing := NewQuota(1000)
	bf := WithQuota(&quotaFake{createErr: errors.New("disk I/O error")}, failing)
	if _, err := bf.Create(ctx, "/k", bytesOf(98), 0); err == nil {
		t.Fatal("the failing create passed")
	}
	if failing.Pending() != 0 {
		t.Fatalf("pending %d after a failed create, want 0", failing.Pending())
	}
}

// TestKubehzP2PendingWindow: the written bytes stay in the figure until the
// good sample two samples after them, because the statistics may not show
// them before that; a failed sample moves nothing, so a sampler that keeps
// failing refuses writes once they add up to the limit (fail closed).
func TestKubehzP2PendingWindow(t *testing.T) {
	out := logrus.StandardLogger().Out
	logrus.SetOutput(io.Discard)
	t.Cleanup(func() { logrus.SetOutput(out) })

	ctx := context.Background()
	quota := NewQuota(1000)
	live := func(context.Context) (int64, error) { return 0, nil }
	b := WithQuota(&quotaFake{}, quota)
	if _, err := b.Create(ctx, "/a", bytesOf(98), 0); err != nil { // 100 bytes with the key
		t.Fatal(err)
	}
	for i, want := range []int64{100, 100, 0} {
		if err := quota.Sample(ctx, live, nil); err != nil {
			t.Fatal(err)
		}
		if quota.Pending() != want {
			t.Fatalf("after good sample %d: pending %d, want %d", i+1, quota.Pending(), want)
		}
	}

	failed := func(context.Context) (int64, error) { return 0, errors.New("connection refused") }
	for i := 0; i < 9; i++ {
		if _, err := b.Create(ctx, "/b", bytesOf(98), 0); err != nil {
			t.Fatalf("create %d below the limit: %v", i, err)
		}
		_ = quota.Sample(ctx, failed, nil)
	}
	if _, err := b.Create(ctx, "/b", bytesOf(98), 0); err != nil {
		t.Fatalf("the create that reaches the limit: %v", err)
	}
	_ = quota.Sample(ctx, failed, nil)
	if _, err := b.Create(ctx, "/b", bytesOf(98), 0); !isNoSpace(err) {
		t.Fatalf("a create after 1000 bytes and only failed samples: got %v, want %v", err, ErrNoSpace)
	}
}

// reliefFake records the relief passes it was asked for.
type reliefFake struct {
	targets []int64
	rows    int64
}

func (r *reliefFake) RelieveQuota(_ context.Context, target int64) (ReliefResult, error) {
	r.targets = append(r.targets, target)
	return ReliefResult{Rows: r.rows, CompactedTo: target}, nil
}

// TestKubehzP2ReliefTick: a tick that finds the quota full compacts to the
// revision of the previous tick and reads the figure again when rows went;
// a tick below the limit, or the first tick, compacts nothing.
func TestKubehzP2ReliefTick(t *testing.T) {
	out := logrus.StandardLogger().Out
	logrus.SetOutput(io.Discard)
	t.Cleanup(func() { logrus.SetOutput(out) })

	ctx := context.Background()
	var size, reads atomic.Int64
	live := func(context.Context) (int64, error) { reads.Add(1); return size.Load(), nil }
	var rev atomic.Int64
	relief := &reliefFake{rows: 5}
	quota := NewQuota(1000)
	quota.EnableRelief(relief, func(context.Context) (int64, error) { return rev.Load(), nil })

	size.Store(2000)
	rev.Store(50)
	quota.tick(ctx, live, nil) // first tick: no previous revision yet
	rev.Store(80)
	quota.tick(ctx, live, nil)
	size.Store(10)
	rev.Store(90)
	quota.tick(ctx, live, nil) // below the limit
	if len(relief.targets) != 1 || relief.targets[0] != 50 {
		t.Fatalf("relief targets %v, want [50]", relief.targets)
	}
	if reads.Load() != 4 {
		t.Fatalf("%d live reads, want 4 (three ticks and one after the relief pass)", reads.Load())
	}
}
