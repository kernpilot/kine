package server

// KUBEHZ-PATCH P2 tests: the size limit on a fake backend, no database.
// unit.yml lists these tests by name before running them, so a rebase that
// drops the wrapper or this file fails CI.

import (
	"context"
	"errors"
	"io"
	"slices"
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
	// no key; createErr fails every Create, updateErr every Update.
	current   []byte
	missing   bool
	createErr error
	updateErr error
	// hold, when set, keeps the first Create inside the backend until it is
	// closed; held is signalled when that Create arrives. heldErr is what
	// that Create then returns.
	hold    chan struct{}
	held    chan struct{}
	holder  atomic.Bool
	heldErr error
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
		if f.heldErr != nil {
			return 0, f.heldErr
		}
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
	if f.updateErr != nil {
		return 0, nil, false, f.updateErr
	}
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

// TestKubehzP2SetTTLDeleteTimeout: the helper Listen calls hands the timeout
// to a backend that takes one, and reports false for a backend that does not.
func TestKubehzP2SetTTLDeleteTimeout(t *testing.T) {
	if SetTTLDeleteTimeout(&quotaFake{}, QuotaTTLDeleteTimeout) {
		t.Fatal("a backend without TTLDeleteDeadline took a TTL delete timeout")
	}
	f := &ttlDeadlineFake{}
	if !SetTTLDeleteTimeout(f, QuotaTTLDeleteTimeout) || f.timeout != QuotaTTLDeleteTimeout {
		t.Fatalf("the backend got the timeout %s, want %s", f.timeout, QuotaTTLDeleteTimeout)
	}
}

type ttlDeadlineFake struct {
	quotaFake
	timeout time.Duration
}

func (f *ttlDeadlineFake) SetTTLDeleteTimeout(d time.Duration) { f.timeout = d }

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
// it is still in flight and is refused. A create of an existing key gives its
// bytes back; a create with any other error keeps them (it may have written),
// and so does nothing else: a lost compare-and-swap gives them back too.
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

	for _, tc := range []struct {
		name string
		err  error
		want int64
	}{
		{"an existing key", ErrKeyExists, 0},
		{"a lost connection", errors.New("conn closed"), 100},
	} {
		failing := NewQuota(1000)
		bf := WithQuota(&quotaFake{createErr: tc.err}, failing)
		if _, err := bf.Create(ctx, "/k", bytesOf(98), 0); err == nil {
			t.Fatalf("%s: the failing create passed", tc.name)
		}
		if failing.Pending() != tc.want {
			t.Fatalf("%s: pending %d after the failed create, want %d", tc.name, failing.Pending(), tc.want)
		}
	}

	lost := NewQuota(1000)
	bl := WithQuota(&quotaFake{}, lost)
	if _, _, ok, err := bl.Update(ctx, "/k", bytesOf(98), 7, 0); ok || err != nil {
		t.Fatalf("a stale compare revision: ok %v, err %v; want a lost compare-and-swap", ok, err)
	}
	if lost.Pending() != 0 {
		t.Fatalf("pending %d after a lost compare-and-swap, want 0", lost.Pending())
	}

	// an update error can follow the write, so its cost stays counted
	broken := NewQuota(1000)
	bb := WithQuota(&quotaFake{updateErr: errors.New("conn closed")}, broken)
	if _, _, _, err := bb.Update(ctx, "/k", bytesOf(98), 1, 0); err == nil {
		t.Fatal("the failing update passed")
	}
	if broken.Pending() != 198 { // key + 2 x value
		t.Fatalf("pending %d after a failed update, want 198", broken.Pending())
	}
}

// TestKubehzP2ReservationAcrossSamples: a sample that starts while a write is
// in flight, and that write then gives its bytes back, never makes the
// figure lower than the bytes really written (B481 review: the marks read
// committed bytes, not reservations).
func TestKubehzP2ReservationAcrossSamples(t *testing.T) {
	ctx := context.Background()
	fake := &quotaFake{hold: make(chan struct{}), held: make(chan struct{}, 1), heldErr: ErrKeyExists}
	quota := NewQuota(1000)
	b := WithQuota(fake, quota)
	live := func(context.Context) (int64, error) { return 0, nil }

	done := make(chan error, 1)
	go func() {
		_, err := b.Create(ctx, "/k", bytesOf(498), 0) // 500 bytes with the key
		done <- err
	}()
	<-fake.held
	if err := quota.Sample(ctx, live, nil); err != nil { // its mark sees the create in flight
		t.Fatal(err)
	}
	close(fake.hold)
	if err := <-done; !errors.Is(err, ErrKeyExists) {
		t.Fatalf("the held create: %v, want %v", err, ErrKeyExists)
	}
	for i := 0; i < 2; i++ {
		if err := quota.Sample(ctx, live, nil); err != nil {
			t.Fatal(err)
		}
	}
	if quota.Pending() != 0 || quota.Figure() != 0 {
		t.Fatalf("pending %d, figure %d after a given-back create; want 0 and 0", quota.Pending(), quota.Figure())
	}
}

// TestKubehzP2SampleOrder: while a sample moves the base, a concurrent figure
// never reads lower than the data really held. The sample stores the live
// figure before the base; the other order would, for a moment, subtract
// the written bytes before the sample that shows them is visible.
func TestKubehzP2SampleOrder(t *testing.T) {
	ctx := context.Background()
	quota := NewQuota(10000)
	b := WithQuota(&quotaFake{}, quota)
	if _, err := b.Create(ctx, "/k", bytesOf(998), 0); err != nil { // 1000 bytes
		t.Fatal(err)
	}
	var shown atomic.Int64
	live := func(context.Context) (int64, error) { return shown.Load(), nil }
	for i := 0; i < 2; i++ { // the statistics do not show the write yet
		if err := quota.Sample(ctx, live, nil); err != nil {
			t.Fatal(err)
		}
	}
	low := int64(-1)
	quotaSampleHook = func() {
		if f := quota.Figure(); f < 1000 {
			low = f
		}
	}
	t.Cleanup(func() { quotaSampleHook = nil })
	shown.Store(1000) // now they do, and this sample moves the base past the write
	if err := quota.Sample(ctx, live, nil); err != nil {
		t.Fatal(err)
	}
	if low >= 0 {
		t.Fatalf("the figure read %d between the two stores of a sample, below the 1000 bytes held", low)
	}
	if quota.Figure() != 1000 {
		t.Fatalf("figure %d after the sample, want 1000", quota.Figure())
	}
}

// TestKubehzP2LaneCaps: new control keys stop at limit + quotaCreateMargin,
// cleanup updates at the ceiling less quotaControlReserve, and control
// updates (the renewals) go on up to the ceiling.
func TestKubehzP2LaneCaps(t *testing.T) {
	ctx := context.Background()
	const lease = "/registry/leases/kube-system/k3s"
	const pod = "/registry/pods/default/a"
	for _, tc := range []struct {
		name   string
		at     func(q *Quota) int64 // the figure the case starts from
		create bool
		key    string
		pass   bool
	}{
		{"control create below the create cap", func(q *Quota) int64 { return q.createCap - 1000 }, true, "/registry/leases/default/n", true},
		{"control create past the create cap", func(q *Quota) int64 { return q.createCap }, true, "/registry/leases/default/n", false},
		{"cleanup update past the cleanup cap", func(q *Quota) int64 { return q.cleanupCap }, false, pod, false},
		{"control update past the cleanup cap", func(q *Quota) int64 { return q.cleanupCap }, false, lease, true},
		{"control update past the ceiling", func(q *Quota) int64 { return q.ceiling }, false, lease, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &quotaFake{current: bytesOf(100)}
			quota := NewQuota(1000)
			quota.Observe(tc.at(quota))
			b := WithQuota(fake, quota)
			var err error
			if tc.create {
				_, err = b.Create(ctx, tc.key, bytesOf(100), 0)
			} else {
				_, _, _, err = b.Update(ctx, tc.key, bytesOf(100), 1, 0)
			}
			if tc.pass && err != nil {
				t.Fatalf("refused: %v", err)
			}
			if !tc.pass && !isNoSpace(err) {
				t.Fatalf("got %v, want %v", err, ErrNoSpace)
			}
		})
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

// reliefFake records the relief passes it was asked for, like sqllog's
// relief on PostgreSQL: a pass refreshes or compacts. A compaction pass
// clamps to the head less one, reports the head (the backend's current
// revision unless head is set), and marks its rows stale. refreshed is what
// a refresh pass reports as done; owed is StatsOwed, and a refresh that ran
// settles it, as the PostgreSQL dialect's does.
// onPass, when set, runs inside each pass (a slow pass moves the fake clock).
type reliefFake struct {
	targets     []int64 // the compaction passes' targets
	refreshes   int     // the refresh passes
	calls       []string
	rows        int64
	refreshed   bool
	owed        bool
	noStats     bool          // the backend keeps no statistics: deleted rows owe no refresh
	uncertain   bool          // the pass's commit failed and can have deleted rows: stale with 0 rows
	head        int64         // 0: the current revision of cur is the head
	cur         *atomic.Int64 // set by reliefSetup
	err         error
	short       bool // the pass stops 10 revisions short of its target
	onPass      func()
	budgets     []time.Duration // the time each pass had left when it began
	block       bool            // a refresh pass waits for its context, 5 s at most
	interrupted bool            // a blocked refresh pass ended because its context did
}

func (r *reliefFake) RelieveQuota(ctx context.Context, target int64, refresh bool) (ReliefResult, error) {
	if d, ok := ctx.Deadline(); ok {
		r.budgets = append(r.budgets, time.Until(d))
	}
	if r.onPass != nil {
		r.onPass()
	}
	if refresh {
		r.refreshes++
		r.calls = append(r.calls, "refresh")
		if r.block {
			select {
			case <-ctx.Done():
				r.interrupted = true
				return ReliefResult{}, ctx.Err()
			case <-time.After(5 * time.Second):
			}
		}
		if !r.refreshed {
			return ReliefResult{}, errors.New("refresh failed")
		}
		r.owed = false
		return ReliefResult{Refreshed: true}, nil
	}
	r.targets = append(r.targets, target)
	r.calls = append(r.calls, "compact")
	head := r.head
	if head == 0 && r.cur != nil {
		head = r.cur.Load()
	}
	if head == 0 {
		head = target
	}
	to := min(target, head-1)
	if r.short {
		to -= 10
	}
	return ReliefResult{Rows: r.rows, CompactedTo: to, Head: head, Stale: (r.rows > 0 || r.uncertain) && !r.noStats}, r.err
}

func (r *reliefFake) StatsOwed() bool { return r.owed }

// revFake is a backend whose current revision the test sets. onRev, when
// set, runs inside each read (a stalled read moves the fake clock).
type revFake struct {
	*quotaFake
	rev   atomic.Int64
	onRev func()
}

func (f *revFake) CurrentRevision(context.Context) (int64, error) {
	if f.onRev != nil {
		f.onRev()
	}
	return f.rev.Load(), nil
}

// reliefSetup is a quota with relief, a fake clock and a 60 s lag.
func reliefSetup(t *testing.T, relief QuotaRelief) (*Quota, *revFake, *time.Time) {
	t.Helper()
	out := logrus.StandardLogger().Out
	logrus.SetOutput(io.Discard)
	t.Cleanup(func() { logrus.SetOutput(out) })
	fake := &revFake{quotaFake: &quotaFake{}}
	if r, ok := relief.(*reliefFake); ok {
		r.cur = &fake.rev
	}
	quota := NewQuota(1000)
	quota.EnableRelief(relief, fake, time.Minute)
	clock := time.Unix(1_000_000, 0)
	quota.now = func() time.Time { return clock }
	return quota, fake, &clock
}

// TestKubehzP2ReliefTick: a tick that finds the quota full compacts to the
// newest revision at least the lag old and reads the figure again when rows
// went; before the lag has passed, and below the limit with nothing owed, it
// compacts nothing.
func TestKubehzP2ReliefTick(t *testing.T) {
	ctx := context.Background()
	var size, reads atomic.Int64
	live := func(context.Context) (int64, error) { reads.Add(1); return size.Load(), nil }
	relief := &reliefFake{}
	quota, fake, clock := reliefSetup(t, relief)

	size.Store(2000)
	for _, rev := range []int64{50, 80, 90} { // ticks at 0 s, 30 s and 60 s
		fake.rev.Store(rev)
		if rev == 90 {
			relief.rows = 5
		}
		quota.tick(ctx, live, nil)
		*clock = clock.Add(30 * time.Second)
	}
	relief.rows = 0
	size.Store(10)
	fake.rev.Store(95)
	*clock = clock.Add(time.Hour) // the deletes settled long ago
	relief.refreshed = true
	quota.tick(ctx, live, nil) // below the limit: only the owed refresh
	if !slices.Equal(relief.targets, []int64{50}) || relief.refreshes != 1 {
		t.Fatalf("compaction targets %v and %d refreshes; want [50] (the revision of the tick 60 s back) and one owed refresh below the limit", relief.targets, relief.refreshes)
	}
	if reads.Load() != 6 {
		t.Fatalf("%d live reads, want 6 (four ticks, one after the compaction, one after the refresh)", reads.Load())
	}
	quota.tick(ctx, live, nil)
	if relief.refreshes != 1 || len(relief.targets) != 1 {
		t.Fatalf("a tick below the limit with nothing owed ran a pass: %v", relief.calls)
	}
	if len(fake.updates) != 0 {
		t.Fatalf("head marker written while revisions came in: %v", fake.updates)
	}
}

// TestKubehzP2ReliefHead: when no revision came during the whole lag, the
// newest row cannot be compacted; the relief writes the health key once so
// the head moves. The pass reaches the row before the head exactly.
func TestKubehzP2ReliefHead(t *testing.T) {
	ctx := context.Background()
	live := func(context.Context) (int64, error) { return 2000, nil }
	quota, fake, clock := reliefSetup(t, &reliefFake{})
	fake.rev.Store(100)
	quota.tick(ctx, live, nil)
	*clock = clock.Add(time.Minute)
	quota.tick(ctx, live, nil)
	if len(fake.updates) != 1 || fake.updates[0] != HealthKey {
		t.Fatalf("updates %v, want one write of %s", fake.updates, HealthKey)
	}
}

// TestKubehzP2ReliefAfterFailedSample: a failed sample does not stop the
// relief, and the full gauge follows the figure even then (the written bytes
// alone filled the quota).
func TestKubehzP2ReliefAfterFailedSample(t *testing.T) {
	ctx := context.Background()
	relief := &reliefFake{}
	quota, fake, clock := reliefSetup(t, relief)
	b := WithQuota(fake, quota)
	if _, err := b.Create(ctx, "/k", bytesOf(1200), 0); err != nil {
		t.Fatal(err)
	}
	failed := func(context.Context) (int64, error) { return 0, errors.New("connection refused") }
	fake.rev.Store(10)
	quota.tick(ctx, failed, nil)
	*clock = clock.Add(time.Minute)
	fake.rev.Store(20)
	quota.tick(ctx, failed, nil)
	if len(relief.targets) != 1 {
		t.Fatalf("relief targets %v, want one pass after the failed samples", relief.targets)
	}
	if !quota.full.Load() {
		t.Fatal("the full state did not follow the figure after a failed sample")
	}
}

// ticksEvery30s runs n ticks 30 s apart, as the sampler's ticker does, also
// when a pass inside a tick took time: the next tick does not move. The
// current revision of tick i is 10(i+1); before, when set, runs before tick i.
func ticksEvery30s(ctx context.Context, quota *Quota, fake *revFake, clock *time.Time, live SizeSource, n int, before func(i int)) {
	start := *clock
	for i := range n {
		*clock = start.Add(time.Duration(i) * 30 * time.Second)
		fake.rev.Store(int64(10 * (i + 1)))
		if before != nil {
			before(i)
		}
		quota.tick(ctx, live, nil)
	}
}

// TestKubehzP2ReliefPasses: one pass per tick, a refresh or a compaction,
// never both. Each pass takes 20 s and the ticks come every 30 s, so the
// tick after a deleting pass comes 10 s after its end: it waits (no pass, so
// the deletes settle), the next refreshes, the one after compacts again. A
// failed refresh stays owed and runs on the next tick.
func TestKubehzP2ReliefPasses(t *testing.T) {
	ctx := context.Background()
	live := func(context.Context) (int64, error) { return 2000, nil }
	relief := &reliefFake{rows: 5, refreshed: true}
	quota, fake, clock := reliefSetup(t, relief)
	relief.onPass = func() { *clock = clock.Add(20 * time.Second) }
	ticksEvery30s(ctx, quota, fake, clock, live, 11, func(i int) {
		relief.refreshed = i != 7 // the refresh of tick 7 fails
	})
	// tick 0, 1: no revision is the lag old; 2: compact; 3: quiet;
	// 4: refresh; 5: compact; 6: quiet; 7: refresh fails; 8: refresh;
	// 9: compact; 10: quiet.
	want := []string{"compact", "refresh", "compact", "refresh", "refresh", "compact"}
	if !slices.Equal(relief.calls, want) {
		t.Fatalf("passes %v, want %v (a quiet tick after each deleting pass; a failed refresh tried again)", relief.calls, want)
	}
}

// TestKubehzP2ReliefNoStarvation: a deleting pass on every tick would put the
// refresh off for as long as there is history to delete. The quiet tick
// after a deleting pass makes sure the refresh comes two ticks later.
func TestKubehzP2ReliefNoStarvation(t *testing.T) {
	ctx := context.Background()
	live := func(context.Context) (int64, error) { return 2000, nil }
	relief := &reliefFake{rows: 5, refreshed: true}
	quota, fake, clock := reliefSetup(t, relief)
	relief.onPass = func() { *clock = clock.Add(20 * time.Second) }
	ticksEvery30s(ctx, quota, fake, clock, live, 14, nil)
	want := []string{"compact", "refresh", "compact", "refresh", "compact", "refresh", "compact", "refresh"}
	if !slices.Equal(relief.calls, want) {
		t.Fatalf("passes %v, want %v (compaction and refresh take turns)", relief.calls, want)
	}
}

// TestKubehzP2ReliefSettleFromPassEnd: the settle wait counts from the end of
// the deleting pass. A pass that took 20 s and a tick 30 s after its start
// leave 10 s, too early for the refresh.
func TestKubehzP2ReliefSettleFromPassEnd(t *testing.T) {
	ctx := context.Background()
	live := func(context.Context) (int64, error) { return 2000, nil }
	relief := &reliefFake{rows: 5, refreshed: true}
	quota, fake, clock := reliefSetup(t, relief)
	fake.rev.Store(10)
	quota.tick(ctx, live, nil)
	*clock = clock.Add(time.Minute)
	relief.onPass = func() { *clock = clock.Add(20 * time.Second) }
	fake.rev.Store(20)
	quota.tick(ctx, live, nil) // deletes; ends 20 s later
	relief.onPass = nil
	*clock = clock.Add(10 * time.Second)
	quota.tick(ctx, live, nil) // 10 s after the pass ended
	if relief.refreshes != 0 {
		t.Fatalf("passes %v: a refresh 10 s after the deleting pass ended", relief.calls)
	}
	*clock = clock.Add(5 * time.Second)
	quota.tick(ctx, live, nil) // 15 s after
	if relief.refreshes != 1 {
		t.Fatalf("passes %v: no refresh 15 s after the deleting pass ended", relief.calls)
	}
}

// TestKubehzP2ReliefHeadOnlyWhenItHelps: no head marker after a pass that
// failed or stopped short of the row before the head, and none when the
// head the pass read is newer than the cached current revision (a marker
// whose acknowledgement was lost already moved it). A marker only helps
// when the newest row is all that keeps the deleted rows back.
func TestKubehzP2ReliefHeadOnlyWhenItHelps(t *testing.T) {
	for _, tc := range []struct {
		name   string
		relief *reliefFake
	}{
		{"a failed pass", &reliefFake{err: errors.New("compact failed")}},
		{"a pass that stopped short", &reliefFake{short: true}},
		{"a head newer than the cache", &reliefFake{head: 101}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			live := func(context.Context) (int64, error) { return 2000, nil }
			quota, fake, clock := reliefSetup(t, tc.relief)
			fake.rev.Store(100)
			quota.tick(ctx, live, nil)
			*clock = clock.Add(time.Minute)
			quota.tick(ctx, live, nil)
			if len(tc.relief.targets) != 1 {
				t.Fatalf("relief targets %v, want one pass", tc.relief.targets)
			}
			if len(fake.updates) != 0 {
				t.Fatalf("head marker written after %s: %v", tc.name, fake.updates)
			}
		})
	}
}

// TestKubehzP2ReliefRefreshPause: after quotaRefreshTries failed refreshes in a
// row the refresh pauses for quotaRefreshPause (the compaction goes on), then
// tries again. A success in between starts the count again.
func TestKubehzP2ReliefRefreshPause(t *testing.T) {
	ctx := context.Background()
	live := func(context.Context) (int64, error) { return 2000, nil }
	relief := &reliefFake{rows: 5}
	quota, fake, clock := reliefSetup(t, relief)
	tick := func(refreshed bool) {
		relief.refreshed = refreshed
		fake.rev.Add(10)
		quota.tick(ctx, live, nil)
		*clock = clock.Add(time.Minute)
	}
	tick(false) // no revision is the lag old yet
	tick(false) // compacts: owed
	tick(false) // refresh fails (1)
	tick(false) // fails (2)
	tick(true)  // succeeds: the count starts again
	tick(false) // compacts: owed
	tick(false) // fails (1)
	tick(false) // fails (2)
	if quota.refreshPaused.After(*clock) {
		t.Fatalf("passes %v: paused after two failures in a row", relief.calls)
	}
	tick(false) // fails (3): pause
	tick(false) // paused: compacts instead
	*clock = clock.Add(quotaRefreshPause)
	tick(true) // after the pause
	want := []string{"compact", "refresh", "refresh", "refresh", "compact", "refresh", "refresh", "refresh", "compact", "refresh"}
	if !slices.Equal(relief.calls, want) {
		t.Fatalf("passes %v, want %v", relief.calls, want)
	}
}

// TestKubehzP2ReliefOwedWhileNotFull: an owed refresh runs after the quota is
// no longer full, it clears the debt and the figure is read again at once.
func TestKubehzP2ReliefOwedWhileNotFull(t *testing.T) {
	ctx := context.Background()
	var size, reads atomic.Int64
	size.Store(2000)
	live := func(context.Context) (int64, error) { reads.Add(1); return size.Load(), nil }
	relief := &reliefFake{rows: 5, refreshed: true}
	quota, fake, clock := reliefSetup(t, relief)
	fake.rev.Store(10)
	quota.tick(ctx, live, nil)
	*clock = clock.Add(time.Minute)
	fake.rev.Store(20)
	quota.tick(ctx, live, nil) // deletes rows
	relief.rows = 0
	size.Store(10)
	*clock = clock.Add(time.Minute)
	fake.rev.Store(30)
	before := reads.Load()
	quota.tick(ctx, live, nil) // not full: the refresh
	if relief.refreshes != 1 || reads.Load() != before+2 {
		t.Fatalf("passes %v, %d reads; want one refresh and the figure read again after it", relief.calls, reads.Load()-before)
	}
	*clock = clock.Add(time.Minute)
	quota.tick(ctx, live, nil)
	if relief.refreshes != 1 {
		t.Fatalf("passes %v: the debt was not cleared by the refresh", relief.calls)
	}
}

// TestKubehzP2ReliefStatsOwed: a statistics reset the source reports makes a
// refresh owed with no deletes, also while not full.
func TestKubehzP2ReliefStatsOwed(t *testing.T) {
	ctx := context.Background()
	live := func(context.Context) (int64, error) { return 10, nil }
	relief := &reliefFake{owed: true, refreshed: true}
	quota, fake, _ := reliefSetup(t, relief)
	fake.rev.Store(10)
	quota.tick(ctx, live, nil)
	if relief.refreshes != 1 || len(relief.targets) != 0 {
		t.Fatalf("passes %v; want one refresh for the reset", relief.calls)
	}
	quota.tick(ctx, live, nil)
	if relief.refreshes != 1 {
		t.Fatalf("passes %v; want no refresh once the reset is settled", relief.calls)
	}
}

// TestKubehzP2ReliefTimestampAfterRead: a revision read that stalls is dated
// when it returns, so the next tick does not compact that revision before it
// is the lag old.
func TestKubehzP2ReliefTimestampAfterRead(t *testing.T) {
	ctx := context.Background()
	live := func(context.Context) (int64, error) { return 2000, nil }
	relief := &reliefFake{}
	quota, fake, clock := reliefSetup(t, relief)
	fake.onRev = func() { *clock = clock.Add(90 * time.Second) } // the read stalls 90 s
	fake.rev.Store(10)
	quota.tick(ctx, live, nil)
	fake.onRev = nil
	*clock = clock.Add(time.Second)
	fake.rev.Store(20)
	quota.tick(ctx, live, nil)
	if len(relief.targets) != 0 {
		t.Fatalf("relief targets %v 1 s after a stalled read; want none, revision 10 is 1 s old", relief.targets)
	}
}

// TestKubehzP2ReliefBudgets: a compaction pass runs under 20 s, a refresh
// pass under 2 min (a VACUUM of a large table needs more than a compaction
// batch). A refresh that fails because the sampler stops is not counted.
func TestKubehzP2ReliefBudgets(t *testing.T) {
	ctx := context.Background()
	live := func(context.Context) (int64, error) { return 2000, nil }
	relief := &reliefFake{rows: 5, refreshed: true}
	quota, fake, clock := reliefSetup(t, relief)
	ticksEvery30s(ctx, quota, fake, clock, live, 4, nil) // 2: compact, 3: refresh (the pass took no time)
	if !slices.Equal(relief.calls, []string{"compact", "refresh"}) || len(relief.budgets) != 2 {
		t.Fatalf("passes %v with budgets %v; want a compaction and a refresh, each with a deadline", relief.calls, relief.budgets)
	}
	for i, want := range []time.Duration{20 * time.Second, 2 * time.Minute} {
		if got := relief.budgets[i]; got > want || got < want-5*time.Second {
			t.Errorf("%s pass had %s, want %s", relief.calls[i], got, want)
		}
	}

	// the sampler stops during a refresh: the refresh ends with it, no
	// failure counted, still owed
	cctx, cancel := context.WithCancel(ctx)
	relief.rows, relief.owed, relief.refreshed, relief.block = 0, true, false, true
	relief.onPass = cancel
	quota.tick(cctx, live, nil)
	if !relief.interrupted {
		t.Fatalf("passes %v; want the refresh to end when the sampler stops, not wait for its budget", relief.calls)
	}
	if quota.refreshFails != 0 || !quota.refreshOwed {
		t.Fatalf("a refresh cut off by the stop: %d failures counted, owed %v; want 0 and still owed", quota.refreshFails, quota.refreshOwed)
	}
}

// TestKubehzP2ReliefStaleOnError: a pass that failed after a commit that can
// have taken effect (stale, 0 rows confirmed) owes a refresh all the same,
// and the refresh waits for the settle from the end of that pass.
func TestKubehzP2ReliefStaleOnError(t *testing.T) {
	ctx := context.Background()
	live := func(context.Context) (int64, error) { return 2000, nil }
	relief := &reliefFake{uncertain: true, refreshed: true, err: errors.New("commit: connection reset")}
	quota, fake, clock := reliefSetup(t, relief)
	relief.onPass = func() { *clock = clock.Add(20 * time.Second) }
	ticksEvery30s(ctx, quota, fake, clock, live, 6, nil)
	// 2: compact (fails), 3: quiet, 4: refresh, 5: compact
	if want := []string{"compact", "refresh", "compact"}; !slices.Equal(relief.calls, want) {
		t.Fatalf("passes %v, want %v", relief.calls, want)
	}
}

// TestKubehzP2ReliefPauseExpiry: the settle also counts deletes made while
// the refresh paused. When the pause ends right after a deleting pass, the
// next tick waits for the settle instead of refreshing at once.
func TestKubehzP2ReliefPauseExpiry(t *testing.T) {
	ctx := context.Background()
	live := func(context.Context) (int64, error) { return 2000, nil }
	relief := &reliefFake{rows: 5, refreshed: true}
	quota, fake, clock := reliefSetup(t, relief)
	quota.refreshPaused = clock.Add(85 * time.Second) // ends after tick 2's pass (60-80 s), before tick 3 (90 s)
	relief.onPass = func() { *clock = clock.Add(20 * time.Second) }
	ticksEvery30s(ctx, quota, fake, clock, live, 5, nil)
	// 2: compact (paused), 3: quiet (10 s after the pass), 4: refresh
	if want := []string{"compact", "refresh"}; !slices.Equal(relief.calls, want) {
		t.Fatalf("passes %v, want %v", relief.calls, want)
	}
}

// TestKubehzP2ReliefNoStats: rows deleted on a backend that keeps no
// statistics (SQLite) owe no refresh: no refresh pass and no quiet tick.
func TestKubehzP2ReliefNoStats(t *testing.T) {
	ctx := context.Background()
	live := func(context.Context) (int64, error) { return 2000, nil }
	relief := &reliefFake{rows: 5, refreshed: true, noStats: true}
	quota, fake, clock := reliefSetup(t, relief)
	relief.onPass = func() { *clock = clock.Add(20 * time.Second) }
	ticksEvery30s(ctx, quota, fake, clock, live, 6, nil)
	if want := []string{"compact", "compact", "compact", "compact"}; !slices.Equal(relief.calls, want) {
		t.Fatalf("passes %v, want %v", relief.calls, want)
	}
}

// TestKubehzP2ReliefPausedCompacts: while the refresh pauses after failures,
// the compaction runs on every tick: no quiet ticks for a refresh that
// cannot run.
func TestKubehzP2ReliefPausedCompacts(t *testing.T) {
	ctx := context.Background()
	live := func(context.Context) (int64, error) { return 2000, nil }
	relief := &reliefFake{rows: 5}
	quota, fake, clock := reliefSetup(t, relief)
	quota.refreshPaused = clock.Add(time.Hour)
	relief.onPass = func() { *clock = clock.Add(20 * time.Second) }
	ticksEvery30s(ctx, quota, fake, clock, live, 6, nil)
	if want := []string{"compact", "compact", "compact", "compact"}; !slices.Equal(relief.calls, want) {
		t.Fatalf("passes %v, want %v", relief.calls, want)
	}
}
