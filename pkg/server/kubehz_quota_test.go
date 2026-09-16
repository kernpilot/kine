package server

// KUBEHZ-PATCH P2 tests: the size limit on a fake backend, no database.
// unit.yml lists these tests by name before running them, so a rebase that
// drops the wrapper or this file fails CI.

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errNotUsed = errors.New("not used by this test")

// quotaFake records which writes reached the backend. Every write succeeds,
// so a refused write can only have been refused by the quota wrapper.
type quotaFake struct {
	creates   []string
	updates   []string
	deletes   []string
	size      atomic.Int64
	sizeErr   atomic.Pointer[error]
	sizeReads atomic.Int64
}

func (f *quotaFake) Start(context.Context) error { return nil }

func (f *quotaFake) Get(context.Context, string, int64, bool) (int64, *KeyValue, error) {
	return 1, &KeyValue{ModRevision: 1}, nil
}

func (f *quotaFake) Create(_ context.Context, key string, _ []byte, _ int64) (int64, error) {
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

func (f *quotaFake) Update(_ context.Context, key string, _ []byte, _, _ int64) (int64, *KeyValue, bool, error) {
	f.updates = append(f.updates, key)
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

// TestKubehzP2Limit: below the limit every write reaches the backend; at and
// above it, every put (Put, the create and update transactions) gets
// ErrNoSpace and never reaches the backend, the compaction-bookkeeping
// transaction fails its compare instead of writing, while deletes, Compact
// and Range still reach the backend. That is etcd's capped applier.
func TestKubehzP2Limit(t *testing.T) {
	const limit = 1000
	ctx := context.Background()

	for _, tc := range []struct {
		name string
		size int64
		full bool
	}{
		{"below", limit - 1, false},
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
			_, err = l.Txn(ctx, putTxn("/c", 1))
			checkWrite(t, "Txn update", err, tc.full)
			// the bookkeeping put is refused too, but kine's compact path
			// swallows backend errors and answers a failed compare, so the
			// apiserver sees "someone else moved the key", not no-space
			compact, err := l.Txn(ctx, compactTxn())
			if err != nil {
				t.Fatalf("Txn compact bookkeeping: %v", err)
			}
			if compact.Succeeded == tc.full {
				t.Fatalf("Txn compact bookkeeping succeeded=%v with full=%v", compact.Succeeded, tc.full)
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
				wantCreates = nil
			}
			if strings.Join(fake.creates, ",") != strings.Join(wantCreates, ",") {
				t.Fatalf("creates that reached the backend: %v, want %v", fake.creates, wantCreates)
			}
			if updated := len(fake.updates) > 0; updated == tc.full {
				t.Fatalf("updates that reached the backend: %v (full=%v)", fake.updates, tc.full)
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
