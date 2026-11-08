package logstructured

// KUBEHZ-PATCH P2 test: the TTL delete timeout (kubehz_ttl.go). A fake log,
// no database. A TTL delete inserts its delete row in one statement, which
// compares the key's newest row with the revision the TTL loop holds
// (upstream ca3c2d5). The statement stalls after that compare and before its
// unique check. While it waits, the key gets two newer values and a
// compaction removes the row the insert would conflict with (the relief has
// no --compact-min-retain floor). The timeout must end the delete before the
// insert, report the context error so the TTL loop tries again, and leave the
// newest value. With no timeout the stalled insert passes its unique check
// and deletes the newest value, so this test fails.

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/server"
)

func TestKubehzP2TTLDeleteTimeout(t *testing.T) {
	const key = "/registry/events/default/a"
	log := newStallLog()
	l := New(log)
	l.SetTTLDeleteTimeout(100 * time.Millisecond)
	b := l.ttlBackend()

	read := log.put(key, "expired") // the revision the TTL loop holds
	type result struct {
		deleted bool
		err     error
	}
	done := make(chan result, 1)
	go func() {
		_, _, deleted, err := b.Delete(context.Background(), key, read)
		done <- result{deleted, err}
	}()
	select {
	case <-log.stalled:
	case <-time.After(5 * time.Second):
		t.Fatal("the delete did not reach its insert")
	}
	log.put(key, "newer")
	newest := log.put(key, "newest")
	log.compactTo(newest)

	var res result
	select {
	case res = <-done:
	case <-time.After(5 * time.Second):
		close(log.release) // no timeout: let the stalled insert go on
		res = <-done
	}
	if res.deleted || !errors.Is(res.err, context.DeadlineExceeded) {
		t.Fatalf("the stalled delete returned deleted=%v err=%v, want not deleted and %v (the TTL loop then tries again)",
			res.deleted, res.err, context.DeadlineExceeded)
	}
	assertNewest(t, l, key, newest)

	// The TTL loop tries again with the revision it holds: the compare fails
	// and nothing is deleted.
	if _, _, deleted, err := b.Delete(context.Background(), key, read); deleted || err != nil {
		t.Fatalf("the retry returned deleted=%v err=%v, want not deleted and no error", deleted, err)
	}
	assertNewest(t, l, key, newest)
}

// TestKubehzP2TTLStart: Start hands the TTL loop the backend with the
// timeout when one is set, and the backend itself (as upstream) when none is
// set (quota 0).
func TestKubehzP2TTLStart(t *testing.T) {
	seam := ttlRun
	t.Cleanup(func() { ttlRun = seam })
	for _, timeout := range []time.Duration{0, 100 * time.Millisecond} {
		t.Run(fmt.Sprintf("timeout %s", timeout), func(t *testing.T) {
			got := make(chan server.Backend, 1)
			ttlRun = func(_ context.Context, b server.Backend) { got <- b }
			l := New(newStallLog())
			if timeout > 0 {
				l.SetTTLDeleteTimeout(timeout)
			}
			if err := l.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			var b server.Backend
			select {
			case b = <-got:
			case <-time.After(5 * time.Second):
				t.Fatal("Start did not start the TTL loop")
			}
			switch b := b.(type) {
			case *LogStructured:
				if timeout > 0 || b != l {
					t.Fatalf("the TTL loop got the bare backend, want the timeout %s on l", timeout)
				}
			case ttlDeadlineBackend:
				if timeout == 0 || b.timeout != timeout || b.Backend != server.Backend(l) {
					t.Fatalf("the TTL loop got the timeout %s on %T, want %s on l", b.timeout, b.Backend, timeout)
				}
			default:
				t.Fatalf("the TTL loop got %T", b)
			}
		})
	}
}

func assertNewest(t *testing.T, l *LogStructured, key string, rev int64) {
	t.Helper()
	_, kv, err := l.Get(context.Background(), key, 0, false)
	if err != nil || kv == nil || kv.ModRevision != rev {
		t.Fatalf("Get %s: kv=%+v err=%v, want the value at revision %d", key, kv, err, rev)
	}
}

// stallLog is an in-memory Log. Append is upstream's insert statement: it
// writes nothing (revision 0, no error) unless the key's newest row is the
// event's previous revision (0 for no row), and the unique
// (name, prev_revision) pair of the SQL schema refuses a second writer. The
// first delete stalls between the compare (the statement's snapshot) and the
// unique check, and waits for its context or for release.
type stallLog struct {
	mu      sync.Mutex
	rows    []*server.Event
	rev     int64
	once    sync.Once
	stalled chan struct{}
	release chan struct{}
}

func newStallLog() *stallLog {
	return &stallLog{stalled: make(chan struct{}), release: make(chan struct{})}
}

// put writes a new value of key and returns its revision.
func (s *stallLog) put(key, value string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rev++
	prev := &server.KeyValue{}
	if last := s.latest(key); last != nil {
		prev = last.KV
	}
	s.rows = append(s.rows, &server.Event{
		Create: prev.ModRevision == 0,
		KV:     &server.KeyValue{Key: key, Value: []byte(value), ModRevision: s.rev, Lease: 60},
		PrevKV: &server.KeyValue{ModRevision: prev.ModRevision},
	})
	return s.rev
}

// compactTo removes every row at or below rev that a newer row of the same
// key replaced.
func (s *stallLog) compactTo(rev int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var kept []*server.Event
	for _, r := range s.rows {
		if r.KV.ModRevision <= rev && s.latest(r.KV.Key) != r {
			continue
		}
		kept = append(kept, r)
	}
	s.rows = kept
}

func (s *stallLog) latest(key string) *server.Event {
	var last *server.Event
	for _, r := range s.rows {
		if r.KV.Key == key && (last == nil || r.KV.ModRevision > last.KV.ModRevision) {
			last = r
		}
	}
	return last
}

func (s *stallLog) List(ctx context.Context, key, _ string, _, _ int64, includeDeletes, _ bool) (server.EventBatch, error) {
	if err := ctx.Err(); err != nil {
		return server.EventBatch{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	last := s.latest(key)
	if last == nil || (last.Delete && !includeDeletes) {
		return server.EventBatch{CurrentRev: s.rev}, nil
	}
	return server.EventBatch{CurrentRev: s.rev, Events: []*server.Event{last}}, nil
}

func (s *stallLog) Append(ctx context.Context, event *server.Event) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	newest := int64(0)
	if last := s.latest(event.KV.Key); last != nil {
		newest = last.KV.ModRevision
	}
	s.mu.Unlock()
	if newest != event.PrevKV.ModRevision {
		return 0, nil
	}
	if event.Delete {
		first := false
		s.once.Do(func() { first = true; close(s.stalled) })
		if first {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-s.release:
			}
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.rows {
		if r.KV.Key == event.KV.Key && r.PrevKV != nil && event.PrevKV != nil &&
			r.PrevKV.ModRevision == event.PrevKV.ModRevision {
			return 0, fmt.Errorf("UNIQUE constraint failed: kine.name, kine.prev_revision (%s, %d)", r.KV.Key, r.PrevKV.ModRevision)
		}
	}
	s.rev++
	s.rows = append(s.rows, &server.Event{
		Delete: event.Delete,
		KV:     &server.KeyValue{Key: event.KV.Key, ModRevision: s.rev},
		PrevKV: event.PrevKV,
	})
	return s.rev, nil
}

func (s *stallLog) CurrentRevision(context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rev, nil
}

func (s *stallLog) Start(context.Context) error                    { return nil }
func (s *stallLog) CompactRevision(context.Context) (int64, error) { return 0, nil }
func (s *stallLog) Count(context.Context, string, string, int64) (int64, int64, error) {
	return 0, 0, errors.New("stallLog: Count is not used")
}
func (s *stallLog) After(context.Context, string, string, int64, int64) (server.EventBatch, error) {
	return server.EventBatch{}, errors.New("stallLog: After is not used")
}
func (s *stallLog) ListStream(context.Context, string, string, int64, int64, bool, bool) server.ListResult {
	return server.ListResult{}
}
func (s *stallLog) Watch(context.Context) <-chan server.EventBatch { return nil }
func (s *stallLog) DbSize(context.Context) (int64, error)          { return 0, nil }
func (s *stallLog) Compact(context.Context, int64) (int64, error)  { return 0, nil }
