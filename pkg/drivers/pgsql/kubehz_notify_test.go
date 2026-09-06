package pgsql

// KUBEHZ-PATCH P1 tests. Hermetic: no PostgreSQL, no network. unit.yml lists
// these by name before running them, so a rebase that drops the file fails CI
// instead of passing on an empty -run.

import (
	"sync"
	"testing"
	"time"
)

func TestKubehzP1RecordIsMonotonic(t *testing.T) {
	var n revisionNotifier
	for _, step := range []struct {
		rev, want int64
	}{
		{5, 5},
		{3, 5},  // older revision never moves latest backwards
		{5, 5},  // same revision is a no-op
		{9, 9},  // newer wins
		{-1, 9}, // garbage never wins
		{10, 10},
	} {
		n.record(step.rev)
		if got := n.latest.Load(); got != step.want {
			t.Fatalf("after record(%d): latest = %d, want %d", step.rev, got, step.want)
		}
	}
}

func TestKubehzP1RecordConcurrent(t *testing.T) {
	var n revisionNotifier
	var wg sync.WaitGroup
	const writers, perWriter = 16, 1000
	for w := range writers {
		wg.Go(func() {
			for i := range perWriter {
				// Interleaved so every writer sees higher and lower values
				// than the current one.
				n.record(int64(i*writers + w + 1))
			}
		})
	}
	wg.Wait()
	if got, want := n.latest.Load(), int64(writers*perWriter); got != want {
		t.Fatalf("latest = %d, want the maximum %d", got, want)
	}
}

func TestKubehzP1ParseRevision(t *testing.T) {
	for payload, want := range map[string]int64{
		"1":                   1,
		"42":                  42,
		"9223372036854775807": 9223372036854775807,
		"0":                   0, // a revision is positive
		"-1":                  0,
		"":                    0,
		" 7":                  0,
		"7\n":                 0,
		"abc":                 0,
		"7abc":                0,
		"9223372036854775808": 0, // overflow
		"{\"rev\":7}":         0, // not our channel's shape
	} {
		rev, ok := parseRevision(payload)
		if ok != (want > 0) || rev != want {
			t.Errorf("parseRevision(%q) = (%d, %v), want (%d, %v)", payload, rev, ok, want, want > 0)
		}
	}
}

func TestKubehzP1ForwardRevisionNeverBlocks(t *testing.T) {
	revs := make(chan int64, 2)
	if !forwardRevision("11", revs) || !forwardRevision("12", revs) {
		t.Fatal("valid payloads must be queued while there is room")
	}
	if forwardRevision("bad", revs) {
		t.Fatal("a malformed payload must not be queued")
	}
	done := make(chan bool, 1)
	go func() { done <- forwardRevision("13", revs) }()
	select {
	case queued := <-done:
		if queued {
			t.Fatal("a full channel must drop, not queue")
		}
	case <-time.After(time.Second):
		t.Fatal("forwardRevision blocked on a full channel")
	}
	if got := []int64{<-revs, <-revs}; got[0] != 11 || got[1] != 12 {
		t.Fatalf("queued revisions = %v, want [11 12]", got)
	}
}

func TestKubehzP1ReconnectBackoff(t *testing.T) {
	var b reconnectBackoff
	want := []time.Duration{1, 2, 4, 8, 16, 30, 30}
	for i, w := range want {
		if got := b.delay(); got != w*time.Second {
			t.Fatalf("delay #%d = %s, want %s", i+1, got, w*time.Second)
		}
	}
	// A session that failed at once keeps the outage's delay.
	b.settle(50 * time.Millisecond)
	if got := b.delay(); got != reconnectCap {
		t.Fatalf("after a short session: delay = %s, want %s", got, reconnectCap)
	}
	// A session that stayed healthy for the cap starts over.
	b.settle(reconnectCap)
	if got := b.delay(); got != reconnectFloor {
		t.Fatalf("after a healthy session: delay = %s, want %s", got, reconnectFloor)
	}
}
