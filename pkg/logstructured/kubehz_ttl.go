package logstructured

// KUBEHZ-PATCH P2 (TTL deadline) — this whole file is ours; see kubehz/MERGE-GUIDE.md#p2.
//
// With --quota-bytes > 0 the relief compacts with no --compact-min-retain
// floor (B481). A TTL delete inserts a delete row in one statement. The
// statement compares the key's newest row with the revision the TTL loop
// holds, and the unique (name, prev_revision) pair refuses a second writer
// (upstream ca3c2d5, v0.17.5). If the statement stalls after its snapshot
// while newer values are written and compacted away, the unique check finds
// no conflicting row and the insert removes the newest value. A timeout below
// server.QuotaReliefLag on the whole delete ends such a statement through its
// cancel: endpoint.Listen sets server.QuotaTTLDeleteTimeout before Start. Up
// to v0.17.0 the read and the insert were two statements, so a pause of the
// kine process between them was enough; since v0.17.5 only a stall inside
// the one statement is.

import (
	"context"
	"time"

	"github.com/k3s-io/kine/pkg/server"
	"github.com/k3s-io/kine/pkg/ttl"
)

// ttlRun is the TTL loop Start starts; a variable so the test can see the
// backend Start hands it. Never set outside tests.
var ttlRun = ttl.Run

// SetTTLDeleteTimeout gives every delete of the TTL loop the timeout d
// (server.TTLDeleteDeadline). Call it before Start, which starts the loop.
func (l *LogStructured) SetTTLDeleteTimeout(d time.Duration) {
	l.ttlDeleteTimeout = d
}

// ttlBackend is the backend Start hands to ttl.Run: l itself when no timeout
// is set, as upstream.
func (l *LogStructured) ttlBackend() server.Backend {
	if l.ttlDeleteTimeout <= 0 {
		return l
	}
	return ttlDeadlineBackend{Backend: l, timeout: l.ttlDeleteTimeout}
}

// ttlDeadlineBackend bounds Delete with a timeout. Every other method is the
// backend's own.
type ttlDeadlineBackend struct {
	server.Backend
	timeout time.Duration
}

// Delete runs the backend's Delete under the timeout. When the timeout ends
// the insert, LogStructured.Delete returns the context error, because its
// re-read runs on the same context. Should it ever report "not deleted" with
// no error after the deadline, the TTL loop would drop the key, so the
// context error is returned instead, and the loop tries again.
func (b ttlDeadlineBackend) Delete(ctx context.Context, key string, revision int64) (int64, *server.KeyValue, bool, error) {
	dctx, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()
	rev, kv, deleted, err := b.Backend.Delete(dctx, key, revision)
	if err == nil && !deleted && dctx.Err() != nil {
		err = dctx.Err()
	}
	return rev, kv, deleted, err
}
