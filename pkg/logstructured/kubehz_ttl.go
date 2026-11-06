package logstructured

// KUBEHZ-PATCH P2 (TTL deadline) — this whole file is ours; see kubehz/MERGE-GUIDE.md#p2.
//
// With --quota-bytes > 0 the relief compacts with no --compact-min-retain
// floor (B481). A TTL delete reads the key, then inserts a delete row whose
// compare-and-swap is the unique (name, prev_revision) pair. If the delete
// pauses between the read and the insert while newer values are written and
// compacted away, the insert finds no conflicting row and removes the newest
// value. A timeout below server.QuotaReliefLag on the whole delete closes
// that gap: endpoint.Listen sets server.QuotaTTLDeleteTimeout before Start.

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

// Delete runs the backend's Delete under the timeout. LogStructured.Delete
// reports a failed insert as "not deleted" with no error. When the timeout
// ends the call, that result would make the TTL loop drop the key, so the
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
