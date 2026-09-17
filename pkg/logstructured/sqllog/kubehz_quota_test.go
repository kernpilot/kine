package sqllog

// KUBEHZ-PATCH P2 test: the live-bytes source reaches endpoint.Listen through
// LogStructured and SQLLog only if both forwarding methods survive a rebase.
// A dialect with LiveSize yields a source at the top of the chain; a dialect
// without yields nil, so the limit falls back to DbSize.

import (
	"context"
	"testing"

	"github.com/k3s-io/kine/pkg/logstructured"
	"github.com/k3s-io/kine/pkg/server"
)

type liveDialect struct {
	wakeDialect
}

func (d *liveDialect) LiveSize() server.SizeSource {
	return func(context.Context) (int64, error) { return 42, nil }
}

func TestKubehzP2LiveSizeChain(t *testing.T) {
	plain := logstructured.New(New(&wakeDialect{}, 0, 0, 0, 0, 0, 0))
	if src := server.LiveSizeOf(plain); src != nil {
		t.Fatal("a dialect without LiveSize yielded a source through the chain")
	}

	withLive := logstructured.New(New(&liveDialect{}, 0, 0, 0, 0, 0, 0))
	src := server.LiveSizeOf(withLive)
	if src == nil {
		t.Fatal("the dialect's LiveSize did not reach the top of the chain")
	}
	if n, err := src(context.Background()); err != nil || n != 42 {
		t.Fatalf("live source through the chain: %d, %v; want 42", n, err)
	}
}
