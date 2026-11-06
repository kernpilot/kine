//go:build linux

package endpoint

// KUBEHZ-PATCH P5 test: the wrapper of the connection limit sets the
// TCP_USER_TIMEOUT that gRPC cannot set on it.

import (
	"net"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// userTimeoutOf reads TCP_USER_TIMEOUT of a TCP connection, in ms.
func userTimeoutOf(t *testing.T, c net.Conn) int {
	t.Helper()
	tc, ok := c.(*net.TCPConn)
	if !ok {
		t.Fatalf("%T is no *net.TCPConn", c)
	}
	raw, err := tc.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var v int
	var gerr error
	if err := raw.Control(func(fd uintptr) {
		v, gerr = unix.GetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_USER_TIMEOUT)
	}); err != nil {
		t.Fatal(err)
	}
	if gerr != nil {
		t.Fatal(gerr)
	}
	return v
}

// TestKubehzP5UserTimeout: a connection the limit accepts has the
// TCP_USER_TIMEOUT gRPC gives its connections; a connection accepted
// without the wrapper has none until gRPC sets it.
func TestKubehzP5UserTimeout(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	// Accept fails instead of waiting for ever when the wrapper refuses
	if err := l.(*net.TCPListener).SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	accept := func(l net.Listener) net.Conn {
		t.Helper()
		client, err := net.Dial("tcp", l.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = client.Close() })
		c, err := l.Accept()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close() })
		return c
	}

	if got := userTimeoutOf(t, accept(l)); got != 0 {
		t.Fatalf("a plain accepted connection has TCP_USER_TIMEOUT %d ms; the test needs 0", got)
	}
	c := accept(limitConnections(l, 1, 20*time.Second))
	lc, ok := c.(*limitConn)
	if !ok {
		t.Fatalf("the limit accepted a %T, want *limitConn", c)
	}
	if got := userTimeoutOf(t, lc.Conn); got != 20000 {
		t.Fatalf("TCP_USER_TIMEOUT is %d ms, want 20000 (gRPC's keepalive timeout)", got)
	}
}
