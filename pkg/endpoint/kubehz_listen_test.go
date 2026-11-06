package endpoint

// KUBEHZ-PATCH P5 tests: the handshake timeout and the connection limit of
// the client listener, on a real kine on sqlite through Listen. unit.yml
// lists these tests by name before it runs them, so a rebase that drops
// this file fails CI.

import (
	"context"
	cryptotls "crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

// p5Address is the host:port of the kine that e describes.
func p5Address(e ETCDConfig) string {
	return strings.TrimPrefix(e.Endpoints[0], "https://")
}

// openFor dials address, sends first, then waits for the server to close the
// connection. It returns how long the connection stayed open, and false when
// it was still open after wait.
func openFor(t *testing.T, address string, first []byte, wait time.Duration) (time.Duration, bool) {
	t.Helper()
	raw, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = raw.Close() }()
	start := time.Now()
	if len(first) > 0 {
		if _, err := raw.Write(first); err != nil {
			t.Fatal(err)
		}
	}
	_ = raw.SetReadDeadline(start.Add(wait))
	_, err = raw.Read(make([]byte, 1))
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return wait, false
	}
	return time.Since(start), true
}

// p5Handshake does a TLS handshake with a valid client certificate and ALPN
// h2, as a gRPC client does, and sends nothing after it. The caller closes
// the connection it returns.
func p5Handshake(address string, pki *testPKI) (net.Conn, error) {
	raw, err := net.DialTimeout("tcp", address, 5*time.Second)
	if err != nil {
		return nil, err
	}
	c := cryptotls.Client(raw, &cryptotls.Config{
		RootCAs:      pool(pki.ca),
		Certificates: []cryptotls.Certificate{pki.client},
		ServerName:   "localhost",
		NextProtos:   []string{"h2"},
	})
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	if err := c.Handshake(); err != nil {
		_ = raw.Close()
		return nil, err
	}
	_ = c.SetDeadline(time.Time{})
	return c, nil
}

// waitCounted waits until the counter went up by exactly want since before.
func waitCounted(t *testing.T, before, want map[string]float64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		counted := countedSince(before)
		if fmt.Sprint(counted) == fmt.Sprint(want) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("counted %v, want %v", counted, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestKubehzP5HandshakeTimeout: a client that sends nothing, and one that
// starts a TLS record and never ends it, are closed after
// --listen-handshake-timeout and count as "timeout". With the default (0)
// gRPC's 120 s stay. A valid client works with the timeout set.
func TestKubehzP5HandshakeTimeout(t *testing.T) {
	pki := newTestPKI(t)
	const timeout = 300 * time.Millisecond
	partial := []byte{0x16, 0x03, 0x01, 0x02, 0x00} // a handshake record header for 512 bytes, then nothing

	def := p5Address(listenP4(t, pki.serverConfig(), nil))
	if took, closed := openFor(t, def, partial, time.Second); closed {
		t.Fatalf("with the default, kine closed an unfinished handshake after %s; gRPC's default is 120 s", took)
	}

	e := listenP4(t, pki.serverConfig(), nil, func(c *Config) { c.ListenHandshakeTimeout = timeout })
	for _, client := range []struct {
		name  string
		first []byte
	}{{"sends nothing", nil}, {"starts a TLS record and stops", partial}} {
		before := handshakeCounts()
		took, closed := openFor(t, p5Address(e), client.first, 5*time.Second)
		if !closed {
			t.Fatalf("a client that %s is still open after 5 s; the handshake timeout is %s", client.name, timeout)
		}
		if took < timeout-50*time.Millisecond {
			t.Fatalf("kine closed a client that %s after %s, before the handshake timeout of %s", client.name, took, timeout)
		}
		t.Logf("a client that %s: closed after %s", client.name, took.Truncate(time.Millisecond))
		waitCounted(t, before, map[string]float64{handshakeTimeout: 1})
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   e.Endpoints,
		DialTimeout: 10 * time.Second,
		TLS:         &cryptotls.Config{Certificates: []cryptotls.Certificate{pki.client}, RootCAs: pool(pki.ca)},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := client.Put(ctx, "/a", "v"); err != nil {
		t.Fatalf("Put with the handshake timeout set: %v", err)
	}
}

// TestKubehzP5MaxConnections: with --listen-max-connections 2, two etcd
// clients connect and connection 3 is closed before its handshake and counts
// as "limit"; the two clients stay up. A closed client frees its slot, and
// the slot of a connection still in its handshake counts.
func TestKubehzP5MaxConnections(t *testing.T) {
	pki := newTestPKI(t)
	e := listenP4(t, pki.serverConfig(), nil, func(c *Config) { c.ListenMaxConnections = 2 })
	address := p5Address(e)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	connect := func(key string) *clientv3.Client {
		t.Helper()
		c, err := clientv3.New(clientv3.Config{
			Endpoints:   e.Endpoints,
			DialTimeout: 10 * time.Second,
			TLS:         &cryptotls.Config{Certificates: []cryptotls.Certificate{pki.client}, RootCAs: pool(pki.ca)},
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close() })
		put, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if _, err := c.Put(put, key, "v"); err != nil {
			t.Fatalf("Put %s: %v", key, err)
		}
		return c
	}
	a, b := connect("/a"), connect("/b")

	before := handshakeCounts()
	if c, err := p5Handshake(address, pki); err == nil {
		_ = c.Close()
		t.Fatal("kine accepted connection 3 with the limit at 2")
	}
	waitCounted(t, before, map[string]float64{handshakeLimit: 1})
	for name, c := range map[string]*clientv3.Client{"/a": a, "/b": b} {
		if _, err := c.Get(ctx, name); err != nil {
			t.Fatalf("client %s after connection 3 was refused: %v", name, err)
		}
	}

	// the server sees the close of a a moment later
	_ = a.Close()
	var held net.Conn
	deadline := time.Now().Add(5 * time.Second)
	for held == nil {
		c, err := p5Handshake(address, pki)
		if err == nil {
			held = c
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no connection accepted 5 s after a client closed: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	defer func() { _ = held.Close() }()
	// b and held (in its HTTP/2 preface) are open: the next one is refused
	before = handshakeCounts()
	if c, err := p5Handshake(address, pki); err == nil {
		_ = c.Close()
		t.Fatal("kine accepted a third connection while one waits for its HTTP/2 preface")
	}
	waitCounted(t, before, map[string]float64{handshakeLimit: 1})
	if _, err := b.Get(ctx, "/b"); err != nil {
		t.Fatalf("client /b at the end: %v", err)
	}
}

// TestKubehzP5Checks: Listen refuses negative values and values it cannot
// apply to a caller's gRPC server, and the defaults leave the listener as it
// is.
func TestKubehzP5Checks(t *testing.T) {
	for name, config := range map[string]Config{
		"negative timeout":          {ListenHandshakeTimeout: -time.Second},
		"negative limit":            {ListenMaxConnections: -1},
		"timeout and a gRPC server": {ListenHandshakeTimeout: time.Second, GRPCServer: grpc.NewServer()},
		"limit and a gRPC server":   {ListenMaxConnections: 1, GRPCServer: grpc.NewServer()},
	} {
		if err := checkListenLimits(config); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	for name, config := range map[string]Config{
		"defaults":                   {},
		"both set":                   {ListenHandshakeTimeout: time.Second, ListenMaxConnections: 1},
		"defaults and a gRPC server": {GRPCServer: grpc.NewServer()},
	} {
		if err := checkListenLimits(config); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	// Listen checks first, before it opens the datastore
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	defer func() {
		cancel()
		wg.Wait()
	}()
	if _, err := Listen(ctx, Config{
		WaitGroup:            wg,
		Listener:             "tcp://127.0.0.1:0",
		Endpoint:             "sqlite://" + t.TempDir() + "/state.db",
		ListenMaxConnections: -1,
	}); err == nil || !strings.Contains(err.Error(), "connection limit") {
		t.Fatalf("Listen with a negative connection limit: %v", err)
	}

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	if got := limitConnections(l, 0, time.Second); got != l {
		t.Fatalf("limitConnections with no limit returned %T, want the listener itself", got)
	}
}
