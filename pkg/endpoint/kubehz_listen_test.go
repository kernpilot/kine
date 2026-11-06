package endpoint

// KUBEHZ-PATCH P5 tests: the handshake timeout, the connection limit and
// the pending budget of the client listener, on a real kine on sqlite
// through Listen, and on the gRPC server and listener wrapper Listen builds
// where a test reads the counts of the wrapper. unit.yml lists these tests
// by name before it runs them, so a rebase that drops this file fails CI.

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

	kinetls "github.com/k3s-io/kine/pkg/tls"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
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

// TestKubehzP5Checks: Listen refuses negative values, values it cannot
// apply to a caller's gRPC server, and a pending budget without TLS. The
// defaults leave the listener as it is, and a copy of the credentials keeps
// the move after the handshake.
func TestKubehzP5Checks(t *testing.T) {
	withTLS := kinetls.Config{CertFile: "server.crt", KeyFile: "server.key"}
	for name, config := range map[string]Config{
		"negative timeout":                 {ListenHandshakeTimeout: -time.Second},
		"negative limit":                   {ListenMaxConnections: -1},
		"negative pending budget":          {ListenMaxPendingHandshakes: -1, ServerTLSConfig: withTLS},
		"timeout and a gRPC server":        {ListenHandshakeTimeout: time.Second, GRPCServer: grpc.NewServer()},
		"limit and a gRPC server":          {ListenMaxConnections: 1, GRPCServer: grpc.NewServer()},
		"pending budget and a gRPC server": {ListenMaxPendingHandshakes: 1, ServerTLSConfig: withTLS, GRPCServer: grpc.NewServer()},
		"pending budget without TLS":       {ListenMaxPendingHandshakes: 1},
	} {
		if err := checkListenLimits(config); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	for name, config := range map[string]Config{
		"defaults":                   {},
		"timeout and limit":          {ListenHandshakeTimeout: time.Second, ListenMaxConnections: 1},
		"all three with TLS":         {ListenHandshakeTimeout: time.Second, ListenMaxConnections: 1, ListenMaxPendingHandshakes: 1, ServerTLSConfig: withTLS},
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
	if got := limitConnections(l, 0, 0, time.Second); got != l {
		t.Fatalf("limitConnections with no limit returned %T, want the listener itself", got)
	}
	if got, ok := limitConnections(l, 0, 1, time.Second).(*limitListener); !ok || got.limit != 0 || got.pending != 1 {
		t.Fatalf("limitConnections with a pending budget only returned %#v", got)
	}
	if _, ok := (admitHandshakes{TransportCredentials: credentials.NewTLS(&cryptotls.Config{})}).Clone().(admitHandshakes); !ok {
		t.Fatal("a copy of the credentials does not move a connection after its handshake")
	}
}

// closedWithin reads from c until the server closes it. It returns how long
// that took from start, and false when c was still open after wait.
func closedWithin(c net.Conn, start time.Time, wait time.Duration) (time.Duration, bool) {
	_ = c.SetReadDeadline(time.Now().Add(wait))
	buf := make([]byte, 512)
	for {
		if _, err := c.Read(buf); err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				return wait, false
			}
			return time.Since(start), true
		}
	}
}

// stalled dials address and sends nothing, as a client that stalls in its
// TLS handshake.
func stalled(t *testing.T, address string) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// serveP5 serves gRPC's health service through the gRPC server and the
// listener wrapper that Listen builds, with the server TLS of pki. It
// returns the wrapper, to read its counts, and the address.
func serveP5(t *testing.T, pki *testPKI, options ...func(*Config)) (*limitListener, string) {
	t.Helper()
	config := Config{ServerTLSConfig: pki.serverConfig()}
	for _, option := range options {
		option(&config)
	}
	if err := checkListenLimits(config); err != nil {
		t.Fatal(err)
	}
	s, err := grpcServer(config)
	if err != nil {
		t.Fatal(err)
	}
	healthpb.RegisterHealthServer(s, health.NewServer())
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	l, ok := limitConnections(raw, config.ListenMaxConnections, config.ListenMaxPendingHandshakes, embed.DefaultGRPCKeepAliveTimeout).(*limitListener)
	if !ok {
		t.Fatal("serveP5 needs a limit or a pending budget")
	}
	go func() { _ = s.Serve(l) }()
	t.Cleanup(s.Stop)
	return l, raw.Addr().String()
}

// waitSlots waits until l counts open connections against the connection
// limit and handshaking ones against the pending budget.
func waitSlots(t *testing.T, when string, l *limitListener, open, handshaking int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for l.open.Load() != open || l.handshaking.Load() != handshaking {
		if time.Now().After(deadline) {
			t.Fatalf("%s: %d connections count against the limit and %d against the pending budget, want %d and %d", when, l.open.Load(), l.handshaking.Load(), open, handshaking)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// p5Client is a gRPC client of address with a valid client certificate.
func p5Client(t *testing.T, address string, pki *testPKI) *grpc.ClientConn {
	t.Helper()
	cc, err := grpc.NewClient("passthrough:///"+address, grpc.WithTransportCredentials(credentials.NewTLS(&cryptotls.Config{
		Certificates: []cryptotls.Certificate{pki.client},
		RootCAs:      pool(pki.ca),
		ServerName:   "localhost",
	})))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	return cc
}

// TestKubehzP5PendingBudget: with --listen-max-connections 1 and
// --listen-max-pending-handshakes 2, a client that stalls in its handshake
// does not take the one slot of the limit: an etcd client connects beside
// it. The etcd client frees its pending slot when its handshake ends, so a
// second stalled client gets in, and a third is closed before its handshake
// and counts as "pending_limit". The stalled clients stay open and the etcd
// client keeps working.
func TestKubehzP5PendingBudget(t *testing.T) {
	pki := newTestPKI(t)
	e := listenP4(t, pki.serverConfig(), nil, func(c *Config) { c.ListenMaxConnections, c.ListenMaxPendingHandshakes = 1, 2 })
	address := p5Address(e)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	before := handshakeCounts()

	s1 := stalled(t, address)
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   e.Endpoints,
		DialTimeout: 10 * time.Second,
		TLS:         &cryptotls.Config{Certificates: []cryptotls.Certificate{pki.client}, RootCAs: pool(pki.ca)},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	put, cancelPut := context.WithTimeout(ctx, 5*time.Second)
	defer cancelPut()
	if _, err := client.Put(put, "/a", "v"); err != nil {
		t.Fatalf("Put while a client stalls in its handshake: %v", err)
	}

	s2 := stalled(t, address)
	s3 := stalled(t, address)
	if _, closed := closedWithin(s3, time.Now(), 5*time.Second); !closed {
		t.Fatal("kine kept a third client in its handshake with the pending budget at 2")
	}
	waitCounted(t, before, map[string]float64{handshakePendingLimit: 1})
	for name, c := range map[string]net.Conn{"the first": s1, "the second": s2} {
		if _, closed := closedWithin(c, time.Now(), 300*time.Millisecond); closed {
			t.Fatalf("kine closed %s stalled client; gRPC's handshake timeout is 120 s", name)
		}
	}
	if _, err := client.Get(ctx, "/a"); err != nil {
		t.Fatalf("Get after the pending budget refused a client: %v", err)
	}
}

// TestKubehzP5LimitAfterHandshake: with --listen-max-connections 1 and a
// pending budget, a client that starts its handshake while the limit has
// room, and ends it after an etcd client took the slot, is closed after its
// handshake and counts as "limit". The etcd client keeps working.
func TestKubehzP5LimitAfterHandshake(t *testing.T) {
	pki := newTestPKI(t)
	e := listenP4(t, pki.serverConfig(), nil, func(c *Config) { c.ListenMaxConnections, c.ListenMaxPendingHandshakes = 1, 2 })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	before := handshakeCounts()

	late := stalled(t, p5Address(e))
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   e.Endpoints,
		DialTimeout: 10 * time.Second,
		TLS:         &cryptotls.Config{Certificates: []cryptotls.Certificate{pki.client}, RootCAs: pool(pki.ca)},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	if _, err := client.Put(ctx, "/a", "v"); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	c := cryptotls.Client(late, &cryptotls.Config{
		RootCAs:      pool(pki.ca),
		Certificates: []cryptotls.Certificate{pki.client},
		ServerName:   "localhost",
		NextProtos:   []string{"h2"},
	})
	_ = c.SetDeadline(start.Add(5 * time.Second))
	if err := c.Handshake(); err != nil {
		t.Fatalf("the TLS handshake itself must succeed: %v", err)
	}
	if _, closed := closedWithin(c, start, 5*time.Second); !closed {
		t.Fatal("kine kept a second connection after its handshake with the limit at 1")
	}
	waitCounted(t, before, map[string]float64{handshakeLimit: 1})
	if _, err := client.Get(ctx, "/a"); err != nil {
		t.Fatalf("Get after a connection was refused after its handshake: %v", err)
	}
}

// TestKubehzP5SlotRecovery: with one slot in each budget and a timeout of
// 300 ms, every kind of failed handshake frees its pending slot. A client
// that ends its TLS handshake and never sends its HTTP/2 preface moves to
// the limit, is closed when the timeout ends (gRPC's ConnectionTimeout also
// bounds the HTTP/2 setup), and frees that slot. A valid client then
// connects, holds one slot of the limit and none of the pending budget, and
// frees it at its close.
func TestKubehzP5SlotRecovery(t *testing.T) {
	pki := newTestPKI(t)
	const timeout = 300 * time.Millisecond
	l, address := serveP5(t, pki, func(c *Config) {
		c.ListenHandshakeTimeout, c.ListenMaxConnections, c.ListenMaxPendingHandshakes = timeout, 1, 1
	})
	before := handshakeCounts()
	partial := []byte{0x16, 0x03, 0x01, 0x02, 0x00}
	for _, failure := range []struct {
		name   string
		client func(net.Conn) error
	}{
		{"no client certificate", tlsProbe(cryptotls.VersionTLS13, pool(pki.ca), nil, true, false)},
		{"a certificate of another CA", tlsProbe(cryptotls.VersionTLS12, pool(pki.ca), &pki.otherClient, true, false)},
		{"plain HTTP", func(c net.Conn) error { _, _ = c.Write([]byte("GET / HTTP/1.1\r\n\r\n")); return readOnce(c) }},
		{"sends nothing", readOnce},
		{"starts a TLS record and stops", func(c net.Conn) error { _, _ = c.Write(partial); return readOnce(c) }},
		{"closes at once", func(net.Conn) error { return nil }},
	} {
		raw, err := net.Dial("tcp", address)
		if err != nil {
			t.Fatal(err)
		}
		_ = failure.client(raw)
		_ = raw.Close()
		waitSlots(t, "after a client that "+failure.name, l, 0, 0)
	}
	failed := map[string]float64{handshakeNoClientCert: 1, handshakeUnknownAuthority: 1, handshakeNotTLS: 1, handshakeTimeout: 2, handshakeClosed: 1}
	waitCounted(t, before, failed)

	start := time.Now()
	c, err := p5Handshake(address, pki)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	waitSlots(t, "after a TLS handshake with no HTTP/2 preface", l, 1, 0)
	took, closed := closedWithin(c, start, 5*time.Second)
	if !closed {
		t.Fatalf("a client with no HTTP/2 preface is still open after 5 s; the handshake timeout is %s", timeout)
	}
	if took < timeout-50*time.Millisecond {
		t.Fatalf("kine closed a client with no HTTP/2 preface after %s, before the timeout of %s", took, timeout)
	}
	t.Logf("a client with no HTTP/2 preface: closed after %s", took.Truncate(time.Millisecond))
	waitSlots(t, "after the HTTP/2 setup timed out", l, 0, 0)

	cc := p5Client(t, address, pki)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := healthpb.NewHealthClient(cc).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatalf("a valid client after the failures: %v", err)
	}
	waitSlots(t, "with a valid client", l, 1, 0)
	_ = cc.Close()
	waitSlots(t, "after the valid client closed", l, 0, 0)
	// the HTTP/2 timeout is no failed TLS handshake
	waitCounted(t, before, failed)
}

// TestKubehzP5WatchOutlivesTimeout: the handshake timeout bounds only the
// setup. A watch on a connection that went through both limits gets an
// event after five timeout periods, on the same stream: a closed transport
// would end the stream, which a raw gRPC stream does not hide (an etcd
// client would watch again on a new connection).
func TestKubehzP5WatchOutlivesTimeout(t *testing.T) {
	pki := newTestPKI(t)
	const timeout = 300 * time.Millisecond
	e := listenP4(t, pki.serverConfig(), nil, func(c *Config) {
		c.ListenHandshakeTimeout, c.ListenMaxConnections, c.ListenMaxPendingHandshakes = timeout, 1, 1
	})
	before := handshakeCounts()
	cc := p5Client(t, p5Address(e), pki)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	watch, err := etcdserverpb.NewWatchClient(cc).Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := watch.Send(&etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
		CreateRequest: &etcdserverpb.WatchCreateRequest{Key: []byte("/w")},
	}}); err != nil {
		t.Fatal(err)
	}
	if resp, err := watch.Recv(); err != nil || !resp.Created {
		t.Fatalf("watch create: %v, %v", resp, err)
	}
	time.Sleep(5 * timeout)
	if _, err := etcdserverpb.NewKVClient(cc).Put(ctx, &etcdserverpb.PutRequest{Key: []byte("/w"), Value: []byte("v")}); err != nil {
		t.Fatalf("Put after five timeout periods: %v", err)
	}
	for {
		resp, err := watch.Recv()
		if err != nil {
			t.Fatalf("the watch ended after five timeout periods: %v", err)
		}
		if len(resp.Events) > 0 {
			if string(resp.Events[0].Kv.Key) != "/w" {
				t.Fatalf("the watch got %q", resp.Events[0].Kv.Key)
			}
			break
		}
	}
	waitCounted(t, before, map[string]float64{})
}

// TestKubehzP5CloseOnce: a connection frees its budget exactly once: closed
// twice, by many goroutines, or while its handshake ends. The move after
// the handshake keeps the connection limit and frees the pending slot.
func TestKubehzP5CloseOnce(t *testing.T) {
	conn := func(l *limitListener, state int32) *limitConn {
		a, b := net.Pipe()
		t.Cleanup(func() { _ = b.Close() })
		c := &limitConn{Conn: a, l: l}
		c.state.Store(state)
		if state == connPending {
			l.handshaking.Add(1)
		} else {
			l.open.Add(1)
		}
		return c
	}
	check := func(when string, l *limitListener, open, handshaking int64) {
		t.Helper()
		if l.open.Load() != open || l.handshaking.Load() != handshaking {
			t.Fatalf("%s: %d connections count against the limit and %d against the pending budget, want %d and %d", when, l.open.Load(), l.handshaking.Load(), open, handshaking)
		}
	}

	l := &limitListener{limit: 1000, pending: 1000}
	for _, state := range []int32{connCounted, connPending} {
		c := conn(l, state)
		_ = c.Close()
		_ = c.Close()
		check(fmt.Sprintf("a connection in state %d closed twice", state), l, 0, 0)
	}
	for i := range 200 {
		c := conn(l, int32(i%2))
		var wg sync.WaitGroup
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = c.Close()
			}()
		}
		wg.Wait()
	}
	check("200 connections, each closed by 8 goroutines at once", l, 0, 0)
	for range 200 {
		c := conn(l, connPending)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = c.handshakeDone()
		}()
		go func() {
			defer wg.Done()
			_ = c.Close()
		}()
		wg.Wait()
	}
	check("200 connections, each moved and closed at once", l, 0, 0)

	c := conn(l, connPending)
	_ = c.Close()
	if err := c.handshakeDone(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("the move of a closed connection: %v, want %v", err, net.ErrClosed)
	}
	check("a move after Close", l, 0, 0)

	full := &limitListener{limit: 1, pending: 2}
	a := conn(full, connPending)
	if err := a.handshakeDone(); err != nil {
		t.Fatal(err)
	}
	check("one connection moved", full, 1, 0)
	b := conn(full, connPending)
	if err := b.handshakeDone(); !errors.Is(err, errConnectionLimit) {
		t.Fatalf("a move over the limit: %v, want %v", err, errConnectionLimit)
	}
	check("a move over the limit", full, 1, 1)
	_ = b.Close()
	_ = a.Close()
	check("both closed", full, 0, 0)
}
