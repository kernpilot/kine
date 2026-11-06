package endpoint

// KUBEHZ-PATCH P5 — limits on the client listener. This whole file is ours;
// see kubehz/MERGE-GUIDE.md#p5.
//
// INTENT: two optional limits for a kine that clients it does not trust can
//   reach, for example through a TLS passthrough gateway that cannot bound a
//   handshake or count connections. --listen-handshake-timeout is the time a
//   new connection has for the TLS handshake and the HTTP/2 preface (gRPC's
//   ConnectionTimeout; gRPC's default is 120 s). --listen-max-connections is
//   the most open client connections: a connection over the limit is closed
//   before kine reads from it, and counts as "limit" in
//   kine_tls_handshake_failures_total (P4).
//
// INVARIANT: with both at 0 (the defaults) kine builds the same gRPC server
//   and serves the same listener as upstream.
//
// INVARIANT: a connection counts from Accept until its first Close, also
//   while its handshake runs, so slow handshakes cannot get past the limit.
//   gRPC closes the accepted connection on every path: a failed handshake,
//   a failed transport setup and the end of the transport.
//
// INVARIANT: gRPC sets TCP_USER_TIMEOUT (the keepalive timeout) only on a
//   *net.TCPConn, and the wrapper of the limit hides that type. So the
//   wrapper sets the same value itself, and a dead peer is found as fast as
//   without the limit. Where gRPC sets nothing (not Linux), it sets nothing.

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/k3s-io/kine/pkg/metrics"
	"github.com/sirupsen/logrus"
)

// checkListenLimits refuses values the limits cannot use. Both apply only
// to the gRPC server kine builds: the handshake timeout is an option of that
// server, and the wrapper of the limit sets that server's keepalive timeout.
func checkListenLimits(config Config) error {
	if config.ListenHandshakeTimeout < 0 {
		return fmt.Errorf("the listen handshake timeout is %s; it must be 0 (gRPC's default) or more", config.ListenHandshakeTimeout)
	}
	if config.ListenMaxConnections < 0 {
		return fmt.Errorf("the listen connection limit is %d; it must be 0 (no limit) or more", config.ListenMaxConnections)
	}
	if config.GRPCServer != nil && (config.ListenHandshakeTimeout > 0 || config.ListenMaxConnections > 0) {
		return errors.New("the listen handshake timeout and connection limit apply only to the gRPC server kine builds, not to Config.GRPCServer")
	}
	return nil
}

// limitConnections returns l with at most limit open connections. Accept
// closes a connection over the limit at once and waits for the next one.
// userTimeout is the TCP_USER_TIMEOUT that gRPC would set. A limit of 0
// returns l itself.
func limitConnections(l net.Listener, limit int, userTimeout time.Duration) net.Listener {
	if limit <= 0 {
		return l
	}
	return &limitListener{Listener: l, limit: int64(limit), userTimeout: userTimeout}
}

type limitListener struct {
	net.Listener
	limit       int64
	userTimeout time.Duration
	open        atomic.Int64
}

func (l *limitListener) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		if l.open.Add(1) > l.limit {
			l.open.Add(-1)
			_ = c.Close()
			metrics.TLSHandshakeFailures.WithLabelValues(handshakeLimit).Inc()
			logrus.Debugf("Closed a connection from %s: %d client connections are open (--listen-max-connections)", c.RemoteAddr(), l.limit)
			continue
		}
		lc := &limitConn{Conn: c, release: func() { l.open.Add(-1) }}
		if err := setUserTimeout(c, l.userTimeout); err != nil {
			// gRPC refuses a connection when this fails; so does the limit
			_ = lc.Close()
			logrus.Debugf("Closed a connection from %s: setting TCP_USER_TIMEOUT: %v", c.RemoteAddr(), err)
			continue
		}
		return lc, nil
	}
}

// limitConn frees its slot of the limit at its first Close.
type limitConn struct {
	net.Conn
	once    sync.Once
	release func()
}

func (c *limitConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}
