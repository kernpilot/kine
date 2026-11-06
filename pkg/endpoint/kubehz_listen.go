package endpoint

// KUBEHZ-PATCH P5 — limits on the client listener. This whole file is ours;
// see kubehz/MERGE-GUIDE.md#p5.
//
// INTENT: three optional limits for a kine that clients it does not trust
//   can reach, for example through a TLS passthrough gateway that cannot
//   bound a handshake or count connections. --listen-handshake-timeout is
//   the time a new connection has for the TLS handshake and the HTTP/2
//   preface (gRPC's ConnectionTimeout; gRPC's default is 120 s).
//   --listen-max-connections is the most open client connections: a
//   connection over the limit is closed and counts as "limit" in
//   kine_tls_handshake_failures_total (P4).
//   --listen-max-pending-handshakes splits that budget. A connection whose
//   TLS handshake has not ended counts against it, and moves to
//   --listen-max-connections when the handshake ends. A connection over the
//   pending budget is closed before kine reads from it and counts as
//   "pending_limit". So clients that stall in their handshake cannot take
//   the slots of connections that completed one. A connection that ends its
//   handshake while --listen-max-connections connections are open is closed
//   then, and counts as "limit". Without a pending budget,
//   --listen-max-connections counts a connection from Accept, its handshake
//   included, and closes one over the limit before kine reads from it.
//
// INVARIANT: with all three at 0 (the defaults) kine builds the same gRPC
//   server and serves the same listener as upstream.
//
// INVARIANT: a connection counts against exactly one budget from Accept
//   until its first Close, also while its handshake runs, so slow handshakes
//   cannot get past the limits. The move after the handshake and Close are
//   atomic with each other: the state is swapped once, and only the budget it
//   held is freed. gRPC closes the accepted connection on every path: a
//   failed handshake, a failed transport setup and the end of the transport.
//
// INVARIANT: gRPC sets TCP_USER_TIMEOUT (the keepalive timeout) only on a
//   *net.TCPConn, and the wrapper of the limit hides that type. So the
//   wrapper sets the same value itself, and a dead peer is found as fast as
//   without the limit. Where gRPC sets nothing (not Linux), it sets nothing.

import (
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/k3s-io/kine/pkg/metrics"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/credentials"
)

// checkListenLimits refuses values the limits cannot use. They apply only
// to the gRPC server kine builds: the handshake timeout is an option of that
// server, the wrapper of the limit sets that server's keepalive timeout, and
// the pending budget ends at that server's TLS handshake, so it needs TLS.
func checkListenLimits(config Config) error {
	if config.ListenHandshakeTimeout < 0 {
		return fmt.Errorf("the listen handshake timeout is %s; it must be 0 (gRPC's default) or more", config.ListenHandshakeTimeout)
	}
	if config.ListenMaxConnections < 0 {
		return fmt.Errorf("the listen connection limit is %d; it must be 0 (no limit) or more", config.ListenMaxConnections)
	}
	if config.ListenMaxPendingHandshakes < 0 {
		return fmt.Errorf("the listen pending-handshake limit is %d; it must be 0 (off) or more", config.ListenMaxPendingHandshakes)
	}
	if config.GRPCServer != nil && (config.ListenHandshakeTimeout > 0 || config.ListenMaxConnections > 0 || config.ListenMaxPendingHandshakes > 0) {
		return errors.New("the listen handshake timeout and connection limits apply only to the gRPC server kine builds, not to Config.GRPCServer")
	}
	if config.ListenMaxPendingHandshakes > 0 && (config.ServerTLSConfig.CertFile == "" || config.ServerTLSConfig.KeyFile == "") {
		return errors.New("the listen pending-handshake limit needs TLS on the client listener (--cert-file and --key-file)")
	}
	return nil
}

// limitConnections returns l with at most limit open connections and, when
// pending is above 0, at most pending connections in their TLS handshake.
// Accept closes a connection over its budget at once and waits for the next
// one. userTimeout is the TCP_USER_TIMEOUT that gRPC would set. With both
// at 0 it returns l itself.
func limitConnections(l net.Listener, limit, pending int, userTimeout time.Duration) net.Listener {
	if limit <= 0 && pending <= 0 {
		return l
	}
	return &limitListener{Listener: l, limit: int64(max(limit, 0)), pending: int64(max(pending, 0)), userTimeout: userTimeout}
}

// The states of a limitConn: the budget it counts against.
const (
	// connCounted counts against --listen-max-connections.
	connCounted int32 = iota
	// connPending counts against --listen-max-pending-handshakes.
	connPending
	// connClosed counts against nothing.
	connClosed
)

// errConnectionLimit refuses a connection whose handshake ended while
// --listen-max-connections connections were open.
var errConnectionLimit = errors.New("too many client connections (--listen-max-connections)")

type limitListener struct {
	net.Listener
	// limit and pending are the budgets; 0 is no limit, and no pending
	// budget. open and handshaking are the connections counted against them.
	limit, pending    int64
	userTimeout       time.Duration
	open, handshaking atomic.Int64
}

func (l *limitListener) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		lc := &limitConn{Conn: c, l: l}
		count, budget, reason, flag := &l.open, l.limit, handshakeLimit, "--listen-max-connections"
		if l.pending > 0 {
			lc.state.Store(connPending)
			count, budget, reason, flag = &l.handshaking, l.pending, handshakePendingLimit, "--listen-max-pending-handshakes"
		}
		if count.Add(1) > budget {
			count.Add(-1)
			_ = c.Close()
			metrics.TLSHandshakeFailures.WithLabelValues(reason).Inc()
			logrus.Debugf("Closed a connection from %s: %d connections are open (%s)", c.RemoteAddr(), budget, flag)
			continue
		}
		if err := setUserTimeout(c, l.userTimeout); err != nil {
			// gRPC refuses a connection when this fails; so does the limit
			_ = lc.Close()
			logrus.Debugf("Closed a connection from %s: setting TCP_USER_TIMEOUT: %v", c.RemoteAddr(), err)
			continue
		}
		return lc, nil
	}
}

// limitConn frees the budget it counts against at its first Close.
type limitConn struct {
	net.Conn
	l     *limitListener
	state atomic.Int32
}

func (c *limitConn) Close() error {
	err := c.Conn.Close()
	switch c.state.Swap(connClosed) {
	case connCounted:
		c.l.open.Add(-1)
	case connPending:
		c.l.handshaking.Add(-1)
	}
	return err
}

// handshakeDone moves c from the pending budget to the connection limit. It
// returns errConnectionLimit when that limit is full, and net.ErrClosed when
// c was closed meanwhile; c then stays where it was, and the caller closes
// it.
func (c *limitConn) handshakeDone() error {
	l := c.l
	if n := l.open.Add(1); l.limit > 0 && n > l.limit {
		l.open.Add(-1)
		return errConnectionLimit
	}
	if !c.state.CompareAndSwap(connPending, connCounted) {
		l.open.Add(-1) // Close freed the pending slot
		return net.ErrClosed
	}
	l.handshaking.Add(-1)
	return nil
}

// admitHandshakes wraps the server credentials when there is a pending
// budget: a connection whose handshake succeeded moves to the connection
// limit, or is closed when that limit is full. A failed handshake returns
// unchanged; gRPC then closes the connection, which frees its pending slot.
type admitHandshakes struct {
	credentials.TransportCredentials
}

func (a admitHandshakes) ServerHandshake(raw net.Conn) (net.Conn, credentials.AuthInfo, error) {
	conn, info, err := a.TransportCredentials.ServerHandshake(raw)
	if err != nil {
		return conn, info, err
	}
	lc, ok := raw.(*limitConn)
	if !ok {
		return conn, info, nil
	}
	err = lc.handshakeDone()
	if err == nil {
		return conn, info, nil
	}
	if errors.Is(err, errConnectionLimit) {
		metrics.TLSHandshakeFailures.WithLabelValues(handshakeLimit).Inc()
		logrus.Debugf("Closed a connection from %s after its handshake: %d connections are open (--listen-max-connections)", raw.RemoteAddr(), lc.l.limit)
	}
	_ = conn.Close()
	return nil, nil, err
}

// Clone keeps the move on the copy.
func (a admitHandshakes) Clone() credentials.TransportCredentials {
	return admitHandshakes{TransportCredentials: a.TransportCredentials.Clone()}
}
