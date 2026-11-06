package endpoint

// KUBEHZ-PATCH P4 — failed TLS handshakes on the client listener. This whole
// file is ours; see kubehz/MERGE-GUIDE.md#p4.
//
// INTENT: every TLS handshake that fails on the gRPC client listener adds one
//   to kine_tls_handshake_failures_total{reason}. Upstream kine shows nothing
//   for it: gRPC closes the connection and logs the error only at its own
//   info level, which kine does not show. With --trusted-ca-file, a client
//   with no certificate, or with one from another CA, is then visible.
//
// INVARIANT: the wrapper changes nothing that is accepted or refused. It
//   calls the wrapped credentials and returns their results unchanged; it
//   only reads the error. P4 is an observation: without it, kine accepts
//   and refuses the same handshakes.
//
// INVARIANT: the reasons are a fixed set, so the label cannot grow with
//   what clients send. A reason comes from the error's type where crypto/tls
//   or crypto/x509 gives one, else from the text of a crypto/tls error. If a
//   Go release changes that text, the failure counts as "other";
//   TestKubehzP4Reasons does real handshakes and fails then.

import (
	cryptotls "crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"strings"
	"syscall"

	"github.com/k3s-io/kine/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/credentials"
)

// The reasons of kine_tls_handshake_failures_total.
const (
	// handshakeNoClientCert: the client sent no certificate, and the listener
	// requires one (--trusted-ca-file).
	handshakeNoClientCert = "no_client_cert"
	// handshakeUnknownAuthority: no trusted CA signed the client certificate.
	handshakeUnknownAuthority = "unknown_authority"
	// handshakeExpired: the client certificate expired or is not valid yet.
	handshakeExpired = "expired"
	// handshakeBadCertificate: the client certificate is not valid for
	// another reason, for example a wrong key usage, a bad signature or a
	// certificate that does not parse.
	handshakeBadCertificate = "bad_certificate"
	// handshakeClientAlert: the client stopped the handshake with an alert,
	// for example because it does not trust the server certificate.
	handshakeClientAlert = "client_alert"
	// handshakeNotTLS: the first bytes are not a TLS record, for example a
	// client that speaks plain HTTP/2.
	handshakeNotTLS = "not_tls"
	// handshakeClosed: the client closed or reset the connection during the
	// handshake, for example a TCP probe.
	handshakeClosed = "closed"
	// handshakeTimeout: the handshake did not end in time (gRPC's connection
	// timeout: --listen-handshake-timeout, P5, else 120 s).
	handshakeTimeout = "timeout"
	// handshakeOther: any other failure, also one after a complete TLS
	// handshake, such as gRPC's check for the h2 protocol (ALPN).
	handshakeOther = "other"
	// handshakeLimit: P5 closed the connection, because
	// --listen-max-connections connections were open: before its handshake,
	// or after it with --listen-max-pending-handshakes.
	handshakeLimit = "limit"
	// handshakePendingLimit: P5 closed the connection before its handshake,
	// because --listen-max-pending-handshakes connections were in theirs.
	handshakePendingLimit = "pending_limit"
)

var handshakeFailureReasons = []string{
	handshakeNoClientCert, handshakeUnknownAuthority, handshakeExpired, handshakeBadCertificate,
	handshakeClientAlert, handshakeNotTLS, handshakeClosed, handshakeTimeout, handshakeOther,
	handshakeLimit, handshakePendingLimit,
}

// countHandshakeFailures wraps creds so that each failed server handshake is
// counted, and registers the counter with reg when reg is not nil. Every
// reason starts at 0, so a rate has a value from the first scrape.
func countHandshakeFailures(creds credentials.TransportCredentials, reg prometheus.Registerer) credentials.TransportCredentials {
	if reg != nil {
		reg.MustRegister(metrics.TLSHandshakeFailures)
	}
	for _, reason := range handshakeFailureReasons {
		metrics.TLSHandshakeFailures.WithLabelValues(reason)
	}
	return handshakeCounter{TransportCredentials: creds}
}

// handshakeCounter is gRPC server transport credentials that count the
// failed server handshakes of the credentials they wrap.
type handshakeCounter struct {
	credentials.TransportCredentials
}

// ServerHandshake runs the wrapped handshake and returns its results
// unchanged. A failure adds one to its reason. A connection that the
// wrapped credentials handed off (ErrConnDispatched) is no failure.
func (c handshakeCounter) ServerHandshake(raw net.Conn) (net.Conn, credentials.AuthInfo, error) {
	conn, info, err := c.TransportCredentials.ServerHandshake(raw)
	if err != nil && !errors.Is(err, credentials.ErrConnDispatched) {
		reason := handshakeFailureReason(err)
		metrics.TLSHandshakeFailures.WithLabelValues(reason).Inc()
		logrus.Debugf("TLS handshake from %s failed (%s): %v", raw.RemoteAddr(), reason, err)
	}
	return conn, info, err
}

// Clone keeps the count on the copy.
func (c handshakeCounter) Clone() credentials.TransportCredentials {
	return handshakeCounter{TransportCredentials: c.TransportCredentials.Clone()}
}

// handshakeFailureReason maps a failed server handshake to one of
// handshakeFailureReasons. The typed checks come first; the text checks
// only see the crypto/tls errors that have no type.
func handshakeFailureReason(err error) string {
	var (
		op        *net.OpError
		unknownCA x509.UnknownAuthorityError
		invalid   x509.CertificateInvalidError
		verify    *cryptotls.CertificateVerificationError
		record    cryptotls.RecordHeaderError
		netErr    net.Error
	)
	msg := err.Error()
	switch {
	case errors.As(err, &op) && op.Op == "remote error":
		return handshakeClientAlert
	case errors.As(err, &unknownCA):
		return handshakeUnknownAuthority
	case errors.As(err, &invalid) && invalid.Reason == x509.Expired:
		return handshakeExpired
	case errors.As(err, &verify), errors.As(err, &invalid):
		return handshakeBadCertificate
	// crypto/tls gives these as plain text (handshake_server.go,
	// processCertsFromClient); TestKubehzP4Reasons pins them
	case strings.Contains(msg, "client didn't provide a certificate"):
		return handshakeNoClientCert
	case strings.Contains(msg, "client certificate"), strings.Contains(msg, "client sent certificate"):
		return handshakeBadCertificate
	case errors.As(err, &record):
		return handshakeNotTLS
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, net.ErrClosed),
		errors.Is(err, syscall.ECONNRESET), errors.Is(err, syscall.EPIPE):
		return handshakeClosed
	case errors.As(err, &netErr) && netErr.Timeout():
		return handshakeTimeout
	}
	return handshakeOther
}
