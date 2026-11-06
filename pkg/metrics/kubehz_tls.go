package metrics

// KUBEHZ-PATCH P4 (metrics) — this whole file is ours; see kubehz/MERGE-GUIDE.md#p4.
// The counter of failed TLS handshakes on the client listener. It is
// registered only when that listener serves TLS, so a kine without TLS
// exposes the same metric set as upstream.

import "github.com/prometheus/client_golang/prometheus"

var TLSHandshakeFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "kine_tls_handshake_failures_total",
	Help: "TLS handshakes on the client listener that failed, by reason: no_client_cert, unknown_authority, expired, bad_certificate, client_alert, not_tls, closed, timeout, other, limit (closed because --listen-max-connections connections were open) and pending_limit (closed before the handshake because --listen-max-pending-handshakes connections were in theirs).",
}, []string{"reason"})
