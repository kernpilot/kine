package metrics

// KUBEHZ-PATCH P2 (metrics) — this whole file is ours; see kubehz/MERGE-GUIDE.md#p2.
// Three gauges for the per-database size limit. They are registered only
// when the limit is on (--quota-bytes > 0), so a kine without a limit exposes
// the same metric set as upstream.

import "github.com/prometheus/client_golang/prometheus"

var (
	QuotaBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kine_quota_bytes",
		Help: "Configured limit on live data in bytes. 0 means no limit.",
	})

	LiveBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kine_live_bytes",
		Help: "Live data in bytes, as last sampled. This is the figure --quota-bytes compares.",
	})

	DBSizeBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kine_db_size_bytes",
		Help: "Physical database size in bytes, as last sampled. The figure the Status RPC reports; not compared to the limit.",
	})
)
