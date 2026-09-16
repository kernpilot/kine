package metrics

// KUBEHZ-PATCH P2 (metrics) — this whole file is ours; see kubehz/MERGE-GUIDE.md#p2.
// Two gauges for the per-database size limit. They are registered only when
// the limit is on (--quota-bytes > 0), so a kine without a limit exposes the
// same metric set as upstream.

import "github.com/prometheus/client_golang/prometheus"

var (
	QuotaBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kine_quota_bytes",
		Help: "Configured database size limit in bytes. 0 means no limit.",
	})

	DBSizeBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kine_db_size_bytes",
		Help: "Database size in bytes, as last sampled for the quota check.",
	})
)
