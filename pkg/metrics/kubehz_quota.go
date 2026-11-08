package metrics

// KUBEHZ-PATCH P2 (metrics) — this whole file is ours; see kubehz/MERGE-GUIDE.md#p2.
// The metrics of the per-database size limit. They are registered only when
// the limit is on (--quota-bytes > 0), so a kine without a limit exposes the
// same metric set as upstream. QuotaMetrics lists them for the registration.

import "github.com/prometheus/client_golang/prometheus"

var (
	QuotaBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kine_quota_bytes",
		Help: "Configured limit on live data in bytes. 0 means no limit.",
	})

	LiveBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kine_live_bytes",
		Help: "Live data in bytes, as last sampled. With kine_quota_pending_bytes, the figure --quota-bytes compares.",
	})

	DBSizeBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kine_db_size_bytes",
		Help: "Physical database size in bytes, as last sampled. The figure the Status RPC reports; not compared to the limit.",
	})

	QuotaFull = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kine_quota_full",
		Help: "1 while the figure is at or above --quota-bytes: new keys are refused, the lanes still write.",
	})

	QuotaPendingBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kine_quota_pending_bytes",
		Help: "Bytes this process wrote that the figure counts on top of the last live sample, as of that sample.",
	})

	QuotaRefused = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kine_quota_refused_total",
		Help: "Puts refused with the no-space error, by reason: create, growth, rate, ceiling, read.",
	}, []string{"reason"})

	QuotaLaneWrites = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kine_quota_lane_writes_total",
		Help: "Puts let through while full, by lane: control (Leases, master leases, the compaction key) or cleanup (no-growth updates).",
	}, []string{"lane"})

	QuotaReliefPasses = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kine_quota_relief_passes_total",
		Help: "Compaction passes the quota ran while full.",
	})

	QuotaReliefRows = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kine_quota_relief_rows_total",
		Help: "Rows the quota's relief passes deleted.",
	})

	QuotaSampleErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kine_quota_sample_errors_total",
		Help: "Size samples that failed. The last good figure and the bytes written since stay in use.",
	})

	QuotaLastSample = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kine_quota_last_sample_timestamp_seconds",
		Help: "Unix time of the last good live sample.",
	})

	QuotaLastRelief = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kine_quota_last_relief_timestamp_seconds",
		Help: "Unix time of the last relief pass that ended without an error.",
	})
)

// QuotaMetrics are the collectors endpoint.Listen registers with a limit.
func QuotaMetrics() []prometheus.Collector {
	return []prometheus.Collector{
		QuotaBytes, LiveBytes, DBSizeBytes, QuotaFull, QuotaPendingBytes, QuotaRefused,
		QuotaLaneWrites, QuotaReliefPasses, QuotaReliefRows, QuotaSampleErrors,
		QuotaLastSample, QuotaLastRelief,
	}
}
