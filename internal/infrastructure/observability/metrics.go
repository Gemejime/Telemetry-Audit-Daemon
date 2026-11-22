package observability

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds all Prometheus collectors for rag-telemetry-audit-daemon.
type Metrics struct {
	Registry                  *prometheus.Registry
	RecordsConsumedTotal      *prometheus.CounterVec
	ConsumerLagRecords        prometheus.Gauge
	HotPersistDurationSeconds prometheus.Histogram
	ColdFlushDurationSeconds  prometheus.Histogram
	CallerDroppedRecordsTotal prometheus.Counter
	WALSpilloverActive        prometheus.Gauge
	S3UploadErrorsTotal       *prometheus.CounterVec
	S3IntervalOverlapsTotal   prometheus.Counter
	MalformedRecordsTotal     prometheus.Counter
}

// NewMetrics registers all daemon Prometheus collectors into a dedicated registry.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()

	m := &Metrics{
		Registry: reg,
		RecordsConsumedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "audit_records_consumed_total",
			Help: "Total compliance audit records consumed from Kafka, partitioned by tenant_id.",
		}, []string{"tenant_id"}),
		ConsumerLagRecords: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "audit_consumer_lag_records",
			Help: "Current estimated Kafka consumer lag in records across assigned partitions.",
		}),
		HotPersistDurationSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "audit_hot_persist_duration_seconds",
			Help:    "Duration of ClickHouse RowBinary batch persistence operations in seconds.",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 1.5, 2.0, 5.0},
		}),
		ColdFlushDurationSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "audit_cold_flush_duration_seconds",
			Help:    "Duration of S3 WORM Parquet batch flush operations in seconds.",
			Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0},
		}),
		CallerDroppedRecordsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "audit_caller_dropped_records_total",
			Help: "Total records dropped at caller buffers.",
		}),
		WALSpilloverActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "audit_wal_spillover_active",
			Help: "Indicates whether local NVMe mmap_wal spillover currently holds un-drained records (1=active, 0=inactive).",
		}),
		S3UploadErrorsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "audit_s3_upload_errors_total",
			Help: "Total S3 WORM upload errors partitioned by HTTP/error status.",
		}, []string{"status"}),
		S3IntervalOverlapsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "audit_s3_interval_overlaps_total",
			Help: "Total overlapping [min_seq, max_seq] Parquet intervals detected across S3 WORM objects.",
		}),
		MalformedRecordsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "audit_malformed_records_total",
			Help: "Total malformed or poison-pill Kafka audit records skipped.",
		}),
	}

	reg.MustRegister(
		m.RecordsConsumedTotal,
		m.ConsumerLagRecords,
		m.HotPersistDurationSeconds,
		m.ColdFlushDurationSeconds,
		m.CallerDroppedRecordsTotal,
		m.WALSpilloverActive,
		m.S3UploadErrorsTotal,
		m.S3IntervalOverlapsTotal,
		m.MalformedRecordsTotal,
	)

	return m
}

// Handler returns an HTTP handler serving Prometheus metrics in text exposition format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}

// IncRecordsConsumed increments audit_records_consumed_total for tenantID.
func (m *Metrics) IncRecordsConsumed(tenantID string, count int) {
	if m == nil || count <= 0 {
		return
	}
	m.RecordsConsumedTotal.WithLabelValues(tenantID).Add(float64(count))
}

// SetConsumerLag sets audit_consumer_lag_records.
func (m *Metrics) SetConsumerLag(lag float64) {
	if m == nil {
		return
	}
	m.ConsumerLagRecords.Set(lag)
}

// ObserveHotPersistDuration implements clickhouse.MetricsRecorder.
func (m *Metrics) ObserveHotPersistDuration(d time.Duration) {
	if m == nil {
		return
	}
	m.HotPersistDurationSeconds.Observe(d.Seconds())
}

// ObserveColdFlushDuration records S3 WORM Parquet flush latency.
func (m *Metrics) ObserveColdFlushDuration(d time.Duration) {
	if m == nil {
		return
	}
	m.ColdFlushDurationSeconds.Observe(d.Seconds())
}

// SetWALSpilloverActive updates audit_wal_spillover_active to 1 or 0.
func (m *Metrics) SetWALSpilloverActive(active bool) {
	if m == nil {
		return
	}
	if active {
		m.WALSpilloverActive.Set(1)
	} else {
		m.WALSpilloverActive.Set(0)
	}
}

// IncS3UploadError increments audit_s3_upload_errors_total for status.
func (m *Metrics) IncS3UploadError(status string) {
	if m == nil {
		return
	}
	if status == "" {
		status = "unknown"
	}
	m.S3UploadErrorsTotal.WithLabelValues(status).Inc()
}

// IncS3IntervalOverlaps increments audit_s3_interval_overlaps_total.
func (m *Metrics) IncS3IntervalOverlaps(count int) {
	if m == nil || count <= 0 {
		return
	}
	m.S3IntervalOverlapsTotal.Add(float64(count))
}

// IncMalformedRecords increments audit_malformed_records_total.
func (m *Metrics) IncMalformedRecords(count int) {
	if m == nil || count <= 0 {
		return
	}
	m.MalformedRecordsTotal.Add(float64(count))
}
