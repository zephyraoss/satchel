package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	MountedVolumes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "satchel_mounted_volumes",
		Help: "Number of volumes currently mounted on this node.",
	})
	UnpublishedBytes = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "satchel_unpublished_bytes",
		Help: "Bytes of changed blocks not yet published to S3, per volume.",
	}, []string{"volume"})
	LeaseHeld = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "satchel_lease_held",
		Help: "1 while this node holds the lease for the volume.",
	}, []string{"volume"})
	LeaseFenced = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "satchel_lease_fenced_total",
		Help: "Times this node lost a lease and fenced the volume.",
	}, []string{"volume"})
	MountFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "satchel_mount_failures_total",
		Help: "Mount attempts that failed, by reason.",
	}, []string{"reason"})
	SyncDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "satchel_sync_duration_seconds",
		Help:    "Time spent publishing and checkpointing at unmount.",
		Buckets: prometheus.ExponentialBuckets(0.1, 2, 10),
	})
	RestoreDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "satchel_restore_duration_seconds",
		Help:    "Time spent preparing a writable lazy image or materializing a read-only image at mount.",
		Buckets: prometheus.ExponentialBuckets(0.1, 2, 10),
	})
	ReplicationStageDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "satchel_replication_stage_duration_seconds",
		Help:    "Time spent in each replication stage.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
	}, []string{"stage"})
	ReplicationGenerations = promauto.NewCounter(prometheus.CounterOpts{
		Name: "satchel_replication_generations_total",
		Help: "Block generations encoded for replication.",
	})
	ReplicationInputBytes = promauto.NewCounter(prometheus.CounterOpts{
		Name: "satchel_replication_input_bytes_total",
		Help: "Uncompressed changed block bytes encoded for replication.",
	})
	ReplicationStoredBytes = promauto.NewCounter(prometheus.CounterOpts{
		Name: "satchel_replication_stored_bytes_total",
		Help: "Compressed segment bytes produced for replication.",
	})
	ReplicationSegments = promauto.NewCounter(prometheus.CounterOpts{
		Name: "satchel_replication_segments_total",
		Help: "Compressed segments produced for replication, including segments embedded in manifests.",
	})
)

func Handler() http.Handler {
	return promhttp.Handler()
}

var BackpressureEvents = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "satchel_backpressure_events_total",
	Help: "Times writes were delayed because unpublished block generations exceeded the configured limit.",
}, []string{"volume"})
