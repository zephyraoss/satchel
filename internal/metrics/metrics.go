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
		Help:    "Time spent in litestream sync at unmount.",
		Buckets: prometheus.ExponentialBuckets(0.1, 2, 10),
	})
	RestoreDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "satchel_restore_duration_seconds",
		Help:    "Time spent in litestream restore at mount.",
		Buckets: prometheus.ExponentialBuckets(0.1, 2, 10),
	})
)

func Handler() http.Handler {
	return promhttp.Handler()
}

var (
	WALBytes = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "satchel_wal_bytes",
		Help: "Bytes of WAL frames not yet checkpointed into the database (litestream checkpoints after staging), sampled on write.",
	}, []string{"volume"})
	BackpressureEvents = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "satchel_backpressure_events_total",
		Help: "Times writes were delayed because the WAL exceeded the configured limit.",
	}, []string{"volume"})
)
