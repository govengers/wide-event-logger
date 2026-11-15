// Package prometheusadapter exports the metrics of postgresadapter on a Prometheus registry the
// caller passes in (LIB-011). Metrics implements postgresadapter.Observer; give it to
// postgresadapter.Options.Observer.
package prometheusadapter

import (
	"errors"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/govengers/wide-event-logger/postgresadapter"
)

const subsystem = "wide_events"

// The values of the outcome label.
const (
	outcomeSuccess = "success"
	outcomeFailure = "failure"
)

// dropWriteFailed is the reason label of the events of a failed batch.
const dropWriteFailed = "write_failed"

// Options configures Metrics.
type Options struct {
	Registerer prometheus.Registerer // required; the caller's registry, never the default one
	Namespace  string                // required: the service name, e.g. "billing" for billing_wide_events_written_total
}

// Metrics counts what the emitter and the store did; it implements postgresadapter.Observer and is
// safe for concurrent use. Every label is a closed set.
//
//	<namespace>_wide_events_queued_total
//	<namespace>_wide_events_written_total
//	<namespace>_wide_events_dropped_total{reason="queue_full|closed|write_failed"}
//	<namespace>_wide_events_flush_duration_seconds{outcome}
//	<namespace>_wide_events_maintenance_total{outcome}
//	<namespace>_wide_events_partitions_created_total, _partitions_dropped_total
//	<namespace>_wide_events_archives_total{outcome}
//	<namespace>_wide_events_archive_duration_seconds
type Metrics struct {
	queued            prometheus.Counter
	written           prometheus.Counter
	dropped           *prometheus.CounterVec
	flushDurations    *prometheus.HistogramVec
	maintenance       *prometheus.CounterVec
	partitionsCreated prometheus.Counter
	partitionsDropped prometheus.Counter
	archives          *prometheus.CounterVec
	archiveDurations  prometheus.Histogram
}

// New registers the metrics on options.Registerer. A failed registration, a duplicate for
// example, is returned as an error and leaves nothing registered.
func New(options *Options) (*Metrics, error) {
	if options == nil || options.Registerer == nil {
		return nil, errors.New("prometheusadapter: Registerer is required")
	}
	if options.Namespace == "" {
		return nil, errors.New("prometheusadapter: Namespace is required")
	}
	metrics := newMetrics(options.Namespace)
	collectors := []prometheus.Collector{
		metrics.queued, metrics.written, metrics.dropped, metrics.flushDurations, metrics.maintenance,
		metrics.partitionsCreated, metrics.partitionsDropped, metrics.archives, metrics.archiveDurations,
	}
	for index, collector := range collectors {
		if err := options.Registerer.Register(collector); err != nil {
			for _, registered := range collectors[:index] {
				options.Registerer.Unregister(registered)
			}
			return nil, fmt.Errorf("prometheusadapter: register the %s_%s metrics: %v", options.Namespace, subsystem, err) //nolint:errorlint // LIB-010: client_golang errors are not part of the API
		}
	}
	return metrics, nil
}

func newMetrics(namespace string) *Metrics {
	counter := func(name, help string) prometheus.Counter {
		return prometheus.NewCounter(prometheus.CounterOpts{Namespace: namespace, Subsystem: subsystem, Name: name, Help: help})
	}
	counterVec := func(name, help, label string, values ...string) *prometheus.CounterVec {
		vector := prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: namespace, Subsystem: subsystem, Name: name, Help: help}, []string{label})
		for _, value := range values {
			vector.WithLabelValues(value) // the series exist at zero, so alerts see them
		}
		return vector
	}
	metrics := &Metrics{
		queued:  counter("queued_total", "Wide events queued for the database."),
		written: counter("written_total", "Wide events written to the database."),
		dropped: counterVec("dropped_total", "Wide events lost: the queue was full, the emitter closed, or the write failed.", "reason",
			string(postgresadapter.DropQueueFull), string(postgresadapter.DropClosed), dropWriteFailed),
		flushDurations: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: subsystem, Name: "flush_duration_seconds",
			Help: "Duration of the batch writes of wide events.", Buckets: prometheus.DefBuckets,
		}, []string{"outcome"}),
		maintenance:       counterVec("maintenance_total", "Maintain calls of the wide event table by outcome.", "outcome", outcomeSuccess, outcomeFailure),
		partitionsCreated: counter("partitions_created_total", "Day partitions of the wide event table created."),
		partitionsDropped: counter("partitions_dropped_total", "Day partitions of the wide event table dropped after their retention."),
		archives:          counterVec("archives_total", "Days of wide events handed to the archiver, by outcome.", "outcome", outcomeSuccess, outcomeFailure),
		archiveDurations: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: subsystem, Name: "archive_duration_seconds",
			Help: "Duration of archiving one day of wide events.", Buckets: prometheus.ExponentialBuckets(0.1, 3, 10),
		}),
	}
	return metrics
}

func outcomeOf(err error) string {
	if err != nil {
		return outcomeFailure
	}
	return outcomeSuccess
}

// EventQueued implements postgresadapter.Observer.
func (m *Metrics) EventQueued() { m.queued.Inc() }

// EventDropped implements postgresadapter.Observer.
func (m *Metrics) EventDropped(reason postgresadapter.DropReason) {
	m.dropped.WithLabelValues(string(reason)).Inc()
}

// BatchWritten implements postgresadapter.Observer: the events of a failed batch count as dropped.
func (m *Metrics) BatchWritten(batch *postgresadapter.Batch) {
	if batch.Err != nil {
		m.dropped.WithLabelValues(dropWriteFailed).Add(float64(batch.Events))
	} else {
		m.written.Add(float64(batch.Events))
	}
	m.flushDurations.WithLabelValues(outcomeOf(batch.Err)).Observe(batch.Duration.Seconds())
}

// Maintained implements postgresadapter.Observer.
func (m *Metrics) Maintained(maintenance *postgresadapter.Maintenance) {
	m.maintenance.WithLabelValues(outcomeOf(maintenance.Err)).Inc()
	m.partitionsCreated.Add(float64(maintenance.Created))
	m.partitionsDropped.Add(float64(maintenance.Dropped))
}

// DayArchived implements postgresadapter.Observer.
func (m *Metrics) DayArchived(archive *postgresadapter.Archive) {
	m.archives.WithLabelValues(outcomeOf(archive.Err)).Inc()
	m.archiveDurations.Observe(archive.Duration.Seconds())
}
