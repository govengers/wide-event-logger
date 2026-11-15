package prometheusadapter_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/govengers/wide-event-logger/postgresadapter"
	"github.com/govengers/wide-event-logger/postgresadapter/prometheusadapter"
)

func TestMetricsCountEveryReport(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := prometheusadapter.New(&prometheusadapter.Options{Registerer: registry, Namespace: "app"})
	require.NoError(t, err)
	failure := errors.New("down")

	metrics.EventQueued()
	metrics.EventQueued()
	metrics.EventDropped(postgresadapter.DropQueueFull)
	metrics.BatchWritten(&postgresadapter.Batch{Events: 2, Duration: time.Millisecond})
	metrics.BatchWritten(&postgresadapter.Batch{Events: 3, Duration: time.Millisecond, Err: failure})
	metrics.Maintained(&postgresadapter.Maintenance{Created: 4, Dropped: 1})
	metrics.DayArchived(&postgresadapter.Archive{Duration: time.Second, Err: failure})

	expected := `
# HELP app_wide_events_dropped_total Wide events lost: the queue was full, the emitter closed, or the write failed.
# TYPE app_wide_events_dropped_total counter
app_wide_events_dropped_total{reason="closed"} 0
app_wide_events_dropped_total{reason="queue_full"} 1
app_wide_events_dropped_total{reason="write_failed"} 3
# HELP app_wide_events_queued_total Wide events queued for the database.
# TYPE app_wide_events_queued_total counter
app_wide_events_queued_total 2
# HELP app_wide_events_written_total Wide events written to the database.
# TYPE app_wide_events_written_total counter
app_wide_events_written_total 2
# HELP app_wide_events_maintenance_total Maintain calls of the wide event table by outcome.
# TYPE app_wide_events_maintenance_total counter
app_wide_events_maintenance_total{outcome="failure"} 0
app_wide_events_maintenance_total{outcome="success"} 1
# HELP app_wide_events_partitions_created_total Day partitions of the wide event table created.
# TYPE app_wide_events_partitions_created_total counter
app_wide_events_partitions_created_total 4
# HELP app_wide_events_partitions_dropped_total Day partitions of the wide event table dropped after their retention.
# TYPE app_wide_events_partitions_dropped_total counter
app_wide_events_partitions_dropped_total 1
# HELP app_wide_events_archives_total Days of wide events handed to the archiver, by outcome.
# TYPE app_wide_events_archives_total counter
app_wide_events_archives_total{outcome="failure"} 1
app_wide_events_archives_total{outcome="success"} 0
`
	require.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(expected),
		"app_wide_events_dropped_total", "app_wide_events_queued_total", "app_wide_events_written_total",
		"app_wide_events_maintenance_total", "app_wide_events_partitions_created_total",
		"app_wide_events_partitions_dropped_total", "app_wide_events_archives_total"))
	require.Equal(t, 2, testutil.CollectAndCount(registry, "app_wide_events_flush_duration_seconds"), "one series per outcome")
	require.Equal(t, 1, testutil.CollectAndCount(registry, "app_wide_events_archive_duration_seconds"))
}

func TestNewValidatesAndLeavesNothingRegisteredOnFailure(t *testing.T) {
	_, err := prometheusadapter.New(nil)
	require.Error(t, err)
	_, err = prometheusadapter.New(&prometheusadapter.Options{Registerer: prometheus.NewRegistry()})
	require.Error(t, err)

	registry := prometheus.NewRegistry()
	registry.MustRegister(prometheus.NewCounter(prometheus.CounterOpts{Name: "app_wide_events_archives_total", Help: "taken"}))
	_, err = prometheusadapter.New(&prometheusadapter.Options{Registerer: registry, Namespace: "app"})
	require.Error(t, err)
	_, err = prometheusadapter.New(&prometheusadapter.Options{Registerer: registry, Namespace: "other"})
	require.NoError(t, err, "the failed registration did not keep other collectors")
	families, err := registry.Gather()
	require.NoError(t, err)
	for _, family := range families {
		require.False(t, strings.HasPrefix(family.GetName(), "app_wide_events_") && family.GetName() != "app_wide_events_archives_total", family.GetName())
	}
}
