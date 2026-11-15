package postgresadapter_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	wideevent "github.com/govengers/wide-event-logger"
	"github.com/govengers/wide-event-logger/postgresadapter"
)

func emitEvent(emitter wideevent.Emitter, name string, decorate func(we *wideevent.Event)) {
	we := wideevent.Begin(emitter, name)
	if decorate != nil {
		decorate(we)
	}
	we.Emit()
}

func TestEmitterWritesTheColumnsAndTheEvent(t *testing.T) {
	fixture := newFixture(t, nil)
	emitter := fixture.emitter(t, nil)

	emitEvent(emitter, "update_product", func(we *wideevent.Event) {
		we.String(wideevent.KeyRequestID, "request-1").String(wideevent.KeyUserID, "user-1").String(wideevent.KeyTraceID, "trace-1")
		we.Int(wideevent.KeyResponseStatus, 503).Failure(errors.New("conflict")).String("db.operation", "update")
	})
	require.NoError(t, emitter.Flush(t.Context()))

	page, err := fixture.store.Query(t.Context(), nil)
	require.NoError(t, err)
	require.Len(t, page.Records, 1)
	record := page.Records[0]
	require.Equal(t, fixture.clock.Now(), record.Time)
	require.Equal(t, "update_product", record.Name)
	require.Equal(t, wideevent.LevelError, record.Level)
	require.Equal(t, wideevent.OutcomeFailure, record.Outcome)
	require.Equal(t, 503, record.Status)
	require.Equal(t, "user-1", record.UserID)
	require.Equal(t, "request-1", record.RequestID)
	require.Equal(t, "trace-1", record.TraceID)
	require.Positive(t, record.Duration)
	var event map[string]any
	require.NoError(t, json.Unmarshal(record.Event, &event))
	require.Equal(t, map[string]any{"operation": "update"}, event["db"])
	require.Equal(t, "conflict", event["error"])
	require.Equal(t, "error", event["level"])
	require.Empty(t, page.Next)
}

func TestEmitterWritesAFullBatchAtOnce(t *testing.T) {
	fixture := newFixture(t, nil)
	emitter := fixture.emitter(t, &postgresadapter.EmitterOptions{BatchSize: 3, FlushInterval: time.Hour})

	for range 3 {
		emitEvent(emitter, "job", nil)
	}

	batch := fixture.recorder.nextBatch(t)
	require.NoError(t, batch.Err)
	require.Equal(t, 3, batch.Events)
	require.Equal(t, 3, fixture.countRows(t))
}

func TestEmitterWritesAPartialBatchAfterTheInterval(t *testing.T) {
	fixture := newFixture(t, nil)
	emitter := fixture.emitter(t, &postgresadapter.EmitterOptions{BatchSize: 100, FlushInterval: time.Millisecond})

	emitEvent(emitter, "job", nil)

	batch := fixture.recorder.nextBatch(t)
	require.NoError(t, batch.Err)
	require.Equal(t, 1, batch.Events)
}

func TestEmitterDropsWhenTheQueueIsFull(t *testing.T) {
	fixture := newFixture(t, nil)
	emitter := fixture.emitter(t, &postgresadapter.EmitterOptions{QueueSize: 2, BatchSize: 1, FlushInterval: time.Hour})
	release := fixture.lockTable(t)

	for range 10 {
		emitEvent(emitter, "request", nil) // never blocks although the writer waits for the lock
	}

	queued, dropped := fixture.recorder.counts()
	require.Equal(t, 10, queued+dropped[postgresadapter.DropQueueFull])
	require.LessOrEqual(t, queued, 3, "the queue and the batch the writer holds")
	release()
	require.NoError(t, emitter.Flush(t.Context()))
	require.Equal(t, queued, fixture.countRows(t))
}

func TestEmitterCloseWritesTheQueue(t *testing.T) {
	fixture := newFixture(t, nil)
	emitter, err := postgresadapter.NewEmitter(&postgresadapter.EmitterOptions{Store: fixture.store, FlushInterval: time.Hour})
	require.NoError(t, err)
	for range 50 {
		emitEvent(emitter, "request", nil)
	}

	require.NoError(t, emitter.Close(t.Context()))

	require.Equal(t, 50, fixture.countRows(t))
	emitEvent(emitter, "late", nil)
	_, dropped := fixture.recorder.counts()
	require.Equal(t, 1, dropped[postgresadapter.DropClosed])
	require.ErrorIs(t, emitter.Flush(t.Context()), postgresadapter.ErrClosed)
	require.NoError(t, emitter.Close(t.Context()), "a second Close")
}

func TestEmitterCloseGivesUpWhenItsContextEnds(t *testing.T) {
	fixture := newFixture(t, nil)
	emitter, err := postgresadapter.NewEmitter(&postgresadapter.EmitterOptions{Store: fixture.store, FlushInterval: time.Hour})
	require.NoError(t, err)
	fixture.lockTable(t)
	emitEvent(emitter, "request", nil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	require.ErrorIs(t, emitter.Close(ctx), context.Canceled)

	batch := fixture.recorder.nextBatch(t)
	require.Equal(t, 1, batch.Events)
	require.Error(t, batch.Err, "the aborted write is reported as lost")
}

func TestEmitterFlushReturnsTheWriteError(t *testing.T) {
	fixture := newFixture(t, nil)
	emitter := fixture.emitter(t, &postgresadapter.EmitterOptions{WriteTimeout: 50 * time.Millisecond})
	fixture.lockTable(t)
	emitEvent(emitter, "request", nil)

	err := emitter.Flush(t.Context())

	require.ErrorContains(t, err, "write 1 events")
}

func TestEmitterKeepsOnlyTheChosenLevels(t *testing.T) {
	fixture := newFixture(t, nil)
	emitter := fixture.emitter(t, &postgresadapter.EmitterOptions{Levels: []string{wideevent.LevelWarning, wideevent.LevelError}})

	emitEvent(emitter, "ok", func(we *wideevent.Event) { we.Int(wideevent.KeyResponseStatus, 200).Success() })
	emitEvent(emitter, "not_found", func(we *wideevent.Event) { we.Int(wideevent.KeyResponseStatus, 404) })
	emitEvent(emitter, "failed", func(we *wideevent.Event) { we.Failure(nil) })
	require.NoError(t, emitter.Flush(t.Context()))

	require.ElementsMatch(t, []string{"not_found", "failed"}, storedNames(t, fixture.store))
}

func TestEmitterKeepsOnlyTheChosenOutcomes(t *testing.T) {
	fixture := newFixture(t, nil)
	emitter := fixture.emitter(t, &postgresadapter.EmitterOptions{Outcomes: []string{wideevent.OutcomeFailure, ""}})

	emitEvent(emitter, "succeeded", func(we *wideevent.Event) { we.Success() })
	emitEvent(emitter, "failed", func(we *wideevent.Event) { we.Failure(nil) })
	emitEvent(emitter, "unknown", nil)
	require.NoError(t, emitter.Flush(t.Context()))

	require.ElementsMatch(t, []string{"failed", "unknown"}, storedNames(t, fixture.store))
}

func TestEmitterComposesWithMultiAndSampledEmitters(t *testing.T) {
	fixture := newFixture(t, nil)
	emitter := fixture.emitter(t, nil)
	var seen []string
	composed := wideevent.MultiEmitter(
		wideevent.SampledEmitter(emitter, wideevent.AlwaysOnError()),
		wideevent.EmitterFunc(func(we *wideevent.Event) { seen = append(seen, fieldString(we, wideevent.KeyName)) }),
	)

	emitEvent(composed, "succeeded", func(we *wideevent.Event) { we.Success() })
	emitEvent(composed, "failed", func(we *wideevent.Event) { we.Failure(nil) })
	require.NoError(t, emitter.Flush(t.Context()))

	require.Equal(t, []string{"failed"}, storedNames(t, fixture.store))
	require.Equal(t, []string{"succeeded", "failed"}, seen)
}

func TestEmitterCreatesAMissingPartition(t *testing.T) {
	fixture := newFixture(t, nil)
	emitter := fixture.emitter(t, nil)

	emitEvent(emitter, "first", nil) // no Maintain ran: the day has no partition yet
	require.NoError(t, emitter.Flush(t.Context()))

	require.Equal(t, []string{partitionOf(testDay)}, fixture.partitions(t))
	require.Equal(t, 1, fixture.countRows(t))
}

func TestEventsRollOverToTheNextDaysPartition(t *testing.T) {
	fixture := newFixture(t, nil)
	emitter := fixture.emitter(t, nil)
	fixture.clock.Set(testDay.Add(day - time.Second))
	require.NoError(t, fixture.store.Maintain(t.Context()))

	emitEvent(emitter, "before_midnight", nil)
	fixture.clock.Set(testDay.Add(day + time.Second))
	emitEvent(emitter, "after_midnight", nil)
	require.NoError(t, emitter.Flush(t.Context()))

	rows, err := fixture.pool.Query(t.Context(), "SELECT tableoid::regclass::text, name FROM govengers_wide_events ORDER BY time")
	require.NoError(t, err)
	placed := map[string]string{}
	for rows.Next() {
		var partition, name string
		require.NoError(t, rows.Scan(&partition, &name))
		placed[name] = partition
	}
	require.NoError(t, rows.Err())
	require.Equal(t, map[string]string{
		"before_midnight": partitionOf(testDay),
		"after_midnight":  partitionOf(testDay.Add(day)),
	}, placed)
}

func TestEmitterStoresTextPostgresWouldReject(t *testing.T) {
	fixture := newFixture(t, nil)
	emitter := fixture.emitter(t, nil)
	longUserID := strings.Repeat("ü", 200) // 400 bytes

	emitEvent(emitter, "nul\x00name", func(we *wideevent.Event) {
		we.String("payload", "a\x00b").String("escaped", `\u0000 stays text`).String(wideevent.KeyUserID, longUserID)
		we.String(wideevent.KeyRequestID, "bad\xffutf8")
	})
	require.NoError(t, emitter.Flush(t.Context()))

	page, err := fixture.store.Query(t.Context(), nil)
	require.NoError(t, err)
	require.Len(t, page.Records, 1)
	record := page.Records[0]
	require.Equal(t, "nul�name", record.Name)
	require.Equal(t, "bad�utf8", record.RequestID)
	require.Equal(t, strings.Repeat("ü", 128), record.UserID, "cut to 256 bytes on a character boundary")
	var event map[string]any
	require.NoError(t, json.Unmarshal(record.Event, &event))
	require.Equal(t, "a�b", event["payload"])
	require.Equal(t, `\u0000 stays text`, event["escaped"])
	require.Equal(t, longUserID, event["user"].(map[string]any)["id"], "the event keeps the whole value")
}

func TestNewEmitterValidatesOptions(t *testing.T) {
	fixture := newFixture(t, nil)
	for name, options := range map[string]*postgresadapter.EmitterOptions{
		"nil":            nil,
		"no store":       {},
		"negative queue": {Store: fixture.store, QueueSize: -1},
		"negative batch": {Store: fixture.store, BatchSize: -1},
		"unknown level":  {Store: fixture.store, Levels: []string{"debug"}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := postgresadapter.NewEmitter(options)
			require.Error(t, err)
		})
	}
}

func TestNewValidatesOptions(t *testing.T) {
	_, err := postgresadapter.New(nil)
	require.Error(t, err)
	fixture := newFixture(t, nil)
	_, err = postgresadapter.New(&postgresadapter.Options{Pool: fixture.pool, RetentionDays: -1})
	require.Error(t, err)
}

const day = 24 * time.Hour

func storedNames(t *testing.T, store *postgresadapter.Store) []string {
	t.Helper()
	page, err := store.Query(t.Context(), &postgresadapter.Filter{Limit: postgresadapter.MaximumLimit})
	require.NoError(t, err)
	names := make([]string, 0, len(page.Records))
	for _, record := range page.Records {
		names = append(names, record.Name)
	}
	return names
}

func fieldString(we *wideevent.Event, key string) string {
	for _, field := range we.Fields() {
		if field.Key == key {
			value, _ := field.Value.(string)
			return value
		}
	}
	return ""
}
