package postgresadapter_test

import (
	"context"
	"encoding/json"
	"net"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	wideevent "github.com/govengers/wide-event-logger"
	"github.com/govengers/wide-event-logger/postgresadapter"
)

// Values the calling program passes through unchecked, often from a remote party: raw JSON with a
// lone surrogate escape, \u0000 or invalid UTF-8, and a status outside PostgreSQL's integer.
func TestEmitterStoresRawJSONThatJSONBWouldRefuse(t *testing.T) {
	fixture := newFixture(t, nil)
	emitter := fixture.emitter(t, &postgresadapter.EmitterOptions{FlushInterval: time.Hour})

	emitEvent(emitter, "ordinary", nil)
	emitEvent(emitter, "surrogate", func(we *wideevent.Event) { we.Set("request.body", json.RawMessage(`{"q":"\ud800"}`)) })
	emitEvent(emitter, "invalid_utf8", func(we *wideevent.Event) { we.Set("upstream.body", json.RawMessage("{\"q\":\"\xff\"}")) })
	emitEvent(emitter, "null_escape", func(we *wideevent.Event) { we.Set("webhook.body", json.RawMessage(`{"q":"\u0000"}`)) })
	emitEvent(emitter, "huge_status", func(we *wideevent.Event) { we.Int(wideevent.KeyResponseStatus, 3_000_000_000) })
	require.NoError(t, emitter.Flush(t.Context()))

	require.ElementsMatch(t, []string{"ordinary", "surrogate", "invalid_utf8", "null_escape", "huge_status"}, storedNames(t, fixture.store))
}

// A row PostgreSQL refuses for a reason the emitter cannot know (here a CHECK constraint of the
// application) costs that row only: the batch is split, the other rows are stored.
func TestEmitterLosesOnlyTheRowPostgresRefuses(t *testing.T) {
	fixture := newFixture(t, nil)
	require.NoError(t, fixture.store.Maintain(t.Context()))
	_, err := fixture.pool.Exec(t.Context(), "ALTER TABLE govengers_wide_events ADD CONSTRAINT no_poison CHECK (name <> 'poison')")
	require.NoError(t, err)
	emitter := fixture.emitter(t, &postgresadapter.EmitterOptions{FlushInterval: time.Hour})

	for _, name := range []string{"a", "b", "poison", "c", "d", "e"} {
		emitEvent(emitter, name, nil)
	}
	err = emitter.Flush(t.Context())

	require.ErrorContains(t, err, "write 1 events")
	require.ElementsMatch(t, []string{"a", "b", "c", "d", "e"}, storedNames(t, fixture.store))
	stored, lost := 0, 0
	for _, batch := range fixture.recorder.writtenBatches() {
		if batch.Err != nil {
			require.Equal(t, 1, batch.Events, "the refused row is reported on its own")
			lost += batch.Events
		} else {
			stored += batch.Events
		}
	}
	require.Equal(t, 5, stored)
	require.Equal(t, 1, lost)
}

// A database restart kills the pool's connections: the first attempt fails on its dead
// connection and the retry, on a fresh one, stores the batch once.
func TestEmitterRetriesAfterALostConnection(t *testing.T) {
	fixture := newFixture(t, nil)
	require.NoError(t, fixture.store.Maintain(t.Context()))
	emitter := fixture.emitter(t, &postgresadapter.EmitterOptions{FlushInterval: time.Hour})
	emitEvent(emitter, "before", nil)
	require.NoError(t, emitter.Flush(t.Context()))
	fixture.terminateConnections(t)

	emitEvent(emitter, "after", nil)
	require.NoError(t, emitter.Flush(t.Context()))

	require.ElementsMatch(t, []string{"before", "after"}, storedNames(t, fixture.store))
}

// A restart or failover refuses connections for a while: the writer backs off and retries until
// the database accepts them again, and the queue holds what arrives meanwhile.
func TestEmitterWaitsForADatabaseThatRefusesConnections(t *testing.T) {
	fixture := newFixture(t, nil)
	require.NoError(t, fixture.store.Maintain(t.Context()))
	var refusals atomic.Int32
	configuration := fixture.pool.Config()
	dialer := &net.Dialer{}
	configuration.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		if refusals.Add(-1) >= 0 {
			return nil, syscall.ECONNREFUSED
		}
		return dialer.DialContext(ctx, network, address)
	}
	pool, err := pgxpool.NewWithConfig(t.Context(), configuration)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	store, err := postgresadapter.New(&postgresadapter.Options{Pool: pool, Now: fixture.clock.Now, Observer: fixture.recorder})
	require.NoError(t, err)
	emitter := fixture.emitter(t, &postgresadapter.EmitterOptions{Store: store, FlushInterval: time.Hour})
	refusals.Store(4) // more attempts than one retry

	emitEvent(emitter, "during_the_outage", nil)
	require.NoError(t, emitter.Flush(t.Context()))

	require.Equal(t, []string{"during_the_outage"}, storedNames(t, fixture.store))
	require.Negative(t, refusals.Load(), "every refusal was retried")
}

// A connection lost on the first attempt and a day without a partition on the retry: the
// partition is created and the batch, one event, is stored.
func TestEmitterCreatesAMissingPartitionAfterALostConnection(t *testing.T) {
	fixture := newFixture(t, nil)
	emitter := fixture.emitter(t, &postgresadapter.EmitterOptions{FlushInterval: time.Hour})
	emitEvent(emitter, "today", nil)
	require.NoError(t, emitter.Flush(t.Context()))
	fixture.terminateConnections(t)
	fixture.clock.Set(testDay.Add(day + time.Hour)) // no Maintain created the next day

	emitEvent(emitter, "tomorrow", nil)
	require.NoError(t, emitter.Flush(t.Context()))

	require.ElementsMatch(t, []string{"today", "tomorrow"}, storedNames(t, fixture.store))
}

func TestFlushReturnsTheErrorOfAWriteBeforeIt(t *testing.T) {
	fixture := newFixture(t, nil)
	emitter := fixture.emitter(t, &postgresadapter.EmitterOptions{BatchSize: 1, WriteTimeout: 20 * time.Millisecond})
	release := fixture.lockTable(t)
	emitEvent(emitter, "lost", nil) // a full batch: written at once, not by Flush
	require.Error(t, fixture.recorder.nextBatch(t).Err)
	release()

	require.ErrorContains(t, emitter.Flush(t.Context()), "write 1 events")
	require.NoError(t, emitter.Flush(t.Context()), "an error is reported once")
}

// panickingObserver panics when a batch is reported.
type panickingObserver struct{ *recorder }

func (panickingObserver) BatchWritten(*postgresadapter.Batch) { panic("broken observer") }

func TestEmitterSurvivesAPanickingObserver(t *testing.T) {
	fixture := newFixture(t, nil)
	store, err := postgresadapter.New(&postgresadapter.Options{Pool: fixture.pool, Now: fixture.clock.Now, Observer: panickingObserver{newRecorder()}})
	require.NoError(t, err)
	emitter, err := postgresadapter.NewEmitter(&postgresadapter.EmitterOptions{Store: store, FlushInterval: time.Hour})
	require.NoError(t, err)

	emitEvent(emitter, "first", nil)
	require.ErrorContains(t, emitter.Flush(t.Context()), "panic: broken observer")
	emitEvent(emitter, "second", nil)
	require.ErrorContains(t, emitter.Flush(t.Context()), "panic", "the writer goes on")
	require.NoError(t, emitter.Close(t.Context()))

	require.ElementsMatch(t, []string{"first", "second"}, storedNames(t, fixture.store))
}

// terminateConnections ends the server side of every idle connection of the fixture's pool, as a
// database restart does.
func (f *fixture) terminateConnections(t *testing.T) {
	t.Helper()
	idle := f.pool.AcquireAllIdle(t.Context())
	processIDs := make([]uint32, 0, len(idle))
	for _, connection := range idle {
		processIDs = append(processIDs, connection.Conn().PgConn().PID())
		connection.Release()
	}
	admin, err := pgx.Connect(t.Context(), f.pool.Config().ConnString())
	require.NoError(t, err)
	defer func() { require.NoError(t, admin.Close(t.Context())) }()
	for _, processID := range processIDs {
		_, err := admin.Exec(t.Context(), "SELECT pg_terminate_backend($1::integer, 5000)", processID)
		require.NoError(t, err)
	}
}
