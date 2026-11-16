package postgresadapter_test

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/govengers/wide-event-logger/postgresadapter"
)

// archive is an Archiver that keeps the names of the events of every day it received.
type archive struct {
	mutex sync.Mutex
	days  map[time.Time][]string
	calls map[time.Time]int
}

func newArchive() *archive {
	return &archive{days: map[time.Time][]string{}, calls: map[time.Time]int{}}
}

func (a *archive) Archive(_ context.Context, archivedDay time.Time, events io.Reader) error {
	decompressed, err := gzip.NewReader(events)
	if err != nil {
		return err
	}
	var names []string
	lines := bufio.NewScanner(decompressed)
	for lines.Scan() {
		var event struct{ Name string }
		if err := json.Unmarshal(lines.Bytes(), &event); err != nil {
			return err
		}
		names = append(names, event.Name)
	}
	if err := lines.Err(); err != nil {
		return err
	}
	a.mutex.Lock()
	defer a.mutex.Unlock()
	a.days[archivedDay] = names
	a.calls[archivedDay]++
	return nil
}

// emitOn writes one event per name at noon of eventDay.
func (f *fixture) emitOn(t *testing.T, eventDay time.Time, names ...string) {
	t.Helper()
	emitter, err := postgresadapter.NewEmitter(&postgresadapter.EmitterOptions{Store: f.store})
	require.NoError(t, err)
	f.clock.Set(eventDay.Add(12 * time.Hour))
	for _, name := range names {
		emitEvent(emitter, name, nil)
	}
	require.NoError(t, emitter.Close(t.Context()))
}

func TestMaintainCreatesTodayAndTheDaysAhead(t *testing.T) {
	fixture := newFixture(t, nil)

	require.NoError(t, fixture.store.Maintain(t.Context()))
	require.NoError(t, fixture.store.Maintain(t.Context()))

	require.Equal(t, []string{
		partitionOf(testDay), partitionOf(testDay.Add(day)), partitionOf(testDay.Add(2 * day)), partitionOf(testDay.Add(3 * day)),
	}, fixture.partitions(t))
	require.Equal(t, 4, fixture.recorder.maintenances[0].Created)
	require.Zero(t, fixture.recorder.maintenances[1].Created, "the second call finds every partition")
}

func TestMaintainWithoutRetentionKeepsEveryDay(t *testing.T) {
	fixture := newFixture(t, nil)
	fixture.emitOn(t, testDay, "old")
	fixture.clock.Set(testDay.Add(400 * day))

	require.NoError(t, fixture.store.Maintain(t.Context()))

	require.Contains(t, fixture.partitions(t), partitionOf(testDay))
	require.Equal(t, 1, fixture.countRows(t))
}

func TestMaintainDropsTheDaysOlderThanTheRetention(t *testing.T) {
	fixture := newFixture(t, &postgresadapter.Options{RetentionDays: 2})
	fixture.emitOn(t, testDay, "expired")
	fixture.emitOn(t, testDay.Add(day), "kept")
	fixture.clock.Set(testDay.Add(3*day + time.Hour)) // testDay+1 and +2 are within the two days before today

	require.NoError(t, fixture.store.Maintain(t.Context()))

	require.NotContains(t, fixture.partitions(t), partitionOf(testDay))
	require.Contains(t, fixture.partitions(t), partitionOf(testDay.Add(day)))
	require.Equal(t, []string{"kept"}, storedNames(t, fixture.store))
	require.Equal(t, 1, fixture.recorder.maintenances[0].Dropped)
}

func TestMaintainArchivesADayBeforeDroppingIt(t *testing.T) {
	archived := newArchive()
	fixture := newFixture(t, &postgresadapter.Options{RetentionDays: 1, Archiver: archived})
	fixture.emitOn(t, testDay, "first", "second", "third")
	fixture.clock.Set(testDay.Add(2 * day))

	require.NoError(t, fixture.store.Maintain(t.Context()))

	require.Equal(t, map[time.Time][]string{testDay: {"first", "second", "third"}}, archived.days)
	require.NotContains(t, fixture.partitions(t), partitionOf(testDay))
	require.Len(t, fixture.recorder.archives, 1)
	require.NoError(t, fixture.recorder.archives[0].Err)
	require.Equal(t, 3, fixture.recorder.archives[0].Events)
}

func TestMaintainKeepsADayWhoseArchiveFailed(t *testing.T) {
	failure := errors.New("bucket unavailable")
	failing := postgresadapter.ArchiverFunc(func(context.Context, time.Time, io.Reader) error { return failure })
	fixture := newFixture(t, &postgresadapter.Options{RetentionDays: 1, Archiver: failing})
	fixture.emitOn(t, testDay, "event")
	fixture.clock.Set(testDay.Add(2 * day))

	err := fixture.store.Maintain(t.Context())

	require.ErrorIs(t, err, failure)
	require.Contains(t, fixture.partitions(t), partitionOf(testDay))
	require.Equal(t, 1, fixture.countRows(t))
	require.ErrorIs(t, fixture.recorder.archives[0].Err, failure)
	require.ErrorIs(t, fixture.recorder.maintenances[0].Err, failure)

	archived := newArchive()
	retry, err := postgresadapter.New(&postgresadapter.Options{Pool: fixture.pool, RetentionDays: 1, Archiver: archived, Now: fixture.clock.Now})
	require.NoError(t, err)
	require.NoError(t, retry.Maintain(t.Context()))
	require.Equal(t, []string{"event"}, archived.days[testDay], "the next run archives the kept day")
	require.NotContains(t, fixture.partitions(t), partitionOf(testDay))
}

func TestMaintainKeepsADayTheArchiverDidNotReadToTheEnd(t *testing.T) {
	lazy := postgresadapter.ArchiverFunc(func(_ context.Context, _ time.Time, events io.Reader) error {
		_, err := events.Read(make([]byte, 4))
		return err
	})
	fixture := newFixture(t, &postgresadapter.Options{RetentionDays: 1, Archiver: lazy})
	fixture.emitOn(t, testDay, "event")
	fixture.clock.Set(testDay.Add(2 * day))

	err := fixture.store.Maintain(t.Context())

	require.ErrorContains(t, err, "returned before the end of the stream")
	require.Contains(t, fixture.partitions(t), partitionOf(testDay))
}

func TestMaintainSurvivesAPanickingArchiver(t *testing.T) {
	panicking := postgresadapter.ArchiverFunc(func(context.Context, time.Time, io.Reader) error { panic("broken") })
	fixture := newFixture(t, &postgresadapter.Options{RetentionDays: 1, Archiver: panicking})
	fixture.emitOn(t, testDay, "event")
	fixture.clock.Set(testDay.Add(2 * day))

	require.ErrorContains(t, fixture.store.Maintain(t.Context()), "panicked")
	require.Contains(t, fixture.partitions(t), partitionOf(testDay))
}

// Replicas maintain at once while a writer keeps writing, on a small pool whose transactions
// default to REPEATABLE READ: every day is archived exactly once, and neither Maintain nor a write
// fails or waits for a connection the others hold.
func TestConcurrentMaintainArchivesEachDayOnce(t *testing.T) {
	archived := newArchive()
	fixture := newFixture(t, &postgresadapter.Options{RetentionDays: 1, Archiver: archived})
	for offset := range 3 {
		fixture.emitOn(t, testDay.Add(time.Duration(offset)*day), "event")
	}
	fixture.clock.Set(testDay.Add(4*day + time.Hour)) // drops testDay, +1 and +2
	pool := repeatableReadPool(t, fixture.pool)
	emitter := fixture.emitter(t, &postgresadapter.EmitterOptions{BatchSize: 10, FlushInterval: time.Millisecond})
	stopWriting := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for {
			select {
			case <-stopWriting:
				return
			default:
				emitEvent(emitter, "concurrent", nil)
			}
		}
	}()

	var replicas sync.WaitGroup
	errs := make(chan error, 4)
	for range 4 {
		replica, err := postgresadapter.New(&postgresadapter.Options{Pool: pool, RetentionDays: 1, Archiver: archived, Now: fixture.clock.Now})
		require.NoError(t, err)
		replicas.Go(func() { errs <- replica.Maintain(t.Context()) })
	}
	replicas.Wait()
	close(stopWriting)
	<-writerDone
	require.NoError(t, emitter.Flush(t.Context()))

	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, map[time.Time]int{testDay: 1, testDay.Add(day): 1, testDay.Add(2 * day): 1}, archived.calls)
	for _, batch := range fixture.recorder.writtenBatches() {
		require.NoError(t, batch.Err)
	}
}

func repeatableReadPool(t *testing.T, pool *pgxpool.Pool) *pgxpool.Pool {
	t.Helper()
	configuration := pool.Config().Copy()
	configuration.ConnConfig.RuntimeParams["default_transaction_isolation"] = "repeatable read"
	configuration.MaxConns = 2 // fewer than the replicas: Maintain must not wait for a second connection
	repeatable, err := pgxpool.NewWithConfig(t.Context(), configuration)
	require.NoError(t, err)
	t.Cleanup(repeatable.Close)
	return repeatable
}

// Writers on several replicas meet the same missing day at once, on a pool whose transactions
// default to REPEATABLE READ: the partition is created once and no write is lost.
func TestConcurrentWritersCreateAMissingPartitionOnce(t *testing.T) {
	fixture := newFixture(t, nil)
	pool := repeatableReadPool(t, fixture.pool)
	const replicas = 6
	emitters := make([]*postgresadapter.Emitter, replicas)
	for index := range emitters {
		replica, err := postgresadapter.New(&postgresadapter.Options{Pool: pool, Now: fixture.clock.Now, Observer: fixture.recorder})
		require.NoError(t, err)
		emitters[index], err = postgresadapter.NewEmitter(&postgresadapter.EmitterOptions{Store: replica, FlushInterval: time.Hour})
		require.NoError(t, err)
		emitEvent(emitters[index], "event", nil)
	}

	var flushes sync.WaitGroup
	for _, emitter := range emitters {
		flushes.Go(func() { require.NoError(t, emitter.Close(t.Context())) })
	}
	flushes.Wait()

	require.Equal(t, replicas, fixture.countRows(t))
	require.Equal(t, []string{partitionOf(testDay)}, fixture.partitions(t))
	for _, batch := range fixture.recorder.writtenBatches() {
		require.NoError(t, batch.Err)
	}
}

// A Maintain that gives up waiting for another replica's maintenance leaves no lock behind.
func TestMaintainThatStopsWaitingForTheLockLeavesNoneBehind(t *testing.T) {
	fixture := newFixture(t, nil)
	holder, err := fixture.pool.Acquire(t.Context())
	require.NoError(t, err)
	const key = "(x'77656c67'::bigint << 32) | 'govengers_wide_events'::regclass::oid::bigint"
	_, err = holder.Exec(t.Context(), "SELECT pg_advisory_lock("+key+")")
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	require.ErrorContains(t, fixture.store.Maintain(ctx), "maintenance lock")

	_, err = holder.Exec(t.Context(), "SELECT pg_advisory_unlock("+key+")")
	require.NoError(t, err)
	holder.Release()
	require.NoError(t, fixture.store.Maintain(t.Context()), "the lock is free again")
	var locks int
	require.NoError(t, fixture.pool.QueryRow(t.Context(),
		"SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND database = (SELECT oid FROM pg_database WHERE datname = current_database())").Scan(&locks))
	require.Zero(t, locks)
}

func TestMaintainLeavesPartitionsItDidNotNameAlone(t *testing.T) {
	fixture := newFixture(t, &postgresadapter.Options{RetentionDays: 1})
	_, err := fixture.pool.Exec(t.Context(), `CREATE TABLE govengers_wide_events_legacy PARTITION OF govengers_wide_events
FOR VALUES FROM ('2000-01-01 00:00:00+00') TO ('2001-01-01 00:00:00+00')`)
	require.NoError(t, err)

	require.NoError(t, fixture.store.Maintain(t.Context()))

	require.Contains(t, fixture.partitions(t), "govengers_wide_events_legacy")
}

func TestMaintainArchivesEachEventWithItsTime(t *testing.T) {
	var lines []map[string]any
	keepLines := postgresadapter.ArchiverFunc(func(_ context.Context, _ time.Time, events io.Reader) error {
		decompressed, err := gzip.NewReader(events)
		if err != nil {
			return err
		}
		scanner := bufio.NewScanner(decompressed)
		for scanner.Scan() {
			var line map[string]any
			if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
				return err
			}
			lines = append(lines, line)
		}
		return scanner.Err()
	})
	fixture := newFixture(t, &postgresadapter.Options{RetentionDays: 1, Archiver: keepLines})
	fixture.emitOn(t, testDay, "job")
	// more events than one read of the cursor fetches
	_, err := fixture.pool.Exec(t.Context(), `INSERT INTO govengers_wide_events (time, id, name, level, event)
SELECT $1::timestamptz + make_interval(secs => n), n, 'bulk', 'info', '{"name":"bulk"}' FROM generate_series(1, 2500) n`, testDay.Add(13*time.Hour))
	require.NoError(t, err)
	fixture.clock.Set(testDay.Add(2 * day))

	require.NoError(t, fixture.store.Maintain(t.Context()))

	require.Len(t, lines, 2501)
	require.Equal(t, "job", lines[0]["name"])
	require.Equal(t, "2026-03-14T12:00:00.000000Z", lines[0]["time"])
	require.Equal(t, "2026-03-14T13:41:40.000000Z", lines[2500]["time"], "oldest first, to the last event")
}

func TestMaintainStopsArchivingWhenItsContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	calls := 0
	cancelling := postgresadapter.ArchiverFunc(func(context.Context, time.Time, io.Reader) error {
		calls++
		cancel() // the application shuts down during the first archive
		return context.Canceled
	})
	fixture := newFixture(t, &postgresadapter.Options{RetentionDays: 1, Archiver: cancelling})
	for offset := range 3 {
		fixture.emitOn(t, testDay.Add(time.Duration(offset)*day), "event")
	}
	fixture.clock.Set(testDay.Add(5 * day))

	err := fixture.store.Maintain(ctx)

	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, calls, "no archive starts after the context ended")
	require.Subset(t, fixture.partitions(t), []string{partitionOf(testDay), partitionOf(testDay.Add(day)), partitionOf(testDay.Add(2 * day))}, "every day is kept")
}

func TestMaintainStopsArchivingWhenItsConnectionCloses(t *testing.T) {
	var fixture *fixture
	calls := 0
	terminating := postgresadapter.ArchiverFunc(func(_ context.Context, _ time.Time, events io.Reader) error {
		calls++
		if _, err := gzip.NewReader(events); err != nil { // the cursor has fetched once
			return err
		}
		_, err := fixture.pool.Exec(context.Background(),
			"SELECT pg_terminate_backend(pid, 5000) FROM pg_stat_activity WHERE datname = current_database() AND query LIKE 'FETCH % FROM archived_day'")
		if err != nil {
			return err
		}
		_, err = io.Copy(io.Discard, events)
		return err
	})
	fixture = newFixture(t, &postgresadapter.Options{RetentionDays: 1, Archiver: terminating})
	for offset := range 4 {
		fixture.emitOn(t, testDay.Add(time.Duration(offset)*day), "event")
	}
	fixture.clock.Set(testDay.Add(6 * day))

	err := fixture.store.Maintain(t.Context())

	require.ErrorContains(t, err, "the maintenance connection closed")
	require.Equal(t, 1, calls, "no archive starts on a closed connection")
	require.Subset(t, fixture.partitions(t), []string{partitionOf(testDay), partitionOf(testDay.Add(3 * day))}, "every day is kept")
}
