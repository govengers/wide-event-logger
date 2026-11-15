package postgresadapter_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/govengers/wide-event-logger/internal/postgrestest"
	"github.com/govengers/wide-event-logger/postgresadapter"
)

func TestMain(m *testing.M) { postgrestest.Main(m) }

// testDay is the day the tests' clocks start on; any day works, a fixed one keeps the test cacheable.
var testDay = time.Date(2026, time.March, 14, 0, 0, 0, 0, time.UTC)

// clock is the store's injected clock.
type clock struct {
	mutex sync.Mutex
	now   time.Time
}

func newClock(now time.Time) *clock { return &clock{now: now} }

func (c *clock) Now() time.Time {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.now
}

func (c *clock) Set(now time.Time) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.now = now
}

// recorder is an Observer that keeps every report.
type recorder struct {
	mutex        sync.Mutex
	queued       int
	dropped      map[postgresadapter.DropReason]int
	maintenances []postgresadapter.Maintenance
	archives     []postgresadapter.Archive
	batches      []postgresadapter.Batch
	consumed     int           // batches nextBatch returned
	written      chan struct{} // signalled after each batch
}

func newRecorder() *recorder {
	return &recorder{dropped: map[postgresadapter.DropReason]int{}, written: make(chan struct{}, 1)}
}

func (r *recorder) EventQueued() {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.queued++
}

func (r *recorder) EventDropped(reason postgresadapter.DropReason) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.dropped[reason]++
}

func (r *recorder) BatchWritten(batch *postgresadapter.Batch) {
	r.mutex.Lock()
	r.batches = append(r.batches, *batch)
	r.mutex.Unlock()
	select {
	case r.written <- struct{}{}:
	default: // a signal is pending already
	}
}

// writtenBatches returns every batch reported so far.
func (r *recorder) writtenBatches() []postgresadapter.Batch {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return append([]postgresadapter.Batch(nil), r.batches...)
}

func (r *recorder) Maintained(maintenance *postgresadapter.Maintenance) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.maintenances = append(r.maintenances, *maintenance)
}

func (r *recorder) DayArchived(archive *postgresadapter.Archive) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.archives = append(r.archives, *archive)
}

func (r *recorder) counts() (int, map[postgresadapter.DropReason]int) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	dropped := make(map[postgresadapter.DropReason]int, len(r.dropped))
	for reason, count := range r.dropped {
		dropped[reason] = count
	}
	return r.queued, dropped
}

// nextBatch waits for the next written batch; the test's deadline bounds the wait.
func (r *recorder) nextBatch(t *testing.T) postgresadapter.Batch {
	t.Helper()
	for {
		r.mutex.Lock()
		if r.consumed < len(r.batches) {
			batch := r.batches[r.consumed]
			r.consumed++
			r.mutex.Unlock()
			return batch
		}
		r.mutex.Unlock()
		select {
		case <-r.written:
		case <-t.Context().Done():
			t.Fatal("no batch was written")
		}
	}
}

// fixture is a store on a fresh schema with an injected clock and a recorder.
type fixture struct {
	pool     *pgxpool.Pool
	store    *postgresadapter.Store
	clock    *clock
	recorder *recorder
}

func newFixture(t *testing.T, options *postgresadapter.Options) *fixture {
	t.Helper()
	if options == nil {
		options = &postgresadapter.Options{}
	}
	pool := postgrestest.New(t, postgresadapter.MigrationV1)
	testClock := newClock(testDay.Add(12 * time.Hour))
	observer := newRecorder()
	options.Pool, options.Now, options.Observer = pool, testClock.Now, observer
	store, err := postgresadapter.New(options)
	require.NoError(t, err)
	return &fixture{pool: pool, store: store, clock: testClock, recorder: observer}
}

// emitter starts an emitter on the fixture's store and closes it when the test ends.
func (f *fixture) emitter(t *testing.T, options *postgresadapter.EmitterOptions) *postgresadapter.Emitter {
	t.Helper()
	if options == nil {
		options = &postgresadapter.EmitterOptions{}
	}
	options.Store = f.store
	emitter, err := postgresadapter.NewEmitter(options)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, emitter.Close(context.Background())) })
	return emitter
}

func (f *fixture) countRows(t *testing.T) int {
	t.Helper()
	var count int
	require.NoError(t, f.pool.QueryRow(context.Background(), "SELECT count(*) FROM govengers_wide_events").Scan(&count))
	return count
}

// partitions returns the names of the table's partitions, ascending.
func (f *fixture) partitions(t *testing.T) []string {
	t.Helper()
	rows, err := f.pool.Query(context.Background(), `
SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
WHERE i.inhparent = 'govengers_wide_events'::regclass ORDER BY 1`)
	require.NoError(t, err)
	var names []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		names = append(names, name)
	}
	require.NoError(t, rows.Err())
	return names
}

// lockTable holds an ACCESS EXCLUSIVE lock on the table until the returned function is called,
// so every write waits.
func (f *fixture) lockTable(t *testing.T) func() {
	t.Helper()
	ctx := context.Background()
	transaction, err := f.pool.Begin(ctx)
	require.NoError(t, err)
	_, err = transaction.Exec(ctx, "LOCK TABLE govengers_wide_events IN ACCESS EXCLUSIVE MODE")
	require.NoError(t, err)
	var once sync.Once
	release := func() { once.Do(func() { require.NoError(t, transaction.Rollback(ctx)) }) }
	t.Cleanup(release)
	return release
}

func partitionOf(day time.Time) string {
	return "govengers_wide_events_" + day.Format("20060102")
}
