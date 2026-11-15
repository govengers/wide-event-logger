package postgresadapter

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	wideevent "github.com/govengers/wide-event-logger"
)

// Defaults of EmitterOptions.
const (
	DefaultQueueSize     = 10000
	DefaultBatchSize     = 500
	DefaultFlushInterval = time.Second
	DefaultWriteTimeout  = 10 * time.Second
)

// ErrClosed is returned by Flush after Close.
var ErrClosed = errors.New("postgresadapter: emitter closed")

// sqlstateNoPartition is check_violation, which an insert into a day without a partition raises.
const sqlstateNoPartition = "23514"

// EmitterOptions configures an Emitter.
type EmitterOptions struct {
	Store         *Store        // required
	QueueSize     int           // events waiting for the writer; Emit drops beyond it. Zero means DefaultQueueSize
	BatchSize     int           // events per write; zero means DefaultBatchSize
	FlushInterval time.Duration // the longest an event waits for a full batch; zero means DefaultFlushInterval
	WriteTimeout  time.Duration // bounds one write attempt; zero means DefaultWriteTimeout
	// Levels keeps only the events whose Level is listed (wideevent.LevelInfo, LevelWarning,
	// LevelError); empty keeps every level.
	Levels []string
	// Outcomes keeps only the events whose outcome field is listed, "" standing for events
	// without one; empty keeps every outcome. An event is kept when both lists keep it.
	Outcomes []string
}

// Emitter is a wideevent.Emitter that writes events to the Store's table. Emit never blocks and
// does no I/O: it encodes the event and queues it, and one writer goroutine writes the queue in
// batches. It composes with wideevent.MultiEmitter and wideevent.SampledEmitter. It is safe for
// concurrent use.
//
// A write that fails is retried once, after creating the missing day partition when that was the
// cause; a batch that fails twice is lost and reported to the Observer. A write whose
// acknowledgement is lost may be stored twice by the retry.
type Emitter struct {
	store         *Store
	batchSize     int
	flushInterval time.Duration
	writeTimeout  time.Duration
	levels        []string
	outcomes      []string

	queue   chan row
	flushes chan chan error
	stop    chan struct{}
	done    chan struct{}
	abort   context.CancelFunc

	mutex  sync.RWMutex // Emit's sends hold it shared, Close exclusively, so no send follows Close
	closed bool
}

// NewEmitter validates options and starts the writer goroutine; Close stops it. It does no I/O.
func NewEmitter(options *EmitterOptions) (*Emitter, error) {
	if err := validateEmitterOptions(options); err != nil {
		return nil, err
	}
	emitter := &Emitter{
		store:         options.Store,
		batchSize:     valueOr(options.BatchSize, DefaultBatchSize),
		flushInterval: valueOr(options.FlushInterval, DefaultFlushInterval),
		writeTimeout:  valueOr(options.WriteTimeout, DefaultWriteTimeout),
		levels:        slices.Clone(options.Levels),
		outcomes:      slices.Clone(options.Outcomes),
		queue:         make(chan row, valueOr(options.QueueSize, DefaultQueueSize)),
		flushes:       make(chan chan error),
		stop:          make(chan struct{}),
		done:          make(chan struct{}),
	}
	ctx, abort := context.WithCancel(context.Background())
	emitter.abort = abort
	go emitter.run(ctx)
	return emitter, nil
}

func validateEmitterOptions(options *EmitterOptions) error {
	if options == nil || options.Store == nil {
		return errors.New("postgresadapter: EmitterOptions.Store is required")
	}
	if options.QueueSize < 0 || options.BatchSize < 0 || options.FlushInterval < 0 || options.WriteTimeout < 0 {
		return errors.New("postgresadapter: EmitterOptions sizes and durations must not be negative")
	}
	for _, level := range options.Levels {
		if level != wideevent.LevelInfo && level != wideevent.LevelWarning && level != wideevent.LevelError {
			return fmt.Errorf("postgresadapter: EmitterOptions.Levels: unknown level %q", level)
		}
	}
	return nil
}

func valueOr[T int | time.Duration](value, fallback T) T {
	if value == 0 {
		return fallback
	}
	return value
}

// Emit queues we, or drops it when the queue is full or the emitter closed. It never blocks on
// the database.
func (e *Emitter) Emit(we *wideevent.Event) {
	if we == nil {
		return
	}
	eventRow, kept := e.rowOf(we)
	if !kept {
		return
	}
	if reason, dropped := e.enqueue(eventRow); dropped {
		e.store.observer.EventDropped(reason)
		return
	}
	e.store.observer.EventQueued()
}

// rowOf encodes we, or reports that the Levels or Outcomes options do not keep it.
func (e *Emitter) rowOf(we *wideevent.Event) (row, bool) {
	level := we.Level()
	if len(e.levels) > 0 && !slices.Contains(e.levels, level) {
		return row{}, false
	}
	values := fieldValues(we)
	if len(e.outcomes) > 0 {
		outcome, _ := values[wideevent.KeyOutcome].(string)
		if !slices.Contains(e.outcomes, outcome) {
			return row{}, false
		}
	}
	return newRow(we, values, level, e.store.now()), true
}

func (e *Emitter) enqueue(eventRow row) (DropReason, bool) {
	e.mutex.RLock()
	defer e.mutex.RUnlock()
	if e.closed {
		return DropClosed, true
	}
	select {
	case e.queue <- eventRow:
		return "", false
	default:
		return DropQueueFull, true
	}
}

// Flush writes every event queued before the call and returns the errors of those writes. It
// returns ErrClosed after Close, and ctx's error when ctx ends first; the write goes on then.
func (e *Emitter) Flush(ctx context.Context) error {
	reply := make(chan error, 1)
	select {
	case e.flushes <- reply:
	case <-e.done:
		return ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-reply:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close stops accepting events, writes the queued ones and stops the writer. When ctx ends first
// it aborts the writes, waits for the writer and returns ctx's error; the events not written are
// reported as failed batches. Later calls wait for the first one, bounded by their own ctx.
func (e *Emitter) Close(ctx context.Context) error {
	e.mutex.Lock()
	alreadyClosed := e.closed
	e.closed = true
	e.mutex.Unlock()
	if alreadyClosed {
		select {
		case <-e.done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	close(e.stop)
	select {
	case <-e.done:
		return nil
	case <-ctx.Done():
		e.abort()
		<-e.done
		return ctx.Err()
	}
}

// run is the writer goroutine. ctx ends only when Close aborts.
func (e *Emitter) run(ctx context.Context) {
	defer close(e.done)
	defer e.abort()
	ticker := time.NewTicker(e.flushInterval)
	defer ticker.Stop()
	batch := make([]row, 0, e.batchSize)
	for {
		select {
		case eventRow := <-e.queue:
			batch = append(batch, eventRow)
			if len(batch) >= e.batchSize {
				_ = e.write(ctx, batch) // reported to the Observer
				batch = batch[:0]
			}
		case <-ticker.C:
			_ = e.write(ctx, batch) // reported to the Observer
			batch = batch[:0]
		case reply := <-e.flushes:
			reply <- e.drain(ctx, batch)
			batch = batch[:0]
		case <-e.stop:
			_ = e.drain(ctx, batch) // reported to the Observer
			return
		}
	}
}

// drain writes batch and every event in the queue when it starts, in batches. Events queued
// meanwhile wait for the next round, so a steady stream cannot keep it going.
func (e *Emitter) drain(ctx context.Context, batch []row) error {
	var errs []error
	for waiting := len(e.queue); waiting > 0; waiting-- {
		batch = append(batch, <-e.queue)
		if len(batch) >= e.batchSize {
			errs = append(errs, e.write(ctx, batch))
			batch = batch[:0]
		}
	}
	errs = append(errs, e.write(ctx, batch))
	return errors.Join(errs...)
}

// write stores batch, retrying once, and reports the result. The caller reuses batch after it
// returns.
func (e *Emitter) write(ctx context.Context, batch []row) error {
	if len(batch) == 0 {
		return nil
	}
	start := time.Now()
	err := e.attempt(ctx, batch)
	if err != nil && ctx.Err() == nil {
		err = e.retry(ctx, batch, err)
	}
	e.store.observer.BatchWritten(&Batch{Events: len(batch), Duration: time.Since(start), Err: err})
	return err
}

// retry writes batch a second time, after creating the missing partitions when they made the
// first attempt fail.
func (e *Emitter) retry(ctx context.Context, batch []row, first error) error {
	if errors.Is(first, errMissingPartition) {
		ensureContext, cancel := context.WithTimeout(ctx, e.writeTimeout)
		defer cancel()
		if _, err := e.store.ensurePartitions(ensureContext, batchDays(batch)); err != nil {
			return errors.Join(first, err)
		}
	}
	return e.attempt(ctx, batch)
}

// errMissingPartition marks a write that failed because a day of the batch has no partition.
var errMissingPartition = errors.New("no partition for the day")

// attempt copies batch into the table once. A panic is its error (CON-001): the writer goroutine
// must survive any batch.
func (e *Emitter) attempt(ctx context.Context, batch []row) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("postgresadapter: write %d events: panic: %v", len(batch), recovered)
		}
	}()
	ctx, cancel := context.WithTimeout(ctx, e.writeTimeout)
	defer cancel()
	_, err = e.store.pool.CopyFrom(ctx, pgx.Identifier{tableName}, columns, pgx.CopyFromSlice(len(batch), func(index int) ([]any, error) {
		return batch[index].values(), nil
	}))
	if err == nil {
		return nil
	}
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) && postgresError.Code == sqlstateNoPartition {
		return fmt.Errorf("postgresadapter: write %d events: %w: %v", len(batch), errMissingPartition, err) //nolint:errorlint // LIB-010: pgx errors are not part of the API
	}
	return fmt.Errorf("postgresadapter: write %d events: %v", len(batch), err) //nolint:errorlint // LIB-010
}

// batchDays returns the UTC days of the batch's events.
func batchDays(batch []row) []time.Time {
	days := make([]time.Time, 0, 1)
	for index := range batch {
		eventDay := batch[index].time.UTC().Truncate(day)
		if !slices.ContainsFunc(days, eventDay.Equal) {
			days = append(days, eventDay)
		}
	}
	return days
}
