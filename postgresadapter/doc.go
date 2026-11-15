// Package postgresadapter stores wide events in PostgreSQL 18 for a log viewer.
//
// An Emitter is a wideevent.Emitter: Emit encodes the event and puts it into a bounded queue
// without blocking, and one writer goroutine copies the queue into the table in batches, flushed
// by size and interval. A full queue drops the event and reports it to the Observer. Close writes
// what is queued.
//
// The table of MigrationV1 is partitioned by UTC day. Its columns hold what a viewer filters on
// (time, name, level, outcome, status, duration, user, request and trace ID), the event column
// the whole event as JSON. Store.Maintain creates the partitions ahead and drops the days older
// than the retention; an Archiver receives each day as a gzip NDJSON stream before it is dropped.
// Store.Query reads a page of events for a viewer.
//
//	store, err := postgresadapter.New(&postgresadapter.Options{Pool: pool, RetentionDays: 30})
//	emitter, err := postgresadapter.NewEmitter(&postgresadapter.EmitterOptions{Store: store})
//	defer emitter.Close(shutdownContext)
//	middleware, err := ginadapter.NewMiddleware(&ginadapter.Options{
//	    Emitter: wideevent.MultiEmitter(wideevent.NewJSONEmitter(nil), emitter),
//	})
//
// The prometheusadapter subpackage turns the Observer into metrics.
package postgresadapter
