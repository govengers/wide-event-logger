// Package wideevent records one structured event per unit of work (a request, a job, a cron run) and
// emits it once at the end, instead of many scattered log lines.
//
// A unit of work starts an event with Begin, puts it into the context with NewContext, and every layer
// below adds fields through FromContext. Emit hands the finished event to an Emitter: NewJSONEmitter for
// production, NewDevEmitter for a terminal. SampledEmitter keeps only the events a Sampler selects.
//
// Every method of *Event is safe for concurrent use and a no-op on a nil *Event, so code can write
// wideevent.FromContext(ctx).Str("db.operation", "update") without checking whether a unit of work is
// running. The Gin middleware lives in the ginadapter subpackage, so this package has no web framework
// dependency.
package wideevent
