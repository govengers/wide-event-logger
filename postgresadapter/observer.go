package postgresadapter

import "time"

// Observer receives what the emitter and the store would otherwise have to log (LIB-003). It is
// called from Emit's caller, from the emitter's writer goroutine and from Maintain; implementations
// must be safe for concurrent use and fast, and must not emit wide events into the emitter they
// observe. The interface may gain methods in minor versions, so an implementation outside this
// module should expect that.
type Observer interface {
	// EventQueued reports an event Emit put into the queue.
	EventQueued()
	// EventDropped reports an event that is not written: Emit could not queue it, or it waited in
	// the queue until its day was about to leave the retention.
	EventDropped(reason DropReason)
	// BatchWritten reports one write of queued events, successful or not. A failed batch is lost.
	// A batch the database refused for a row is split, and each part is reported on its own.
	BatchWritten(batch *Batch)
	// Maintained reports one Maintain call.
	Maintained(maintenance *Maintenance)
	// DayArchived reports one attempt to archive a day before its partition is dropped.
	DayArchived(archive *Archive)
}

// DropReason says why an event was dropped.
type DropReason string

// The reasons an Observer receives.
const (
	DropQueueFull DropReason = "queue_full" // the writer is behind: the database is slow or down
	DropClosed    DropReason = "closed"     // Emit after Close
	DropExpired   DropReason = "expired"    // the event waited until Maintain may archive its day
)

// Batch is one write of queued events.
type Batch struct {
	Events   int           // events in the batch, written or, with Err, lost
	Duration time.Duration // of every attempt and the waits between them
	Err      error
}

// Maintenance is one Maintain call.
type Maintenance struct {
	Created  int // partitions created
	Dropped  int // partitions dropped after their retention
	Duration time.Duration
	Err      error // every error of the call, joined
}

// Archive is one attempt to archive a day.
type Archive struct {
	Day      time.Time // midnight UTC of the archived day
	Events   int       // events streamed to the Archiver
	Duration time.Duration
	Err      error // nil only when the Archiver read the whole stream and returned nil
}

type silentObserver struct{}

func (silentObserver) EventQueued()            {}
func (silentObserver) EventDropped(DropReason) {}
func (silentObserver) BatchWritten(*Batch)     {}
func (silentObserver) Maintained(*Maintenance) {}
func (silentObserver) DayArchived(*Archive)    {}
