package postgresadapter

import "time"

// NewEmitterWithRetryDelay is NewEmitter with a backoff that starts at delay, so tests retry
// without waiting.
func NewEmitterWithRetryDelay(options *EmitterOptions, delay time.Duration) (*Emitter, error) {
	return newEmitter(options, delay)
}
