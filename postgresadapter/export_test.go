package postgresadapter

import (
	"time"

	"github.com/govengers/postgrestest"
)

// Postgres is the PostgreSQL of this package's tests, internal and external: every test database
// is a copy of a template with MigrationV1 applied. TestMain in main_test.go closes it.
var Postgres = postgrestest.New(&postgrestest.Options{
	Migrate:     postgrestest.Statements(MigrationV1),
	Fingerprint: MigrationV1,
})

// NewEmitterWithRetryDelay is NewEmitter with a backoff that starts at delay, so tests retry
// without waiting.
func NewEmitterWithRetryDelay(options *EmitterOptions, delay time.Duration) (*Emitter, error) {
	return newEmitter(options, delay)
}
