package postgresadapter

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	wideevent "github.com/govengers/wide-event-logger"
	"github.com/govengers/wide-event-logger/internal/postgrestest"
)

func TestClassifyRetriesOnlyTransientFailures(t *testing.T) {
	expired, cancel := context.WithDeadline(t.Context(), time.Unix(0, 0))
	defer cancel()
	for name, test := range map[string]struct {
		attemptExpired bool
		err            error
		expected       failure
	}{
		"connection lost":     {false, io.ErrUnexpectedEOF, failureTransient},
		"admin shutdown":      {false, &pgconn.PgError{Code: "57P01"}, failureTransient},
		"too many clients":    {false, &pgconn.PgError{Code: "53300"}, failureTransient},
		"missing partition":   {false, &pgconn.PgError{Code: "23514"}, failureMissingPartition},
		"invalid json":        {false, &pgconn.PgError{Code: "22P02"}, failureData},
		"not null":            {false, &pgconn.PgError{Code: "23502"}, failureData},
		"too deep":            {false, &pgconn.PgError{Code: "54001"}, failureData},
		"permission denied":   {false, &pgconn.PgError{Code: "42501"}, failurePermanent},
		"attempt ran out":     {true, &pgconn.PgError{Code: "57014"}, failurePermanent},
		"attempt ran out too": {true, context.DeadlineExceeded, failurePermanent},
	} {
		t.Run(name, func(t *testing.T) {
			attemptContext := t.Context()
			if test.attemptExpired {
				attemptContext = expired
			}
			require.Equal(t, test.expected, classify(attemptContext, test.err))
		})
	}
}

// The retry after a connection lost behind a committed COPY must not store the batch twice.
func TestInsertNewSkipsTheStoredEvents(t *testing.T) {
	start0 := time.Now()
	pool := postgrestest.New(t, MigrationV1)
	t.Log("pool", time.Since(start0))
	store, err := New(&Options{Pool: pool})
	require.NoError(t, err)
	emitter, err := NewEmitter(&EmitterOptions{Store: store, FlushInterval: time.Hour})
	require.NoError(t, err)
	defer func() { require.NoError(t, emitter.Close(t.Context())) }()
	_, err = ensurePartitions(t.Context(), pool, []time.Time{store.today()})
	require.NoError(t, err)
	batch := make([]row, 0, 3)
	for range cap(batch) {
		eventRow, _ := emitter.rowOf(wideevent.Begin(nil, "event"))
		batch = append(batch, eventRow)
	}

	start := time.Now()
	require.NoError(t, emitter.copyRows(t.Context(), batch[:2]))
	t.Log("copy", time.Since(start))
	require.NoError(t, emitter.insertNew(t.Context(), batch))
	t.Log("insert", time.Since(start))
	require.NoError(t, emitter.insertNew(t.Context(), batch))
	t.Log("insert2", time.Since(start))

	var count int
	require.NoError(t, pool.QueryRow(t.Context(), "SELECT count(*) FROM govengers_wide_events").Scan(&count))
	require.Equal(t, 3, count)
}

func TestDatabaseErrorKeepsTheContextsEnd(t *testing.T) {
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, databaseError(cancelled, "read", errors.New("pgx: conn closed")), context.Canceled)
	require.NotErrorIs(t, databaseError(t.Context(), "read", context.Canceled), context.Canceled, "a pgx error stays flattened")
}
