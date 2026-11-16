package postgresadapter

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	wideevent "github.com/govengers/wide-event-logger"
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

// The retry after a connection lost behind a committed COPY must not store the batch twice, on a
// pool in any exec mode.
func TestInsertNewSkipsTheStoredEvents(t *testing.T) {
	for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeCacheStatement, pgx.QueryExecModeExec, pgx.QueryExecModeSimpleProtocol} {
		t.Run(mode.String(), func(t *testing.T) {
			configuration, err := pgxpool.ParseConfig(Postgres.Database(t))
			require.NoError(t, err)
			configuration.ConnConfig.DefaultQueryExecMode = mode
			pool, err := pgxpool.NewWithConfig(t.Context(), configuration)
			require.NoError(t, err)
			defer pool.Close()
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

			require.NoError(t, emitter.copyRows(t.Context(), batch[:2]))
			require.NoError(t, emitter.insertNew(t.Context(), batch))
			require.NoError(t, emitter.insertNew(t.Context(), batch))

			var count int
			require.NoError(t, pool.QueryRow(t.Context(), "SELECT count(*) FROM govengers_wide_events").Scan(&count))
			require.Equal(t, 3, count)
		})
	}
}

func TestBackoffDoublesUpToTheCapWithJitter(t *testing.T) {
	for attempt, expected := range map[int]time.Duration{
		1:  time.Second,
		2:  2 * time.Second,
		4:  8 * time.Second,
		5:  maximumRetryDelay,
		62: maximumRetryDelay,
	} {
		delay := backoff(time.Second, attempt)
		require.GreaterOrEqual(t, delay, expected, attempt)
		require.LessOrEqual(t, delay, expected+expected/5, attempt)
	}
	require.Equal(t, time.Duration(0), backoff(0, 1), "no jitter panics on a zero delay")
}

func TestDatabaseErrorKeepsTheContextsEnd(t *testing.T) {
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, databaseError(cancelled, "read", errors.New("pgx: conn closed")), context.Canceled)
	require.NotErrorIs(t, databaseError(t.Context(), "read", context.Canceled), context.Canceled, "a pgx error stays flattened")
}
