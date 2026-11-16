package postgresadapter

// MigrationV1 creates the partitioned event table. Run it from one of the application's goose
// migrations, in the schema the store's pool uses:
//
//	-- +goose Up
//	-- the content of postgresadapter.MigrationV1
//
// or from a goose Go migration with tx.ExecContext(ctx, postgresadapter.MigrationV1). Later
// versions of the module add MigrationV2 and so on; never edit an applied migration. It runs
// inside the migration's transaction (SET LOCAL) and passes squawk. It creates no partition: the
// application calls Store.Maintain at startup and once a day, and the emitter creates a missing
// day on demand.
//
// The columns hold the fields a log viewer filters on; event holds the whole event as the JSON
// emitter writes it. time is the moment the event was emitted, in UTC days per partition.
const MigrationV1 = `
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE govengers_wide_events (
    time        timestamptz NOT NULL,
    -- random, assigned by the emitter: with time it identifies an event across a retried write
    id          bigint NOT NULL,
    name        text NOT NULL,
    level       text NOT NULL,
    outcome     text,
    -- an HTTP status fits an integer
    -- squawk-ignore prefer-bigint-over-int
    status      integer,
    duration_ms double precision,
    user_id     text,
    request_id  text,
    trace_id    text,
    event       jsonb NOT NULL,
    PRIMARY KEY (time, id)
) PARTITION BY RANGE (time);

CREATE INDEX govengers_wide_events_name ON govengers_wide_events (name, time);
CREATE INDEX govengers_wide_events_user ON govengers_wide_events (user_id, time) WHERE user_id IS NOT NULL;
CREATE INDEX govengers_wide_events_request ON govengers_wide_events (request_id) WHERE request_id IS NOT NULL;
CREATE INDEX govengers_wide_events_trace ON govengers_wide_events (trace_id) WHERE trace_id IS NOT NULL;
`
