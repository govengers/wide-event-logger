# govengers/wide-event-logger: design

Decisions are numbered `W1` onwards; a later change gets a new number, never a rewrite. The 1.0.0
API decisions (ginadapter split, one value per key, frozen events after Emit, redacted queries) are
in the 1.0.0 entry of [CHANGELOG.md](../CHANGELOG.md).

## postgresadapter: wide events in PostgreSQL

A log viewer, an audit of what a request did, and an archive of old days need the events in the
database the application already runs. `postgresadapter` writes them there without slowing the
request down, keeps the table bounded by day partitions, and reads them back for a viewer.

### W1 The adapter lives in the root module

`postgresadapter` and `postgresadapter/prometheusadapter` are packages of
`github.com/govengers/wide-event-logger`, not a nested module. The root module already carries Gin
for `ginadapter`; with module graph pruning a consumer that imports only `wideevent` builds none of
pgx, client_golang or embedded-postgres, and the other govengers libraries (jobs, livewire) keep pgx
in their root module too. A nested module would need its own tags, its own CI wiring in the shared
`go-library` workflow and coordinated releases for a single adapter. Revisit when a consumer reports
a version conflict that the root requirement causes.

### W2 The core exports what an emitter outside it needs

`Event.Level()` (with `LevelInfo`, `LevelWarning`, `LevelError`) and `Event.MarshalJSON()` are the
level and the line the JSON emitter writes. The table stores that line (jsonb reorders the keys),
so an archived day is the NDJSON a log collector reads from stdout. `KeyRequestID`, `KeyUserID` and `KeyTraceID`
(`request.id`, `user.id`, `trace.id`, as ginadapter, `UserID` and govengers/telemetry write them)
name the fields that get their own columns. All additions are backwards compatible.

### W3 Emit encodes and queues; one writer goroutine writes

`Emit` encodes the event in the caller's goroutine (CPU only, no I/O): the event's values may be
maps or pointers the application changes after `Emit`, so the writer must not read them later. The
row goes into a bounded channel with a non-blocking send; a full queue or a closed emitter drops
the event and reports `EventDropped`. Sends hold a read lock and `Close` the write lock, so no send
follows `Close` and nothing is lost between them.

`NewEmitter` starts the writer goroutine. LIB-004 forbids goroutines the caller did not ask for; an
asynchronous emitter is asked for by constructing it, like `sql.DB` or OpenTelemetry's batch span
processor, and `Close(ctx)` is its documented owner (CON-006). The goroutine's context is local to
it; the emitter keeps only its cancel function, which `Close` calls when its own context ends.

### W4 Batches are copied with COPY and retried once

The writer flushes when a batch is full (`BatchSize`, default 500) and every `FlushInterval`
(default 1 s), and on `Flush`/`Close` drains the events queued at that moment (later ones wait for
the next round, so a steady stream cannot keep a drain going). A batch is written with
`pgx.CopyFrom` into the partitioned table, bounded by `WriteTimeout` (default 10 s). A failed write
is retried once while the emitter is not aborted: a pool connection killed by a database restart
fails the first attempt and a fresh one serves the second. When the failure is SQLSTATE 23514 (no
partition for the day) the retry first creates the missing days (W6). A batch that fails twice is
lost and reported in `BatchWritten`; a write whose acknowledgement was lost may be stored twice.
Logs tolerate a duplicate better than a gap; an application that needs every event also writes
them to stdout (`MultiEmitter`). A panic in a write is that batch's error (CON-001).

### W5 One partition per UTC day, no default partition

`time` is the moment `Emit` ran, from `Options.Now` (tests inject a clock). The emit time, not the
start time, decides the partition: a day is then closed a few seconds after midnight, which the
archive relies on, while a three-day job's event still lands in a partition that exists. Partitions
are named `govengers_wide_events_YYYYMMDD`; partitions with other names are never touched.

There is no default partition. It would collect rows of days without a partition, block the cheap
`DROP` of a day (rows of that day could sit in the default) and make creating a day's partition scan
it. Instead `Maintain` creates today and three days ahead, and the writer creates a missing day on
demand (W4), so neither a missed maintenance nor a fresh install loses events.

### W6 Creating and dropping a partition locks the table first

Measured on PostgreSQL 18: `CREATE TABLE ... PARTITION OF` and `DROP TABLE` of a partition take
ACCESS EXCLUSIVE on the partitioned table. Both run in a READ COMMITTED transaction (pinned, so a
REPEATABLE READ server default cannot hide a partition another replica just created) that sets
`lock_timeout = 2s`, locks the table ACCESS EXCLUSIVE before touching a partition (the other order
deadlocks with a reader that holds the table and waits for the partition), lists the partitions
again under the lock and only then creates or drops. The lock is requested only when something is
missing or expired. Writers queue behind a waiting ACCESS EXCLUSIVE request, so the timeout is
short; a timed-out maintenance is repeated by the next run, a timed-out on-demand creation fails
that batch.

### W7 Maintain is serialized by an advisory lock

Replicas run `Maintain` at startup and on a schedule, possibly at once. A session advisory lock on a
connection of its own, keyed by `0x77656c67 << 32 | oid of the table` (so schemas of one database do
not block each other), serializes them: the second waits until the first is done, then finds nothing
to do, and each day is archived once. Waiting is bounded by the caller's context. Everything a
`Maintain` does (listing, creating, streaming the archive, dropping) runs on the connection that
holds the lock: taking a second connection while holding one deadlocked as soon as the waiting
`Maintain` calls of one process held the rest of the pool (found in CI, where the pool has four
connections). A connection whose
lock state is uncertain after an error (a cancelled wait, a failed unlock) is closed, which releases
the lock with the session. A `SHARE UPDATE EXCLUSIVE` table lock would serialize as well, but a
replica's ACCESS EXCLUSIVE request queued behind it during a long archive would stall every writer.

### W8 Retention drops whole days, after the archive succeeded

`RetentionDays` keeps that many full days before today; zero keeps everything. With an `Archiver`
each expired day is streamed before its partition is dropped: one goroutine reads the partition
(`event::text`, ordered by time and ID, rows streamed as they are consumed), writes gzip NDJSON into
an `io.Pipe`, and the Archiver reads the other end. The day counts as archived only when the
Archiver returned nil and the stream ended cleanly; an Archiver that returns nil before reading to
the end fails the day, so a short upload never leads to a drop. A failed day keeps its partition,
is reported in `DayArchived` and the joined error of `Maintain`, and the other days go on. A drop
that fails after a successful archive leaves the day for the next run, which archives it again:
archivers store under a key derived from the day and overwrite.

### W9 Text the database would reject is made storable

One bad row fails a whole COPY, so the row is made acceptable before it is queued: text columns get
invalid UTF-8 and NUL replaced by U+FFFD and are cut to 256 bytes on a character boundary (index
entries stay small; the event column keeps the whole value); the JSON escape `\u0000`, which jsonb
refuses, becomes `�`. Filter values are cut the same way, so a long user ID still matches.

### W10 Query pages newest first with an opaque keyset cursor

`Query` orders by `(time, id)` descending, served by the primary key, and continues after the
cursor's `(time, id)` with a row comparison, so ties within one microsecond page correctly.
Conditions compare bare columns (indexes on name, user, request and trace ID); the free-text search
is `event::text ILIKE` with escaped wildcards and reads every event of the time range, which is why
callers bound it with a time range and a context deadline. `Limit` defaults to 100 and is cut to
1000. No trigram index: it needs an extension and slows every insert; add one in the application's
own migration when the text search is hot.

### W11 Observer and Prometheus metrics

`Observer` (LIB-012) receives `EventQueued`, `EventDropped`, `BatchWritten`, `Maintained` and
`DayArchived`. `prometheusadapter.Metrics` implements it on the caller's registerer (LIB-011):
queued, written, dropped by reason (`queue_full`, `closed`, `write_failed` for the events of a
failed batch), flush duration by outcome, maintenance runs, partitions created and dropped, archive
results and duration. Label values are closed sets and pre-created at zero.

### W12 Tests share one embedded PostgreSQL per package

`internal/postgrestest` starts one embedded PostgreSQL 18 per test binary and gives each test a
fresh schema with `MigrationV1` applied. Concurrency tests run on a pool whose transactions default
to REPEATABLE READ, and they were checked to fail without the advisory lock and without the READ
COMMITTED pin. No test sleeps: they wait for observer reports or use `Flush`, and clocks are
injected. The harness unpacks the cached PostgreSQL archive with the system `tar` (2 s) instead of
the library's xz decoder, which takes 30-40 s inside a `-race` test binary.

### Not done

- No per-row fallback when a batch fails on data: W9 removes the known causes.
- No queue-length gauge: queued minus written minus dropped gives the backlog.
- No index for level or outcome filters: they are scanned within the time range.
- No end-to-end test with PostgreSQL in `test/e2e`: the unit tests already run against a real
  PostgreSQL, and a second instance would add its start-up time to every CI run.
