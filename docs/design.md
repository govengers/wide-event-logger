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

`Event.Level()` (with `LevelInfo`, `LevelWarning`, `LevelError`) and `Event.JSON()` are the
level and the line the JSON emitter writes (not `MarshalJSON`: an event reachable from its own
fields would recurse until the stack overflows). The table stores that line (jsonb reorders the keys),
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

### W4 Batches are copied with COPY; failures are classified before anything is retried

The writer flushes when a batch is full (`BatchSize`, default 500) and every `FlushInterval`
(default 1 s), and on `Flush`/`Close` drains the events queued at that moment (later ones wait for
the next round, so a steady stream cannot keep a drain going). A batch is written with
`pgx.CopyFrom` into the partitioned table, bounded by `WriteTimeout` (default 10 s). The failure of
an attempt decides what follows (RES-002), classified before the pgx error is flattened:

| Failure | Next |
|---|---|
| connection error without a server reply, SQLSTATE class 08, 57P, 53, 40001, 40P01 | retries with backoff (W13): a pool connection killed by a restart fails, a fresh one serves the retry; a database that refuses connections for a while is waited for |
| 23514 (no partition for the day) | create the missing days (W6), then one retry; a 23514 after that is a CHECK constraint of the application, so data. Also when the 23514 answers a retry after a transient failure |
| class 22, 23, 54 (the database refuses a value of a row) | split the batch in halves and write each, down to the single refused row |
| the attempt's own `WriteTimeout`, the emitter aborted by `Close`, anything else (42501) | none: the batch is lost |

The retry is idempotent (RES-004): every event carries an `id` the emitter assigns, and the retry
inserts with `ON CONFLICT (time, id) DO NOTHING` (one `unnest` array per column, so any batch size
fits one statement), so a COPY that committed although its acknowledgement was lost is not stored
twice. IDs count up per emitter from a random start: emit order breaks ties of `time`, and two
emitters share a `(time, id)` only within one microsecond and with overlapping number ranges, in
which case the second row is refused alone (23505) by the split. A deadline is not retried: a slow
database would get the same batch again at once while the queue overflows. The retry statement is
sent in `QueryExecModeDescribeExec`, so its arrays encode on a pool in any exec mode.

The split replaces a list of known causes (W9 removes them at the source, but a json.RawMessage
from a remote party, an application constraint or a jsonb limit can still refuse a row): a refused
row costs about 2 log2(n) COPYs and only itself. Each part is reported in `BatchWritten`, so a
lost row is its own `Batch` with `Events` 1. A batch whose every row is refused costs about 2n
fast-failing statements; that needs a schema the emitter does not match, which the first
`BatchWritten` error shows.

`Flush` returns the errors of every write since the previous `Flush` (at most 16), not only of the
writes it does itself: an event whose size-triggered write failed while `Flush` waited was queued
before the call. A panic in a write, in partition creation or in the Observer's `BatchWritten` is
that batch's error (CON-001), so the writer goroutine survives it.

### W5 One partition per UTC day, no default partition

`time` is the moment `Emit` ran, from `Options.Now` (tests inject a clock). The emit time, not the
start time, decides the partition: a day is then closed a few seconds after midnight, which the
archive relies on, while a three-day job's event still lands in a partition that exists. Partitions
are named `govengers_wide_events_YYYYMMDD`; partitions with other names are never touched.

There is no default partition. It would collect rows of days without a partition, block the cheap
`DROP` of a day (rows of that day could sit in the default) and make creating a day's partition scan
it. Instead `Maintain` creates today and three days ahead, and the writer creates a missing day on
demand (W4), so neither a missed maintenance nor a fresh install loses events.

The primary key is `(time, id)`, with `id` assigned by the emitter (W4) instead of an identity
column: only a key known before the write lets a retry skip what the first attempt stored.

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
through a cursor in a transaction (`FETCH` 1000 at a time, ordered by time and ID), writes gzip
NDJSON into an `io.Pipe`, and the Archiver reads the other end. An Archiver that fails early costs
the events fetched so far, not a read of the whole day. Each line is the event plus the root key
`time` (RFC 3339 UTC with microseconds), which replaces a field of that key as `level` and
`message` do in the JSON emitter: the emit time is otherwise only in the `time` column, and an
archive without it could never be repaired. A `Maintain` whose context ends starts no further
archive, nor does one whose connection closed; the remaining days wait for the next run. Errors keep the end of the context (`errors.Is`
with `context.Canceled` works), so a scheduler tells a shutdown from a failing database. The day counts as archived only when the
Archiver returned nil and the stream ended cleanly; an Archiver that returns nil before reading to
the end fails the day, so a short upload never leads to a drop. A failed day keeps its partition,
is reported in `DayArchived` and the joined error of `Maintain`, and the other days go on. A drop
that fails after a successful archive leaves the day for the next run, which archives it again:
archivers store under a key derived from the day and overwrite.

### W9 Text the database would reject is made storable

One bad row would cost a split (W4), so the row is made acceptable before it is queued: text
columns get invalid UTF-8 and NUL replaced by U+FFFD and are cut to 256 bytes on a character
boundary (index entries stay small; the event column keeps the whole value). encoding/json passes a
`json.RawMessage` or a `MarshalJSON` result through without checking UTF-8 or escapes, so the
encoded event gets invalid UTF-8, the escape `\u0000` and UTF-16 surrogate escapes without their
pair replaced by U+FFFD; jsonb refuses all three. A status outside PostgreSQL's integer is stored
as NULL (the event keeps it). Filter values are cleaned and cut the same way, so a long user ID
still matches.

### W10 Query pages newest first with an opaque keyset cursor

`Query` orders by `(time, id)` descending, served by the primary key, and continues after the
cursor's `(time, id)` with a row comparison, so ties within one microsecond page correctly.
Conditions compare bare columns (indexes on name, user, request and trace ID); the free-text search
is `event::text ILIKE` with escaped wildcards and reads every event of the time range, which is why
callers bound it with a time range and a context deadline. The search text is cleaned like a stored
value and escaped as PostgreSQL writes a jsonb string, so `input "abc"` and `C:\tmp` match the
values they appear in; a JSON fragment with its own quotes does not. `Limit` defaults to 100 and
is cut to 1000. No trigram index: it needs an extension and slows every insert; add one in the application's
own migration when the text search is hot.

### W11 Observer and Prometheus metrics

`Observer` (LIB-012) receives `EventQueued`, `EventDropped`, `BatchWritten`, `Maintained` and
`DayArchived`. `prometheusadapter.Metrics` implements it on the caller's registerer (LIB-011):
queued, written, dropped by reason (`queue_full`, `closed`, `expired` (W14), `write_failed` for the events of a
failed batch), flush duration by outcome, maintenance runs, partitions created and dropped, archive
results and duration. Label values are closed sets and pre-created at zero.

### W12 Tests share one embedded PostgreSQL per package

The harness part is replaced by W15.

`internal/postgrestest` starts one embedded PostgreSQL 18 per test binary and gives each test a
fresh schema with `MigrationV1` applied. Concurrency tests run on a pool whose transactions default
to REPEATABLE READ, and they were checked to fail without the advisory lock and without the READ
COMMITTED pin. No test sleeps: they wait for observer reports or use `Flush`, and clocks are
injected. The harness unpacks the cached PostgreSQL archive with the system `tar` (2 s) instead of
the library's xz decoder, which takes 30-40 s inside a `-race` test binary.

### W13 Transient failures are retried with exponential backoff

Round 1 retried a transient failure once, at once. A database that refuses connections (a restart,
a failover) then lost every batch flushed during the outage within milliseconds, while the queue,
which exists to buffer exactly that, stayed empty. Now a transient failure is attempted up to 10
times, waiting 100 ms doubled per attempt up to 10 s, plus 0-20 % jitter (RES-003): about 33 s in
all, which spans a restart or a failover as long as the database refuses or resets connections.
A host that stops answering (node loss, network partition) blocks the dial or the COPY until
`WriteTimeout`, which is not retried, so its batches are lost (W4). The wait is a `select` on the writer's context, so
`Close` with an ended context still stops it at once. Meanwhile the writer reads no new events:
the queue fills, and only an overflow drops (counted as `queue_full`). The cost: `Flush` and
`Close` wait for the retries during an outage, so `Close` needs a context with the shutdown
deadline. A deadline of an attempt (`WriteTimeout`) is still not retried. The tests start the
backoff at 1 ns through an unexported constructor, so no test waits.

### W14 The writer never writes into a day Maintain may archive

An event can wait in the emitter for long (a backlog of `QueueSize/BatchSize` batches, each up to
10 attempts of `WriteTimeout`). If its day expired meanwhile, its write could commit after
`Maintain` streamed the day to the Archiver and before the drop, so it was dropped without being
archived; or, after the drop, recreate the day's partition, which the next `Maintain` archives
again, overwriting the day's archive with the late events. Locking the day against writers during
the archive would hold a lock for as long as the upload takes and conflicts with the table lock
order of W6. Instead the writer fences: before each write it drops (`DropExpired`, reported per
event) the events whose day leaves the retention within one hour plus the longest write
(`10 x (WriteTimeout + 10 s)`), measured on the store clock. `Maintain` drops a day only after it
left the retention, so a write in flight or a replica clock up to an hour behind cannot reach a
day being archived. The fence never reaches today, which `Maintain` never drops, whatever the
`WriteTimeout`. The fence uses the `RetentionDays` of the emitter's own Store: every Store behind an
emitter must carry the retention of the Store that runs `Maintain`. With `RetentionDays` zero
nothing is fenced. Normal events are written within
seconds and never reach the fence; it costs one comparison per event.

### W15 The tests use govengers/postgrestest

The copied `internal/postgrestest` is gone; `postgresadapter`'s tests use
`github.com/govengers/postgrestest`, pinned to a pseudo-version of its `main` until it is tagged.
`export_test.go` holds the package's `postgrestest.Server` (exported there so the internal and the
external tests share one server; `.govengers.yml` keeps the DES-003 exception for it), and
`TestMain` wraps its `Main` in `goleak.VerifyTestMain`. Each test gets a database copied from a
template with `MigrationV1` applied (`CREATE DATABASE ... TEMPLATE`) instead of a schema migrated
per test, so the tables are in `public`. The test that terminates the archiving backend filters
`pg_stat_activity` on `current_database()`, so it can only hit its own test's connection on a
shared server.

The shared-server launcher (`cmd/postgrestest`) is not wired into `just test`: `postgresadapter`
is the only package with PostgreSQL tests, so a run starts one server either way (measured 5.9 s
through the launcher, 6.0 s without, cold test cache). Wire it when a second PostgreSQL package
appears. The binaries now live in `~/.embedded-postgres-go/postgrestest`, which CI restores,
instead of the OS temp directory, which every fresh runner unpacked again.

### Not done

- No per-row fallback COPY by COPY: the split of W4 finds a refused row in about 2 log2(n) writes.
- No `id` in the archived line: it would replace an application field `id`; `time` and the order
  identify a line within its day.
- No queue-length gauge: queued minus written minus dropped gives the backlog.
- No index for level or outcome filters: they are scanned within the time range.
- No end-to-end test with PostgreSQL in `test/e2e`: the unit tests already run against a real
  PostgreSQL, and a second instance would add its start-up time to every CI run.
