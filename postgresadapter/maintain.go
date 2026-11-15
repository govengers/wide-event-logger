package postgresadapter

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// maintenanceLockSpace is the high half of Maintain's advisory lock key; the low half is the
// table's oid, so the maintenance of tables in different schemas runs independently.
const maintenanceLockSpace = 0x77656c67

// Archiver stores the events of a day before its partition is dropped, usually in object storage.
type Archiver interface {
	// Archive reads events to its end and stores them durably; day is midnight UTC of the archived
	// day. events is gzip-compressed NDJSON: one event per line, oldest first, as the JSON emitter
	// writes it (key order aside) plus the root key "time", the emit time in RFC 3339 UTC with
	// microseconds, which replaces a field of that key. Return nil only when the day is stored: its partition is dropped
	// next. Returning nil before the end of the stream counts as a failure. A day may be archived
	// again when dropping its partition failed after the archive, so store it under a key derived
	// from day and overwrite.
	Archive(ctx context.Context, day time.Time, events io.Reader) error
}

// ArchiverFunc adapts a function to the Archiver interface.
type ArchiverFunc func(ctx context.Context, day time.Time, events io.Reader) error

// Archive calls archive(ctx, day, events).
func (archive ArchiverFunc) Archive(ctx context.Context, day time.Time, events io.Reader) error {
	return archive(ctx, day, events)
}

var newline = []byte{'\n'}

// errArchiverReturned ends the stream of a day the Archiver stopped reading.
var errArchiverReturned = errors.New("the Archiver returned before the end of the stream")

// Maintain creates the partitions of today and the next days, and drops the days older than the
// retention, each after the Archiver stored it. A day whose archive fails keeps its partition and
// is tried again by the next call. Run it at startup and once a day, for example as a
// govengers/jobs schedule; calls on several replicas at once are serialized by an advisory lock,
// so each day is archived once. Writers and readers go on meanwhile, except for the moment a
// partition is created or dropped. It returns every error of the call, joined.
func (s *Store) Maintain(ctx context.Context) error {
	start := time.Now()
	maintenance := &Maintenance{}
	if err := s.maintain(ctx, maintenance); err != nil {
		maintenance.Err = fmt.Errorf("postgresadapter: maintain: %w", err)
	}
	maintenance.Duration = time.Since(start)
	s.observer.Maintained(maintenance)
	return maintenance.Err
}

func (s *Store) maintain(ctx context.Context, maintenance *Maintenance) error {
	connection, unlock, err := s.lockMaintenance(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	today := s.today()
	wanted := make([]time.Time, 0, daysAhead+1)
	for offset := range daysAhead + 1 {
		wanted = append(wanted, today.Add(time.Duration(offset)*day))
	}
	created, createError := ensurePartitions(ctx, connection, wanted)
	maintenance.Created = created
	if s.retentionDays == 0 {
		return createError
	}
	dropped, dropError := s.dropExpired(ctx, connection, s.retentionCutoff())
	maintenance.Dropped = dropped
	return errors.Join(createError, dropError)
}

// retentionCutoff is the first day the retention keeps; AddDate cannot overflow like a Duration
// of many days would.
func (s *Store) retentionCutoff() time.Time {
	return s.today().AddDate(0, 0, -s.retentionDays)
}

// lockMaintenance waits for Maintain's advisory lock on a connection of its own and returns it
// with the function that releases the lock and the connection. A connection whose lock state is
// unknown after an error is closed, which releases the lock with the session.
func (s *Store) lockMaintenance(ctx context.Context) (*pgxpool.Conn, func(), error) {
	connection, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, nil, databaseError(ctx, "acquire a connection for the maintenance lock", err)
	}
	const key = "($1::bigint << 32) | $2::text::regclass::oid::bigint"
	if _, err := connection.Exec(ctx, "SELECT pg_advisory_lock("+key+")", maintenanceLockSpace, tableName); err != nil {
		discard(ctx, connection)
		return nil, nil, databaseError(ctx, "take the maintenance lock", err)
	}
	return connection, func() {
		if _, err := connection.Exec(ctx, "SELECT pg_advisory_unlock("+key+")", maintenanceLockSpace, tableName); err != nil {
			discard(ctx, connection)
			return
		}
		connection.Release()
	}, nil
}

// discard closes the connection and hands it back, so the pool drops it.
func discard(ctx context.Context, connection *pgxpool.Conn) {
	_ = connection.Conn().Close(ctx) // closes the socket even when ctx has ended
	connection.Release()
}

// dropExpired archives and drops the partitions of the days before cutoff, oldest first, and
// returns how many it dropped.
func (s *Store) dropExpired(ctx context.Context, connection *pgxpool.Conn, cutoff time.Time) (int, error) {
	days, err := partitionDays(ctx, connection)
	if err != nil {
		return 0, err
	}
	dropped := 0
	var errs []error
	for _, partitionDay := range days {
		if !partitionDay.Before(cutoff) {
			break
		}
		if ctx.Err() != nil {
			errs = append(errs, ctx.Err()) // the days left wait for the next run
			break
		}
		if s.archiver != nil {
			if err := s.archiveDay(ctx, connection, partitionDay); err != nil {
				errs = append(errs, fmt.Errorf("archive %s: %w", partitionDay.Format(time.DateOnly), err))
				continue
			}
		}
		if err := dropPartition(ctx, connection, partitionDay); err != nil {
			errs = append(errs, err)
			continue
		}
		dropped++
	}
	return dropped, errors.Join(errs...)
}

// archiveDay streams the day's partition from connection to the Archiver and reports the attempt.
// The connection is free again when it returns.
func (s *Store) archiveDay(ctx context.Context, connection *pgxpool.Conn, archivedDay time.Time) error {
	start := time.Now()
	reader, writer := io.Pipe()
	type streamResult struct {
		events int
		err    error
	}
	streamed := make(chan streamResult, 1)
	go func() {
		var result streamResult
		defer func() {
			if recovered := recover(); recovered != nil {
				result.err = fmt.Errorf("stream the day: panic: %v", recovered)
			}
			_ = writer.CloseWithError(result.err) // nil ends the stream with io.EOF
			streamed <- result
		}()
		result.events, result.err = streamDay(ctx, connection, archivedDay, writer)
	}()
	archiveError := s.callArchiver(ctx, archivedDay, reader)
	_ = reader.CloseWithError(errArchiverReturned) // unblocks the stream when the Archiver stopped reading
	result := <-streamed
	err := archiveError
	if err == nil && result.err != nil {
		err = result.err
	}
	s.observer.DayArchived(&Archive{Day: archivedDay, Events: result.events, Duration: time.Since(start), Err: err})
	return err
}

// callArchiver runs the Archiver; a panic is its error.
func (s *Store) callArchiver(ctx context.Context, archivedDay time.Time, events io.Reader) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("the Archiver panicked: %v", recovered)
		}
	}()
	return s.archiver.Archive(ctx, archivedDay, events)
}

// archiveFetchSize is how many events streamDay reads from its cursor at a time.
const archiveFetchSize = 1000

// streamDay writes the day's events to out as gzip-compressed NDJSON and returns how many it
// wrote. It reads through a cursor as out consumes the events, so an Archiver that stops early
// costs no more than the events it read.
func streamDay(ctx context.Context, connection *pgxpool.Conn, streamedDay time.Time, out io.Writer) (int, error) {
	transaction, err := connection.Begin(ctx)
	if err != nil {
		return 0, databaseError(ctx, "begin reading the day", err)
	}
	defer func() { _ = transaction.Rollback(ctx) }() // closes the cursor; a failed rollback ends with the connection
	table, err := resolveTable(ctx, transaction)
	if err != nil {
		return 0, err
	}
	_, err = transaction.Exec(ctx, `DECLARE archived_day NO SCROLL CURSOR FOR
SELECT (event || jsonb_build_object('time', to_char(time AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')))::text
FROM `+table.partition(streamedDay)+" ORDER BY time, id")
	if err != nil {
		return 0, databaseError(ctx, "read the day", err)
	}
	compressor := gzip.NewWriter(out)
	events := 0
	for {
		fetched, err := streamEvents(ctx, transaction, compressor)
		events += fetched
		if err != nil {
			return events, err
		}
		if fetched < archiveFetchSize {
			break
		}
	}
	if err := compressor.Close(); err != nil {
		return events, fmt.Errorf("stream the day: %w", err)
	}
	return events, nil
}

// streamEvents writes the next events of streamDay's cursor to compressor and returns how many.
func streamEvents(ctx context.Context, transaction pgx.Tx, compressor io.Writer) (int, error) {
	rows, err := transaction.Query(ctx, "FETCH "+strconv.Itoa(archiveFetchSize)+" FROM archived_day")
	if err != nil {
		return 0, databaseError(ctx, "read the day", err)
	}
	defer rows.Close()
	events := 0
	for rows.Next() {
		// the raw value is pgx's read buffer: write it as it is, never append to it
		if _, err := compressor.Write(rows.RawValues()[0]); err != nil {
			return events, fmt.Errorf("stream the day: %w", err)
		}
		if _, err := compressor.Write(newline); err != nil {
			return events, fmt.Errorf("stream the day: %w", err)
		}
		events++
	}
	if err := rows.Err(); err != nil {
		return events, databaseError(ctx, "read the day", err)
	}
	return events, nil
}
