package postgresadapter

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// maintenanceLockSpace is the high half of Maintain's advisory lock key; the low half is the
// table's oid, so the maintenance of tables in different schemas runs independently.
const maintenanceLockSpace = 0x77656c67

// Archiver stores the events of a day before its partition is dropped, usually in object storage.
type Archiver interface {
	// Archive reads events to its end and stores them durably; day is midnight UTC of the archived
	// day. events is gzip-compressed NDJSON: one event per line, as the JSON emitter writes it (key
	// order aside), oldest first. Return nil only when the day is stored: its partition is dropped
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
	maintenance.Err = s.maintain(ctx, maintenance)
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
		return nil, nil, fmt.Errorf("acquire a connection for the maintenance lock: %v", err) //nolint:errorlint // LIB-010
	}
	const key = "($1::bigint << 32) | $2::text::regclass::oid::bigint"
	if _, err := connection.Exec(ctx, "SELECT pg_advisory_lock("+key+")", maintenanceLockSpace, tableName); err != nil {
		discard(ctx, connection)
		return nil, nil, fmt.Errorf("take the maintenance lock: %v", err) //nolint:errorlint // LIB-010
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

// streamDay writes the day's events to out as gzip-compressed NDJSON and returns how many it
// wrote. Rows stream from the database as out consumes them.
func streamDay(ctx context.Context, connection *pgxpool.Conn, streamedDay time.Time, out io.Writer) (int, error) {
	table, err := resolveTable(ctx, connection)
	if err != nil {
		return 0, err
	}
	rows, err := connection.Query(ctx, "SELECT event::text FROM "+table.partition(streamedDay)+" ORDER BY time, id")
	if err != nil {
		return 0, fmt.Errorf("read the day: %v", err) //nolint:errorlint // LIB-010
	}
	defer rows.Close()
	compressor := gzip.NewWriter(out)
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
		return events, fmt.Errorf("read the day: %v", err) //nolint:errorlint // LIB-010
	}
	if err := compressor.Close(); err != nil {
		return events, fmt.Errorf("stream the day: %w", err)
	}
	return events, nil
}
