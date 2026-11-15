package postgresadapter

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// tableName is the partitioned table of MigrationV1; partitions are named tableName_YYYYMMDD.
const tableName = "govengers_wide_events"

// partitionDayLayout is the suffix of a partition's name: the UTC day it holds.
const partitionDayLayout = "20060102"

// daysAhead is how many days after today Maintain creates, so a missed run or two does not leave
// the writer without a partition at midnight.
const daysAhead = 3

// partitionLockTimeout bounds the wait for the ACCESS EXCLUSIVE lock that creating or dropping a
// partition takes on the table. Writers queue behind the waiting lock, so the wait stays short; a
// timed-out maintenance is repeated by the next run.
const partitionLockTimeout = "2s"

const day = 24 * time.Hour

// Store keeps the event table: partitions, retention, archive (Maintain) and the log viewer's
// reads (Query). Events reach it through an Emitter. It is safe for concurrent use.
type Store struct {
	pool          *pgxpool.Pool
	retentionDays int
	archiver      Archiver
	observer      Observer
	now           func() time.Time
}

// Options configures a Store.
type Options struct {
	Pool *pgxpool.Pool // required; its search_path must reach the table of MigrationV1
	// RetentionDays is how many full days Maintain keeps before today's; older days are dropped,
	// after the Archiver stored them. Zero keeps every day.
	RetentionDays int
	Archiver      Archiver // optional; receives each day before it is dropped
	Observer      Observer // optional; nil reports nothing
	// Now is the clock of the events' time and of Maintain's today; nil means time.Now. Tests
	// inject a clock to roll partitions over.
	Now func() time.Time
}

// New validates options and returns a Store. It does no I/O.
func New(options *Options) (*Store, error) {
	if options == nil || options.Pool == nil {
		return nil, errors.New("postgresadapter: Options.Pool is required")
	}
	if options.RetentionDays < 0 {
		return nil, errors.New("postgresadapter: Options.RetentionDays must not be negative")
	}
	store := &Store{
		pool:          options.Pool,
		retentionDays: options.RetentionDays,
		archiver:      options.Archiver,
		observer:      options.Observer,
		now:           options.Now,
	}
	if store.observer == nil {
		store.observer = silentObserver{}
	}
	if store.now == nil {
		store.now = time.Now
	}
	return store, nil
}

// today returns midnight UTC of the store clock's day.
func (s *Store) today() time.Time {
	return s.now().UTC().Truncate(day)
}

// partitionTable names the table and its partitions with the schema the pool's search_path
// resolves the table to, so partitions land next to it whatever the search_path order.
type partitionTable struct {
	schema string
}

func resolveTable(ctx context.Context, querier pgxQuerier) (partitionTable, error) {
	var schema string
	err := querier.QueryRow(ctx, `
SELECT n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.oid = $1::text::regclass`, tableName).Scan(&schema)
	if err != nil {
		return partitionTable{}, fmt.Errorf("find the %s table: %v", tableName, err) //nolint:errorlint // LIB-010: pgx errors are not part of the API
	}
	return partitionTable{schema: schema}, nil
}

func (table partitionTable) parent() string {
	return pgx.Identifier{table.schema, tableName}.Sanitize()
}

func (table partitionTable) partition(day time.Time) string {
	return pgx.Identifier{table.schema, partitionName(day)}.Sanitize()
}

func partitionName(day time.Time) string {
	return tableName + "_" + day.Format(partitionDayLayout)
}

// pgxQuerier is what reading the catalog needs: the pool, a connection or a transaction.
type pgxQuerier interface {
	QueryRow(ctx context.Context, statement string, arguments ...any) pgx.Row
	Query(ctx context.Context, statement string, arguments ...any) (pgx.Rows, error)
}

// database is the pool or one connection of it. Maintain runs everything on the connection that
// holds its advisory lock: taking a second connection while holding one deadlocks when the other
// Maintain calls of the process hold the rest of the pool.
type database interface {
	pgxQuerier
	BeginTx(ctx context.Context, options pgx.TxOptions) (pgx.Tx, error)
}

// partitionDays returns the days of the table's partitions, ascending. Partitions not named by
// this package are ignored, so they are never dropped.
func partitionDays(ctx context.Context, querier pgxQuerier) ([]time.Time, error) {
	rows, err := querier.Query(ctx, `
SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
WHERE i.inhparent = $1::text::regclass`, tableName)
	if err != nil {
		return nil, fmt.Errorf("list the partitions: %v", err) //nolint:errorlint // LIB-010
	}
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("list the partitions: %v", err) //nolint:errorlint // LIB-010
	}
	days := make([]time.Time, 0, len(names))
	for _, name := range names {
		suffix, found := strings.CutPrefix(name, tableName+"_")
		if !found {
			continue
		}
		partitionDay, err := time.Parse(partitionDayLayout, suffix)
		if err != nil || partitionName(partitionDay) != name {
			continue
		}
		days = append(days, partitionDay)
	}
	slices.SortFunc(days, time.Time.Compare)
	return days, nil
}

// ensurePartitions creates the partitions of days that do not exist and returns how many it
// created. Writers and Maintain on every replica may call it at once: the ACCESS EXCLUSIVE lock on
// the table serializes them, and the partitions are listed again under it.
func ensurePartitions(ctx context.Context, executor database, days []time.Time) (int, error) {
	existing, err := partitionDays(ctx, executor)
	if err != nil {
		return 0, err
	}
	if len(missingDays(days, existing)) == 0 {
		return 0, nil
	}
	created := 0
	err = underTableLock(ctx, executor, func(transaction pgx.Tx, table partitionTable) error {
		existing, err := partitionDays(ctx, transaction)
		if err != nil {
			return err
		}
		for _, missing := range missingDays(days, existing) {
			statement := fmt.Sprintf("CREATE TABLE %s PARTITION OF %s FOR VALUES FROM ('%s') TO ('%s')",
				table.partition(missing), table.parent(), timestampLiteral(missing), timestampLiteral(missing.Add(day)))
			if _, err := transaction.Exec(ctx, statement); err != nil {
				return fmt.Errorf("create the partition of %s: %v", missing.Format(time.DateOnly), err) //nolint:errorlint // LIB-010
			}
			created++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return created, nil
}

// dropPartition drops the partition of day, if it still exists.
func dropPartition(ctx context.Context, executor database, partitionDay time.Time) error {
	return underTableLock(ctx, executor, func(transaction pgx.Tx, table partitionTable) error {
		if _, err := transaction.Exec(ctx, "DROP TABLE IF EXISTS "+table.partition(partitionDay)); err != nil {
			return fmt.Errorf("drop the partition of %s: %v", partitionDay.Format(time.DateOnly), err) //nolint:errorlint // LIB-010
		}
		return nil
	})
}

// underTableLock runs change in a READ COMMITTED transaction that holds the ACCESS EXCLUSIVE lock
// on the table. The lock is taken on the table first: creating or dropping a partition locks it
// anyway, and locking the partition first would deadlock with a reader that holds the table and
// waits for the partition.
func underTableLock(ctx context.Context, executor database, change func(transaction pgx.Tx, table partitionTable) error) error {
	transaction, err := executor.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin the partition change: %v", err) //nolint:errorlint // LIB-010
	}
	defer func() { _ = transaction.Rollback(ctx) }() // after Commit a no-op; a failed rollback ends with the connection
	table, err := resolveTable(ctx, transaction)
	if err != nil {
		return err
	}
	if _, err := transaction.Exec(ctx, "SET LOCAL lock_timeout = '"+partitionLockTimeout+"'"); err != nil {
		return fmt.Errorf("set the lock timeout: %v", err) //nolint:errorlint // LIB-010
	}
	if _, err := transaction.Exec(ctx, "LOCK TABLE "+table.parent()+" IN ACCESS EXCLUSIVE MODE"); err != nil {
		return fmt.Errorf("lock the %s table: %v", tableName, err) //nolint:errorlint // LIB-010
	}
	if err := change(transaction, table); err != nil {
		return err
	}
	if err := transaction.Commit(ctx); err != nil {
		return fmt.Errorf("commit the partition change: %v", err) //nolint:errorlint // LIB-010
	}
	return nil
}

// missingDays returns the days of wanted that existing, sorted ascending, lacks.
func missingDays(wanted, existing []time.Time) []time.Time {
	var missing []time.Time
	for _, wantedDay := range wanted {
		if _, found := slices.BinarySearchFunc(existing, wantedDay, time.Time.Compare); !found && !slices.ContainsFunc(missing, wantedDay.Equal) {
			missing = append(missing, wantedDay)
		}
	}
	return missing
}

// timestampLiteral writes a UTC instant as a timestamptz literal that does not depend on the
// session's TimeZone or DateStyle.
func timestampLiteral(instant time.Time) string {
	return instant.UTC().Format("2006-01-02 15:04:05") + "+00"
}
