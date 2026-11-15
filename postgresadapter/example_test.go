package postgresadapter_test

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	wideevent "github.com/govengers/wide-event-logger"
	"github.com/govengers/wide-event-logger/postgresadapter"
)

// An application writes its events to stdout and to PostgreSQL, keeps 30 days and archives older
// days to object storage. The example only compiles: it needs a database with MigrationV1 applied.
func ExampleNewEmitter() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, "postgres://localhost/orders")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer pool.Close()
	store, err := postgresadapter.New(&postgresadapter.Options{
		Pool:          pool,
		RetentionDays: 30,
		Archiver: postgresadapter.ArchiverFunc(func(ctx context.Context, day time.Time, events io.Reader) error {
			// upload events to "wide-events/" + day.Format(time.DateOnly) + ".ndjson.gz"
			_, err := io.Copy(io.Discard, events)
			return err
		}),
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	if err := store.Maintain(ctx); err != nil { // at startup, then once a day
		fmt.Println(err)
	}
	emitter, err := postgresadapter.NewEmitter(&postgresadapter.EmitterOptions{Store: store})
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = emitter.Close(shutdown) // events not written in time are reported to the Observer
	}()

	we := wideevent.Begin(wideevent.MultiEmitter(wideevent.NewJSONEmitter(nil), emitter), "orders.place")
	we.Success()
	we.Emit()
}

// A log viewer shows the server errors of the last hour, a page at a time.
func ExampleStore_Query() {
	var store *postgresadapter.Store // from postgresadapter.New
	ctx := context.Background()
	filter := &postgresadapter.Filter{From: time.Now().Add(-time.Hour), MinimumStatus: 500, Limit: 50}
	for {
		page, err := store.Query(ctx, filter)
		if err != nil {
			fmt.Println(err)
			return
		}
		for _, record := range page.Records {
			fmt.Println(record.Time, record.Name, record.Status)
		}
		if page.Next == "" {
			return
		}
		filter.Cursor = page.Next
	}
}
