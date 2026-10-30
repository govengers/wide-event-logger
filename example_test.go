package wideevent_test

import (
	"context"
	"errors"
	"fmt"
	"os"

	wideevent "github.com/govengers/wide-event-logger"
)

// A background job starts its own event, passes it down through the context and emits it when done.
func ExampleBegin() {
	emitter := wideevent.EmitterFunc(func(we *wideevent.Event) {
		fields := we.FieldsMap()
		fmt.Println(fields["name"], fields["outcome"], fields["error"], fields["job"])
	})

	runJob := func(ctx context.Context) error {
		wideevent.FromContext(ctx).Int("job.items", 3)
		return errors.New("upstream timeout")
	}

	we := wideevent.Begin(emitter, "job.sync")
	if err := runJob(wideevent.NewContext(context.Background(), we)); err != nil {
		we.Failure(err)
	} else {
		we.Success()
	}
	we.Emit()
	// Output: job.sync failure upstream timeout map[items:3]
}

// FromContext returns nil outside a unit of work, and the methods of a nil event do nothing.
func ExampleFromContext() {
	we := wideevent.FromContext(context.Background())
	we.String("db.operation", "update")
	fmt.Println(we == nil)
	// Output: true
}

// Keep every failure but only every 100th successful event.
func ExampleSampledEmitter() {
	emitter := wideevent.SampledEmitter(
		wideevent.NewJSONEmitter(&wideevent.JSONEmitterOptions{Writer: os.Stdout}),
		wideevent.CompositeSampler(wideevent.AlwaysOnError(), wideevent.Rate(100)),
	)
	wideevent.Begin(emitter, "job.cleanup").Success().Emit() // dropped
	// Output:
}

func ExampleNewDevEmitter() {
	emitter, err := wideevent.NewDevEmitter(&wideevent.DevEmitterOptions{
		Categories:      []wideevent.Category{{Name: "health", PathPrefixes: []string{"/health"}}},
		MutedCategories: []string{"health"},
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	wideevent.Begin(emitter, "startup").Success().Emit()
}
