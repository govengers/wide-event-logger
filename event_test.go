package wideevent

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// captureEmitter collects emitted events for assertions. It is safe for concurrent use.
type captureEmitter struct {
	mutex  sync.Mutex
	events []*Event
}

func (capture *captureEmitter) Emit(we *Event) {
	capture.mutex.Lock()
	defer capture.mutex.Unlock()
	capture.events = append(capture.events, we)
}

func (capture *captureEmitter) emitted() []*Event {
	capture.mutex.Lock()
	defer capture.mutex.Unlock()
	return append([]*Event(nil), capture.events...)
}

// valuesOf returns the fields of we by key.
func valuesOf(we *Event) map[string]any {
	values := map[string]any{}
	for _, field := range we.Fields() {
		values[field.Key] = field.Value
	}
	return values
}

type textID string

func (id textID) String() string { return string(id) }

// nilPointerError has an Error method that panics on a nil receiver.
type nilPointerError struct{ message string }

func (failure *nilPointerError) Error() string { return failure.message }

func TestTypedSettersRecordValues(t *testing.T) {
	now := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	we := Begin(nil, "job").
		String("string", "value").
		Int("int", 42).
		Int64("int64", 7).
		Float64("float", 1.5).
		Bool("bool", true).
		Duration("duration_field", time.Second).
		Time("time", now).
		Err("error_field", errors.New("boom")).
		UUID("uuid", textID("0192")).
		UserID(textID("user-1")).
		UserName("ada").
		UserRole("admin").
		Set("any", []int{1})

	require.Equal(t, map[string]any{
		KeyName:          "job",
		"string":         "value",
		"int":            42,
		"int64":          int64(7),
		"float":          1.5,
		"bool":           true,
		"duration_field": time.Second,
		"time":           now,
		"error_field":    "boom",
		"uuid":           "0192",
		"user.id":        "user-1",
		"user.name":      "ada",
		"user.role":      "admin",
		"any":            []int{1},
	}, valuesOf(we))
}

func TestNilArgumentsRecordNothing(t *testing.T) {
	var typedNil *nilPointerError
	we := Begin(nil, "job").Err("nil_error", nil).UUID("nil_id", nil).UserID(nil).Err("typed_nil", typedNil)

	values := valuesOf(we)
	require.NotContains(t, values, "nil_error")
	require.NotContains(t, values, "nil_id")
	require.NotContains(t, values, "user.id")
	require.Contains(t, values, "typed_nil", "a typed nil error is not nil and must not panic")
}

func TestSettingAKeyAgainReplacesItsValueAndKeepsItsPosition(t *testing.T) {
	we := Begin(nil, "job").String("first", "a").String("second", "b").String("first", "c")

	require.Equal(t, []Field{{KeyName, "job"}, {"first", "c"}, {"second", "b"}}, we.Fields())
}

func TestSuccessAndFailureSetTheOutcome(t *testing.T) {
	we := Begin(nil, "job").Failure(errors.New("conflict"))
	require.Equal(t, OutcomeFailure, valuesOf(we)[KeyOutcome])
	require.Equal(t, "conflict", valuesOf(we)[KeyError])

	we.Success()
	require.Equal(t, OutcomeSuccess, valuesOf(we)[KeyOutcome])

	require.NotContains(t, valuesOf(Begin(nil, "job").Failure(nil)), KeyError)
}

func TestNilEventMethodsDoNothing(t *testing.T) {
	var we *Event
	require.NotPanics(t, func() {
		we.String("a", "b").Int("c", 1).Success().Failure(errors.New("x")).UserID(textID("1")).Emit()
	})
	require.Nil(t, we.Fields())
	require.Empty(t, we.FieldsMap())
	require.False(t, we.HasError())
	require.Zero(t, we.StatusCode())
}

func TestEmitSetsDurationAndEmitsOnce(t *testing.T) {
	capture := &captureEmitter{}
	we := Begin(capture, "job")

	we.Emit()
	we.Emit()

	require.Len(t, capture.emitted(), 1)
	duration, ok := valuesOf(we)[KeyDuration].(time.Duration)
	require.True(t, ok)
	require.GreaterOrEqual(t, duration, time.Duration(0))
}

func TestWritesAfterEmitAreDropped(t *testing.T) {
	we := Begin(&captureEmitter{}, "job")
	we.Emit()

	we.String("late", "write").Failure(errors.New("too late"))

	values := valuesOf(we)
	require.NotContains(t, values, "late")
	require.NotContains(t, values, KeyOutcome)
}

func TestBeginWithNilEmitterDiscards(t *testing.T) {
	require.NotPanics(t, Begin(nil, "job").Emit)
}

func TestHasErrorAndStatusCodeReadTheLatestValues(t *testing.T) {
	testCases := []struct {
		name     string
		record   func(we *Event)
		hasError bool
		status   int
	}{
		{"nothing", func(*Event) {}, false, 0},
		{"failure", func(we *Event) { we.Failure(nil) }, true, 0},
		{"success after failure", func(we *Event) { we.Failure(nil).Success() }, false, 0},
		{"server error", func(we *Event) { we.Int(KeyResponseStatus, 503) }, true, 503},
		{"client error", func(we *Event) { we.Int(KeyResponseStatus, 404) }, false, 404},
		{"status replaced", func(we *Event) { we.Int(KeyResponseStatus, 500).Int(KeyResponseStatus, 200) }, false, 200},
		{"status not an int", func(we *Event) { we.String(KeyResponseStatus, "500") }, false, 0},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			we := Begin(nil, "job")
			testCase.record(we)
			require.Equal(t, testCase.hasError, we.HasError())
			require.Equal(t, testCase.status, we.StatusCode())
		})
	}
}

func TestConcurrentWritesKeepEveryKey(t *testing.T) {
	we := Begin(nil, "job")
	var waitGroup sync.WaitGroup
	for worker := range 50 {
		waitGroup.Go(func() {
			we.Int(fmt.Sprintf("worker.%d", worker), worker)
			_ = we.HasError()
			_ = we.Fields()
		})
	}
	waitGroup.Wait()

	require.Len(t, we.Fields(), 51)
}

func TestFieldsMapNests(t *testing.T) {
	we := Begin(nil, "job").String("db.operation", "update").Int("db.rows", 3)

	require.Equal(t, map[string]any{
		KeyName: "job",
		"db":    map[string]any{"operation": "update", "rows": 3},
	}, we.FieldsMap())
}

func TestContextRoundTrip(t *testing.T) {
	require.Nil(t, FromContext(t.Context()))

	we := Begin(nil, "job")
	require.Same(t, we, FromContext(NewContext(t.Context(), we)))
}

// countingError counts calls of String and Error.
type countingError struct{ calls *int }

func (stringer countingError) String() string {
	*stringer.calls++
	return "id"
}

func (stringer countingError) Error() string { return stringer.String() }

func TestNilEventCallsNoValueMethods(t *testing.T) {
	calls := 0
	var we *Event

	we.UUID("id", countingError{&calls}).UserID(countingError{&calls}).Err("error", countingError{&calls})

	require.Zero(t, calls)
}
