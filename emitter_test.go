package wideevent

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// emitJSON emits one event built by record through a JSONEmitter and returns the decoded line.
func emitJSON(t *testing.T, record func(we *Event)) map[string]any {
	t.Helper()
	var buffer bytes.Buffer
	we := Begin(NewJSONEmitter(&JSONEmitterOptions{Writer: &buffer}), "job")
	record(we)
	we.Emit()

	require.Equal(t, 1, strings.Count(buffer.String(), "\n"), "one line per event")
	var document map[string]any
	require.NoError(t, json.Unmarshal(buffer.Bytes(), &document))
	return document
}

func TestJSONEmitterWritesNestedFieldsLevelAndMessage(t *testing.T) {
	document := emitJSON(t, func(we *Event) {
		we.String("db.operation", "update").String("html", "<b>").Success()
	})

	require.Equal(t, "job", document["name"])
	require.Equal(t, "success", document["outcome"])
	require.Equal(t, map[string]any{"operation": "update"}, document["db"])
	require.Equal(t, "<b>", document["html"])
	require.Equal(t, "info", document["level"])
	require.Equal(t, "wide_event", document["message"])
	require.Contains(t, document, "duration")
}

func TestJSONEmitterConvertsValues(t *testing.T) {
	emittedAt := time.Date(2026, 10, 30, 12, 0, 0, 5, time.UTC)
	document := emitJSON(t, func(we *Event) {
		we.Duration("elapsed", 1500*time.Microsecond).
			Time("at", emittedAt).
			Set("error_value", errors.New("boom")).
			Float64("not_a_number", math.NaN()).
			Float64("infinite", math.Inf(1)).
			Set("float32", float32(math.Inf(-1))).
			Set("channel", make(chan int)).
			Float64("ratio", 0.5)
	})

	require.InDelta(t, 1.5, document["elapsed"], 1e-9)
	require.Equal(t, "2026-10-30T12:00:00.000000005Z", document["at"])
	require.Equal(t, "boom", document["error_value"])
	require.Equal(t, "NaN", document["not_a_number"])
	require.Equal(t, "+Inf", document["infinite"])
	require.Equal(t, "-Inf", document["float32"])
	require.IsType(t, "", document["channel"], "an unencodable value is written as text, the event survives")
	require.InDelta(t, 0.5, document["ratio"], 1e-9)
}

func TestJSONEmitterSurvivesANestedUnencodableValue(t *testing.T) {
	document := emitJSON(t, func(we *Event) {
		we.Set("nested", map[string]any{"value": math.NaN()}).String("kept", "yes")
	})

	require.Equal(t, "yes", document["kept"])
	require.IsType(t, "", document["nested"])
}

func TestJSONEmitterLevel(t *testing.T) {
	testCases := []struct {
		name   string
		record func(we *Event)
		level  string
	}{
		{"success", func(we *Event) { we.Success() }, "info"},
		{"client error", func(we *Event) { we.Int(KeyResponseStatus, 404) }, "warn"},
		{"server error", func(we *Event) { we.Int(KeyResponseStatus, 502) }, "error"},
		{"failure", func(we *Event) { we.Int(KeyResponseStatus, 200).Failure(nil) }, "error"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.level, emitJSON(t, testCase.record)["level"])
		})
	}
}

func TestJSONEmitterWritesWholeLinesConcurrently(t *testing.T) {
	var buffer bytes.Buffer
	emitter := NewJSONEmitter(&JSONEmitterOptions{Writer: &buffer})
	var waitGroup sync.WaitGroup
	for range 20 {
		waitGroup.Go(func() { Begin(emitter, "job").String("payload", strings.Repeat("x", 1000)).Emit() })
	}
	waitGroup.Wait()

	lines := strings.Split(strings.TrimSuffix(buffer.String(), "\n"), "\n")
	require.Len(t, lines, 20)
	for _, line := range lines {
		require.True(t, json.Valid([]byte(line)), line)
	}
}

func TestNewJSONEmitterDefaultsToStdout(t *testing.T) {
	require.NotNil(t, NewJSONEmitter(nil).writer)
}

func TestMultiEmitterHandsEveryEventToEachEmitter(t *testing.T) {
	first, second := &captureEmitter{}, &captureEmitter{}

	Begin(MultiEmitter(first, second), "job").Emit()

	require.Len(t, first.emitted(), 1)
	require.Len(t, second.emitted(), 1)
}

func TestSampledEmitterSeesTheFinishedEvent(t *testing.T) {
	capture := &captureEmitter{}
	emitter := SampledEmitter(capture, AlwaysOnError())

	Begin(emitter, "kept").Failure(errors.New("x")).Emit()
	Begin(emitter, "dropped").Success().Emit()

	events := capture.emitted()
	require.Len(t, events, 1)
	require.Equal(t, "kept", valuesOf(events[0])[KeyName])
}

// credentials has a field JSON must never show and a NaN that makes JSON fail.
type credentials struct {
	User     string  `json:"user"`
	Password string  `json:"-"`
	Score    float64 `json:"score"`
	token    string
}

func TestJSONEmitterFallbackShowsNoHiddenFields(t *testing.T) {
	document := emitJSON(t, func(we *Event) {
		we.Set("login", credentials{User: "ada", Password: "hunter2", Score: math.NaN(), token: "abc"})
	})

	require.Equal(t, "!unencodable wideevent.credentials", document["login"])
}

func TestJSONEmitterSurvivesACycle(t *testing.T) {
	cycle := map[string]any{}
	cycle["self"] = cycle

	document := emitJSON(t, func(we *Event) { we.Set("cycle", cycle).String("kept", "yes") })

	require.Equal(t, "!unencodable map[string]interface {}", document["cycle"])
	require.Equal(t, "yes", document["kept"])
}

func TestJSONEmitterReservesLevelAndMessage(t *testing.T) {
	document := emitJSON(t, func(we *Event) { we.String("level", "debug") })
	require.Equal(t, "info", document["level"])
}

// failingWriter writes the given number of bytes of its first writes and fails them; later writes succeed.
type failingWriter struct {
	bytes.Buffer
	acceptedBytes []int
}

func (writer *failingWriter) Write(data []byte) (int, error) {
	if len(writer.acceptedBytes) == 0 {
		return writer.Buffer.Write(data)
	}
	accepted := min(writer.acceptedBytes[0], len(data))
	writer.acceptedBytes = writer.acceptedBytes[1:]
	written, _ := writer.Buffer.Write(data[:accepted])
	return written, errors.New("disk full")
}

func TestJSONEmitterEndsAFragmentAfterAPartialWrite(t *testing.T) {
	// a partial write, then a write that fails completely, then success
	writer := &failingWriter{acceptedBytes: []int{10, 0}}
	emitter := NewJSONEmitter(&JSONEmitterOptions{Writer: writer})

	Begin(emitter, "partial").Emit()
	Begin(emitter, "lost").Emit()
	Begin(emitter, "kept").Emit()

	lines := strings.Split(strings.TrimSuffix(writer.String(), "\n"), "\n")
	require.Len(t, lines, 2)
	require.False(t, json.Valid([]byte(lines[0])))
	require.True(t, json.Valid([]byte(lines[1])), "the next event is a whole line")
	require.Contains(t, lines[1], `"kept"`)
}

// panickingMarshaler panics in MarshalJSON.
type panickingMarshaler struct{}

func (panickingMarshaler) MarshalJSON() ([]byte, error) { panic("broken marshaler") }

func TestJSONEmitterSurvivesAPanickingMarshaler(t *testing.T) {
	document := emitJSON(t, func(we *Event) { we.Set("broken", panickingMarshaler{}).String("kept", "yes") })

	require.Equal(t, "!unencodable wideevent.panickingMarshaler", document["broken"])
	require.Equal(t, "yes", document["kept"])
}

func TestJSONEmitterSurvivesTooDeepNesting(t *testing.T) {
	var deep any = "leaf"
	for range 10001 {
		deep = []any{deep}
	}

	document := emitJSON(t, func(we *Event) { we.Set("deep", deep).String("kept", "yes") })

	require.Equal(t, "!unencodable []interface {}", document["deep"])
	require.Equal(t, "yes", document["kept"])
}

// cyclicPanicMarshaler panics with a map that contains itself.
type cyclicPanicMarshaler struct{}

func (cyclicPanicMarshaler) MarshalJSON() ([]byte, error) {
	cycle := map[string]any{}
	cycle["self"] = cycle
	panic(cycle)
}

func TestJSONEmitterSurvivesACyclicPanicValue(t *testing.T) {
	document := emitJSON(t, func(we *Event) { we.Set("broken", cyclicPanicMarshaler{}) })
	require.Equal(t, "!unencodable wideevent.cyclicPanicMarshaler", document["broken"])
}

func TestJSONEmitterCountsTheKeyPathIntoTheNestingLimit(t *testing.T) {
	var deep any = "leaf"
	for range 9998 {
		deep = []any{deep}
	}

	document := emitJSON(t, func(we *Event) { we.Set("fits", deep).Set("too.deep", deep) })

	require.IsType(t, []any{}, document["fits"], "9998 levels, the root object and a reserved _value level")
	require.Equal(t, map[string]any{"deep": "!unencodable []interface {}"}, document["too"])
}

func TestJSONEmitterCountsAConflictLevelIntoTheNestingLimit(t *testing.T) {
	var deep any = "leaf"
	for range 9999 {
		deep = []any{deep}
	}

	document := emitJSON(t, func(we *Event) { we.Set("a", deep).Int("a.b", 1) })

	require.Equal(t, map[string]any{"_value": "!unencodable []interface {}", "b": float64(1)}, document["a"])
}

func TestJSONEmitterWritesTooDeepKeysFlat(t *testing.T) {
	key := strings.Repeat("a.", 10000) + "b"

	document := emitJSON(t, func(we *Event) { we.Int(key, 1).String("kept", "yes") })

	require.Contains(t, document, key)
	require.Equal(t, "yes", document["kept"])
}

// panickingMessageError panics in Error with a map that contains itself.
type panickingMessageError struct{}

func (panickingMessageError) Error() string {
	cycle := map[string]any{}
	cycle["self"] = cycle
	panic(cycle)
}

// failingMarshaler returns an error whose Error method panics.
type failingMarshaler struct{}

func (failingMarshaler) MarshalJSON() ([]byte, error) { return nil, panickingMessageError{} }

func TestJSONEmitterNeverFormatsAMarshalError(t *testing.T) {
	document := emitJSON(t, func(we *Event) { we.Set("broken", failingMarshaler{}) })
	require.Equal(t, "!unencodable wideevent.failingMarshaler", document["broken"])
}
