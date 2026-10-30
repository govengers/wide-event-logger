package wideevent

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"time"
)

// Emitter outputs finished events. Event.Emit calls it once per event; an Emitter shared between units
// of work must be safe for concurrent use.
type Emitter interface {
	Emit(we *Event)
}

// EmitterFunc adapts a function to the Emitter interface.
type EmitterFunc func(we *Event)

// Emit calls emit(we).
func (emit EmitterFunc) Emit(we *Event) { emit(we) }

// MultiEmitter hands every event to each of emitters in order.
func MultiEmitter(emitters ...Emitter) Emitter {
	return EmitterFunc(func(we *Event) {
		for _, emitter := range emitters {
			emitter.Emit(we)
		}
	})
}

// SampledEmitter hands an event to emitter only when sampler keeps it. The sampler sees the finished
// event (tail sampling), so it can keep every failure; wrap the emitter given to Begin or the Gin
// middleware with it.
func SampledEmitter(emitter Emitter, sampler Sampler) Emitter {
	return EmitterFunc(func(we *Event) {
		if sampler.ShouldSample(we) {
			emitter.Emit(we)
		}
	})
}

// JSONEmitterOptions configures a JSONEmitter.
type JSONEmitterOptions struct {
	// Writer receives one JSON line per event, in a single Write call. Default: os.Stdout.
	Writer io.Writer
}

// JSONEmitter writes each event as one JSON line with dot-separated keys nested, plus "level" (info,
// warn or error) and "message" ("wide_event"), which replace fields of the same root key. Field values
// that are durations are written in milliseconds, times in RFC 3339 with nanoseconds, errors as their
// message, NaN and infinities as strings; values inside maps, slices and structs are encoded as they
// are. A value JSON cannot encode at all (a channel, a cycle, a NaN inside a map) is replaced by
// "!unencodable <type>" instead of losing the event. It is safe for concurrent use.
type JSONEmitter struct {
	mutex  sync.Mutex
	writer io.Writer
}

// NewJSONEmitter returns a JSONEmitter. A nil options writes to os.Stdout.
func NewJSONEmitter(options *JSONEmitterOptions) *JSONEmitter {
	var resolved JSONEmitterOptions
	if options != nil {
		resolved = *options
	}
	if resolved.Writer == nil {
		resolved.Writer = os.Stdout
	}
	return &JSONEmitter{writer: resolved.Writer}
}

// Emit writes we as one JSON line. A failed write loses this event only; there is nowhere to report it.
func (emitter *JSONEmitter) Emit(we *Event) {
	line := jsonLine(we)
	emitter.mutex.Lock()
	defer emitter.mutex.Unlock()
	written, err := emitter.writer.Write(line)
	if err != nil && written > 0 && written < len(line) {
		_, _ = emitter.writer.Write([]byte{'\n'}) // end the fragment, so the next event starts on its own line
	}
}

func jsonLine(we *Event) []byte {
	fields := we.Fields()
	line, err := encodeJSONLine(we, fields, jsonValue)
	if err != nil {
		// A value json cannot encode: replace those values. This pass cannot fail, every value in it was
		// marshaled on its own or is a string.
		line, _ = encodeJSONLine(we, fields, encodableJSONValue)
	}
	return line
}

func encodeJSONLine(we *Event, fields []Field, convert func(any) any) ([]byte, error) {
	converted := make([]Field, len(fields))
	for index, field := range fields {
		converted[index] = Field{Key: field.Key, Value: convert(field.Value)}
	}
	document := nestFields(converted)
	document["level"] = levelOf(we)
	document["message"] = defaultEventMessage

	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(document); err != nil {
		return nil, fmt.Errorf("encode wide event: %w", err)
	}
	return buffer.Bytes(), nil
}

// jsonValue converts the types JSON has no good representation for.
func jsonValue(value any) any {
	switch typed := value.(type) {
	case time.Duration:
		return float64(typed) / float64(time.Millisecond)
	case time.Time:
		return typed.Format(time.RFC3339Nano)
	case error:
		return fmt.Sprint(typed)
	case float64:
		return finiteFloat(typed)
	case float32:
		return finiteFloat(float64(typed))
	default:
		return value
	}
}

// finiteFloat returns value, or its text for NaN and the infinities, which JSON cannot represent.
func finiteFloat(value float64) any {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return fmt.Sprint(value)
	}
	return value
}

// encodableJSONValue is jsonValue, replacing a value json cannot encode with its type. Its fmt text could
// show unexported or json:"-" fields, and recurses forever on a cycle.
func encodableJSONValue(value any) any {
	converted := jsonValue(value)
	if _, err := json.Marshal(converted); err != nil {
		return fmt.Sprintf("!unencodable %T", value)
	}
	return converted
}

// Log levels the emitters derive from an event.
const (
	levelInfo    = "info"
	levelWarning = "warn"
	levelError   = "error"
)

// defaultEventMessage is the JSON message of every event and the dev title of an event without a name.
const defaultEventMessage = "wide_event"

// minimumClientErrorStatus is the first response status logged as a warning.
const minimumClientErrorStatus = 400

// levelOf derives the log level: error for HasError, warn for a 4xx response, info otherwise.
func levelOf(we *Event) string {
	if we.HasError() {
		return levelError
	}
	if we.StatusCode() >= minimumClientErrorStatus {
		return levelWarning
	}
	return levelInfo
}
