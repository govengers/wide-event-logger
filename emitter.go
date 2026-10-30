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
// are. A value JSON cannot encode (a channel, a cycle, a NaN inside a map, a MarshalJSON that fails or
// panics) is replaced by "!unencodable <type>" instead of losing the event. It is safe for concurrent use.
type JSONEmitter struct {
	mutex   sync.Mutex
	writer  io.Writer
	midLine bool // a failed write left a fragment without its newline
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
// A fragment a failed write left behind is ended first, so the next event starts on its own line.
func (emitter *JSONEmitter) Emit(we *Event) {
	line := jsonLine(we)
	emitter.mutex.Lock()
	defer emitter.mutex.Unlock()
	if emitter.midLine {
		line = append([]byte{'\n'}, line...)
	}
	written, _ := emitter.writer.Write(line)
	if written > 0 {
		emitter.midLine = written < len(line)
	}
}

// jsonLine encodes every field value on its own, so one value JSON cannot encode costs that value only.
func jsonLine(we *Event) []byte {
	fields := we.Fields()
	encoded := make([]Field, len(fields))
	for index, field := range fields {
		encoded[index] = Field{Key: field.Key, Value: encodeJSONValue(field.Value)}
	}
	document := nestFields(encoded)
	document["level"] = levelOf(we)
	document["message"] = defaultEventMessage
	line, err := marshalJSON(document)
	if err != nil {
		return nil // unreachable: the document holds only strings and values marshalJSON encoded already
	}
	return append(line, '\n')
}

// encodeJSONValue encodes one field value, or "!unencodable <type>". The placeholder never shows the
// value: its fmt text could reveal unexported or json:"-" fields, and recurses forever on a cycle. The
// encoding is validated like the final line will be, which rejects nesting deeper than 10000 levels.
func encodeJSONValue(value any) json.RawMessage {
	encoded, err := marshalJSON(jsonValue(value))
	if err != nil || !json.Valid(encoded) {
		encoded, _ = marshalJSON(unencodable(value)) // a string always encodes
	}
	return encoded
}

// marshalJSON encodes value without escaping HTML. A panicking MarshalJSON or MarshalText is an error.
func marshalJSON(value any) (encoded []byte, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("marshal %T: panic: %v", value, recovered)
		}
	}()
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if encodeError := encoder.Encode(value); encodeError != nil {
		return nil, fmt.Errorf("marshal %T: %w", value, encodeError)
	}
	return bytes.TrimSuffix(buffer.Bytes(), []byte{'\n'}), nil
}

func unencodable(value any) string {
	return fmt.Sprintf("!unencodable %T", value)
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
