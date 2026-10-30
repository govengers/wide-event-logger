package wideevent

import (
	"fmt"
	"sync"
	"time"
)

// Keys of the fields this package reads itself: HasError, StatusCode and the emitters interpret them.
const (
	KeyName           = "name"
	KeyOutcome        = "outcome"
	KeyError          = "error"
	KeyDuration       = "duration"
	KeyRequestMethod  = "request.method"
	KeyRequestPath    = "request.path"
	KeyResponseStatus = "response.status"
)

// Values of the outcome field, set by Success and Failure.
const (
	OutcomeSuccess = "success"
	OutcomeFailure = "failure"
)

// minimumServerErrorStatus is the first response status HasError counts as an error.
const minimumServerErrorStatus = 500

// initialFieldCapacity fits a typical HTTP request event without growing.
const initialFieldCapacity = 32

// Field is one key-value pair of an event. Dots in the key nest the value in the JSON output.
type Field struct {
	Key   string
	Value any
}

// Event accumulates the fields of one unit of work and emits them once. Setting a key again replaces its
// value; fields keep the order in which their keys were first set. All methods are safe for concurrent use
// and do nothing on a nil *Event. After Emit the event is frozen: later writes are dropped, so work that
// outlives the unit of work needs its own event.
//
// Reserved keys: the key segment "_value" holds the value of a key that is also a prefix of other keys
// (see FieldsMap), and the JSON emitter writes the root keys "level" and "message" itself.
type Event struct {
	mutex     sync.Mutex
	values    map[string]any
	keys      []string // in the order they were first set
	emitter   Emitter
	startTime time.Time
	emitted   bool
}

// Begin starts an event named name that Emit hands to emitter. A nil emitter discards the event.
// The caller ends the unit of work with Emit, usually deferred.
func Begin(emitter Emitter, name string) *Event {
	we := &Event{
		values:    make(map[string]any, initialFieldCapacity),
		keys:      make([]string, 0, initialFieldCapacity),
		emitter:   emitter,
		startTime: time.Now(),
	}
	return we.String(KeyName, name)
}

// Emit sets the duration since Begin and hands the event to its emitter. Only the first call emits.
func (we *Event) Emit() {
	if we == nil {
		return
	}
	we.mutex.Lock()
	if we.emitted {
		we.mutex.Unlock()
		return
	}
	we.put(KeyDuration, time.Since(we.startTime))
	we.emitted = true
	we.mutex.Unlock()

	if we.emitter != nil {
		we.emitter.Emit(we)
	}
}

// Set records value under key, replacing an earlier value of the same key.
func (we *Event) Set(key string, value any) *Event {
	if we == nil {
		return nil
	}
	we.mutex.Lock()
	defer we.mutex.Unlock()
	if !we.emitted {
		we.put(key, value)
	}
	return we
}

// put stores a value. The caller holds the mutex.
func (we *Event) put(key string, value any) {
	if _, exists := we.values[key]; !exists {
		we.keys = append(we.keys, key)
	}
	we.values[key] = value
}

// String records a string.
func (we *Event) String(key, value string) *Event {
	return we.Set(key, value)
}

// Int records an int.
func (we *Event) Int(key string, value int) *Event {
	return we.Set(key, value)
}

// Int64 records an int64.
func (we *Event) Int64(key string, value int64) *Event {
	return we.Set(key, value)
}

// Float64 records a float64. The JSON emitter writes NaN and infinities as strings.
func (we *Event) Float64(key string, value float64) *Event {
	return we.Set(key, value)
}

// Bool records a bool.
func (we *Event) Bool(key string, value bool) *Event {
	return we.Set(key, value)
}

// Duration records a duration. The JSON emitter writes it in milliseconds.
func (we *Event) Duration(key string, value time.Duration) *Event {
	return we.Set(key, value)
}

// Time records a point in time. The JSON emitter writes it in RFC 3339 with nanoseconds.
func (we *Event) Time(key string, value time.Time) *Event {
	return we.Set(key, value)
}

// Err records the message of err. A nil err records nothing.
func (we *Event) Err(key string, err error) *Event {
	if we == nil || err == nil {
		return we
	}
	return we.Set(key, fmt.Sprint(err)) // fmt survives an Error method that panics on a nil receiver
}

// UUID records the text of value, typically a UUID. A nil value records nothing.
func (we *Event) UUID(key string, value fmt.Stringer) *Event {
	if we == nil || value == nil {
		return we
	}
	return we.Set(key, fmt.Sprint(value))
}

// Success sets the outcome to "success".
func (we *Event) Success() *Event {
	return we.Set(KeyOutcome, OutcomeSuccess)
}

// Failure sets the outcome to "failure" and records the message of err as error, if err is not nil.
func (we *Event) Failure(err error) *Event {
	return we.Set(KeyOutcome, OutcomeFailure).Err(KeyError, err)
}

// UserID records user.id from the text of id, typically a UUID.
func (we *Event) UserID(id fmt.Stringer) *Event {
	return we.UUID("user.id", id)
}

// UserName records user.name.
func (we *Event) UserName(name string) *Event {
	return we.Set("user.name", name)
}

// UserRole records user.role.
func (we *Event) UserRole(role string) *Event {
	return we.Set("user.role", role)
}

// HasError reports whether the outcome is "failure" or the response status is 500 or higher. A failure
// handled locally (recorded with Err only) is not an error here; mark it with a field a SamplerFunc reads.
func (we *Event) HasError() bool {
	if we == nil {
		return false
	}
	we.mutex.Lock()
	defer we.mutex.Unlock()
	return we.values[KeyOutcome] == OutcomeFailure || we.statusCode() >= minimumServerErrorStatus
}

// StatusCode returns the response status, or 0 if none was recorded with an int value.
func (we *Event) StatusCode() int {
	if we == nil {
		return 0
	}
	we.mutex.Lock()
	defer we.mutex.Unlock()
	return we.statusCode()
}

// statusCode reads the response status. The caller holds the mutex.
func (we *Event) statusCode() int {
	status, _ := we.values[KeyResponseStatus].(int)
	return status
}

// Fields returns a copy of the fields in the order their keys were first set.
func (we *Event) Fields() []Field {
	if we == nil {
		return nil
	}
	we.mutex.Lock()
	defer we.mutex.Unlock()
	fields := make([]Field, len(we.keys))
	for index, key := range we.keys {
		fields[index] = Field{Key: key, Value: we.values[key]}
	}
	return fields
}

// FieldsMap returns the fields as nested maps, expanding dot-separated keys, for custom emitters. A key
// that is also the prefix of other keys keeps its value under "_value": {"db": {"_value": "pg", "operation": "update"}}.
func (we *Event) FieldsMap() map[string]any {
	return nestFields(we.Fields())
}
