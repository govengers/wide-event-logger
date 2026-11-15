package postgresadapter

import (
	"bytes"
	"math"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"
	"unicode/utf16"
	"unicode/utf8"

	wideevent "github.com/govengers/wide-event-logger"
)

// maximumColumnBytes caps the text columns: an index entry must stay far below PostgreSQL's
// limit, and a long value is still complete in the event column.
const maximumColumnBytes = 256

// columns are the columns the emitter writes, in the order of row.values.
var columns = []string{"time", "id", "name", "level", "outcome", "status", "duration_ms", "user_id", "request_id", "trace_id", "event"}

// row is one event as the emitter writes it. It is built in Emit, so a value the application
// changes after Emit cannot race with the writer.
type row struct {
	time       time.Time
	id         int64 // random: with time it identifies the event, so a retried write can skip what is stored
	name       string
	level      string
	outcome    any // string or nil
	status     any // int or nil
	durationMS any // float64 or nil
	userID     any // string or nil
	requestID  any // string or nil
	traceID    any // string or nil
	event      []byte
}

// newRow builds the row of we from its fields and level.
func newRow(we *wideevent.Event, values map[string]any, level string, emitted time.Time) row {
	name, _ := values[wideevent.KeyName].(string)
	eventRow := row{
		time:      emitted,
		id:        rand.Int64(), //nolint:gosec // G404: an identifier, not a secret
		name:      columnText(name),
		level:     level,
		outcome:   textColumn(values[wideevent.KeyOutcome]),
		userID:    textColumn(values[wideevent.KeyUserID]),
		requestID: textColumn(values[wideevent.KeyRequestID]),
		traceID:   textColumn(values[wideevent.KeyTraceID]),
		event:     storableJSON(we.JSON()),
	}
	if status := we.StatusCode(); status != 0 && status >= math.MinInt32 && status <= math.MaxInt32 {
		eventRow.status = status
	}
	if duration, ok := values[wideevent.KeyDuration].(time.Duration); ok {
		eventRow.durationMS = float64(duration) / float64(time.Millisecond)
	}
	return eventRow
}

// fieldValues returns the fields of we by key.
func fieldValues(we *wideevent.Event) map[string]any {
	fields := we.Fields()
	values := make(map[string]any, len(fields))
	for _, field := range fields {
		values[field.Key] = field.Value
	}
	return values
}

func (eventRow *row) values() []any {
	return []any{
		eventRow.time, eventRow.id, eventRow.name, eventRow.level, eventRow.outcome, eventRow.status, eventRow.durationMS,
		eventRow.userID, eventRow.requestID, eventRow.traceID, eventRow.event,
	}
}

// textColumn returns a string or integer field value as column text, anything else as NULL.
func textColumn(value any) any {
	switch typed := value.(type) {
	case string:
		return columnText(typed)
	case int:
		return strconv.Itoa(typed)
	case int64:
		return strconv.FormatInt(typed, 10)
	default:
		return nil
	}
}

// columnText makes text PostgreSQL accepts in a text column and an index: storable, at most
// maximumColumnBytes long.
func columnText(text string) string {
	text = storableText(text)
	if len(text) <= maximumColumnBytes {
		return text
	}
	cut := maximumColumnBytes
	for cut > 0 && !utf8.RuneStart(text[cut]) {
		cut--
	}
	return text[:cut]
}

// storableText replaces invalid UTF-8 and NUL, which PostgreSQL text refuses, with U+FFFD.
func storableText(text string) string {
	return strings.ReplaceAll(strings.ToValidUTF8(text, replacement), "\x00", replacement)
}

const replacement = "\uFFFD"

// storableJSON makes the encoded event acceptable to jsonb. encoding/json passes a
// json.RawMessage or a MarshalJSON result through without checking UTF-8 or escapes, so a request
// body the application records can hold invalid UTF-8, \u0000 or a lone UTF-16 surrogate escape,
// each of which jsonb refuses. All three become U+FFFD.
func storableJSON(encoded []byte) []byte {
	if !utf8.Valid(encoded) {
		encoded = bytes.ToValidUTF8(encoded, []byte(replacement)) // invalid bytes of valid JSON sit inside strings
	}
	if !bytes.Contains(encoded, []byte(`\u`)) {
		return encoded
	}
	return storableEscapes(encoded)
}

// storableEscapes replaces the escapes jsonb refuses. Outside strings valid JSON has no backslash,
// and inside one a backslash always starts an escape, so stepping over every escape finds exactly
// the real ones.
func storableEscapes(encoded []byte) []byte {
	result := make([]byte, 0, len(encoded))
	for index := 0; index < len(encoded); index++ {
		if encoded[index] != '\\' {
			result = append(result, encoded[index])
			continue
		}
		code, isUnicode := unicodeEscape(encoded[index:])
		switch {
		case !isUnicode:
			result = append(result, encoded[index:min(index+2, len(encoded))]...)
			index++
		case utf16.IsSurrogate(rune(code)) && code < 0xdc00 && isLowSurrogateEscape(encoded[index+6:]):
			result = append(result, encoded[index:index+12]...) // a valid pair
			index += 11
		case code == 0 || utf16.IsSurrogate(rune(code)):
			result = append(result, replacement...)
			index += 5
		default:
			result = append(result, encoded[index:index+6]...)
			index += 5
		}
	}
	return result
}

// unicodeEscape returns the code of the \uXXXX escape at the start of escape.
func unicodeEscape(escape []byte) (uint64, bool) {
	if len(escape) < 6 || escape[1] != 'u' {
		return 0, false
	}
	code, err := strconv.ParseUint(string(escape[2:6]), 16, 16)
	return code, err == nil
}

func isLowSurrogateEscape(escape []byte) bool {
	code, isUnicode := unicodeEscape(escape)
	return isUnicode && code >= 0xdc00 && code <= 0xdfff
}
