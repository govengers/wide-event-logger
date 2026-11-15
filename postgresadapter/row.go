package postgresadapter

import (
	"bytes"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	wideevent "github.com/govengers/wide-event-logger"
)

// maximumColumnBytes caps the text columns: an index entry must stay far below PostgreSQL's
// limit, and a long value is still complete in the event column.
const maximumColumnBytes = 256

// columns are the columns the emitter writes, in the order of row.values.
var columns = []string{"time", "name", "level", "outcome", "status", "duration_ms", "user_id", "request_id", "trace_id", "event"}

// row is one event as the emitter writes it. It is built in Emit, so a value the application
// changes after Emit cannot race with the writer.
type row struct {
	time       time.Time
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
	encoded := we.JSON()
	name, _ := values[wideevent.KeyName].(string)
	eventRow := row{
		time:      emitted,
		name:      columnText(name),
		level:     level,
		outcome:   textColumn(values[wideevent.KeyOutcome]),
		userID:    textColumn(values[wideevent.KeyUserID]),
		requestID: textColumn(values[wideevent.KeyRequestID]),
		traceID:   textColumn(values[wideevent.KeyTraceID]),
		event:     withoutNullEscapes(encoded),
	}
	if status := we.StatusCode(); status != 0 {
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
		eventRow.time, eventRow.name, eventRow.level, eventRow.outcome, eventRow.status, eventRow.durationMS,
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

// columnText makes text PostgreSQL accepts in a text column and an index: valid UTF-8 without
// NUL, at most maximumColumnBytes long.
func columnText(text string) string {
	text = strings.ToValidUTF8(text, "�")
	text = strings.ReplaceAll(text, "\x00", "�")
	if len(text) <= maximumColumnBytes {
		return text
	}
	cut := maximumColumnBytes
	for cut > 0 && !utf8.RuneStart(text[cut]) {
		cut--
	}
	return text[:cut]
}

// withoutNullEscapes replaces the escape \u0000 in JSON strings with �: jsonb rejects it,
// which would fail the whole batch. Outside strings valid JSON has no backslash, and inside one a
// backslash always starts an escape, so stepping over every escape finds exactly the real ones.
func withoutNullEscapes(encoded []byte) []byte {
	const nullEscape = `\u0000`
	if !bytes.Contains(encoded, []byte(nullEscape)) {
		return encoded
	}
	result := make([]byte, 0, len(encoded))
	for index := 0; index < len(encoded); index++ {
		if encoded[index] != '\\' {
			result = append(result, encoded[index])
			continue
		}
		if bytes.HasPrefix(encoded[index:], []byte(nullEscape)) {
			result = append(result, `�`...)
			index += len(nullEscape) - 1
			continue
		}
		result = append(result, encoded[index:min(index+2, len(encoded))]...)
		index++
	}
	return result
}
