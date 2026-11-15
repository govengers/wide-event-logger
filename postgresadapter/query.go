package postgresadapter

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Bounds of Filter.Limit.
const (
	DefaultLimit = 100  // when Limit is zero
	MaximumLimit = 1000 // a larger Limit is cut to it
)

// ErrInvalidCursor is returned by Query for a Filter.Cursor it did not issue.
var ErrInvalidCursor = errors.New("postgresadapter: invalid cursor")

// Filter selects the events Query returns. Empty fields do not filter; set fields are combined
// with AND. Bound a query that searches Text over many days with ctx's deadline: the text search
// reads every event in the time range.
type Filter struct {
	From time.Time // inclusive; zero means no lower bound
	To   time.Time // exclusive; zero means no upper bound

	Name      string // exact
	Level     string // exact: wideevent.LevelInfo, LevelWarning or LevelError
	Outcome   string // exact: wideevent.OutcomeSuccess or OutcomeFailure
	UserID    string // exact
	RequestID string // exact
	TraceID   string // exact

	MinimumStatus int // inclusive; zero means no bound. Events without a status never match a bound
	MaximumStatus int // inclusive; zero means no bound

	// Text is a case-insensitive substring of the event's JSON text. Quotes and backslashes in it
	// match the characters inside a key or string value as they read, so a JSON fragment with its
	// own quotes, such as "name":"job", does not match.
	Text string

	Limit  int    // events per page; zero means DefaultLimit, more than MaximumLimit means MaximumLimit
	Cursor string // Page.Next of the previous page; empty starts at the newest event
}

// Record is one stored event.
type Record struct {
	ID        int64
	Time      time.Time // when the event was emitted, in UTC, to the microsecond
	Name      string
	Level     string
	Outcome   string        // empty when the event has none
	Status    int           // zero when the event has none
	Duration  time.Duration // zero when the event has none
	UserID    string        // the columns hold at most 256 bytes of these; Event holds them whole
	RequestID string
	TraceID   string
	Event     json.RawMessage // the whole event as the JSON emitter writes it (key order aside)
}

// Page is one page of Query's result, newest first.
type Page struct {
	Records []*Record
	Next    string // the Cursor of the next page; empty on the last page
}

const selectRecords = `
SELECT time, id, name, level, outcome, status, duration_ms, user_id, request_id, trace_id, event::text
FROM govengers_wide_events`

// Query returns the events filter selects, newest first, one page at a time. It returns
// ErrInvalidCursor for a cursor it did not issue and an error for a negative limit.
func (s *Store) Query(ctx context.Context, filter *Filter) (*Page, error) {
	if filter == nil {
		filter = &Filter{}
	}
	if filter.Limit < 0 {
		return nil, errors.New("postgresadapter: Filter.Limit must not be negative")
	}
	limit := min(valueOr(filter.Limit, DefaultLimit), MaximumLimit)
	conditions, arguments, err := filterConditions(filter)
	if err != nil {
		return nil, err
	}
	statement := selectRecords
	if len(conditions) > 0 {
		statement += "\nWHERE " + strings.Join(conditions, " AND ")
	}
	arguments = append(arguments, limit+1)
	statement += fmt.Sprintf("\nORDER BY time DESC, id DESC LIMIT $%d", len(arguments))
	rows, err := s.pool.Query(ctx, statement, arguments...)
	if err != nil {
		return nil, databaseError(ctx, "postgresadapter: query events", err)
	}
	records, err := pgx.CollectRows(rows, scanRecord)
	if err != nil {
		return nil, databaseError(ctx, "postgresadapter: query events", err)
	}
	page := &Page{Records: records}
	if len(records) > limit {
		page.Records = records[:limit]
		page.Next = encodeCursor(page.Records[limit-1])
	}
	return page, nil
}

// filterConditions turns filter into SQL conditions on numbered parameters. Every condition
// compares a bare column, so the table's indexes serve it.
func filterConditions(filter *Filter) ([]string, []any, error) {
	var conditions []string
	var arguments []any
	add := func(condition string, values ...any) {
		placeholders := make([]any, len(values))
		for index, value := range values {
			arguments = append(arguments, value)
			placeholders[index] = len(arguments)
		}
		conditions = append(conditions, fmt.Sprintf(condition, placeholders...))
	}
	if !filter.From.IsZero() {
		add("time >= $%d", filter.From)
	}
	if !filter.To.IsZero() {
		add("time < $%d", filter.To)
	}
	for _, equality := range []struct{ column, value string }{
		{"name", filter.Name},
		{"level", filter.Level},
		{"outcome", filter.Outcome},
		{"user_id", filter.UserID},
		{"request_id", filter.RequestID},
		{"trace_id", filter.TraceID},
	} {
		if equality.value != "" {
			add(equality.column+" = $%d", columnText(equality.value)) // the column holds the value cut like this
		}
	}
	if filter.MinimumStatus != 0 {
		add("status >= $%d", filter.MinimumStatus)
	}
	if filter.MaximumStatus != 0 {
		add("status <= $%d", filter.MaximumStatus)
	}
	if filter.Text != "" {
		add(`event::text ILIKE $%d ESCAPE '\'`, "%"+escapeLike(jsonEscaped(storableText(filter.Text)))+"%")
	}
	if filter.Cursor != "" {
		cursorTime, cursorID, err := decodeCursor(filter.Cursor)
		if err != nil {
			return nil, nil, err
		}
		add("(time, id) < ($%d, $%d)", cursorTime, cursorID)
	}
	return conditions, arguments, nil
}

// jsonEscaped escapes text as PostgreSQL writes a jsonb string, so it matches inside event::text.
func jsonEscaped(text string) string {
	var escaped strings.Builder
	for _, character := range text {
		switch character {
		case '"', '\\':
			escaped.WriteByte('\\')
			escaped.WriteRune(character)
		case '\b':
			escaped.WriteString(`\b`)
		case '\f':
			escaped.WriteString(`\f`)
		case '\n':
			escaped.WriteString(`\n`)
		case '\r':
			escaped.WriteString(`\r`)
		case '\t':
			escaped.WriteString(`\t`)
		default:
			if character < ' ' {
				fmt.Fprintf(&escaped, `\u%04x`, character)
			} else {
				escaped.WriteRune(character)
			}
		}
	}
	return escaped.String()
}

func escapeLike(text string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(text)
}

func scanRecord(row pgx.CollectableRow) (*Record, error) {
	var record Record
	var outcome, userID, requestID, traceID *string
	var status *int32
	var durationMS *float64
	var event []byte
	err := row.Scan(&record.Time, &record.ID, &record.Name, &record.Level, &outcome, &status, &durationMS,
		&userID, &requestID, &traceID, &event)
	if err != nil {
		return nil, err
	}
	record.Time = record.Time.UTC()
	record.Outcome, record.UserID, record.RequestID, record.TraceID = text(outcome), text(userID), text(requestID), text(traceID)
	if status != nil {
		record.Status = int(*status)
	}
	if durationMS != nil {
		record.Duration = time.Duration(*durationMS * float64(time.Millisecond))
	}
	record.Event = event
	return &record, nil
}

func text(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

// encodeCursor writes the keyset position after record: its time in microseconds and its ID.
func encodeCursor(record *Record) string {
	position := strconv.FormatInt(record.Time.UnixMicro(), 10) + "." + strconv.FormatInt(record.ID, 10)
	return base64.RawURLEncoding.EncodeToString([]byte(position))
}

func decodeCursor(cursor string) (time.Time, int64, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, 0, ErrInvalidCursor
	}
	microseconds, id, found := strings.Cut(string(decoded), ".")
	if !found {
		return time.Time{}, 0, ErrInvalidCursor
	}
	unixMicro, timeError := strconv.ParseInt(microseconds, 10, 64)
	recordID, idError := strconv.ParseInt(id, 10, 64)
	if timeError != nil || idError != nil {
		return time.Time{}, 0, ErrInvalidCursor
	}
	return time.UnixMicro(unixMicro).UTC(), recordID, nil
}
