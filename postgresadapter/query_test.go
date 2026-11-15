package postgresadapter_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	wideevent "github.com/govengers/wide-event-logger"
	"github.com/govengers/wide-event-logger/postgresadapter"
)

// queryFixture stores one event per minute of testDay's noon hour, named after their order.
func queryFixture(t *testing.T) *fixture {
	t.Helper()
	fixture := newFixture(t, nil)
	emitter, err := postgresadapter.NewEmitter(&postgresadapter.EmitterOptions{Store: fixture.store})
	require.NoError(t, err)
	events := []func(we *wideevent.Event){
		func(we *wideevent.Event) { we.String("name", "login").UserName("100% sure").Success() },
		func(we *wideevent.Event) {
			we.String("name", "login").Int(wideevent.KeyResponseStatus, 401).String(wideevent.KeyUserID, "user-1")
		},
		func(we *wideevent.Event) {
			we.String("name", "order").Int(wideevent.KeyResponseStatus, 500).Failure(errors.New("database down"))
		},
		func(we *wideevent.Event) {
			we.String("name", "order").Int(wideevent.KeyResponseStatus, 201).String(wideevent.KeyRequestID, "request-1").Success()
		},
		func(we *wideevent.Event) {
			we.String("name", "export").String(wideevent.KeyTraceID, "trace-1").UserName("a_b")
		},
	}
	for minute, decorate := range events {
		fixture.clock.Set(testDay.Add(12*time.Hour + time.Duration(minute)*time.Minute))
		emitEvent(emitter, "", decorate)
	}
	require.NoError(t, emitter.Close(t.Context()))
	return fixture
}

func queryNames(t *testing.T, store *postgresadapter.Store, filter *postgresadapter.Filter) []string {
	t.Helper()
	page, err := store.Query(t.Context(), filter)
	require.NoError(t, err)
	names := make([]string, 0, len(page.Records))
	for _, record := range page.Records {
		names = append(names, record.Name+"@"+record.Time.Format("15:04"))
	}
	return names
}

func TestQueryFilters(t *testing.T) {
	fixture := queryFixture(t)
	noon := testDay.Add(12 * time.Hour)
	for name, test := range map[string]struct {
		filter   *postgresadapter.Filter
		expected []string
	}{
		"everything newest first": {nil, []string{"export@12:04", "order@12:03", "order@12:02", "login@12:01", "login@12:00"}},
		"time range":              {&postgresadapter.Filter{From: noon.Add(time.Minute), To: noon.Add(3 * time.Minute)}, []string{"order@12:02", "login@12:01"}},
		"name":                    {&postgresadapter.Filter{Name: "order"}, []string{"order@12:03", "order@12:02"}},
		"level":                   {&postgresadapter.Filter{Level: wideevent.LevelWarning}, []string{"login@12:01"}},
		"outcome":                 {&postgresadapter.Filter{Outcome: wideevent.OutcomeSuccess}, []string{"order@12:03", "login@12:00"}},
		"status range":            {&postgresadapter.Filter{MinimumStatus: 400, MaximumStatus: 499}, []string{"login@12:01"}},
		"minimum status":          {&postgresadapter.Filter{MinimumStatus: 500}, []string{"order@12:02"}},
		"user":                    {&postgresadapter.Filter{UserID: "user-1"}, []string{"login@12:01"}},
		"request":                 {&postgresadapter.Filter{RequestID: "request-1"}, []string{"order@12:03"}},
		"trace":                   {&postgresadapter.Filter{TraceID: "trace-1"}, []string{"export@12:04"}},
		"text, any case":          {&postgresadapter.Filter{Text: "DATABASE"}, []string{"order@12:02"}},
		"text with a percent":     {&postgresadapter.Filter{Text: "100%"}, []string{"login@12:00"}},
		"text with an underscore": {&postgresadapter.Filter{Text: "a_b"}, []string{"export@12:04"}},
		"combined":                {&postgresadapter.Filter{Name: "login", Outcome: wideevent.OutcomeSuccess}, []string{"login@12:00"}},
		"nothing matches":         {&postgresadapter.Filter{Name: "missing"}, []string{}},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, test.expected, queryNames(t, fixture.store, test.filter))
		})
	}
}

func TestQueryPagesWithACursor(t *testing.T) {
	fixture := queryFixture(t)
	var names []string
	filter := &postgresadapter.Filter{Limit: 2}
	for pages := 0; ; pages++ {
		require.Less(t, pages, 5, "the pages end")
		page, err := fixture.store.Query(t.Context(), filter)
		require.NoError(t, err)
		for _, record := range page.Records {
			names = append(names, record.Name)
		}
		if page.Next == "" {
			break
		}
		filter.Cursor = page.Next
	}
	require.Equal(t, []string{"export", "order", "order", "login", "login"}, names)
}

func TestQueryPagesThroughEventsOfTheSameMicrosecond(t *testing.T) {
	fixture := newFixture(t, nil)
	emitter := fixture.emitter(t, nil)
	for range 5 {
		emitEvent(emitter, "same_time", nil) // the clock stands still
	}
	require.NoError(t, emitter.Flush(t.Context()))

	seen := map[int64]bool{}
	filter := &postgresadapter.Filter{Limit: 2}
	for {
		page, err := fixture.store.Query(t.Context(), filter)
		require.NoError(t, err)
		for _, record := range page.Records {
			require.False(t, seen[record.ID], "no event twice")
			seen[record.ID] = true
		}
		if page.Next == "" {
			break
		}
		filter.Cursor = page.Next
	}
	require.Len(t, seen, 5)
}

func TestQueryRejectsABadCursorAndLimit(t *testing.T) {
	fixture := newFixture(t, nil)
	for _, cursor := range []string{"not base64!", "bm8tZG90", "YS5i"} { // "no-dot", "a.b"
		_, err := fixture.store.Query(t.Context(), &postgresadapter.Filter{Cursor: cursor})
		require.ErrorIs(t, err, postgresadapter.ErrInvalidCursor, cursor)
	}
	_, err := fixture.store.Query(t.Context(), &postgresadapter.Filter{Limit: -1})
	require.Error(t, err)
	page, err := fixture.store.Query(t.Context(), &postgresadapter.Filter{Limit: postgresadapter.MaximumLimit + 1})
	require.NoError(t, err, "a large limit is cut, not refused")
	require.Empty(t, page.Records)
}
