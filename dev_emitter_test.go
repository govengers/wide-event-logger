package wideevent

import (
	"bytes"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// emitDev emits one event built by record through a DevEmitter and returns the output.
func emitDev(t *testing.T, options DevEmitterOptions, record func(we *Event)) string {
	t.Helper()
	var buffer bytes.Buffer
	options.Writer = &buffer
	emitter, err := NewDevEmitter(&options)
	require.NoError(t, err)
	we := Begin(emitter, "job")
	record(we)
	we.Emit()
	return buffer.String()
}

func recordHTTPRequest(we *Event) {
	we.String(KeyRequestMethod, "GET").
		String(KeyRequestPath, "/api/v1/users").
		String("request.host", "localhost:8080").
		Int(KeyResponseStatus, 200).
		Int("response.body_size", 1842).
		String("user.name", "ada").
		Success()
}

func TestDevEmitterWritesHeaderGroupsAndFooter(t *testing.T) {
	output := emitDev(t, DevEmitterOptions{}, recordHTTPRequest)

	lines := strings.Split(output, "\n")
	require.Contains(t, lines[0], "┌ ")
	require.Contains(t, lines[0], "GET /api/v1/users  200")
	require.Contains(t, output, "│  request   host=localhost:8080\n")
	require.Contains(t, output, "│  response  body_size=1842\n")
	require.Contains(t, output, "│  user      name=ada\n")
	require.Contains(t, output, "└ outcome=success  duration=")
	require.NotContains(t, output, "\033[", "a buffer is not a terminal")
}

func TestDevEmitterLeavesSummaryFieldsOutOfTheBody(t *testing.T) {
	output := emitDev(t, DevEmitterOptions{}, func(we *Event) {
		recordHTTPRequest(we)
		we.Failure(errors.New("conflict"))
	})

	require.NotContains(t, output, "method=")
	require.NotContains(t, output, "path=")
	require.NotContains(t, output, "status=")
	require.NotContains(t, output, "name=job")
	require.Contains(t, output, "└ outcome=failure  error=conflict  duration=")
}

func TestDevEmitterTitlesAnEventWithoutRequestByName(t *testing.T) {
	output := emitDev(t, DevEmitterOptions{}, func(we *Event) { we.String("job.id", "7") })
	require.Contains(t, strings.Split(output, "\n")[0], "  job")

	output = emitDev(t, DevEmitterOptions{}, func(we *Event) { we.String(KeyName, "") })
	require.Contains(t, strings.Split(output, "\n")[0], "  wide_event")
	require.NotContains(t, output, "│", "an event without body fields has no body")
}

func TestDevEmitterOrdersGroups(t *testing.T) {
	output := emitDev(t, DevEmitterOptions{}, func(we *Event) {
		we.String("standalone", "x").String("db.operation", "update").String("user.id", "1").
			String("response.size", "2").String("request.host", "h").String("cache.hit", "yes")
	})

	order := []string{"request", "response", "user", "db", "cache", "standalone="}
	previous := -1
	for _, label := range order {
		position := strings.Index(output, label)
		require.Greater(t, position, previous, "%s out of order in\n%s", label, output)
		previous = position
	}
}

func TestDevEmitterPrintsTheLatestValueOnce(t *testing.T) {
	output := emitDev(t, DevEmitterOptions{}, func(we *Event) {
		we.String("request.query", "code=secret").String("request.query", "code=redacted")
	})

	require.NotContains(t, output, "secret")
	require.Equal(t, 1, strings.Count(output, "query="))
}

func TestDevEmitterFormatsValues(t *testing.T) {
	output := emitDev(t, DevEmitterOptions{}, func(we *Event) {
		we.Duration("timing.fast", 250*time.Microsecond).
			Duration("timing.medium", 12500*time.Microsecond).
			Duration("timing.slow", 2500*time.Millisecond).
			Time("timing.at", time.Date(2026, 1, 2, 15, 4, 5, 123000000, time.UTC)).
			String("timing.empty", "")
	})

	require.Contains(t, output, "fast=250µs")
	require.Contains(t, output, "medium=12.5ms")
	require.Contains(t, output, "slow=2.50s")
	require.Contains(t, output, "at=15:04:05.123")
	require.Contains(t, output, `empty=""`)
}

func TestDevEmitterWrapsLongGroups(t *testing.T) {
	output := emitDev(t, DevEmitterOptions{}, func(we *Event) {
		for _, key := range []string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"} {
			we.String("group."+key, strings.Repeat("v", 10))
		}
	})

	for line := range strings.SplitSeq(output, "\n") {
		require.LessOrEqual(t, len([]rune(line)), maximumLineWidth, line)
	}
	require.Contains(t, output, "│            ", "continuation lines are indented past the label")
}

func TestDevEmitterColors(t *testing.T) {
	output := emitDev(t, DevEmitterOptions{Color: ColorAlways}, func(we *Event) {
		we.Int(KeyResponseStatus, 500).Failure(errors.New("boom"))
	})

	require.Contains(t, output, ansiRed+ansiBold+"┌ ")
	require.Contains(t, output, ansiRed+OutcomeFailure+ansiReset, "a failure is red")
	require.Contains(t, output, ansiRed+"boom"+ansiReset)

	output = emitDev(t, DevEmitterOptions{Color: ColorAlways}, func(we *Event) { we.Success() })
	require.Contains(t, output, ansiGreen+OutcomeSuccess+ansiReset)
}

func TestDevEmitterDurationColors(t *testing.T) {
	emitter, err := NewDevEmitter(&DevEmitterOptions{Writer: &bytes.Buffer{}, SlowDuration: time.Second, VerySlowDuration: 3 * time.Second})
	require.NoError(t, err)

	require.Equal(t, ansiGreen, emitter.durationColor(999*time.Millisecond))
	require.Equal(t, ansiYellow, emitter.durationColor(time.Second))
	require.Equal(t, ansiRed, emitter.durationColor(3*time.Second))
}

func TestDevEmitterStatusColors(t *testing.T) {
	require.Equal(t, ansiGreen, statusColor(204))
	require.Equal(t, ansiYellow, statusColor(404))
	require.Equal(t, ansiRed, statusColor(500))
}

func TestDevEmitterCategories(t *testing.T) {
	options := DevEmitterOptions{
		Categories: []Category{
			{Name: "health", PathPrefixes: []string{"/health", "/ready"}},
			{Name: "api", PathPrefixes: []string{"/api/"}},
		},
		MutedCategories: []string{"health"},
	}
	request := func(path string) func(we *Event) {
		return func(we *Event) { we.String(KeyRequestMethod, "GET").String(KeyRequestPath, path) }
	}

	require.Empty(t, emitDev(t, options, request("/ready/db")), "a muted category writes nothing")
	require.Contains(t, emitDev(t, options, request("/api/v1/users")), "GET /api/v1/users  [api]")
	require.NotContains(t, emitDev(t, options, request("/static/app.js")), "[")
}

func TestNewDevEmitterCopiesOptions(t *testing.T) {
	prefixes := []string{"/api/"}
	options := &DevEmitterOptions{Writer: &bytes.Buffer{}, Categories: []Category{{Name: "api", PathPrefixes: prefixes}}}
	emitter, err := NewDevEmitter(options)
	require.NoError(t, err)

	prefixes[0] = "/changed/"
	require.Equal(t, "api", emitter.categoryOf("/api/users"))
	require.Equal(t, time.Duration(0), options.SlowDuration, "the caller's options are not modified")
}

func TestNewDevEmitterRejectsInvalidOptions(t *testing.T) {
	testCases := []struct {
		name    string
		options DevEmitterOptions
	}{
		{"negative slow duration", DevEmitterOptions{SlowDuration: -time.Second}},
		{"inverted thresholds", DevEmitterOptions{SlowDuration: time.Second, VerySlowDuration: time.Millisecond}},
		{"category without name", DevEmitterOptions{Categories: []Category{{PathPrefixes: []string{"/"}}}}},
		{"undeclared muted category", DevEmitterOptions{MutedCategories: []string{"health"}}},
		{"unknown color mode", DevEmitterOptions{Color: ColorMode(9)}},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			emitter, err := NewDevEmitter(&testCase.options)
			require.Error(t, err)
			require.Nil(t, emitter)
		})
	}
}

func TestNewDevEmitterDefaults(t *testing.T) {
	emitter, err := NewDevEmitter(nil)
	require.NoError(t, err)
	require.Equal(t, os.Stdout, emitter.writer)
	require.Equal(t, defaultSlowDuration, emitter.slowDuration)
	require.Equal(t, defaultVerySlowDuration, emitter.verySlowDuration)

	emitter, err = NewDevEmitter(&DevEmitterOptions{SlowDuration: 5 * time.Second})
	require.NoError(t, err)
	require.Equal(t, 5*time.Second, emitter.verySlowDuration, "the default red threshold never undercuts the yellow one")
}

func TestColorAutoIsOffForAFileThatIsNotATerminal(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "dev-emitter")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, file.Close()) })

	color, err := useColor(ColorAuto, file)
	require.NoError(t, err)
	require.False(t, color)
}

func TestDevEmitterEscapesControlCharacters(t *testing.T) {
	output := emitDev(t, DevEmitterOptions{}, func(we *Event) {
		we.String(KeyRequestMethod, "GET").
			String(KeyRequestPath, "/x\n┌ forged line\033[2J").
			String("request.user_agent", "agent\r\nnext").
			Failure(errors.New("bad\ninput"))
	})

	require.NotContains(t, output, "\033[2J")
	require.Contains(t, output, `"GET /x\n┌ forged line\x1b[2J"`)
	require.Contains(t, output, `user_agent="agent\r\nnext"`)
	require.Contains(t, output, `error="bad\ninput"`)
	require.Equal(t, 1, strings.Count(output, "┌ forged"), "the forged header is inside the quoted path")
}

func TestDevEmitterShowsANonStringError(t *testing.T) {
	output := emitDev(t, DevEmitterOptions{}, func(we *Event) { we.Set(KeyError, errors.New("boom")) })
	require.Contains(t, output, "error=boom")
}

func TestDevEmitterSurvivesACycleAndInvalidUTF8(t *testing.T) {
	cycle := map[string]any{}
	cycle["self"] = cycle

	output := emitDev(t, DevEmitterOptions{}, func(we *Event) {
		we.Set("data.cycle", cycle).String("data.raw", "a\x9bb").String("data.text", "µs ünïcode")
	})

	require.Contains(t, output, "cycle=!unencodable map[string]interface {}")
	require.Contains(t, output, `raw="a\x9bb"`)
	require.Contains(t, output, "text=µs ünïcode")
}

// hiddenCycle hides a cycle and a secret from JSON behind json:"-".
type hiddenCycle struct {
	Visible string         `json:"visible"`
	Hidden  map[string]any `json:"-"`
}

func TestDevEmitterRendersCompositesAsJSON(t *testing.T) {
	hidden := map[string]any{"secret": "hunter2"}
	hidden["self"] = hidden

	output := emitDev(t, DevEmitterOptions{}, func(we *Event) {
		we.Set("data.value", hiddenCycle{Visible: "shown", Hidden: hidden}).Set("data.list", []string{"a", "b"})
	})

	require.Contains(t, output, `value={"visible":"shown"}`)
	require.Contains(t, output, `list=["a","b"]`)
	require.NotContains(t, output, "hunter2")
}
