package wideevent

import (
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/mattn/go-isatty"
)

// ANSI escape codes for terminal colors.
const (
	ansiReset  = "\033[0m"
	ansiBold   = "\033[1m"
	ansiDim    = "\033[2m"
	ansiRed    = "\033[31m"
	ansiGreen  = "\033[32m"
	ansiYellow = "\033[33m"
	ansiCyan   = "\033[36m"
)

// Default duration thresholds of the dev emitter's footer colors.
const (
	defaultSlowDuration     = 500 * time.Millisecond
	defaultVerySlowDuration = 2 * time.Second
)

// Layout of the dev emitter's body lines: "│  <label padded to labelWidth><key=value  key=value ...>".
const (
	maximumLineWidth = 90
	labelWidth       = 10
	bodyIndent       = 3 // "│  "
	pairSeparator    = "  "
)

// ColorMode selects whether the dev emitter writes ANSI colors.
type ColorMode int

// Color modes of the dev emitter.
const (
	ColorAuto   ColorMode = iota // colors when the writer is a terminal
	ColorAlways                  // colors regardless of the writer
	ColorNever                   // plain text
)

// Category names the requests whose path starts with one of PathPrefixes. The dev emitter shows the name
// in the header and can mute the category.
type Category struct {
	Name         string
	PathPrefixes []string
}

// DevEmitterOptions configures a DevEmitter.
type DevEmitterOptions struct {
	// Writer receives the formatted events. Default: os.Stdout.
	Writer io.Writer
	// Color selects ANSI colors. Default: ColorAuto.
	Color ColorMode
	// Categories are matched in order against request.path; the first match wins.
	Categories []Category
	// MutedCategories names categories whose events are not written at all, such as health checks.
	MutedCategories []string
	// SlowDuration colors the duration yellow from this value on. Default: 500ms.
	SlowDuration time.Duration
	// VerySlowDuration colors the duration red from this value on. Default: 2s.
	VerySlowDuration time.Duration
}

// DevEmitter writes events as colorized, human-readable blocks for local development: a header with
// method, path and status (or the event name), the fields grouped by their first key segment, and a
// footer with outcome, error and duration. It is safe for concurrent use. Use NewJSONEmitter in
// deployed environments.
type DevEmitter struct {
	mutex            sync.Mutex
	writer           io.Writer
	color            bool
	categories       []Category
	muted            map[string]struct{}
	slowDuration     time.Duration
	verySlowDuration time.Duration
}

// NewDevEmitter returns a DevEmitter. A nil options writes to os.Stdout with the defaults. It returns an
// error for an unknown color mode, a category without a name, a muted category that is not declared,
// or a negative or inverted duration threshold.
func NewDevEmitter(options *DevEmitterOptions) (*DevEmitter, error) {
	var resolved DevEmitterOptions
	if options != nil {
		resolved = *options
	}
	if resolved.Writer == nil {
		resolved.Writer = os.Stdout
	}
	if resolved.SlowDuration == 0 {
		resolved.SlowDuration = defaultSlowDuration
	}
	if resolved.VerySlowDuration == 0 {
		resolved.VerySlowDuration = max(defaultVerySlowDuration, resolved.SlowDuration)
	}
	if err := validateDevEmitterOptions(&resolved); err != nil {
		return nil, err
	}
	color, err := useColor(resolved.Color, resolved.Writer)
	if err != nil {
		return nil, err
	}

	muted := make(map[string]struct{}, len(resolved.MutedCategories))
	for _, name := range resolved.MutedCategories {
		muted[name] = struct{}{}
	}
	categories := make([]Category, len(resolved.Categories))
	for index, category := range resolved.Categories {
		categories[index] = Category{Name: category.Name, PathPrefixes: slices.Clone(category.PathPrefixes)}
	}
	return &DevEmitter{
		writer:           resolved.Writer,
		color:            color,
		categories:       categories,
		muted:            muted,
		slowDuration:     resolved.SlowDuration,
		verySlowDuration: resolved.VerySlowDuration,
	}, nil
}

func validateDevEmitterOptions(options *DevEmitterOptions) error {
	if options.SlowDuration < 0 || options.VerySlowDuration < options.SlowDuration {
		return fmt.Errorf("dev emitter: need 0 <= SlowDuration (%s) <= VerySlowDuration (%s)", options.SlowDuration, options.VerySlowDuration)
	}
	declared := make(map[string]struct{}, len(options.Categories))
	for _, category := range options.Categories {
		if category.Name == "" {
			return errors.New("dev emitter: a category has no name")
		}
		declared[category.Name] = struct{}{}
	}
	for _, name := range options.MutedCategories {
		if _, ok := declared[name]; !ok {
			return fmt.Errorf("dev emitter: muted category %q is not declared in Categories", name)
		}
	}
	return nil
}

func useColor(mode ColorMode, writer io.Writer) (bool, error) {
	switch mode {
	case ColorAuto:
		file, isFile := writer.(*os.File)
		return isFile && (isatty.IsTerminal(file.Fd()) || isatty.IsCygwinTerminal(file.Fd())), nil
	case ColorAlways:
		return true, nil
	case ColorNever:
		return false, nil
	default:
		return false, fmt.Errorf("dev emitter: unknown color mode %d", mode)
	}
}

// summary holds the fields the header and footer show; they are left out of the body.
type summary struct {
	method, path, name, outcome, errorText string
	status                                 int
	duration                               time.Duration
	hasDuration                            bool
}

// summaryKeys are the keys summarize reads.
var summaryKeys = map[string]struct{}{
	KeyRequestMethod: {}, KeyRequestPath: {}, KeyResponseStatus: {},
	KeyName: {}, KeyOutcome: {}, KeyError: {}, KeyDuration: {},
}

func summarize(fields []Field) summary {
	var result summary
	for _, field := range fields {
		switch field.Key {
		case KeyRequestMethod:
			result.method, _ = field.Value.(string)
		case KeyRequestPath:
			result.path, _ = field.Value.(string)
		case KeyResponseStatus:
			result.status, _ = field.Value.(int)
		case KeyName:
			result.name, _ = field.Value.(string)
		case KeyOutcome:
			result.outcome, _ = field.Value.(string)
		case KeyError:
			result.errorText, _ = field.Value.(string)
		case KeyDuration:
			result.duration, result.hasDuration = field.Value.(time.Duration)
		}
	}
	return result
}

// Emit writes we as one block, unless its category is muted.
func (emitter *DevEmitter) Emit(we *Event) {
	fields := we.Fields()
	eventSummary := summarize(fields)
	category := emitter.categoryOf(eventSummary.path)
	if _, muted := emitter.muted[category]; muted {
		return
	}

	var builder strings.Builder
	emitter.writeHeader(&builder, levelOf(we), eventSummary, category)
	if groups := groupFields(fields); len(groups) > 0 {
		emitter.writeDim(&builder, "│")
		builder.WriteByte('\n')
		for _, group := range groups {
			emitter.writeGroup(&builder, group)
		}
		emitter.writeDim(&builder, "│")
		builder.WriteByte('\n')
	}
	emitter.writeFooter(&builder, eventSummary)
	builder.WriteByte('\n')

	emitter.mutex.Lock()
	defer emitter.mutex.Unlock()
	_, _ = io.WriteString(emitter.writer, builder.String()) // a failed write loses this event only
}

// fieldGroup holds the fields sharing the first key segment label, with that segment stripped. The
// group with the empty label holds the keys without a dot.
type fieldGroup struct {
	label   string
	entries []Field
}

// groupPriority puts these groups first; the rest follow in the order their first key was set, and the
// keys without a dot come last.
var groupPriority = map[string]int{"request": 0, "response": 1, "user": 2}

func groupFields(fields []Field) []fieldGroup {
	var groups []fieldGroup
	positions := make(map[string]int)
	for _, field := range fields {
		if _, inSummary := summaryKeys[field.Key]; inSummary {
			continue
		}
		label, key, _ := strings.Cut(field.Key, ".")
		if key == "" {
			label, key = "", field.Key
		}
		position, exists := positions[label]
		if !exists {
			position = len(groups)
			positions[label] = position
			groups = append(groups, fieldGroup{label: label})
		}
		groups[position].entries = append(groups[position].entries, Field{Key: key, Value: field.Value})
	}
	slices.SortStableFunc(groups, func(left, right fieldGroup) int {
		return groupRank(left.label) - groupRank(right.label)
	})
	return groups
}

func groupRank(label string) int {
	if label == "" {
		return len(groupPriority) + 1
	}
	if rank, prioritized := groupPriority[label]; prioritized {
		return rank
	}
	return len(groupPriority)
}

// categoryOf returns the name of the first category with a prefix of path, or "".
func (emitter *DevEmitter) categoryOf(path string) string {
	for _, category := range emitter.categories {
		for _, prefix := range category.PathPrefixes {
			if strings.HasPrefix(path, prefix) {
				return category.Name
			}
		}
	}
	return ""
}

// writeHeader writes "┌ 15:04:05.000  METHOD PATH  [category]  STATUS", or the event name for an event
// that is not an HTTP request.
func (emitter *DevEmitter) writeHeader(builder *strings.Builder, level string, eventSummary summary, category string) {
	levelColor := levelColors[level]
	emitter.writeColored(builder, levelColor+ansiBold, "┌ ")
	emitter.writeDim(builder, time.Now().Format("15:04:05.000"))
	builder.WriteString(pairSeparator)

	if eventSummary.method == "" || eventSummary.path == "" {
		title := eventSummary.name
		if title == "" {
			title = "wide_event"
		}
		emitter.writeColored(builder, levelColor+ansiBold, title)
		builder.WriteByte('\n')
		return
	}

	emitter.writeColored(builder, levelColor+ansiBold, eventSummary.method+" "+eventSummary.path)
	if category != "" {
		builder.WriteString(pairSeparator)
		emitter.writeDim(builder, "["+category+"]")
	}
	if eventSummary.status > 0 {
		builder.WriteString(pairSeparator)
		emitter.writeColored(builder, statusColor(eventSummary.status)+ansiBold, fmt.Sprint(eventSummary.status))
	}
	builder.WriteByte('\n')
}

// writeGroup writes "│  label     key=value  key=value", wrapping at maximumLineWidth.
func (emitter *DevEmitter) writeGroup(builder *strings.Builder, group fieldGroup) {
	emitter.writeDim(builder, "│")
	builder.WriteString(pairSeparator)
	emitter.writeColored(builder, ansiCyan, fmt.Sprintf("%-*s", labelWidth, group.label))

	lineLength := bodyIndent + labelWidth
	for index, entry := range group.entries {
		value := devFormatValue(entry.Value)
		pairLength := len(entry.Key) + 1 + len(value)
		switch {
		case index == 0:
		case lineLength+len(pairSeparator)+pairLength > maximumLineWidth:
			builder.WriteByte('\n')
			emitter.writeDim(builder, "│")
			builder.WriteString(strings.Repeat(" ", len(pairSeparator)+labelWidth))
			lineLength = bodyIndent + labelWidth
		default:
			builder.WriteString(pairSeparator)
			lineLength += len(pairSeparator)
		}
		emitter.writeDim(builder, entry.Key+"=")
		builder.WriteString(value)
		lineLength += pairLength
	}
	builder.WriteByte('\n')
}

// footerPart is one key=value pair of the footer.
type footerPart struct {
	key, color, text string
}

// writeFooter writes "└ outcome=success  error=...  duration=12.5ms", leaving out what is not set.
func (emitter *DevEmitter) writeFooter(builder *strings.Builder, eventSummary summary) {
	var parts []footerPart
	if eventSummary.outcome != "" {
		parts = append(parts, footerPart{KeyOutcome, outcomeColors[eventSummary.outcome], eventSummary.outcome})
	}
	if eventSummary.errorText != "" {
		parts = append(parts, footerPart{KeyError, ansiRed, eventSummary.errorText})
	}
	if eventSummary.hasDuration {
		parts = append(parts, footerPart{KeyDuration, emitter.durationColor(eventSummary.duration), devFormatDuration(eventSummary.duration)})
	}

	emitter.writeDim(builder, "└ ")
	for index, part := range parts {
		if index > 0 {
			builder.WriteString(pairSeparator)
		}
		emitter.writeDim(builder, part.key+"=")
		emitter.writeColored(builder, part.color, part.text)
	}
	builder.WriteByte('\n')
}

// writeColored writes text in the given color, or plain when colors are off or color is "".
func (emitter *DevEmitter) writeColored(builder *strings.Builder, color, text string) {
	if !emitter.color || color == "" {
		builder.WriteString(text)
		return
	}
	builder.WriteString(color)
	builder.WriteString(text)
	builder.WriteString(ansiReset)
}

func (emitter *DevEmitter) writeDim(builder *strings.Builder, text string) {
	emitter.writeColored(builder, ansiDim, text)
}

var (
	levelColors   = map[string]string{levelError: ansiRed, levelWarning: ansiYellow, levelInfo: ansiGreen}
	outcomeColors = map[string]string{OutcomeSuccess: ansiGreen, OutcomeFailure: ansiRed}
)

func (emitter *DevEmitter) durationColor(duration time.Duration) string {
	switch {
	case duration >= emitter.verySlowDuration:
		return ansiRed
	case duration >= emitter.slowDuration:
		return ansiYellow
	default:
		return ansiGreen
	}
}

func statusColor(status int) string {
	switch {
	case status >= minimumServerErrorStatus:
		return ansiRed
	case status >= minimumClientErrorStatus:
		return ansiYellow
	default:
		return ansiGreen
	}
}

func devFormatValue(value any) string {
	switch typed := value.(type) {
	case time.Duration:
		return devFormatDuration(typed)
	case time.Time:
		return typed.Format("15:04:05.000")
	case string:
		if typed == "" {
			return `""`
		}
		return typed
	default:
		return fmt.Sprint(value)
	}
}

func devFormatDuration(duration time.Duration) string {
	switch {
	case duration < time.Millisecond:
		return fmt.Sprintf("%dµs", duration.Microseconds())
	case duration < time.Second:
		return fmt.Sprintf("%.1fms", float64(duration)/float64(time.Millisecond))
	default:
		return fmt.Sprintf("%.2fs", duration.Seconds())
	}
}
