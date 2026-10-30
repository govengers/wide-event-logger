// Package ginadapter is the Gin integration of wideevent: a middleware that records one wide event per
// request and emits it when the handler returns, and FromContext to reach that event in a handler.
package ginadapter

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"runtime/debug"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	wideevent "github.com/govengers/wide-event-logger"
)

// RequestIDHeader is read for request.id and set on every response.
const RequestIDHeader = "X-Request-ID"

// RedactedValue replaces the value of every query parameter not in Options.QueryAllowlist.
const RedactedValue = "redacted"

// requestEventName is the name of every request event; a handler may set a more specific one.
const requestEventName = "http.request"

// ginContextKey stores the event in the Gin key store, next to the request context.
const ginContextKey = "wideevent"

// Limits on client-controlled text, so a request cannot inflate its event.
const (
	maximumRequestIDLength  = 128
	maximumMethodBytes      = 32
	maximumHostBytes        = 256
	maximumPathBytes        = 2048
	maximumParsedQueryBytes = 4096 // the raw query is cut before parsing, bounding the work for huge queries
	maximumQueryBytes       = 512
	maximumUserAgentBytes   = 512
	maximumClientIPBytes    = 64 // Gin returns a trusted platform header verbatim
	maximumStackBytes       = 16384
)

// The printable ASCII range a client's request ID must stay in: no spaces, no control characters.
const (
	firstPrintableASCII = '!'
	lastPrintableASCII  = '~'
)

// Enricher adds fields to the request event before or after the handler runs.
type Enricher func(we *wideevent.Event, ginContext *gin.Context)

// Options configures the middleware.
type Options struct {
	// Emitter receives every request event. Required. Wrap it with wideevent.SampledEmitter to sample.
	Emitter wideevent.Emitter
	// SkipPaths are request paths without an event, such as health checks. FromContext returns nil there.
	SkipPaths []string
	// QueryAllowlist names the query parameters whose values request.query records. Every other value is
	// recorded as RedactedValue, so tokens, OAuth codes and personal data in URLs stay out of the logs.
	QueryAllowlist []string
	// PreEnricher runs before the handler, after the request fields are set.
	PreEnricher Enricher
	// PostEnricher runs after the handler, after the response fields are set.
	PostEnricher Enricher
}

// middleware is the resolved configuration of one NewMiddleware call.
type middleware struct {
	emitter        wideevent.Emitter
	skipPaths      map[string]struct{}
	queryAllowlist map[string]struct{}
	preEnricher    Enricher
	postEnricher   Enricher
}

// ownedEventKey marks the event this middleware started for a request, so engine.HandleContext, which
// runs the middleware again for the same request, keeps that event instead of starting a second one.
type ownedEventKey struct{}

// NewMiddleware returns a Gin middleware that starts a wide event named "http.request" per request,
// records the request and response, and emits the event when the handler returns. A panic in the
// handler or an enricher is recorded as a failure with status 500 and its stack as panic.stack, and
// raised again for an outer recovery middleware. It returns an error when options or its Emitter is nil.
//
// Recorded fields: request.method, path, query (redacted, see Options.QueryAllowlist), host, proto,
// content_length, client_ip, user_agent, id, start_time; response.status, body_size, gin_errors; on a
// panic, panic.stack; and the event's duration. Client-controlled text is cut to a few hundred bytes
// (path: 2048). request.id is the client's X-Request-ID when it is printable ASCII of at most 128
// characters, otherwise a generated one; it is echoed in the X-Request-ID response header.
// request.client_ip follows Gin's trusted proxy settings (engine.SetTrustedProxies).
//
// Mount it behind a recovery middleware that does not print the request, such as
// gin.CustomRecoveryWithWriter(io.Discard, ...): the event already holds the panic and its stack, and
// gin.Recovery prints the raw request, cookies included, in debug mode and for broken connections.
//
// Handlers must not call Emit on the request event: an early Emit freezes it before the response fields
// are recorded, and the middleware's own Emit becomes a no-op. Requests Gin answers before running any
// middleware, such as its automatic trailing-slash redirects, produce no event; Gin's default 404 and 405
// bodies are written after the event, which records their body_size as 0.
func NewMiddleware(options *Options) (gin.HandlerFunc, error) {
	if options == nil || options.Emitter == nil {
		return nil, errors.New("wide event middleware: Emitter is required")
	}
	resolved := &middleware{
		emitter:        options.Emitter,
		skipPaths:      setOf(options.SkipPaths),
		queryAllowlist: setOf(options.QueryAllowlist),
		preEnricher:    options.PreEnricher,
		postEnricher:   options.PostEnricher,
	}
	return resolved.handle, nil
}

// FromContext returns the request's event, or nil on a skipped path or without the middleware. The
// methods of a nil *wideevent.Event do nothing.
func FromContext(ginContext *gin.Context) *wideevent.Event {
	if value, ok := ginContext.Get(ginContextKey); ok {
		if we, isEvent := value.(*wideevent.Event); isEvent {
			return we
		}
	}
	return wideevent.FromContext(ginContext.Request.Context())
}

func (m *middleware) handle(ginContext *gin.Context) {
	if _, skip := m.skipPaths[ginContext.Request.URL.Path]; skip {
		ginContext.Next()
		return
	}
	if owned, isEvent := ginContext.Request.Context().Value(ownedEventKey{}).(*wideevent.Event); isEvent {
		ginContext.Set(ginContextKey, owned)         // engine.HandleContext cleared the key store; the outer run emits
		defer recordAndRaisePanic(owned, ginContext) // an inner recovery may swallow it before the outer run sees it
		ginContext.Next()
		return
	}

	we := wideevent.Begin(m.emitter, requestEventName)
	defer m.finish(we, ginContext) // first, so a panicking pre-enricher still produces the event

	ctx := context.WithValue(wideevent.NewContext(ginContext.Request.Context(), we), ownedEventKey{}, we)
	ginContext.Request = ginContext.Request.WithContext(ctx)
	ginContext.Set(ginContextKey, we)
	m.recordRequest(we, ginContext)
	if m.preEnricher != nil {
		m.preEnricher(we, ginContext)
	}
	ginContext.Next()
}

func (m *middleware) recordRequest(we *wideevent.Event, ginContext *gin.Context) {
	request := ginContext.Request
	id := requestID(request.Header.Get(RequestIDHeader))
	ginContext.Header(RequestIDHeader, id)

	// ParseQuery keeps the valid pairs of a malformed query, like URL.Query.
	query, _ := url.ParseQuery(truncate(request.URL.RawQuery, maximumParsedQueryBytes))
	we.String(wideevent.KeyRequestMethod, truncate(request.Method, maximumMethodBytes)).
		String(wideevent.KeyRequestPath, truncate(request.URL.Path, maximumPathBytes)).
		String("request.query", redactQuery(query, m.queryAllowlist)).
		String("request.host", truncate(request.Host, maximumHostBytes)).
		String("request.proto", request.Proto).
		Int64("request.content_length", request.ContentLength).
		String("request.client_ip", truncate(ginContext.ClientIP(), maximumClientIPBytes)).
		String("request.user_agent", truncate(request.UserAgent(), maximumUserAgentBytes)).
		String("request.id", id).
		Time("request.start_time", time.Now())
}

// finish records the response and emits the event. It is deferred, so a panic in the handler or an
// enricher still produces the event; the first panic is raised again afterwards.
func (m *middleware) finish(we *wideevent.Event, ginContext *gin.Context) {
	recovered := recover()
	defer func() {
		if finishPanic := recover(); finishPanic != nil && recovered == nil {
			recovered = finishPanic
			recordPanic(we, ginContext, finishPanic)
		}
		we.Emit()
		if recovered != nil {
			panic(recovered)
		}
	}()

	// Gin reports -1 when the handler wrote no body.
	we.Int(wideevent.KeyResponseStatus, ginContext.Writer.Status()).Int("response.body_size", max(ginContext.Writer.Size(), 0))
	if recovered != nil {
		recordPanic(we, ginContext, recovered)
	}
	if len(ginContext.Errors) > 0 {
		we.Set("response.gin_errors", errorMessages(ginContext.Errors))
	}
	if m.postEnricher != nil {
		m.postEnricher(we, ginContext)
	}
}

// recordAndRaisePanic records a panic on the event and raises it again. It must be deferred directly.
func recordAndRaisePanic(we *wideevent.Event, ginContext *gin.Context) {
	if recovered := recover(); recovered != nil {
		recordPanic(we, ginContext, recovered)
		panic(recovered)
	}
}

// recordPanic marks the event failed with the panic and its stack, and records status 500 when the
// response is not written yet, which is what a recovery middleware sends.
func recordPanic(we *wideevent.Event, ginContext *gin.Context, recovered any) {
	we.Failure(fmt.Errorf("panic: %v", recovered)).String("panic.stack", truncate(string(debug.Stack()), maximumStackBytes))
	if !ginContext.Writer.Written() {
		we.Int(wideevent.KeyResponseStatus, http.StatusInternalServerError)
	}
}

// errorMessages formats each error with fmt, which survives an Error method that panics on a typed nil.
func errorMessages(ginErrors []*gin.Error) []string {
	messages := make([]string, len(ginErrors))
	for index, ginError := range ginErrors {
		messages[index] = fmt.Sprint(ginError.Err)
	}
	return messages
}

// requestID returns incoming when it is a valid request ID, otherwise a new random one.
func requestID(incoming string) string {
	if isValidRequestID(incoming) {
		return incoming
	}
	return rand.Text()
}

func isValidRequestID(incoming string) bool {
	return incoming != "" && len(incoming) <= maximumRequestIDLength && isPrintableASCII(incoming)
}

func isPrintableASCII(text string) bool {
	for index := range len(text) {
		if text[index] < firstPrintableASCII || text[index] > lastPrintableASCII {
			return false
		}
	}
	return true
}

// redactQuery encodes query with every value replaced by RedactedValue, except those of the allowed
// parameter names, cut to maximumQueryBytes.
func redactQuery(query url.Values, allowlist map[string]struct{}) string {
	redacted := make(url.Values, len(query))
	for name, values := range query {
		if _, allowed := allowlist[name]; allowed {
			redacted[name] = values
			continue
		}
		redacted[name] = []string{RedactedValue}
	}
	return truncate(redacted.Encode(), maximumQueryBytes)
}

// truncate cuts text to at most limit bytes without splitting a UTF-8 sequence.
func truncate(text string, limit int) string {
	if len(text) <= limit {
		return text
	}
	return strings.ToValidUTF8(text[:limit], "")
}

func setOf(values []string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}
