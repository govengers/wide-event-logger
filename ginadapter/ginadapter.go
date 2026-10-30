// Package ginadapter is the Gin integration of wideevent: a middleware that records one wide event per
// request and emits it when the handler returns, and FromContext to reach that event in a handler.
package ginadapter

import (
	"crypto/rand"
	"errors"
	"fmt"
	"net/http"
	"net/url"
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
	maximumRequestIDLength = 128
	maximumQueryBytes      = 512
	maximumUserAgentBytes  = 512
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

// NewMiddleware returns a Gin middleware that starts a wide event named "http.request" per request,
// records the request and response, and emits the event when the handler returns, unless the handler
// emitted it already. A panicking handler is recorded as a failure with status 500 and the panic is
// raised again for an outer gin.Recovery. It returns an error when options or its Emitter is nil.
//
// Recorded fields: request.method, path, query (redacted, see Options.QueryAllowlist), host, proto,
// content_length, client_ip, user_agent, id, start_time; response.status, body_size, gin_errors; and the
// event's duration. request.id is the client's X-Request-ID when it is printable ASCII of at most 128
// characters, otherwise a generated one; it is echoed in the X-Request-ID response header.
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

	we := wideevent.Begin(m.emitter, requestEventName)
	ginContext.Set(ginContextKey, we)
	ginContext.Request = ginContext.Request.WithContext(wideevent.NewContext(ginContext.Request.Context(), we))
	m.recordRequest(we, ginContext)
	if m.preEnricher != nil {
		m.preEnricher(we, ginContext)
	}

	defer m.finish(we, ginContext)
	ginContext.Next()
}

func (m *middleware) recordRequest(we *wideevent.Event, ginContext *gin.Context) {
	request := ginContext.Request
	id := requestID(request.Header.Get(RequestIDHeader))
	ginContext.Header(RequestIDHeader, id)

	we.Str(wideevent.KeyRequestMethod, request.Method).
		Str(wideevent.KeyRequestPath, request.URL.Path).
		Str("request.query", redactQuery(request.URL.Query(), m.queryAllowlist)).
		Str("request.host", request.Host).
		Str("request.proto", request.Proto).
		Int64("request.content_length", request.ContentLength).
		Str("request.client_ip", ginContext.ClientIP()).
		Str("request.user_agent", truncate(request.UserAgent(), maximumUserAgentBytes)).
		Str("request.id", id).
		Time("request.start_time", time.Now())
}

// finish records the response and emits the event. It is deferred, so a panicking handler still
// produces an event; the panic is raised again afterwards.
func (m *middleware) finish(we *wideevent.Event, ginContext *gin.Context) {
	recovered := recover()
	status := ginContext.Writer.Status()
	if recovered != nil {
		we.Failure(fmt.Errorf("panic: %v", recovered))
		if !ginContext.Writer.Written() {
			status = http.StatusInternalServerError
		}
	}

	// Gin reports -1 when the handler wrote no body.
	we.Int(wideevent.KeyResponseStatus, status).Int("response.body_size", max(ginContext.Writer.Size(), 0))
	if len(ginContext.Errors) > 0 {
		we.Set("response.gin_errors", ginContext.Errors.Errors())
	}
	if m.postEnricher != nil {
		m.postEnricher(we, ginContext)
	}
	we.Emit() // a no-op when the handler emitted the event itself

	if recovered != nil {
		panic(recovered)
	}
}

// requestID returns incoming when it is printable ASCII of at most maximumRequestIDLength characters,
// otherwise a new random ID.
func requestID(incoming string) string {
	if incoming != "" && len(incoming) <= maximumRequestIDLength && isPrintableASCII(incoming) {
		return incoming
	}
	return rand.Text()
}

func isPrintableASCII(text string) bool {
	for index := range len(text) {
		if text[index] < '!' || text[index] > '~' {
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
