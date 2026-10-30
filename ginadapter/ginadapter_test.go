package ginadapter

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	wideevent "github.com/govengers/wide-event-logger"
)

// captureEmitter collects emitted events for assertions. It is safe for concurrent use.
type captureEmitter struct {
	mutex  sync.Mutex
	events []*wideevent.Event
}

func (capture *captureEmitter) Emit(we *wideevent.Event) {
	capture.mutex.Lock()
	defer capture.mutex.Unlock()
	capture.events = append(capture.events, we)
}

func (capture *captureEmitter) emitted() []*wideevent.Event {
	capture.mutex.Lock()
	defer capture.mutex.Unlock()
	return append([]*wideevent.Event(nil), capture.events...)
}

// newEngine returns an engine with the middleware configured by options, emitting into capture.
func newEngine(t *testing.T, options Options) (*gin.Engine, *captureEmitter) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	capture := &captureEmitter{}
	if options.Emitter == nil {
		options.Emitter = capture
	}
	middleware, err := NewMiddleware(&options)
	require.NoError(t, err)
	engine := gin.New()
	engine.Use(middleware)
	return engine, capture
}

func serve(t *testing.T, engine *gin.Engine, request *http.Request) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	engine.ServeHTTP(recorder, request)
	return recorder
}

func newRequest(t *testing.T, method, target string) *http.Request {
	t.Helper()
	return httptest.NewRequestWithContext(t.Context(), method, target, strings.NewReader(""))
}

// onlyEvent returns the fields of the single emitted event by key.
func onlyEvent(t *testing.T, capture *captureEmitter) map[string]any {
	t.Helper()
	events := capture.emitted()
	require.Len(t, events, 1)
	values := map[string]any{}
	for _, field := range events[0].Fields() {
		values[field.Key] = field.Value
	}
	return values
}

func TestMiddlewareRecordsRequestAndResponse(t *testing.T) {
	engine, capture := newEngine(t, Options{})
	engine.POST("/api/users", func(ginContext *gin.Context) {
		FromContext(ginContext).String("user.action", "create").Success()
		ginContext.String(http.StatusCreated, "created")
	})
	request := newRequest(t, http.MethodPost, "/api/users")
	request.Header.Set("User-Agent", "test-agent")
	request.Header.Set(RequestIDHeader, "request-123")
	request.ContentLength = 0

	recorder := serve(t, engine, request)

	values := onlyEvent(t, capture)
	require.Equal(t, "http.request", values[wideevent.KeyName])
	require.Equal(t, http.MethodPost, values[wideevent.KeyRequestMethod])
	require.Equal(t, "/api/users", values[wideevent.KeyRequestPath])
	require.Empty(t, values["request.query"])
	require.Equal(t, "example.com", values["request.host"])
	require.Equal(t, "HTTP/1.1", values["request.proto"])
	require.Equal(t, int64(0), values["request.content_length"])
	require.Equal(t, "192.0.2.1", values["request.client_ip"])
	require.Equal(t, "test-agent", values["request.user_agent"])
	require.Equal(t, "request-123", values["request.id"])
	require.Contains(t, values, "request.start_time")
	require.Equal(t, http.StatusCreated, values[wideevent.KeyResponseStatus])
	require.Equal(t, len("created"), values["response.body_size"])
	require.NotContains(t, values, "response.gin_errors")
	require.Equal(t, "create", values["user.action"])
	require.Equal(t, wideevent.OutcomeSuccess, values[wideevent.KeyOutcome])
	require.Contains(t, values, wideevent.KeyDuration)
	require.Equal(t, "request-123", recorder.Header().Get(RequestIDHeader))
}

func TestMiddlewareStoresTheEventInBothContexts(t *testing.T) {
	engine, _ := newEngine(t, Options{})
	engine.GET("/", func(ginContext *gin.Context) {
		fromGin := FromContext(ginContext)
		require.NotNil(t, fromGin)
		require.Same(t, fromGin, wideevent.FromContext(ginContext.Request.Context()))
	})
	serve(t, engine, newRequest(t, http.MethodGet, "/"))
}

func TestFromContextFallsBackToTheRequestContext(t *testing.T) {
	ginContext, _ := gin.CreateTestContext(httptest.NewRecorder())
	require.Nil(t, FromContext(withRequest(t, ginContext, newRequest(t, http.MethodGet, "/"))))

	we := wideevent.Begin(nil, "job")
	request := newRequest(t, http.MethodGet, "/")
	request = request.WithContext(wideevent.NewContext(request.Context(), we))
	require.Same(t, we, FromContext(withRequest(t, ginContext, request)))
}

func withRequest(t *testing.T, ginContext *gin.Context, request *http.Request) *gin.Context {
	t.Helper()
	ginContext.Request = request
	return ginContext
}

func TestMiddlewareRedactsTheQuery(t *testing.T) {
	testCases := []struct {
		name      string
		allowlist []string
		target    string
		query     string
	}{
		{"every value redacted by default", nil, "/callback?state=abc&code=secret", "code=redacted&state=redacted"},
		{"allowed values kept", []string{"page", "sort"}, "/users?page=2&sort=name&token=secret", "page=2&sort=name&token=redacted"},
		{"repeated allowed values kept", []string{"id"}, "/users?id=1&id=2", "id=1&id=2"},
		{"no query", nil, "/users", ""},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			engine, capture := newEngine(t, Options{QueryAllowlist: testCase.allowlist})
			engine.GET("/:path", func(*gin.Context) {})
			serve(t, engine, newRequest(t, http.MethodGet, testCase.target))
			require.Equal(t, testCase.query, onlyEvent(t, capture)["request.query"])
		})
	}
}

func TestMiddlewareBoundsClientControlledText(t *testing.T) {
	engine, capture := newEngine(t, Options{})
	engine.GET("/", func(*gin.Context) {})
	request := newRequest(t, http.MethodGet, "/?"+strings.Repeat("parameter", 100)+"=1")
	request.Header.Set("User-Agent", strings.Repeat("ü", 400))

	serve(t, engine, request)

	values := onlyEvent(t, capture)
	require.Len(t, values["request.query"], maximumQueryBytes)
	userAgent, ok := values["request.user_agent"].(string)
	require.True(t, ok)
	require.Equal(t, strings.Repeat("ü", maximumUserAgentBytes/2), userAgent, "cut on a rune boundary")
}

func TestMiddlewareRequestID(t *testing.T) {
	testCases := []struct {
		name     string
		incoming string
		kept     bool
	}{
		{"printable kept", "trace-0192/abc", true},
		{"missing generated", "", false},
		{"too long replaced", strings.Repeat("a", maximumRequestIDLength+1), false},
		{"control character replaced", "id\twith-tab", false},
		{"space replaced", "id with space", false},
		{"non ASCII replaced", "idü", false},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			engine, capture := newEngine(t, Options{})
			engine.GET("/", func(*gin.Context) {})
			request := newRequest(t, http.MethodGet, "/")
			request.Header.Set(RequestIDHeader, testCase.incoming)

			recorder := serve(t, engine, request)

			id := onlyEvent(t, capture)["request.id"]
			require.Equal(t, id, recorder.Header().Get(RequestIDHeader))
			if testCase.kept {
				require.Equal(t, testCase.incoming, id)
				return
			}
			require.NotEqual(t, testCase.incoming, id)
			require.Len(t, id, 26, "crypto/rand.Text")
		})
	}
}

func TestMiddlewareSkipsPaths(t *testing.T) {
	engine, capture := newEngine(t, Options{SkipPaths: []string{"/health"}})
	engine.GET("/health", func(ginContext *gin.Context) {
		require.Nil(t, FromContext(ginContext))
		FromContext(ginContext).String("written", "to a nil event") // must not panic
		ginContext.Status(http.StatusOK)
	})
	engine.GET("/api", func(*gin.Context) {})

	require.Equal(t, http.StatusOK, serve(t, engine, newRequest(t, http.MethodGet, "/health")).Code)
	serve(t, engine, newRequest(t, http.MethodGet, "/api"))

	require.Equal(t, "/api", onlyEvent(t, capture)[wideevent.KeyRequestPath])
}

func TestMiddlewareRunsEnrichers(t *testing.T) {
	var order []string
	engine, capture := newEngine(t, Options{
		PreEnricher: func(we *wideevent.Event, ginContext *gin.Context) {
			order = append(order, "pre")
			we.String("tenant.id", ginContext.GetHeader("X-Tenant"))
		},
		PostEnricher: func(we *wideevent.Event, ginContext *gin.Context) {
			order = append(order, "post")
			we.Int("response.status_seen", ginContext.Writer.Status())
		},
	})
	engine.GET("/", func(ginContext *gin.Context) {
		order = append(order, "handler")
		ginContext.Status(http.StatusAccepted)
	})
	request := newRequest(t, http.MethodGet, "/")
	request.Header.Set("X-Tenant", "acme")

	serve(t, engine, request)

	values := onlyEvent(t, capture)
	require.Equal(t, []string{"pre", "handler", "post"}, order)
	require.Equal(t, "acme", values["tenant.id"])
	require.Equal(t, http.StatusAccepted, values["response.status_seen"])
}

func TestMiddlewareRecordsGinErrors(t *testing.T) {
	engine, capture := newEngine(t, Options{})
	engine.GET("/", func(ginContext *gin.Context) {
		_ = ginContext.Error(errors.New("first"))
		_ = ginContext.Error(errors.New("second"))
		ginContext.Status(http.StatusBadRequest)
	})

	serve(t, engine, newRequest(t, http.MethodGet, "/"))

	values := onlyEvent(t, capture)
	require.Equal(t, []string{"first", "second"}, values["response.gin_errors"])
	require.Equal(t, 0, values["response.body_size"], "no body written")
}

func TestMiddlewareRecordsAPanicAndRaisesItAgain(t *testing.T) {
	testCases := []struct {
		name   string
		before func(ginContext *gin.Context)
		status int
	}{
		{"before writing", func(*gin.Context) {}, http.StatusInternalServerError},
		{"after writing", func(ginContext *gin.Context) { ginContext.String(http.StatusAccepted, "partial") }, http.StatusAccepted},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			capture := &captureEmitter{}
			middleware, err := NewMiddleware(&Options{Emitter: capture})
			require.NoError(t, err)
			engine := gin.New()
			engine.Use(gin.CustomRecoveryWithWriter(io.Discard, func(ginContext *gin.Context, _ any) {
				ginContext.AbortWithStatus(http.StatusInternalServerError)
			}), middleware)
			engine.GET("/", func(ginContext *gin.Context) {
				testCase.before(ginContext)
				panic("boom")
			})

			serve(t, engine, newRequest(t, http.MethodGet, "/"))

			values := onlyEvent(t, capture)
			require.Equal(t, wideevent.OutcomeFailure, values[wideevent.KeyOutcome])
			require.Equal(t, "panic: boom", values[wideevent.KeyError])
			require.Equal(t, testCase.status, values[wideevent.KeyResponseStatus])
		})
	}
}

func TestMiddlewareHonorsAnEmitByTheHandler(t *testing.T) {
	engine, capture := newEngine(t, Options{})
	engine.GET("/", func(ginContext *gin.Context) {
		FromContext(ginContext).Success().Emit()
		ginContext.Status(http.StatusTeapot)
	})

	serve(t, engine, newRequest(t, http.MethodGet, "/"))

	values := onlyEvent(t, capture)
	require.NotContains(t, values, wideevent.KeyResponseStatus, "the middleware's writes come after the emit")
}

func TestMiddlewareSamplesTheFinishedEvent(t *testing.T) {
	capture := &captureEmitter{}
	engine, _ := newEngine(t, Options{Emitter: wideevent.SampledEmitter(capture, wideevent.AlwaysOnError())})
	engine.GET("/ok", func(ginContext *gin.Context) { ginContext.Status(http.StatusOK) })
	engine.GET("/fail", func(ginContext *gin.Context) { ginContext.Status(http.StatusServiceUnavailable) })

	serve(t, engine, newRequest(t, http.MethodGet, "/ok"))
	serve(t, engine, newRequest(t, http.MethodGet, "/fail"))

	require.Equal(t, http.StatusServiceUnavailable, onlyEvent(t, capture)[wideevent.KeyResponseStatus])
}

func TestMiddlewareGivesEachRequestItsOwnEvent(t *testing.T) {
	engine, capture := newEngine(t, Options{})
	engine.GET("/:id", func(ginContext *gin.Context) {
		FromContext(ginContext).String("item.id", ginContext.Param("id"))
	})

	serve(t, engine, newRequest(t, http.MethodGet, "/1"))
	serve(t, engine, newRequest(t, http.MethodGet, "/2"))

	events := capture.emitted()
	require.Len(t, events, 2)
	require.NotSame(t, events[0], events[1])
	require.Equal(t, map[string]any{"id": "1"}, events[0].FieldsMap()["item"])
	require.Equal(t, map[string]any{"id": "2"}, events[1].FieldsMap()["item"])
}

func TestNewMiddlewareRequiresAnEmitter(t *testing.T) {
	for _, options := range []*Options{nil, {}} {
		middleware, err := NewMiddleware(options)
		require.Error(t, err)
		require.Nil(t, middleware)
	}
}

func TestNewMiddlewareCopiesOptions(t *testing.T) {
	gin.SetMode(gin.TestMode)
	capture := &captureEmitter{}
	options := &Options{Emitter: capture, SkipPaths: []string{"/health"}}
	middleware, err := NewMiddleware(options)
	require.NoError(t, err)
	options.SkipPaths[0] = "/api"
	engine := gin.New()
	engine.Use(middleware)
	engine.GET("/api", func(*gin.Context) {})

	serve(t, engine, newRequest(t, http.MethodGet, "/api"))

	require.Len(t, capture.emitted(), 1)
}

// newRecoveringEngine returns an engine with a silent recovery in front of the middleware.
func newRecoveringEngine(t *testing.T, options Options) (*gin.Engine, *captureEmitter) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	capture := &captureEmitter{}
	options.Emitter = capture
	middleware, err := NewMiddleware(&options)
	require.NoError(t, err)
	engine := gin.New()
	engine.Use(gin.CustomRecoveryWithWriter(io.Discard, func(ginContext *gin.Context, _ any) {
		ginContext.AbortWithStatus(http.StatusInternalServerError)
	}), middleware)
	return engine, capture
}

func TestMiddlewareEmitsWhenAnEnricherPanics(t *testing.T) {
	testCases := []struct {
		name    string
		options Options
		handler gin.HandlerFunc
		error   string
	}{
		{
			name:    "pre-enricher",
			options: Options{PreEnricher: func(*wideevent.Event, *gin.Context) { panic("pre") }},
			handler: func(*gin.Context) {},
			error:   "panic: pre",
		},
		{
			name:    "post-enricher",
			options: Options{PostEnricher: func(*wideevent.Event, *gin.Context) { panic("post") }},
			handler: func(ginContext *gin.Context) { ginContext.Status(http.StatusOK) },
			error:   "panic: post",
		},
		{
			name:    "post-enricher after a handler panic keeps the first panic",
			options: Options{PostEnricher: func(*wideevent.Event, *gin.Context) { panic("post") }},
			handler: func(*gin.Context) { panic("handler") },
			error:   "panic: handler",
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			engine, capture := newRecoveringEngine(t, testCase.options)
			engine.GET("/", testCase.handler)

			recorder := serve(t, engine, newRequest(t, http.MethodGet, "/"))

			require.Equal(t, http.StatusInternalServerError, recorder.Code, "the panic reached the recovery")
			values := onlyEvent(t, capture)
			require.Equal(t, wideevent.OutcomeFailure, values[wideevent.KeyOutcome])
			require.Equal(t, testCase.error, values[wideevent.KeyError])
			require.Equal(t, http.StatusInternalServerError, values[wideevent.KeyResponseStatus], "what the recovery sends")
			require.Contains(t, values["panic.stack"], "runtime/debug.Stack")
		})
	}
}

// nilReceiverError panics in Error on a nil receiver.
type nilReceiverError struct{ message string }

func (failure *nilReceiverError) Error() string { return failure.message }

func TestMiddlewareSurvivesATypedNilGinError(t *testing.T) {
	engine, capture := newEngine(t, Options{})
	engine.GET("/", func(ginContext *gin.Context) {
		var typedNil *nilReceiverError
		_ = ginContext.Error(typedNil)
		ginContext.Status(http.StatusBadRequest)
	})

	serve(t, engine, newRequest(t, http.MethodGet, "/"))

	require.Equal(t, []string{"!panicked *ginadapter.nilReceiverError"}, onlyEvent(t, capture)["response.gin_errors"])
}

func TestMiddlewareKeepsOneEventAcrossHandleContext(t *testing.T) {
	var engine *gin.Engine
	engine, capture := newEngine(t, Options{})
	engine.GET("/old", func(ginContext *gin.Context) {
		FromContext(ginContext).String("redirect.from", "/old")
		ginContext.Request.URL.Path = "/new"
		engine.HandleContext(ginContext)
	})
	engine.GET("/new", func(ginContext *gin.Context) {
		FromContext(ginContext).String("redirect.to", "/new")
		ginContext.Status(http.StatusOK)
	})

	serve(t, engine, newRequest(t, http.MethodGet, "/old"))

	values := onlyEvent(t, capture)
	require.Equal(t, "/old", values["redirect.from"])
	require.Equal(t, "/new", values["redirect.to"])
	require.Equal(t, http.StatusOK, values[wideevent.KeyResponseStatus])
}

func TestMiddlewareBoundsPathHostAndMethod(t *testing.T) {
	engine, capture := newEngine(t, Options{})
	engine.NoRoute(func(ginContext *gin.Context) { ginContext.Status(http.StatusNotFound) })
	request := newRequest(t, http.MethodGet, "/"+strings.Repeat("p", 3000))
	request.Host = strings.Repeat("h", 300)
	request.Method = strings.Repeat("M", 40)

	serve(t, engine, request)

	values := onlyEvent(t, capture)
	require.Len(t, values[wideevent.KeyRequestPath], maximumPathBytes)
	require.Len(t, values["request.host"], maximumHostBytes)
	require.Len(t, values[wideevent.KeyRequestMethod], maximumMethodBytes)
}

func TestMiddlewareParsesOnlyTheStartOfAHugeQuery(t *testing.T) {
	engine, capture := newEngine(t, Options{QueryAllowlist: []string{"last"}})
	engine.GET("/", func(*gin.Context) {})
	target := "/?" + strings.Repeat("a=1&", 2000) + "last=kept"

	serve(t, engine, newRequest(t, http.MethodGet, target))

	require.Equal(t, "a=redacted", onlyEvent(t, capture)["request.query"], "last lies beyond the parsed prefix")
}

func TestMiddlewareRecordsAPanicAnInnerRecoverySwallowsDuringHandleContext(t *testing.T) {
	var engine *gin.Engine
	engine, capture := newRecoveringEngine(t, Options{})
	engine.GET("/old", func(ginContext *gin.Context) {
		ginContext.Request.URL.Path = "/new"
		engine.HandleContext(ginContext)
	})
	engine.GET("/new", func(*gin.Context) { panic("inner") })

	recorder := serve(t, engine, newRequest(t, http.MethodGet, "/old"))

	require.Equal(t, http.StatusInternalServerError, recorder.Code)
	values := onlyEvent(t, capture)
	require.Equal(t, "panic: inner", values[wideevent.KeyError])
	require.Equal(t, http.StatusInternalServerError, values[wideevent.KeyResponseStatus])
}

func TestMiddlewareBoundsAClientIPFromATrustedPlatformHeader(t *testing.T) {
	engine, capture := newEngine(t, Options{})
	engine.TrustedPlatform = "X-Client-IP"
	engine.GET("/", func(*gin.Context) {})
	request := newRequest(t, http.MethodGet, "/")
	request.Header.Set("X-Client-IP", strings.Repeat("1", 500))

	serve(t, engine, request)

	require.Len(t, onlyEvent(t, capture)["request.client_ip"], maximumClientIPBytes)
}

func TestMiddlewareRecordsAPanicInASkippedPathReachedThroughHandleContext(t *testing.T) {
	var engine *gin.Engine
	engine, capture := newRecoveringEngine(t, Options{SkipPaths: []string{"/health"}})
	engine.GET("/old", func(ginContext *gin.Context) {
		ginContext.Request.URL.Path = "/health"
		engine.HandleContext(ginContext)
	})
	engine.GET("/health", func(*gin.Context) { panic("inner") })

	serve(t, engine, newRequest(t, http.MethodGet, "/old"))

	require.Equal(t, "panic: inner", onlyEvent(t, capture)[wideevent.KeyError])
}

func TestMiddlewareRecordsACompositePanicValueByType(t *testing.T) {
	engine, capture := newRecoveringEngine(t, Options{})
	engine.GET("/", func(*gin.Context) {
		cycle := map[string]any{"secret": "hunter2"}
		cycle["self"] = cycle
		panic(cycle)
	})

	serve(t, engine, newRequest(t, http.MethodGet, "/"))

	require.Equal(t, "panic: map[string]interface {}", onlyEvent(t, capture)[wideevent.KeyError])
}

// panickingCode panics in String with a map that contains itself.
type panickingCode int

func (panickingCode) String() string {
	cycle := map[string]any{}
	cycle["self"] = cycle
	panic(cycle)
}

func TestPanicText(t *testing.T) {
	testCases := []struct {
		name      string
		recovered any
		text      string
	}{
		{"error", errors.New("boom"), "boom"},
		{"string", "boom", "boom"},
		{"int", 42, "42"},
		{"uint", uint8(7), "7"},
		{"float", 1.5, "1.5"},
		{"bool", true, "true"},
		{"named int with a panicking String", panickingCode(3), "3"},
		{"slice", []string{"secret"}, "[]string"},
		{"nil", nil, "<nil>"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.text, panicText(testCase.recovered))
		})
	}
}

func TestErrorMessagesToleratesNilEntries(t *testing.T) {
	require.Equal(t, []string{"<nil>", "<nil>", "boom"}, errorMessages([]*gin.Error{nil, {}, {Err: errors.New("boom")}}))
}
