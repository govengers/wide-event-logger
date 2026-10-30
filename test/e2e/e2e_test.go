//go:build e2e

// Package e2e drives the public API end to end: a real HTTP server with the Gin middleware and the JSON
// emitter, called over TCP, and a background job. It asserts on the JSON lines a log collector would read.
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	wideevent "github.com/govengers/wide-event-logger"
	"github.com/govengers/wide-event-logger/ginadapter"
)

// logSink is the log collector's view: whole JSON lines, safe to read while the server writes.
type logSink struct {
	mutex  sync.Mutex
	buffer bytes.Buffer
}

func (sink *logSink) Write(line []byte) (int, error) {
	sink.mutex.Lock()
	defer sink.mutex.Unlock()
	return sink.buffer.Write(line)
}

// events waits until count lines arrived and decodes them.
func (sink *logSink) events(t *testing.T, count int) []map[string]any {
	t.Helper()
	var lines []string
	require.Eventually(t, func() bool {
		sink.mutex.Lock()
		defer sink.mutex.Unlock()
		lines = strings.Split(strings.TrimSuffix(sink.buffer.String(), "\n"), "\n")
		return sink.buffer.Len() > 0 && len(lines) >= count
	}, 5*time.Second, 10*time.Millisecond)
	require.Len(t, lines, count)

	events := make([]map[string]any, count)
	for index, line := range lines {
		require.NoError(t, json.Unmarshal([]byte(line), &events[index]), line)
	}
	return events
}

// startServer runs a Gin server with the middleware on a real listener.
func startServer(t *testing.T, options ginadapter.Options, register func(engine *gin.Engine)) *httptest.Server {
	t.Helper()
	gin.SetMode(gin.ReleaseMode)
	middleware, err := ginadapter.NewMiddleware(&options)
	require.NoError(t, err)
	engine := gin.New()
	engine.Use(gin.CustomRecovery(func(ginContext *gin.Context, _ any) {
		ginContext.AbortWithStatus(http.StatusInternalServerError)
	}), middleware)
	register(engine)
	server := httptest.NewServer(engine)
	t.Cleanup(server.Close)
	return server
}

// get requests url and returns the response status and header; the body is read and closed.
func get(t *testing.T, url string, header http.Header) (int, http.Header) {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	require.NoError(t, err)
	for name, values := range header {
		request.Header[name] = values
	}
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	_, err = io.Copy(io.Discard, response.Body)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	return response.StatusCode, response.Header
}

// findProduct stands in for a layer below the handler: it records on the event from the context.
func findProduct(ctx context.Context, id string) error {
	wideevent.FromContext(ctx).String("db.operation", "select").String("db.entity", "product").Int("db.rows", 0)
	return errors.New("product " + id + " not found")
}

func TestRequestEventReachesTheLog(t *testing.T) {
	sink := &logSink{}
	server := startServer(t, ginadapter.Options{
		Emitter:        wideevent.NewJSONEmitter(&wideevent.JSONEmitterOptions{Writer: sink}),
		QueryAllowlist: []string{"expand"},
	}, func(engine *gin.Engine) {
		engine.GET("/api/products/:id", func(ginContext *gin.Context) {
			we := ginadapter.FromContext(ginContext).String("name", "get_product")
			if err := findProduct(ginContext.Request.Context(), ginContext.Param("id")); err != nil {
				we.Failure(err)
				ginContext.Status(http.StatusNotFound)
				return
			}
			we.Success()
		})
	})

	status, header := get(t, server.URL+"/api/products/42?expand=price&token=secret", http.Header{"X-Request-Id": {"trace-1"}})

	require.Equal(t, http.StatusNotFound, status)
	require.Equal(t, "trace-1", header.Get(ginadapter.RequestIDHeader))
	event := sink.events(t, 1)[0]
	require.Equal(t, "get_product", event["name"])
	require.Equal(t, "failure", event["outcome"])
	require.Equal(t, "product 42 not found", event["error"])
	require.Equal(t, "error", event["level"])
	require.Equal(t, "wide_event", event["message"])
	require.Equal(t, map[string]any{"operation": "select", "entity": "product", "rows": float64(0)}, event["db"])

	request, ok := event["request"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "GET", request["method"])
	require.Equal(t, "/api/products/42", request["path"])
	require.Equal(t, "expand=price&token=redacted", request["query"])
	require.Equal(t, "trace-1", request["id"])
	require.Equal(t, "Go-http-client/1.1", request["user_agent"])
	require.Equal(t, map[string]any{"status": float64(http.StatusNotFound), "body_size": float64(0)}, event["response"])
	require.Greater(t, event["duration"], float64(0))
}

func TestPanickingHandlerIsLoggedAndAnswered(t *testing.T) {
	sink := &logSink{}
	server := startServer(t, ginadapter.Options{
		Emitter: wideevent.NewJSONEmitter(&wideevent.JSONEmitterOptions{Writer: sink}),
	}, func(engine *gin.Engine) {
		engine.GET("/crash", func(*gin.Context) { panic("nil map") })
	})

	status, header := get(t, server.URL+"/crash", nil)

	require.Equal(t, http.StatusInternalServerError, status)
	generatedID := header.Get(ginadapter.RequestIDHeader)
	require.NotEmpty(t, generatedID)
	event := sink.events(t, 1)[0]
	require.Equal(t, "error", event["level"])
	require.Equal(t, "panic: nil map", event["error"])
	require.Equal(t, generatedID, event["request"].(map[string]any)["id"])
}

func TestSamplingKeepsFailuresAndSkipsHealthChecks(t *testing.T) {
	sink := &logSink{}
	server := startServer(t, ginadapter.Options{
		Emitter: wideevent.SampledEmitter(
			wideevent.NewJSONEmitter(&wideevent.JSONEmitterOptions{Writer: sink}),
			wideevent.CompositeSampler(wideevent.AlwaysOnError(), wideevent.Rate(5)),
		),
		SkipPaths: []string{"/api/health"},
	}, func(engine *gin.Engine) {
		engine.GET("/api/health", func(ginContext *gin.Context) { ginContext.Status(http.StatusOK) })
		engine.GET("/ok", func(ginContext *gin.Context) { ginContext.Status(http.StatusOK) })
		engine.GET("/down", func(ginContext *gin.Context) { ginContext.Status(http.StatusBadGateway) })
	})

	for range 10 {
		get(t, server.URL+"/ok", nil)
		get(t, server.URL+"/api/health", nil)
	}
	get(t, server.URL+"/down", nil)

	events := sink.events(t, 3)
	paths := make([]any, len(events))
	for index, event := range events {
		paths[index] = event["request"].(map[string]any)["path"]
	}
	require.Equal(t, []any{"/ok", "/ok", "/down"}, paths, "every 5th success and every failure")
}

func TestBackgroundJobEmitsItsOwnEvent(t *testing.T) {
	sink := &logSink{}
	emitter := wideevent.NewJSONEmitter(&wideevent.JSONEmitterOptions{Writer: sink})

	runJob := func(ctx context.Context) (err error) {
		we := wideevent.Begin(emitter, "job.reindex")
		defer func() {
			if err != nil {
				we.Failure(err)
			} else {
				we.Success()
			}
			we.Emit()
		}()
		ctx = wideevent.NewContext(ctx, we)
		var waitGroup sync.WaitGroup
		for shard := range 4 {
			waitGroup.Go(func() {
				wideevent.FromContext(ctx).Int("job.shard_"+strconv.Itoa(shard), shard)
			})
		}
		waitGroup.Wait()
		return nil
	}

	require.NoError(t, runJob(t.Context()))

	event := sink.events(t, 1)[0]
	require.Equal(t, "job.reindex", event["name"])
	require.Equal(t, "success", event["outcome"])
	require.Equal(t, "info", event["level"])
	require.Len(t, event["job"], 4)
}
