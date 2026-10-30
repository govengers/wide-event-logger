package ginadapter_test

import (
	"fmt"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"

	wideevent "github.com/govengers/wide-event-logger"
	"github.com/govengers/wide-event-logger/ginadapter"
)

func ExampleNewMiddleware() {
	middleware, err := ginadapter.NewMiddleware(&ginadapter.Options{
		Emitter: wideevent.SampledEmitter(
			wideevent.NewJSONEmitter(nil),
			wideevent.CompositeSampler(wideevent.AlwaysOnError(), wideevent.Rate(10)),
		),
		SkipPaths:      []string{"/api/health"},
		QueryAllowlist: []string{"page", "sort"},
		PreEnricher: func(we *wideevent.Event, ginContext *gin.Context) {
			we.String("tenant.id", ginContext.GetHeader("X-Tenant"))
		},
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	engine := gin.New()
	if err := engine.SetTrustedProxies(nil); err != nil { // request.client_ip trusts no X-Forwarded-For
		fmt.Println(err)
		return
	}
	// The event holds the panic and its stack; a silent recovery keeps the raw request out of the logs.
	engine.Use(gin.CustomRecoveryWithWriter(io.Discard, func(ginContext *gin.Context, _ any) {
		ginContext.AbortWithStatus(http.StatusInternalServerError)
	}), middleware)
	engine.GET("/api/users/:id", func(ginContext *gin.Context) {
		we := ginadapter.FromContext(ginContext)
		we.String("user.lookup_id", ginContext.Param("id")).Success()
		ginContext.Status(http.StatusNoContent)
	})
}
