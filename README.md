# wide-event-logger

[![CI](https://github.com/govengers/wide-event-logger/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/govengers/wide-event-logger/actions/workflows/ci.yml)
[![unit tests](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/govengers/wide-event-logger/badges/unit-tests.json)](https://github.com/govengers/wide-event-logger/actions/workflows/ci.yml)
[![e2e tests](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/govengers/wide-event-logger/badges/e2e-tests.json)](test/e2e)
[![coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/govengers/wide-event-logger/badges/coverage.json)](https://github.com/govengers/wide-event-logger/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/govengers/wide-event-logger.svg)](https://pkg.go.dev/github.com/govengers/wide-event-logger)
[![License](https://img.shields.io/github/license/govengers/wide-event-logger)](LICENSE)

Wide event logging for Go: one structured event per unit of work (a request, a job, a cron run) instead of many scattered log lines.

Every layer adds what it did to the same event, and the event is emitted once at the end. When something goes wrong in production, the one event of that request answers what happened:

```json
{
  "name": "update_product",
  "outcome": "failure",
  "error": "update product 0192: conflict",
  "level": "error",
  "message": "wide_event",
  "request": { "method": "PUT", "path": "/api/v1/products/0192", "query": "token=redacted", "id": "trace-1" },
  "response": { "status": 409, "body_size": 61 },
  "user": { "id": "0191", "role": "admin" },
  "db": { "operation": "update", "entity": "product", "tx": "rolled_back" },
  "duration": 6.8
}
```

## Installation

```bash
go get github.com/govengers/wide-event-logger
```

The core package `wideevent` has no web framework dependency. The Gin middleware is in `wideevent/ginadapter`.

## Usage

### HTTP requests (Gin)

```go
import (
    wideevent "github.com/govengers/wide-event-logger"
    "github.com/govengers/wide-event-logger/ginadapter"
)

func main() {
    middleware, err := ginadapter.NewMiddleware(&ginadapter.Options{
        Emitter:        wideevent.NewJSONEmitter(nil), // stdout
        SkipPaths:      []string{"/api/health"},
        QueryAllowlist: []string{"page", "sort"},
    })
    if err != nil {
        log.Fatal(err)
    }

    engine := gin.New()
    engine.Use(gin.Recovery(), middleware)
    engine.PUT("/api/v1/products/:id", func(ginContext *gin.Context) {
        we := ginadapter.FromContext(ginContext)
        we.Str("name", "update_product")
        if err := products.Update(ginContext.Request.Context(), ginContext.Param("id")); err != nil {
            we.Failure(err)
            ginContext.Status(http.StatusConflict)
            return
        }
        we.Success()
        ginContext.Status(http.StatusNoContent)
    })
}
```

The middleware records `request.method`, `path`, `query`, `host`, `proto`, `content_length`, `client_ip`, `user_agent`, `id`, `start_time`, then `response.status`, `body_size`, `gin_errors` (when there are any) and the event's `duration`. It emits when the handler returns, and also when the handler panics: the event gets `outcome=failure`, `error="panic: ..."` and status 500, and the panic is raised again for `gin.Recovery`.

- **Query redaction:** every query value is recorded as `redacted` unless its parameter is in `QueryAllowlist`, so OAuth codes, tokens and personal data in URLs stay out of the logs.
- **Request ID:** `request.id` is the client's `X-Request-ID` when it is printable ASCII of at most 128 characters, otherwise a generated one. It is echoed in the `X-Request-ID` response header.
- `PreEnricher` and `PostEnricher` add fields before and after the handler, such as the authenticated user.

### Deeper layers

Services and repositories find the event in the context. Outside a unit of work `FromContext` returns nil, and every method of a nil event does nothing, so no nil check is needed:

```go
func (r *ProductRepository) Update(ctx context.Context, product *model.Product) error {
    wideevent.FromContext(ctx).Str("db.operation", "update").Str("db.entity", "product")
    ...
}
```

### Background jobs

```go
func (w *Worker) handle(ctx context.Context, job *model.Job) (err error) {
    we := wideevent.Begin(w.options.Emitter, "job."+job.Queue)
    defer func() {
        if err != nil {
            we.Failure(err)
        } else {
            we.Success()
        }
        we.Emit()
    }()
    return w.options.Handler(wideevent.NewContext(ctx, we), job)
}
```

## The event

| Method | Records |
|--------|---------|
| `Str`, `Int`, `Int64`, `Float64`, `Bool`, `Set` | a value |
| `Dur` | a duration (JSON: milliseconds) |
| `Time` | a time (JSON: RFC 3339 with nanoseconds) |
| `Err(key, err)` | the error message; nil records nothing |
| `UUID(key, id)` | the text of any `fmt.Stringer`; nil records nothing |
| `Success()`, `Failure(err)` | `outcome` (and `error`) |
| `UserID`, `UserName`, `UserRole` | `user.id`, `user.name`, `user.role` |
| `Emit()` | sets `duration` and hands the event to the emitter, once |

- Name the variable `we`: `we.Success()`, `we.Failure(err)`.
- Setting a key again replaces its value. Fields keep the order in which their keys were first set.
- Dots nest: `db.operation` becomes `{"db": {"operation": ...}}`. A key that is also a prefix of others keeps its value under `_value`.
- After `Emit` the event is frozen: later writes are dropped. Work that outlives the request needs its own `Begin`.
- All methods are safe for concurrent use.

## Emitters

| Emitter | Output |
|---------|--------|
| `NewJSONEmitter(&JSONEmitterOptions{Writer: w})` | one JSON line per event with `level` (`info`, `warn` for 4xx, `error` for failures and 5xx) and `message: "wide_event"`. A value JSON cannot represent (NaN, a channel) is written as text instead of losing the event. Default writer: stdout. |
| `NewDevEmitter(&DevEmitterOptions{...})` | colorized blocks for a terminal, fields grouped by their first key segment. `Categories` and `MutedCategories` label or hide requests by path prefix. |
| `MultiEmitter(a, b)` | every event to each emitter |
| `SampledEmitter(emitter, sampler)` | only the events the sampler keeps |
| `EmitterFunc(func(we *Event))` | your own, for example to assert on events in tests |

Use the dev emitter locally and JSON in deployed environments.

### Sampling

Sampling is tail sampling: the sampler sees the finished event, so it can keep every failure.

```go
emitter := wideevent.SampledEmitter(
    wideevent.NewJSONEmitter(nil),
    wideevent.CompositeSampler(wideevent.AlwaysOnError(), wideevent.Rate(100)),
)
```

| Sampler | Keeps |
|---------|-------|
| `AlwaysSample()`, `NeverSample()` | all, none |
| `AlwaysOnError()` | `outcome=failure` or status >= 500 |
| `AlwaysOnStatus(codes...)` | the listed statuses |
| `Rate(n)` | every Nth event |
| `Probability(p)` | each event with probability p |
| `CompositeSampler(samplers...)` | when any of them keeps |
| `SamplerFunc(func(we *Event) bool)` | your own, for example `degraded=true` |

A failure handled locally (a fallback, recorded with `Err` only) is not an error for `AlwaysOnError`; mark it with a field such as `degraded=true` and keep it with a `SamplerFunc`.

## Development

```bash
just check   # lint, unit tests with coverage gate, e2e tests, govulncheck
```

## Changes

See [CHANGELOG.md](CHANGELOG.md). Coming from `github.com/mxcd/go-wide-event-logger`: the migration guide is in the 1.0.0 entry.

## License

[MIT](LICENSE)
