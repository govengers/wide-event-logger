# Changelog

## 1.0.0

First release under `github.com/govengers/wide-event-logger`. `github.com/mxcd/go-wide-event-logger` is deprecated and stays at v0.3.x.

### Breaking changes and migration

| v0.3 (`github.com/mxcd/go-wide-event-logger`) | 1.0 (`github.com/govengers/wide-event-logger`) |
|---|---|
| `wideevent.Middleware(opts...)` | `ginadapter.NewMiddleware(&ginadapter.Options{...})`, returns an error |
| `WithEmitter(e)` | `Options.Emitter`, now required (was stdout JSON by default) |
| `WithSampler(s)` | wrap the emitter: `Emitter: wideevent.SampledEmitter(e, s)` |
| `WithPreEnricher(fn)`, `WithPostEnricher(fn)` | `Options.PreEnricher`, `Options.PostEnricher` |
| `WithSkipPaths(paths...)` | `Options.SkipPaths` |
| `wideevent.FromGin(c)` | `ginadapter.FromContext(c)` |
| `JSONStdoutEmitter()`, `JSONWriterEmitter(w)` | `NewJSONEmitter(nil)`, `NewJSONEmitter(&JSONEmitterOptions{Writer: w})` |
| `DevStdoutEmitter(opts...)`, `DevWriterEmitter(w, opts...)` | `NewDevEmitter(&DevEmitterOptions{...})`, returns an error |
| `WithColor(bool)` | `DevEmitterOptions.Color`: `ColorAuto`, `ColorAlways`, `ColorNever` |
| `WithCategory(name, prefixes...)` | `DevEmitterOptions.Categories: []Category{{Name, PathPrefixes}}` |
| `WithMuteCategories(names...)` | `DevEmitterOptions.MutedCategories`; an undeclared name is an error |
| `WithDurationThresholds(warn, err)` | `DevEmitterOptions.SlowDuration`, `VerySlowDuration` |
| `Event.Str(k, v)`, `Event.Dur(k, v)` | `Event.String(k, v)`, `Event.Duration(k, v)` (`sed -i 's/\.Str(/.String(/g; s/\.Dur(/.Duration(/g'`) |
| `Event.Interface(k, v)` | `Event.Set(k, v)` |

Logged fields of the Gin middleware:

- `request.query` is redacted: every value becomes `redacted` unless its parameter is in `Options.QueryAllowlist`. Drop pre-enrichers that redacted the query themselves (for example `govengers/auth` `ginadapter.RedactQuery`).
- `response.latency` and `response.latency_ms` are gone; query `duration` (milliseconds in JSON) instead.
- `response.gin_error.<n>` and the count `response.gin_errors` are replaced by `response.gin_errors`, a list of messages, present only when there are errors.
- `request.id` is never empty: without a valid `X-Request-ID` header it is generated, and it is echoed in the `X-Request-ID` response header.
- `name` is `http.request` for every request event; a handler may set its own.
- `response.body_size` is 0, not -1, when the handler wrote no body.
- Client-controlled text is cut: `request.path` to 2048 bytes, `request.query` and `request.user_agent` to 512, `request.host` to 256, `request.method` to 32. Only the first 4096 bytes of the raw query are parsed.
- A panicking `PreEnricher` or `PostEnricher` no longer loses the event; it is recorded like a handler panic.
- `engine.HandleContext` keeps the request's event instead of starting a second one.

Event semantics:

- Setting a key again replaces its value instead of appending a second field. `Fields()` returns each key once, in the order it was first set. The dev emitter therefore prints the latest value only.
- Writes after `Emit` are dropped.
- A key that is also the prefix of other keys keeps its value under `_value` (`{"db": {"_value": "pg", "operation": "update"}}`); before, one of them was lost. `_value` and the root keys `level` and `message` are reserved.

### Fixed

- The JSON emitter lost the whole event when a value could not be encoded (a NaN, an infinity, a channel). NaN and infinities are written as strings, other unencodable values as `"!unencodable <type>"`. A partial write no longer glues the next event onto the fragment.
- An `error` set with `Set` was written as `{}`; it is written as its message.
- The dev emitter colored `outcome=failure` like a neutral value; it is red. It escapes control characters, so a request path cannot forge log lines or send terminal escape sequences, and shows an `error` field that is not a string.
- Methods on a nil `*Event` panicked; they do nothing, so `FromContext(ctx).String(...)` is safe outside a unit of work and on skipped paths.
- `Err` and `UUID` panicked on a typed nil whose `Error` or `String` method dereferences its receiver.
- Events from `Begin` could not be sampled; `SampledEmitter` works for any event. A handler that called `Emit` itself bypassed the middleware's sampler.
- The JSON and dev emitters serialize their writes, so any `io.Writer` is safe.

### Added

- `ginadapter` subpackage; the core package no longer imports Gin.
- `SampledEmitter`, `NewJSONEmitter`, `NewDevEmitter` with options structs.
- Constants for the keys and outcomes the library interprets (`KeyResponseStatus`, `OutcomeFailure`, ...).
- Runnable examples, an end-to-end suite over a real HTTP server, CI with lint, race tests, coverage gate and govulncheck.

### Dependencies

- Go 1.26. Gin 1.12.0; the indirect `golang.org/x/net` and `quic-go` versions with known reachable vulnerabilities are gone.
