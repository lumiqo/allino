# allino

[![Go Report Card](https://goreportcard.com/badge/github.com/wh-kuromai/allino)](https://goreportcard.com/report/github.com/wh-kuromai/allino)
[![Go Reference](https://pkg.go.dev/badge/github.com/wh-kuromai/allino.svg)](https://pkg.go.dev/github.com/wh-kuromai/allino)

**Typed function runtime for Go backends.**

allino turns ordinary Go functions with typed input and output into HTTP APIs,
OpenAPI definitions, MCP tools/resources/prompts, CLI-visible handlers, cached
executions, background jobs, Casbin-backed ACL checks, and authenticated
application endpoints.

The core idea is small:

```go
func(r *allino.Runtime, input Input) (*Output, error)
```

Write the function once. Let allino provide the runtime surface around it.

---

## Why allino?

Most Go web frameworks start from routes and middleware. allino starts from a
typed function.

That single function can be:

- exposed as an HTTP API with validation and JSON/HTML response handling
- documented as OpenAPI from its Go input/output types
- exposed as an MCP tool, resource, or prompt
- executed through CLI/job infrastructure by handler name
- cached, deduplicated, dispatched, replayed, or resumed
- protected by Casbin ACL rules with tenant-aware authorization
- given access to shared runtime services such as logging, Redis, SQL, S3, AI
  clients, sessions, authentication, and request IDs

This makes allino useful for backend code that has to be called in several ways:
from browsers, scripts, LLM agents, job workers, tests, and internal Go code.

## Install

```bash
go get github.com/wh-kuromai/allino@latest
```

Your entry point can stay small:

```go
package main

import (
	"github.com/wh-kuromai/allino"
	_ "github.com/wh-kuromai/allino/example/handlers"
)

func main() {
	allino.RunCLI(nil)
}
```

Functions are registered by importing packages that define them. Manual
registration is also supported when you want explicit control.

## A typed HTTP API

```go
package handlers

import (
	"time"

	"github.com/wh-kuromai/allino"
)

type HealthcheckInput struct {
	Echo string `query:"echo"`
}

type HealthcheckOutput struct {
	Status  string    `json:"status"`
	Echo    string    `json:"echo,omitempty"`
	StartAt time.Time `json:"startAt"`
}

var Healthcheck = allino.NewFunction(
	allino.Option{
		Path:        "/api/healthcheck",
		Method:      "GET",
		ContentType: allino.JSON,
		Summary:     "Health check",
		Description: "Returns server status and echoes an optional query value.",
	},
	func(r *allino.Runtime, input HealthcheckInput) (*HealthcheckOutput, error) {
		return &HealthcheckOutput{
			Status:  "OK",
			Echo:    input.Echo,
			StartAt: r.Config.StartAt,
		}, nil
	},
)
```

allino parses path/query/form/header/cookie/JWT values into the input struct,
applies `go-playground/validator` validation, calls the function, and writes the
typed output as the response.

## CLI and docs from the same functions

Every allino app ships with a CLI.

```bash
go run main.go
go run main.go route
go run main.go openapi
go run main.go mcp
go run main.go serve
```

The CLI can list routes, generate OpenAPI, inspect MCP exposure, encrypt config,
generate keys, and start the server. See [CLI docs](./docs/CLI.md) for examples.

## MCP from typed Go functions

Set `Option.MCP` and allino exposes the function through a streamable HTTP MCP
endpoint.

```go
type EchoToolInput struct {
	Message string `json:"message" validate:"required"`
}

type EchoToolOutput struct {
	Echo string `json:"echo"`
}

var EchoTool = allino.NewFunction(
	allino.Option{
		Name:        "echo",
		Description: "Echoes a message.",
		ContentType: allino.JSON,
		MCP:         "tool",
	},
	func(r *allino.Runtime, input *EchoToolInput) (*EchoToolOutput, error) {
		return &EchoToolOutput{Echo: input.Message}, nil
	},
)
```

The same type metadata used for OpenAPI is used for MCP input/output schemas.
`MCP: "resource"` and `MCP: "prompt"` are also supported, and Markdown prompt
directories can be mounted from config. See [MCP docs](./docs/MCP.md).

## Cached and resumable function execution

allino's job system is best understood as execution modes for typed functions.
It is useful when a function calls an AI model, external API, browser automation,
batch process, or any expensive operation that should be cached, deduplicated, or
resumed later.

```go
var ExpensiveLookup = allino.NewFunction(
	allino.Option{
		Name:        "expensive-lookup",
		Version:     "1.0.0",
		Path:        "/api/lookup",
		Method:      "GET",
		ContentType: allino.JSON,
		JobMode:     "cache",
	},
	func(r *allino.Runtime, input LookupInput) (*LookupOutput, error) {
		// Call an LLM, external API, crawler, or long-running computation.
		return lookup(r, input)
	},
)
```

`Function.CallCache(r, input)` reads a saved successful result without executing
the function, including expired results, and returns cache state and timestamps.
`Function.CallRefresh(r, input)` explicitly refreshes using the existing JobMode.
See [cache reads and refreshes](docs/CALL_CACHE.md) for usage and retention semantics.

Supported modes include:

- `cache`: store and reuse results by handler name, version, and input
- `dedupe`: prevent concurrent duplicate work for the same input
- `once`: run once per unique input
- `memoized`: wait for the first execution and reuse its result
- `async` / `dispatch`: enqueue work and resume later
- `fanout` / `replay` / `replayall`: Redis stream based broadcast/replay modes

Queued SQL jobs also support resource-scoped scheduling through an input method
`ResourceKey() string`. Set `Job.MinInterval` and/or
`Job.MaxConcurrentPerResource` to coordinate domains, browser profiles or other
shared resources across handlers and worker processes. See
[resource scheduling](./docs/JOB_RESOURCES.md) for the interface and examples.

These modes make allino especially handy for AI calls, external API aggregation,
large batch registration, resumable workflows, and state restoration.

## Authentication, sessions, and runtime services

allino includes application plumbing that is easy to forget until you need it:

- login cookies, access tokens, CSRF tokens, guest sessions, and JWT claims
- `Runtime.User()` with read/write intent checks
- Casbin ACL enforcement via `Option.ACLResource` and `Option.ACLAction`
- CSV/file or PostgreSQL Casbin policies with tenant-aware domain matching
- Redis, SQL, S3, HTTP client, logger, request ID, and context access
- sticky sessions for stateful or non-serializable runtime objects
- config loading from YAML/JSON/encrypted files
- structured logging, access logs, rotation, and cron-based rotation
- extension hooks for startup, shutdown, authz, injection, SQL schema, and CLI

Casbin is enabled by setting `casbin.model` in config. Policies can be loaded
from a CSV file or PostgreSQL adapter. For handler-level ACL checks, set
`ACLResource` and optionally `ACLAction`; resource/action templates can use
input values tagged with `acl:"name"`.

```go
type BookInput struct {
	BookID string `path:"book_id" acl:"book"`
}

var ReadBook = allino.NewFunction(
	allino.Option{
		Path:        "/books/:book_id",
		Method:      "GET",
		ContentType: allino.JSON,
		ACLResource: "books/{book}",
		ACLAction:   "read",
	},
	func(r *allino.Runtime, input BookInput) (map[string]string, error) {
		return map[string]string{"book": input.BookID}, nil
	},
)
```

At request time, allino reads the authenticated user with `Runtime.User()`, reads
the tenant from the configured JWT claim (`tenant` by default), expands ACL
templates from input fields, and calls Casbin as `(tenant, uid, resource,
action)`. `Runtime.Enforcer()` is available when a handler needs direct access
to the underlying `*casbin.SyncedEnforcer`.

See [CONFIG](./docs/CONFIG.md), [EXTENSION](./docs/EXTENSION.md), and
[TEST](./docs/TEST.md) for the detailed behavior.

## AI-friendly, but not AI-only

allino was originally designed so AI could generate Go backend code without
re-inventing Redis clients, loggers, validation, authentication, and response
formats inside every handler.

That is still useful. But the more general value is that allino gives both humans
and AI a compact, typed target:

```go
input struct -> function -> output struct
```

That shape is easy to test, document, expose to agents, cache, run in workers,
and evolve over time.

## Documentation

- [CLI](./docs/CLI.md)
- [MCP](./docs/MCP.md)
- [Configuration](./docs/CONFIG.md)
- [Extensions](./docs/EXTENSION.md)
- [Testing](./docs/TEST.md)
- [Old README](./docs/OLD_README.md)

---

## AI Prompt Template

You can paste the following after your API idea, and get working `allino` code instantly:

```go
// allino typed function runtime for Go backends
//   allino turns typed Go functions into HTTP APIs, OpenAPI definitions, MCP tools/resources/prompts,
//   CLI-visible handlers, cached executions, background jobs, Casbin-backed ACL checks,
//   and authenticated application endpoints.
//   The core design is input struct -> function -> output struct.
//   It uses Go generics and reflection metadata so validation, schema generation, job input/output,
//   MCP schemas, and tests can share the same function definition.
//   Use this framework to implement the API or tool requested by the user.
// Input:
//   - Fields are populated in order: path parameters → query parameters → form values.
//   - Validated using go-playground/Validator. Then passed to the handler function.
//   - Field types can be string, []byte, int, *multipart.FileHeader, or other primitive types.
//   - If no input is needed, use `any` as the input type to indicate that no data is required.
// Output:
//   JSON:
//     - Return a struct or a pointer to a struct. Automatically wrapped as {"data":{...}}, marshaled via json.Marshal, and written to response.
//     - Avoid using `any` as the return type in JSON APIs, as it prevents OpenAPI schema generation.
//   HTML:
//     - If returning string or []byte, it will be written directly to the response.
//     - If returning any other object and Option.HTMLTemplate is set,
//       the value will be passed to html/template as the template data and rendered.
//     - If an unsupported type is returned without a template, it will be converted to `string` via `fmt.Sprint`.
// Error:
//   JSON:
//     - If returning an error, it is wrapped as {"error": {...}} and written with default status code.
//     - If returning CodeError, it is marshaled and written with specified StatusCode.
//   HTML:
//     - If returning error, it redirects to a default error page.
//     - If returning RedirectError, sends err.StatusCode and redirects to err.Location.
package allino //github.com/wh-kuromai/allino
func NewFunction[T, U any, E error](options Option, handlefunc func(r *Runtime, input T) (output U, err E)) Function
// Create AI function with specified model and prompt.
// tools are used for FunctionCall feature for supported model.
func NewAI[T, U any](option Option, model, prompt string, tools ...Function) Function
type Function interface {
  // Call executes the handler.
  // This can be used inside another handler or from application code.
  Call(r *Runtime, input any) (output any, err error)
}
type Runtime struct {}
func (r *Runtime) Fiber() *fiber.Ctx // only avaiable via http request. (nil if virtual)
func (r *Runtime) Logger() *zap.Logger // use this for logging, inited via config.
func (r *Runtime) Redis() redis.UniversalClient // go-redis Client, inited via config.
func (r *Runtime) SQL() *sql.DB // pre-Opened sql Client, inited via config.
func (r *Runtime) S3() *s3.Client // AWS SDK v2 S3 client, inited via config. (MinIO/AWS)
func (r *Runtime) HttpClient() *http.Client // inited shared http client.
func (r *Runtime) Enforcer() *casbin.SyncedEnforcer // Casbin enforcer, nil when config.casbin.model is not set.
// User() checks and validates using Cookie, Authorization header, X-CSRF-Token header or `csrf_token` form data.
// Returns uid (database key), display name, and sets writable=true only when a write-intent credential is presented
// (e.g., Authorization header or an explicit token in form/query/header) and CSRF validation succeeds.
// Otherwise the user is treated as read-only.
func (r *Runtime) User() (uid, displayname string, writable bool, err error)
// RequestID() returns unique generated xid, X-Request-ID header if config.trustedproxy.trustXRequestID is true,
// async/dispatch JobID, or fanout/replay/replayall redis stream MessageID.
func (r *Runtime) RequestID() string
// SessionID() returns the session ID from the guest cookie.
// If the cookie is missing, it generates a new ID and sets it via fiber.Ctx.
func (r *Runtime) SessionID() string
func (r *Runtime) Context() context.Context
// NewCodeError makes error returning specified http status code.
func NewCodeError(statusCode int, code string, msg string) error
// fmt.Errorf version of NewCodeError. (compatible with errors.Is/errors.As)
func NewCodeErrorf(statusCode int, code string, format strng, a ...any) error
// or implememnts HttpError interface to your own error for specify status code.
type HttpError interface {
	StatusCode() int
}
// NewStatusError makes error returns only status code.
func NewStatusError(status int) error
// NewRedirectError makes error performing redirect. Since allino requires typed responses via generics, redirects are treated as error values.
func NewRedirectError(status int, location string) error
// IssueCSRFToken issues a short-lived token used to protect write operations from CSRF attacks.
// by default, this token should be added to `csrf_token` query or form parameter.
func IssueCSRFToken(r *Runtime, uid string) string
// IssueAccessToken issues a short-lived token used to API access.
// Optional custom JWT claims can be provided; they can later be retrieved via struct fields tagged with `jwt:"key"`.
func IssueAccessToken(r *Runtime, uid, displayname string, jwt_custom_claims ...map[string]any) string
// IssueLoginCookie issues a login cookie for user authentication.
func IssueLoginCookie(r *Runtime, uid, displayname string, jwt_custom_claims ...map[string]any) *fiber.Cookie
type Option struct {
	Path string
	Method string // "GET", "POST", etc.
	SubMethod []string
	ContentType string // e.g. "application/json"
	CORS bool // if true, add Access-Control-Allow-Origin:* to OPTIONS request
	NoWrapJSON bool // if true, do not pack {"data":{...}} or {"error":{...}}, ignore when content-type is not json.
  Summary string // OpenAPI Operation Summary
	Description string // OpenAPI Operation Description
  ResponseStatusCode int // default is 200. Also used as the response code in the OpenAPI spec.
	ErrorStatusCode    int // default is 400.
	HTMLTemplate       string // html/template text
	OnInit     func(s *Server, virtual *Runtime) error // Init code for this handler, use Request for DB or Logger.
	OnShutdown func(s *Server, virtual *Runtime) error // Finalize code for this handler, use Request for DB or Logger.

  // ACLResource enables Casbin authorization for this handler.
  // ACLResource and ACLAction may contain `{name}` or `:name` placeholders populated from input fields tagged `acl:"name"`.
  // At runtime, allino calls Casbin Enforce(tenant, uid, resource, action), where tenant comes from config.casbin.tenant_claim (`tenant` by default).
  ACLResource string
  ACLAction string // optional. Defaults to "access".

  Name    string // Logical name of this handler. Required when using Job mode.
  Version string // Semantic version of the handler (e.g. "1.0.0"). Optional.

  // MCP exposes this function through POST /mcp.
  // Supported values: "tool", "resource", "prompt".
  // Name and Description are used for MCP metadata.
  MCP string

  Class string // Make this handler class method.

  // Cron expression to schedule this handler.
  // custom `?` specifier for random number, `N-M?` for random number between N and M.
  Cron string

  // JobMode defines the execution behavior of this handler.
  // Note: Certain modes require the handler to be idempotent. Allino caches/stores
  // results using a key: Name + Version + hash(input).
  //
  //   "" (Default) : Standard synchronous execution.
  //   "cache"      : Caches the result based on input JSON; returns the cached result if available.
  //   "dedupe"     : Ensures only one execution runs concurrently for the same input; returns allino.ErrJobDuplicated if dupulicated. (Requires idempotency)
  //   "once"       : Ensures the handler runs only once per unique input; Subsequent calls return allino.ErrJobDuplicated. (Requires idempotency)
  //   "memoized"   : Ensures the handler runs only once per unique input; Subsequent calls wait complete and return cached result. (Requires idempotency)
  //
  // Async job mode:
  //   "async"      : Runs the handler asynchronously. (Internal=true only)
  //   "dispatch"   : Hybrid of Async + Cache. Returns a cached result synchronously if found; otherwise, enqueues as 'async'. (Requires idempotency)
  //
  // Broadcast job mode:
  //   "fanout"    : deliver new jobs, retains last node's processed position.
  //   "replay"    : replay all jobs, retains last node's processed position.
  //   "replayall" : replay all jobs every restart. (for in-memory state)
  JobMode string
  Job JobOption

  // Session configures request-scoped shared state.
  Session allino.Session
}
type JobOption struct {
	Priority int // optional. Priority of the handler's jobs. Higher values indicate higher priority.
  MinInterval time.Duration // optional. Minimum interval per ResourceKey (async/dispatch only).
  MaxConcurrentPerResource int // optional. Maximum live jobs per ResourceKey; 0 is unlimited.
  Interval time.Duration // optional. Approximate interval between executions (used in async/dispatch mode).
  CacheExpire time.Duration // optional. Cache expiration duration. Persistent if 0 (default).

  // Upgrade old input/output data into current version instance.
	OnInputUpgrade  func(version string, old_input_at time.Time, old_input []byte) (bool, any)
	OnOutputUpgrade func(version string, old_output_at time.Time, old_output, old_error []byte) (bool, any, error)

  // OnReplayInit returns the stream position to start replaying after. optional.
  // replayAfter MessageID can be retrieved by r.RequestID()
	OnReplayInit func() (replayAfter string, err error)
  // ReplayTTL automatically removes expired jobs/events from the stream. optional.
	ReplayTTL    time.Duration
}

type IdempotentRequest interface {
	IdempotencyKey() string // Override caches/stores key, when input struct of the handler implements this.
}

type SessionOption struct {
	// Type defines the session backend.
	//   "" or "redis":
	//       Standard distributed session backed by Redis.
	//
	//   "sticky":
	//       Keeps the session object entirely in memory on a single node.
	//       The object is never serialized.
	//
	//       Requests are automatically routed to the owning node,
	//       making this suitable for stateful/non-serializable resources
	//       such as browser automation, AI agents, websocket state,
	//       or other long-lived in-memory objects.
	Type string
  // Name identifies the session group.
  // Handlers sharing the same Name will access the same session instance.
  Name string
  // UseResource declares scheduler resource consumption for sticky sessions.
  // When a sticky session is created, the scheduler allocates the specified
  // resources on the selected node.
  // Available node resources are configured via `config.session.resources`.
  UseResource map[string]int
}

// WithSession retrieves/create a typed session instance associated with the request, then call callback within sync.Mutex lock.
// A session ID is automatically issued and managed via cookies.
func WithSession[S any](r *Runtime, fn func(*S) error) error

func (r *Runtime) MarkAbort() // aborts the current execution (prevent caching or storing result/error)
func (r *Runtime) MarkRequeue() // aborts the execution, and schedules a retry with the same input after a short delay.
func (r *Runtime) MarkRequeueAt(waitsec int) // aborts the execution, and schedules a retry with the same input after the specified seconds.

// Global, TTL-trimmed redis stream with in-memory TTL-rotated radix-tree and exact-match map revoke system
// The in-memory radix tree and map are safely restored during server initialization.
// A scope can be revoked either by exact string match or by prefix match using a trailing wildcard (e.g. "some:string:*").
// TTLs for exact and prefix revocations can be configured via server config.
func (r *Runtime) Revoke(scope string, reason string)
func (r *Runtime) IsRevoked(scope string, issuedAt time.Time) (revoked bool, reason string)

func GetClassHandlers(class string) []Function

// EXAMPLE
import (
	"github.com/wh-kuromai/allino"
)
type SampleAPIInput struct {
	Echo string `query:"echo" validate:"required"` // Required query parameter (validated by go-playground/validator)
	Uid  string `path:"uid"` // Supported parameter tag: path:"path", query:"key", form:"key", jwt:"key" (populated only from a successfully verified JWT Claims), cookie:"name", header:"name"
  Version string `query:"ver" default:"v1"`   // Default values (applied before binding; empty input overwrites)
  // Body SampleAPIInputJSONBody `post:"json"` // Automatically binds JSON body to this field. (json.Unmarshal(body, &param.Body))
  // CLIFilePath string `cli:"path"` // CLI variables (yourapp run YourHandler --set path=abc.txt)

  // `inject` tag will make this handler works like class method.
  //  extensions are responsible for
  //    caller send instanceid as query/form/path input string
  //    -> send to extension
  //    -> extension retreive data, check ACL, then return data.
  //    -> unmarshal into this param.
  // Instance SampleClass `query:"instanceid" inject:"extensionName:action"`
}
type SampleAPIOutput struct {
	Echo    string    `json:"echo,omitempty"`
}
var SampleAPIFunction = allino.NewFunction(
	allino.Option{
		Path:        "/api/:uid/userinfosample", // `fiber` style path-parameter allowed. if you don't need :uid, remove it.
		Method:      "GET",
		ContentType: "application/json", // JSON or HTML, content-type will be sent automatically.
	},
	func(r *allino.Runtime, param SampleAPIInput) (*SampleAPIOutput, error) { // Both value and pointer params work; prefer value for small input structs (fewer allocs/escapes), and prefer pointer for output (short returns `return nil, err`).
		return &SampleAPIOutput{
			Echo:    param.Echo,
		}, nil
	})
```
