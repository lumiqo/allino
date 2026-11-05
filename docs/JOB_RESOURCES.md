# Scheduling jobs by ResourceKey

Queued SQL jobs can share a minimum start interval and a concurrency limit across
handlers and worker processes. Input types implement an interface; `JobOption`
remains non-generic and no struct tags are required.

```go
type ResourceRequest interface {
    ResourceKey() string
}
```

A URL-based input can derive its resource from the hostname:

```go
type FetchInput struct {
    URL string `json:"url"`
}

func (in FetchInput) ResourceKey() string {
    u, err := url.Parse(in.URL)
    if err != nil || u.Hostname() == "" {
        return ""
    }
    return "domain:" + strings.ToLower(u.Hostname())
}
```

The method uses `net/url` and `strings`. The handler should independently validate
its URL before sending a request. This example groups subdomains separately,
combines ports/schemes on the same hostname, and does not reduce a hostname to its
registrable parent domain. Return a different key if another grouping is needed.

Configure the typed handler with:

```go
allino.Option{
    Name:    "http-fetch",
    Version: "1.0.0",
    JobMode: "dispatch",
    Job: allino.JobOption{
        MinInterval:              time.Minute,
        MaxConcurrentPerResource: 1,
        CacheExpire:              30 * time.Second,
    },
}
```

The handler must pass `r.Context()` into HTTP/browser operations. One job invocation
represents one resource-limited operation; redirects, internal retry loops or
multiple requests inside the same handler do not acquire additional permits.

## Contract

- Resource limits are supported for SQL `async` and `dispatch` jobs. Negative
  limits and configurations using other modes are rejected during initialization.
- With either resource limit enabled, input must implement `ResourceRequest`.
  A value receiver works for both value and pointer inputs; pointer receivers
  require the actual input value to be a pointer. The method must be deterministic.
- An empty resource key opts that particular input out of scheduling limits.
- Zero `MinInterval` disables interval limiting. Zero
  `MaxConcurrentPerResource` disables the concurrency cap. If both are zero,
  existing behavior is unchanged and no ResourceRequest method is required.
- A key is shared across handler names. `domain:example.com` can coordinate
  several HTTP-fetch functions; `profile:youtube` can protect a browser profile.
- Resource keys are independent of `IdempotencyKey()`, the job ID and cache key.
  Two URLs on the same domain remain different jobs/results. Cached results do
  not perform an operation or consume a new interval.
- Resource-controlled HTTP-style/direct entry points enqueue rather than silently
  executing outside the scheduler. Callers receive the normal pending-job result.
- `MinInterval` measures SQL start-permit times immediately before handler
  invocation, after any legacy per-handler interval delay. It does not claim to
  measure network packet transmission time. `Job.Interval` retains its existing
  approximate, handler-wide behavior and is separate from resource limits.

All handlers using the same key must specify the same interval and concurrency
limit. A conflicting policy returns `ErrResourcePolicyConflict` and does not
queue the new job. Policies persist across process restarts. Changing an existing
policy currently requires an explicit administrative database update after
quiescing that resource, or a newly versioned resource key; silently weakening
another handler's limit is avoided.

## Queue and lease behavior

1. When enqueuing, the framework computes ResourceKey from the typed input and
   persists it on the execution. It registers the policy in `job_resources` in
   the same transaction.
2. Dequeue skips resources whose cooldown has not expired or whose live leases
   fill the concurrency cap. Other eligible resources remain runnable, including
   when blocked work has higher priority.
3. A leased but not-yet-started task reserves its resource until its start check.
   This prevents several workers from claiming the same start opportunity.
4. Immediately before invoking the handler, the task checks lease ownership and
   records the start and next permitted time atomically. Repeated Start calls
   on the same live attempt do not extend the cooldown.
5. Heartbeats keep running reservations alive. Success, failure, requeue, expiry
   and administrative deletion release capacity through execution state/leases.
   Old attempts cannot change the resource cooldown after losing ownership.

Resource waiting normally happens while the job is still queued. It does not
occupy a worker and does not increment `retry_count`. If eligibility changes
between dequeue and start, the task is deferred back to the queue without
consuming a retry. Completing before the cooldown ends does not shorten it;
with concurrency=1 a long-running operation also prevents overlap.

`WaitForJob` checks for remaining queued/leased work before returning, so a queue
that contains only resource-delayed jobs is not mistaken for a completed batch.

## Storage and migration

Existing databases receive additive `executions.resource_key` and
`executions.resource_started_at` columns, a `job_resources` policy/state table and
an index. Existing queued jobs default to an empty key and retain their behavior.
Enable `sql.allow_migrate: true` to upgrade; if disabled, a missing resource schema
is reported instead of starting an incompatible worker. Stop old workers before
upgrading, since they do not enforce these gates. SQLite is the tested backend.

`ResourceKey` is available in queued execution metadata returned by `JobStore.List`.
Policies/next-permitted times remain stored until administratively changed; no
automatic resource-state eviction is implemented yet.

## Tests

```sh
go test ./...
go test -race . -run 'TestResource|TestWorker'
go vet ./...
```

Tests cover interface handling, independent job identities, option validation,
policy conflicts, blocked-resource fairness, concurrency limits, cooldown
persistence, stale starts, migration preserving queued jobs, separate SQLite
connections competing for the same resource, deferred starts without retries,
HTTP-style call routing, and completion waiting across a cooldown.

## Managed resources: initialization and idle release

`Job.Resources` adds lifecycle management and multiple resource requirements to
SQL `async` / `dispatch` jobs. It can be used independently of `ResourceKey()`;
inputs do not need an interface. Share one definition pointer across handlers:

```go
var gpu = &allino.JobResource{
    Name:        "ai-server:gpu0",
    Capacity:    1,
    IdleTimeout: 5 * time.Minute,
    Timeout:     time.Minute,
    Init: func(ctx context.Context) error {
        return modelService.Load(ctx)
    },
    Release: func(ctx context.Context) error {
        return modelService.Unload(ctx)
    },
}

// Use these options for both LLM inference and image generation handlers.
options := allino.Option{
    Name: "llm", Version: "1.0.0", JobMode: "dispatch",
    Job: allino.JobOption{Resources: []*allino.JobResource{gpu}},
}
```

`modelService` represents your inference server adapter. See the compilable
[HTTP adapter example](../example/jobresources/main.go), which includes LLM,
image generation, and an independent CPU job. Its `/load` and `/unload` operations
must be implemented by your model service. Run it with
`go run ./example/jobresources serve` after starting that adapter on port 9000.

For a job requiring several resources, list them together:

```go
Job: allino.JobOption{
    Resources: []*allino.JobResource{gpu, browser},
}
```

Each job consumes one permit from every distinct named resource. Duplicate
references to the same definition consume one permit. `Capacity` must be at
least one; negative durations are rejected. `IdleTimeout: 0` releases on the next
idle maintenance pass. `Timeout: 0` gives each callback a one-minute context.
Callbacks may be nil, for example when only a capacity limit is needed.
Definitions must be treated as immutable after registration. Different pointers
with callbacks cannot register the same name in one server. Across servers, a
name must have identical capacity, idle timeout, callback timeout, and equivalent
callback behavior. Policy conflicts fail with `ErrResourcePolicyConflict`.

### Execution and demand

Dequeue tests every required resource and leases the job in one SQL statement.
It never holds a subset while waiting for the rest, preventing resource-order
deadlocks. Blocked high-priority jobs are skipped; eligible jobs retain the
existing priority/FIFO ordering. CPU jobs can use every available worker slot
while GPU jobs wait. This does not reserve a worker slot for CPU jobs when all
slots are already executing other work. Scheduling is polled using the existing
worker backoff; freeing capacity makes jobs eligible on the next poll.

Lifecycle callbacks run outside the worker pool. Until initialization succeeds,
jobs remain queued and consume neither a lease nor a retry. Ready resources are
reused until there has been no queued or leased demand for `IdleTimeout`.
Scheduled jobs and delayed retries count as demand, even if another required
resource prevents them from running. New demand cancels the idle timer. If release
has already begun, the job stays queued until release and reinitialization finish.
Idle detection runs every 100 ms, so release may occur slightly after the deadline.

Success, failure, abort, administrative deletion, and requeue use existing job
state transitions to release capacity; no separate permit counter needs repair.
Requeue retains requirements. Expired execution leases cease consuming capacity,
and the existing reaper retries or marks the job failed. Handlers must stop when
`r.Context()` is canceled, as described in [JOB_LEASES.md](JOB_LEASES.md).
This scheduler cannot forcibly stop an application goroutine or an external GPU
operation after its lease expires.

### Ownership, failure, and shutdown

The database stores lifecycle state and a 30-second owner lease. Only its owner
runs initialization/release; the owner renews every maintenance pass, including
while a callback runs. Other servers can execute jobs against a ready resource.
On owner loss, readiness expires and new jobs wait. A replacement owner first
waits for live execution leases to drain, runs recovery release, then initializes
again if queued demand exists. Late lifecycle completion from the previous owner
is rejected by the database ownership check.

Callbacks must be idempotent, tolerate partially initialized state, and honor
context cancellation. Errors, panics, and callback deadlines make the resource
unavailable. Cleanup is retried after a one-second backoff; successful cleanup
allows initialization to retry. Jobs keep waiting without consuming application
retries. Persistent callback failures therefore leave jobs queued, with diagnostic
logs and `job_managed_resources.last_error` available for investigation.

Go cannot forcibly interrupt a callback. If it ignores its deadline, the resource
stays unavailable and its owner continues renewing ownership until the callback
returns. Cleanup is not allowed to overlap that callback. During server shutdown,
callback contexts are canceled and cleanup is attempted after live job leases
drain; queued jobs do not prevent shutdown cleanup. Shutdown waiting is bounded by
30 seconds plus the longest callback timeout (and a one-second margin). If jobs
do not drain within 30 seconds, cleanup is left to ownership recovery. A callback
that never returns may leave a goroutine until process exit.

Database fencing cannot stop external side effects from a paused or partitioned
old process. For strict fencing, enforce tokens/ownership in the external service
or isolate/terminate the old process before recovery. All servers using the same
name must address the **same underlying resource**: a shared GPU service, browser
service, container, or process endpoint. Process-local model pointers cannot be
shared through SQL. Independent GPUs on different hosts need different names
and appropriately routed handlers. Avoid running mutually dependent work inside
lifecycle callbacks, and do not synchronously await a job needing a resource held
by the calling job.

### Managed resource storage and compatibility

The additive migration creates `job_managed_resources`, `execution_resources`,
an index, and demand/deletion triggers. Existing jobs retain their original requirements;
new handler requirements are attached when a new execution is enqueued. Enable
`sql.allow_migrate: true` once and stop older workers before the upgrade. Policies
persist; change a policy only after quiescing all affected workers/jobs, or use a
new versioned name. SQLite is the supported/tested backend for this feature;
multiple servers must share the same database with reliable SQLite file locking.
For concurrent connections, configure a busy timeout, for example
`jobs.db?_pragma=busy_timeout(5000)` with `modernc.org/sqlite`.

Legacy `ResourceKey()` limits and managed resource requirements are independent
gates and can be combined. Redis stream jobs and synchronous job modes do not
support managed resources. Resource-free jobs keep the existing execution path
and need no new application configuration. There is no strict starvation-free
fairness across jobs requesting different resource combinations, and lifecycle
initialization of different resources runs independently. If two models cannot
coexist in VRAM, manage their loading/switching inside a single shared GPU service;
declaring separate model resources does not itself implement model eviction.

Managed resource tests add coverage for atomic multiple-resource acquisition,
CPU progress during initialization, reuse, queued/retry demand, idle cancellation,
arrival during release, capacity across separate connections, lease expiry,
owner recovery and fencing, callback errors/panics/deadlines, shutdown, and migration.
