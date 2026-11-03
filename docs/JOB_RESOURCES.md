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
