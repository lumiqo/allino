# SQL worker capacity and lease ownership

SQL jobs are dequeued inside the shared execution pool. They remain queued in the
DB until an execution slot is available, including when Redis stream handlers
occupy the same pool. A handler interval delay retains that slot and continues
heartbeating; delayed work cannot later execute outside `job.concurrency`.

Each SQL dequeue creates a fresh `executions.lease_token`. Worker-task heartbeat,
success, release and requeue operations must match the token, leased status and
an unexpired lease. A stale task receives `ErrJobLeaseLost`, even if its job key
has been requeued, deleted and recreated, or completed by another worker. Saving
persistent output and completing the execution occur in the same transaction.

When a heartbeat detects lease loss, the worker cancels `Runtime.Context()`.
Handlers should pass this context to browser automation, HTTP and database calls.
An uncooperative handler cannot be forcibly stopped by Go; lease guards still
prevent its eventual result from changing the replacement job. Application-side
external effects must remain idempotent.

This protection applies to dequeued SQL `JobTask` instances. Administrative
`JobStore.Free` / `Requeue` operations intentionally remain key-based. The
synchronous cache/dedupe modes and Redis stream delivery retain their existing
ownership semantics. `async` still discards output on completion; use `dispatch`
when callers need to retrieve a cached result.

## Existing databases

Stop old worker processes before upgrading. They do not enforce lease tokens and
must not run alongside upgraded workers. With `sql.allow_migrate: true`, startup
adds the nullable `lease_token` column without removing queued jobs. The migration
is additive and idempotent. When migration is disabled, a missing column is
reported as a job-backend initialization error; that worker is not started.
Existing unfinished leases are recovered by the normal lease-expiry reaper.

The reaper performs retry/error transitions and expired-cache cleanup in one
transaction. A NULL retry count is treated as zero, including when `max_retry`
is zero. Automatic retry classification for application errors is unchanged.

## Validation

`job_worker_lease_test.go` covers capacity before dequeue, capacity shared with
other tasks, handler interval delays, shutdown, lease-loss cancellation, stale
writes/deletes/requeues/heartbeats, key reuse, persistent and TTL results, retry
limits, and migration preserving queued data. Tests use an isolated SQLite DB
and do not require Redis or Chrome.

At the time of this change the checkout's existing `z_dashboard_api_test.go`
imports a missing `dashboard/api` package, so plain `go test ./...` cannot load
all tests. The focused tests can be run without editing that existing test:

```bash
mapfile -t job_sources < <(find . -maxdepth 1 -name '*.go' ! -name '*_test.go' -print)
go test -race "${job_sources[@]}" job_worker_lease_test.go
```

The full package build can be checked with `go build ./...`. SQLite is the backend
covered here; no claim of validation against another SQL dialect is made.

Validation performed for this change:

- `go build ./...`: passed.
- The 10 focused worker/lease tests above: passed with `-race`.
- Existing `TestJob*` tests and the new `TestWorker*` tests across packages:
  passed using a temporary Go overlay to exclude only the missing-dashboard
  test. Redis-dependent tests were skipped because Redis was unavailable.
- `go vet ./...`: passed with the same temporary overlay.

No application database was migrated during development; migration tests used
throwaway databases. Browser-profile locking, scraper-specific cache keys and
retry/error classification remain integration work for the scraper handlers.
