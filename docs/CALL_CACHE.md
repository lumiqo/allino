# Reading cached results without execution

```go
previous, cache, err := Fetch.CallCache(r, input)
if err != nil {
    return nil, err
}
if cache.State == allino.CacheFresh || cache.State == allino.CacheExpired {
    // previous is the last saved successful result; compare it with new data.
}
```

`CallCache(r, input)` returns `(output, CacheInfo, error)`. It never executes,
enqueues, waits for a running function, upgrades output, or extends expiration.
It applies the function's normal input validation and ACL checks. The function
must have a cache-enabled JobMode and a configured SQL job store.

States:

- `CacheMiss`: no stored execution/result exists; output is zero and error is nil.
  Deleted records cannot be distinguished from inputs that have never run.
- `CacheFailed`: no successful result exists and the latest stored execution failed.
  Output is zero and error is nil; this describes the execution, not a read failure.
- `CachePending`: no successful result exists and the latest stored execution is
  queued or running. Output is zero and error is nil.
- `CacheFresh`: the saved result is still valid.
- `CacheExpired`: the saved result has expired but is returned for comparison.
- `CacheVersionMismatch`: the saved major/minor version differs; output is zero.
  This state takes precedence over expiration. No upgrade callback is invoked.

`CacheInfo` also includes `UpdatedAt`, `ExpiresAt` (nil for persistent results),
and `Version`. Database and JSON decode failures are returned as errors, not
misses. Cached function errors are not returned by this API. When a successful result
exists, it takes precedence over newer failed or pending executions. Otherwise,
`UpdatedAt`, `ExpiresAt`, and `Version` describe the failed or pending record.

TTL-enabled functions retain their latest successful result in
`executions_results`, using the existing schema. Queue state remains in
`executions`, so enqueue, refresh failure, and expired queue cleanup do not erase
the saved successful result. Snapshot writes share the completion transaction;
worker writes must pass the lease guard. A successful refresh replaces the same
key, rather than appending history. Existing completed rows can be read until
their first refresh/cleanup; results deleted before this change cannot be recovered.

Snapshots currently have no automatic retention limit. They remain one per unique
handler/input key, so unused keys can accumulate. `ExpiresAt` controls freshness,
not snapshot deletion. Queue `Free` does not delete the saved snapshot. Configuring
snapshot retention is a separate future feature. Normal `Call` still follows its configured JobMode. Persistent successful results
are now preserved when a later attempt fails. See `CallRefresh` below for explicit refreshes.

## Explicit refresh

```go
result, err := Fetch.CallRefresh(r, input)
```

`CallRefresh` returns the same `(output, error)` as `Call`. It bypasses the
initial cache lookup but retains normal validation, ACL, resource scheduling,
and worker lease management. It supports cache, memoized, and dispatch modes;
session functions, once mode, and functions without caching are rejected.
The refresh flag belongs to this call only and does not propagate to child calls.

- `cache`: executes synchronously and returns the newly fetched output/error.
  As with normal cache mode, concurrent calls are not deduplicated.
- `memoized`: executes synchronously or waits for an already queued/running
  execution of the same key, then returns that execution's output/error.
- `dispatch`: queues a refresh and returns `JobPendingError` with its JobID.
  Queued/running jobs are reused; no duplicate is queued. Completed or failed
  jobs may be refreshed regardless of TTL, including persistent results.

Read previous successful data with `CallCache`, including from inside the
function being refreshed. A failed refresh retains the previous successful
snapshot. Persistent successful results likewise survive later function errors.
Ordinary calls continue to use their existing JobMode and cache policy.
JobIDs identify the key and are reused; they do not identify historical attempts.
