package allino

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/wh-kuromai/allino/internal/ema"
	"go.uber.org/zap"
)

type resourceTestInput struct{ Group, Value string }

func (in resourceTestInput) ResourceKey() string { return in.Group }

func enqueueResourceTest(t *testing.T, c *callSQLStrategy, handler, key, resource string, interval time.Duration, concurrency, priority int) {
	t.Helper()
	ok, err := c.Enqueue(context.Background(), handler, &JobMeta{Version: "1.0.0", Status: statusQueued, Priority: priority, ResourceKey: resource, resourceMinInterval: interval, resourceMaxConcurrent: concurrency}, key, []byte(`{}`), 0)
	if err != nil || !ok {
		t.Fatalf("enqueue %s: %v %v", key, ok, err)
	}
}
func dequeueResourceTest(c *callSQLStrategy, handlers ...string) (*sqlJobTask, error) {
	task, err := c.Dequeue(context.Background(), handlers, time.Minute, ema.NewEMACalculator(0.3))
	if err != nil {
		return nil, err
	}
	return task.(*sqlJobTask), nil
}
func resourceNext(t *testing.T, c *callSQLStrategy, key string) time.Time {
	t.Helper()
	var next time.Time
	if err := c.db.QueryRow("SELECT next_start_at FROM job_resources WHERE resource_key=?", key).Scan(&next); err != nil {
		t.Fatal(err)
	}
	return next
}
func TestResourceRequestMetadataAndIdentity(t *testing.T) {
	opt := &Option{Name: "fetch", JobMode: JOBMODE_DISPATCH, Job: JobOption{Async: true, Cache: true, MinInterval: time.Minute, MaxConcurrentPerResource: 1}}
	sv := managerTestServer(context.Background(), 1, opt)
	r := NewRuntime(sv, nil)
	for _, input := range []any{resourceTestInput{"domain:example.com", "a"}, &resourceTestInput{"domain:example.com", "a"}} {
		execution := jobExecutionContext{r: r, opt: opt, input: input}
		if err := execution.MarshalCheck(); err != nil {
			t.Fatal(err)
		}
		meta := execution.JobMeta(statusQueued)
		if meta.ResourceKey != "domain:example.com" || meta.resourceMinInterval != time.Minute || meta.resourceMaxConcurrent != 1 {
			t.Fatal(meta)
		}
	}
	a := jobExecutionContext{r: r, opt: opt, input: resourceTestInput{"domain:example.com", "a"}}
	b := jobExecutionContext{r: r, opt: opt, input: resourceTestInput{"domain:example.com", "b"}}
	a.MarshalCheck()
	b.MarshalCheck()
	if a.JobID() == b.JobID() {
		t.Fatal("resource key replaced job identity")
	}
	missing := jobExecutionContext{r: r, opt: opt, input: struct{ URL string }{"https://example.com"}}
	if err := missing.MarshalCheck(); err == nil {
		t.Fatal("missing ResourceRequest accepted")
	}
	empty := jobExecutionContext{r: r, opt: opt, input: resourceTestInput{Value: "ungrouped"}}
	if err := empty.MarshalCheck(); err != nil {
		t.Fatal(err)
	}
	if empty.JobMeta(statusQueued).ResourceKey != "" {
		t.Fatal("empty key changed")
	}
}
func TestResourceOptionValidation(t *testing.T) {
	for _, o := range []Option{{JobMode: JOBMODE_CACHE, Job: JobOption{MinInterval: time.Minute}}, {JobMode: JOBMODE_ASYNC, Job: JobOption{MinInterval: -1}}, {JobMode: JOBMODE_DISPATCH, Job: JobOption{MaxConcurrentPerResource: -1}}} {
		if err := validateResourceOptions(&o); err == nil {
			t.Fatal("invalid resource options accepted")
		}
	}
	for _, mode := range []string{JOBMODE_ASYNC, JOBMODE_DISPATCH} {
		if err := validateResourceOptions(&Option{JobMode: mode, Job: JobOption{MinInterval: time.Minute}}); err != nil {
			t.Fatal(err)
		}
	}
}
func TestResourceBlockedJobsStayQueuedAcrossHandlers(t *testing.T) {
	c := newLeaseTestStrategy(t)
	ctx := context.Background()
	enqueueResourceTest(t, c, "fetch", "a1", "domain:a", 0, 1, 3)
	enqueueResourceTest(t, c, "other-fetch", "a2", "domain:a", 0, 1, 2)
	enqueueResourceTest(t, c, "fetch", "b1", "domain:b", 0, 1, 1)
	first, err := dequeueResourceTest(c, "fetch", "other-fetch")
	if err != nil || first.Key() != "a1" {
		t.Fatal(first, err)
	}
	if err = first.Start(ctx); err != nil {
		t.Fatal(err)
	}
	other, err := dequeueResourceTest(c, "fetch", "other-fetch")
	if err != nil || other.Key() != "b1" {
		t.Fatal(other, err)
	}
	if _, err = dequeueResourceTest(c, "fetch", "other-fetch"); !errors.Is(err, ErrJobNotFound) {
		t.Fatal(err)
	}
	var status int
	var retries sql.NullInt64
	var token sql.NullString
	if err = c.db.QueryRow("SELECT status,retry_count,lease_token FROM executions WHERE key='a2'").Scan(&status, &retries, &token); err != nil {
		t.Fatal(err)
	}
	if status != statusQueued || retries.Int64 != 0 || token.Valid {
		t.Fatalf("blocked job changed: status=%d retries=%v token=%v", status, retries, token)
	}
	if err = first.Fail(ctx); err != nil {
		t.Fatal(err)
	}
	second, err := dequeueResourceTest(c, "fetch", "other-fetch")
	if err != nil || second.Key() != "a2" {
		t.Fatal(second, err)
	}
}
func TestResourceIntervalPersistsAcrossStrategies(t *testing.T) {
	c := newLeaseTestStrategy(t)
	ctx := context.Background()
	enqueueResourceTest(t, c, "fetch", "first", "domain:a", time.Minute, 0, 2)
	enqueueResourceTest(t, c, "fetch", "second", "domain:a", time.Minute, 0, 1)
	first, err := dequeueResourceTest(c, "fetch")
	if err != nil {
		t.Fatal(err)
	}
	before := time.Now()
	if err = first.Start(ctx); err != nil {
		t.Fatal(err)
	}
	next := resourceNext(t, c, "domain:a")
	if next.Before(before.Add(time.Minute)) {
		t.Fatal("interval not recorded at actual handler start")
	}
	if err = first.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if !resourceNext(t, c, "domain:a").Equal(next) {
		t.Fatal("repeated Start moved the interval")
	}
	if err = first.Fail(ctx); err != nil {
		t.Fatal(err)
	}
	reopened := &callSQLStrategy{db: c.db, issqlite: true, logger: zap.NewNop()}
	if _, err = dequeueResourceTest(reopened, "fetch"); !errors.Is(err, ErrJobNotFound) {
		t.Fatal("cooldown lost across worker strategy", err)
	}
	if _, err = c.db.Exec("UPDATE job_resources SET next_start_at=? WHERE resource_key='domain:a'", time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	second, err := dequeueResourceTest(reopened, "fetch")
	if err != nil || second.Key() != "second" {
		t.Fatal(second, err)
	}
}
func TestResourceConcurrencyAndPendingStart(t *testing.T) {
	c := newLeaseTestStrategy(t)
	ctx := context.Background()
	for i, key := range []string{"first", "second", "third"} {
		enqueueResourceTest(t, c, "fetch", key, "profile:a", 0, 2, 3-i)
	}
	first, err := dequeueResourceTest(c, "fetch")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = dequeueResourceTest(c, "fetch"); !errors.Is(err, ErrJobNotFound) {
		t.Fatal("unstarted reservation bypassed", err)
	}
	if err = first.Start(ctx); err != nil {
		t.Fatal(err)
	}
	second, err := dequeueResourceTest(c, "fetch")
	if err != nil {
		t.Fatal(err)
	}
	if err = second.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = dequeueResourceTest(c, "fetch"); !errors.Is(err, ErrJobNotFound) {
		t.Fatal("concurrency exceeded", err)
	}
	if err = first.Fail(ctx); err != nil {
		t.Fatal(err)
	}
	third, err := dequeueResourceTest(c, "fetch")
	if err != nil || third.Key() != "third" {
		t.Fatal(third, err)
	}
}
func TestResourceStaleStartCannotChangeCooldown(t *testing.T) {
	c := newLeaseTestStrategy(t)
	ctx := context.Background()
	enqueueResourceTest(t, c, "fetch", "old", "domain:a", time.Minute, 1, 2)
	enqueueResourceTest(t, c, "fetch", "new", "domain:a", time.Minute, 1, 1)
	old, err := dequeueResourceTest(c, "fetch")
	if err != nil {
		t.Fatal(err)
	}
	expireLeaseTest(t, c, "old")
	current, err := dequeueResourceTest(c, "fetch")
	if err != nil || current.Key() != "new" {
		t.Fatal(current, err)
	}
	before := resourceNext(t, c, "domain:a")
	if err = old.Start(ctx); !errors.Is(err, ErrJobLeaseLost) {
		t.Fatal(err)
	}
	if !resourceNext(t, c, "domain:a").Equal(before) {
		t.Fatal("stale Start advanced resource cooldown")
	}
	if err = current.Start(ctx); err != nil {
		t.Fatal(err)
	}
}
func TestResourcePolicyConflictRollsBackEnqueue(t *testing.T) {
	c := newLeaseTestStrategy(t)
	enqueueResourceTest(t, c, "one", "first", "domain:a", time.Minute, 1, 0)
	_, err := c.Enqueue(context.Background(), "two", &JobMeta{Status: statusQueued, ResourceKey: "domain:a", resourceMinInterval: time.Second, resourceMaxConcurrent: 1}, "conflict", nil, 0)
	if !errors.Is(err, ErrResourcePolicyConflict) {
		t.Fatal(err)
	}
	var count int
	c.db.QueryRow("SELECT COUNT(*) FROM executions WHERE key='conflict'").Scan(&count)
	if count != 0 {
		t.Fatal("conflicting job persisted")
	}
	enqueueResourceTest(t, c, "two", "same-policy", "domain:a", time.Minute, 1, 0)
}
func TestResourceMigrationPreservesExistingQueue(t *testing.T) {
	c := newLeaseTestStrategy(t)
	enqueueLeaseTest(t, c, "before-upgrade")
	for _, query := range []string{"DROP INDEX idx_exec_resource_lease", "DROP TABLE job_resources", "ALTER TABLE executions DROP COLUMN resource_key", "ALTER TABLE executions DROP COLUMN resource_started_at"} {
		if _, err := c.db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Init(context.Background(), false); err == nil {
		t.Fatal("missing resource schema accepted")
	}
	for i := 0; i < 2; i++ {
		if err := c.Init(context.Background(), true); err != nil {
			t.Fatal(err)
		}
	}
	task := dequeueLeaseTest(t, c)
	if task.Key() != "before-upgrade" || task.Meta().ResourceKey != "" {
		t.Fatal(task)
	}
}
func TestResourceConcurrentDatabaseConnections(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shared.db")
	open := func() *callSQLStrategy {
		db, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		db.SetMaxOpenConns(1)
		t.Cleanup(func() { db.Close() })
		if _, err = db.Exec("PRAGMA busy_timeout=3000"); err != nil {
			t.Fatal(err)
		}
		c := &callSQLStrategy{db: db, logger: zap.NewNop()}
		if err = c.Init(context.Background(), true); err != nil {
			t.Fatal(err)
		}
		return c
	}
	a, b := open(), open()
	enqueueResourceTest(t, a, "fetch", "one", "domain:a", 0, 1, 0)
	enqueueResourceTest(t, a, "fetch", "two", "domain:a", 0, 1, 0)
	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for _, c := range []*callSQLStrategy{a, b} {
		wg.Add(1)
		go func(c *callSQLStrategy) {
			defer wg.Done()
			<-start
			_, err := dequeueResourceTest(c, "fetch")
			results <- err
		}(c)
	}
	close(start)
	wg.Wait()
	close(results)
	acquired, blocked := 0, 0
	for err := range results {
		if err == nil {
			acquired++
		} else if errors.Is(err, ErrJobNotFound) {
			blocked++
		} else {
			t.Fatal(err)
		}
	}
	if acquired != 1 || blocked != 1 {
		t.Fatalf("acquired=%d blocked=%d", acquired, blocked)
	}
}

func TestResourceStartDefersWithoutRetry(t *testing.T) {
	c := newLeaseTestStrategy(t)
	enqueueResourceTest(t, c, "fetch", "one", "domain:a", time.Minute, 1, 0)
	task, err := dequeueResourceTest(c, "fetch")
	if err != nil {
		t.Fatal(err)
	}
	later := time.Now().Add(10 * time.Second)
	if _, err = c.db.Exec("UPDATE job_resources SET next_start_at=? WHERE resource_key='domain:a'", later); err != nil {
		t.Fatal(err)
	}
	if err = task.Start(context.Background()); !errors.Is(err, errResourceDeferred) {
		t.Fatal(err)
	}
	var status int
	var retry sql.NullInt64
	var token sql.NullString
	var runAt time.Time
	if err = c.db.QueryRow("SELECT status,retry_count,lease_token,run_at FROM executions WHERE key='one'").Scan(&status, &retry, &token, &runAt); err != nil {
		t.Fatal(err)
	}
	if status != statusQueued || retry.Int64 != 0 || token.Valid || !runAt.Equal(later) {
		t.Fatalf("defer mutated attempts: %d %v %v %v", status, retry, token, runAt)
	}
}

func TestResourceWorkerWaitsUntilAllQueuedWorkCompletes(t *testing.T) {
	c := newLeaseTestStrategy(t)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	starts := make(chan time.Time, 2)
	opt := &Option{invoker: func(*Runtime, string, string, []byte, bool, func(any) error) (string, []byte, []byte, error) {
		starts <- time.Now()
		return "", nil, nil, nil
	}}
	sv := managerTestServer(ctx, 2, opt)
	sv.SQL = c.db
	sv.Config.JobConfig.IdleInterval = 2 * time.Millisecond
	interval := 150 * time.Millisecond
	enqueueResourceTest(t, c, "fetch", "first", "domain:a", interval, 1, 2)
	enqueueResourceTest(t, c, "fetch", "second", "domain:a", interval, 1, 1)
	jm := newJobManager()
	jm.registerHandler("fetch", opt)
	jm.WorkerInit(c, sv)
	completed := make(chan error, 1)
	go func() { completed <- jm.WaitForJob(ctx, c, "", nil) }()
	var first time.Time
	select {
	case first = <-starts:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case err := <-completed:
		t.Fatalf("wait finished before resource-delayed work: %v", err)
	case <-time.After(60 * time.Millisecond):
	}
	var second time.Time
	select {
	case second = <-starts:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if second.Sub(first) < interval-10*time.Millisecond {
		t.Fatalf("resource interval was shortened: %v", second.Sub(first))
	}
	select {
	case err := <-completed:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestResourceHTTPStyleCallsAreQueued(t *testing.T) {
	for _, mode := range []string{JOBMODE_ASYNC, JOBMODE_DISPATCH} {
		t.Run(mode, func(t *testing.T) {
			c := newLeaseTestStrategy(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			oldLen := len(FunctionList)
			name := "resource-http-test-" + mode
			defer func() { FunctionList = FunctionList[:oldLen]; delete(handlerMarshalMap, name) }()
			called := false
			fn := NewFunction(Option{Name: name, Version: "1.0.0", JobMode: mode, Job: JobOption{MinInterval: time.Minute, MaxConcurrentPerResource: 1}}, func(*Runtime, resourceTestInput) (string, error) {
				called = true
				return "unexpected direct execution", nil
			})
			opt := fn.Options()
			sv := managerTestServer(ctx, 1, opt)
			sv.SQL = c.db
			sv.Validator = validator.New()
			sv.callSQLStrategy = c
			sv.jobManager = newJobManager()
			if err := callSQLInit(sv, opt); err != nil {
				t.Fatal(err)
			}
			_, err := fn.call_internal(NewRuntime(sv, nil), resourceTestInput{Group: "domain:example.com", Value: "one"}, false)
			var pending *JobPendingError
			if !errors.As(err, &pending) || called {
				t.Fatalf("direct call bypassed scheduler: called=%v err=%v", called, err)
			}
			var resource string
			if err = c.db.QueryRow("SELECT resource_key FROM executions WHERE key=?", pending.JobID).Scan(&resource); err != nil || resource != "domain:example.com" {
				t.Fatal(resource, err)
			}
			_, _, errJSON, systemErr := fn.invokeFunctionJSON(NewRuntime(sv, nil), name, "1.0.0", []byte(`{"Group":"domain:example.com","Value":"cli"}`), true, nil)
			if systemErr != nil || errJSON == nil || called {
				t.Fatalf("CLI invocation bypassed scheduler: called=%v systemErr=%v errJSON=%s", called, systemErr, errJSON)
			}
			var count int
			if err = c.db.QueryRow("SELECT COUNT(*) FROM executions WHERE resource_key='domain:example.com'").Scan(&count); err != nil || count != 2 {
				t.Fatalf("queued CLI job missing: count=%d err=%v", count, err)
			}
		})
	}
}
