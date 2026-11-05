package allino

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-playground/validator/v10"
	"go.uber.org/zap"
)

func managedEnqueue(t *testing.T, c *callSQLStrategy, key string, priority int, resources ...*JobResource) {
	t.Helper()
	ok, err := c.Enqueue(context.Background(), "test", &JobMeta{Status: statusQueued, Priority: priority, resources: resources}, key, []byte(`{}`), 0)
	if err != nil || !ok {
		t.Fatalf("enqueue %s: %v %v", key, ok, err)
	}
}
func managedEventually(t *testing.T, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("resource condition did not become true")
}
func managedState(c *callSQLStrategy, name string) string {
	var state string
	_ = c.db.QueryRow("SELECT state FROM job_managed_resources WHERE name=?", name).Scan(&state)
	return state
}
func managedStart(t *testing.T, c *callSQLStrategy, resources ...*JobResource) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	if err := c.startManagedResources(ctx, resources); err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		if err := c.waitManagedResources(); err != nil {
			t.Error(err)
		}
	})
	return cancel
}
func managedReady(t *testing.T, c *callSQLStrategy, resources ...*JobResource) {
	t.Helper()
	managedEventually(t, func() bool {
		for _, r := range resources {
			if managedState(c, r.Name) != "ready" {
				return false
			}
		}
		return true
	})
}

func TestManagedResourceAtomicAcquisitionAndLeaseRecovery(t *testing.T) {
	c := newLeaseTestStrategy(t)
	gpu := &JobResource{Name: "gpu", Capacity: 1, IdleTimeout: time.Hour}
	browser := &JobResource{Name: "browser", Capacity: 1, IdleTimeout: time.Hour}
	managedStart(t, c, gpu, browser)
	managedEnqueue(t, c, "gpu-only", 30, gpu)
	managedEnqueue(t, c, "both", 20, gpu, browser)
	managedEnqueue(t, c, "browser-only", 10, browser)
	managedEnqueue(t, c, "cpu", 0)
	managedReady(t, c, gpu, browser)
	a := dequeueLeaseTest(t, c)
	if a.Key() != "gpu-only" {
		t.Fatal(a.Key())
	}
	b := dequeueLeaseTest(t, c)
	if b.Key() != "browser-only" {
		t.Fatal("partially acquired resources", b.Key())
	}
	cpu := dequeueLeaseTest(t, c)
	if cpu.Key() != "cpu" {
		t.Fatal(cpu.Key())
	}
	if _, err := dequeueResourceTest(c, "test"); !errors.Is(err, ErrJobNotFound) {
		t.Fatal(err)
	}
	if err := a.Requeue(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	again := dequeueLeaseTest(t, c)
	if again.Key() != "gpu-only" {
		t.Fatal(again.Key())
	}
	expireLeaseTest(t, c, again.Key())
	if err := c.Reaping(context.Background()); err != nil {
		t.Fatal(err)
	}
	// A retry retains all resource requirements and stale attempts cannot free it.
	if err := again.Fail(context.Background()); !errors.Is(err, ErrJobLeaseLost) {
		t.Fatal(err)
	}
	if err := c.Free(context.Background(), again.Key()); err != nil {
		t.Fatal(err)
	}
	if err := b.Fail(context.Background()); err != nil {
		t.Fatal(err)
	}
	both := dequeueLeaseTest(t, c)
	if both.Key() != "both" {
		t.Fatal(both.Key())
	}
	if err := both.Fail(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := cpu.Fail(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestManagedResourceReuseIdleAndQueuedDemand(t *testing.T) {
	c := newLeaseTestStrategy(t)
	var init, release atomic.Int32
	r := &JobResource{Name: "gpu", Capacity: 1, IdleTimeout: 200 * time.Millisecond,
		Init: func(context.Context) error { init.Add(1); return nil }, Release: func(context.Context) error { release.Add(1); return nil }}
	managedStart(t, c, r)
	managedEnqueue(t, c, "first", 0, r)
	managedReady(t, c, r)
	first := dequeueLeaseTest(t, c)
	if err := first.Fail(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Delayed retries count as demand, even when they cannot run now.
	managedEnqueue(t, c, "delayed", 0, r)
	if _, err := c.db.Exec("UPDATE executions SET run_at=? WHERE key='delayed'", time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(450 * time.Millisecond)
	if release.Load() != 0 || init.Load() != 1 {
		t.Fatal("unloaded with queued demand", init.Load(), release.Load())
	}
	if err := c.Free(context.Background(), "delayed"); err != nil {
		t.Fatal(err)
	}
	// Observe the idle timer, then enqueue before it expires.
	managedEventually(t, func() bool {
		var idle sql.NullTime
		_ = c.db.QueryRow("SELECT idle_since FROM job_managed_resources WHERE name='gpu'").Scan(&idle)
		return idle.Valid
	})
	managedEnqueue(t, c, "reuse", 0, r)
	reuse := dequeueLeaseTest(t, c)
	time.Sleep(300 * time.Millisecond)
	if release.Load() != 0 || init.Load() != 1 {
		t.Fatal("idle cancellation failed")
	}
	if err := reuse.Fail(context.Background()); err != nil {
		t.Fatal(err)
	}
	managedEventually(t, func() bool { return release.Load() == 1 && managedState(c, r.Name) == "cold" })
	managedEnqueue(t, c, "reload", 0, r)
	managedReady(t, c, r)
	if init.Load() != 2 {
		t.Fatal(init.Load())
	}
	if err := c.Free(context.Background(), "reload"); err != nil {
		t.Fatal(err)
	}
}

func TestManagedResourceLifecycleDoesNotConsumeWorker(t *testing.T) {
	c := newLeaseTestStrategy(t)
	initEntered := make(chan struct{})
	unblock := make(chan struct{})
	r := &JobResource{Name: "gpu", Capacity: 1, IdleTimeout: time.Hour, Init: func(ctx context.Context) error {
		close(initEntered)
		select {
		case <-unblock:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	managedStart(t, c, r)
	managedEnqueue(t, c, "gpu", 100, r)
	<-initEntered
	managedEnqueue(t, c, "cpu", 0)
	opt := &Option{Name: "test"}
	var ran atomic.Int32
	opt.invoker = func(_ *Runtime, _ string, _ string, _ []byte, _ bool, _ func(any) error) (string, []byte, []byte, error) {
		ran.Add(1)
		return "", nil, nil, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sv := managerTestServer(ctx, 1, opt)
	manager := newJobManager()
	manager.registerHandler("test", opt)
	manager.WorkerInit(c, sv)
	managedEventually(t, func() bool { return ran.Load() == 1 })
	if managedState(c, r.Name) != "initializing" {
		t.Fatal("initialization unexpectedly completed")
	}
	close(unblock)
	managedEventually(t, func() bool { return ran.Load() == 2 })
	cancel()
}

func TestManagedResourceArrivalDuringRelease(t *testing.T) {
	c := newLeaseTestStrategy(t)
	entered := make(chan struct{}, 1)
	unblock := make(chan struct{})
	var releases atomic.Int32
	r := &JobResource{Name: "gpu", Capacity: 1, Release: func(ctx context.Context) error {
		if releases.Add(1) == 1 {
			entered <- struct{}{}
			select {
			case <-unblock:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}}
	managedStart(t, c, r)
	managedEnqueue(t, c, "first", 0, r)
	managedReady(t, c, r)
	if err := c.Free(context.Background(), "first"); err != nil {
		t.Fatal(err)
	}
	<-entered
	managedEnqueue(t, c, "second", 0, r)
	if _, err := dequeueResourceTest(c, "test"); !errors.Is(err, ErrJobNotFound) {
		t.Fatal("ran during release", err)
	}
	close(unblock)
	managedReady(t, c, r)
	second := dequeueLeaseTest(t, c)
	if err := second.Fail(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestManagedResourceCallbackFailureTimeoutAndPanic(t *testing.T) {
	for _, mode := range []string{"error", "timeout", "panic"} {
		t.Run(mode, func(t *testing.T) {
			c := newLeaseTestStrategy(t)
			var inits, releases atomic.Int32
			r := &JobResource{Name: "gpu", Capacity: 1, IdleTimeout: time.Hour, Timeout: 30 * time.Millisecond,
				Init: func(ctx context.Context) error {
					if inits.Add(1) > 1 {
						return nil
					}
					switch mode {
					case "timeout":
						<-ctx.Done()
						return ctx.Err()
					case "panic":
						panic("init panic")
					default:
						return errors.New("init failed")
					}
				},
				Release: func(context.Context) error {
					if releases.Add(1) == 1 {
						return errors.New("release failed")
					}
					return nil
				}}
			managedStart(t, c, r)
			managedEnqueue(t, c, "job", 0, r)
			managedReady(t, c, r)
			if inits.Load() != 2 || releases.Load() != 2 {
				t.Fatal(inits.Load(), releases.Load())
			}
			if err := c.Free(context.Background(), "job"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestManagedResourceUncooperativeCallbackStaysUnavailable(t *testing.T) {
	c := newLeaseTestStrategy(t)
	unblock := make(chan struct{})
	entered := make(chan struct{})
	r := &JobResource{Name: "gpu", Capacity: 1, Timeout: 20 * time.Millisecond, Init: func(context.Context) error { close(entered); <-unblock; return nil }}
	cancel := managedStart(t, c, r)
	managedEnqueue(t, c, "job", 0, r)
	<-entered
	time.Sleep(100 * time.Millisecond)
	if _, err := dequeueResourceTest(c, "test"); !errors.Is(err, ErrJobNotFound) {
		t.Fatal(err)
	}
	if managedState(c, "gpu") != "initializing" {
		t.Fatal("timed out callback was allowed to overlap cleanup")
	}
	cancel()
	close(unblock)
}

func TestManagedResourcePolicyAndMetadata(t *testing.T) {
	r := &JobResource{Name: "gpu", Capacity: 1, IdleTimeout: time.Minute}
	for _, invalid := range []*JobResource{nil, {}, {Name: "x"}, {Name: "x", Capacity: 1, IdleTimeout: -1}, {Name: "x", Capacity: 1, Timeout: -1}} {
		if err := validateResourceOptions(&Option{JobMode: JOBMODE_ASYNC, Job: JobOption{Resources: []*JobResource{invalid}}}); err == nil {
			t.Fatal("invalid resource accepted")
		}
	}
	if err := validateResourceOptions(&Option{JobMode: JOBMODE_CACHE, Job: JobOption{Resources: []*JobResource{r}}}); err == nil {
		t.Fatal("sync resource accepted")
	}
	c := newLeaseTestStrategy(t)
	managedStart(t, c, r)
	managedEnqueue(t, c, "first", 0, r)
	conflict := *r
	conflict.Capacity = 2
	_, err := c.Enqueue(context.Background(), "test", &JobMeta{Status: statusQueued, resources: []*JobResource{&conflict}}, "conflict", nil, 0)
	if !errors.Is(err, ErrResourcePolicyConflict) {
		t.Fatal(err)
	}
	var count int
	_ = c.db.QueryRow("SELECT COUNT(*) FROM executions WHERE key='conflict'").Scan(&count)
	if count != 0 {
		t.Fatal("policy conflict did not roll back")
	}
	opt := &Option{Name: "test", JobMode: JOBMODE_ASYNC, Job: JobOption{Resources: []*JobResource{r}}}
	sv := managerTestServer(context.Background(), 1, opt)
	jec := jobExecutionContext{r: NewRuntime(sv, nil), opt: opt, input: struct{}{}}
	if err := jec.MarshalCheck(); err != nil {
		t.Fatal(err)
	}
	if len(jec.JobMeta(statusQueued).resources) != 1 {
		t.Fatal("requirements missing")
	}
	if err := c.Free(context.Background(), "first"); err != nil {
		t.Fatal(err)
	}
}

func TestManagedResourceCrossServerCompetitionAndRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shared.db")
	open := func() *callSQLStrategy {
		db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)")
		if err != nil {
			t.Fatal(err)
		}
		db.SetMaxOpenConns(1)
		t.Cleanup(func() { db.Close() })
		c := &callSQLStrategy{db: db, logger: zap.NewNop(), maxretry: 2}
		if err := c.Init(context.Background(), true); err != nil {
			t.Fatal(err)
		}
		return c
	}
	a, b := open(), open()
	var init, release atomic.Int32
	r := &JobResource{Name: "gpu", Capacity: 2, IdleTimeout: time.Hour, Init: func(context.Context) error { init.Add(1); return nil }, Release: func(context.Context) error { release.Add(1); return nil }}
	managedStart(t, a, r)
	managedStart(t, b, r)
	for i, key := range []string{"one", "two", "three"} {
		managedEnqueue(t, a, key, 3-i, r)
	}
	managedReady(t, a, r)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var tasks []*sqlJobTask
	var errs []error
	for _, c := range []*callSQLStrategy{a, b, a, b} {
		wg.Add(1)
		go func(c *callSQLStrategy) {
			defer wg.Done()
			task, err := dequeueResourceTest(c, "test")
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				tasks = append(tasks, task)
			} else if !errors.Is(err, ErrJobNotFound) {
				errs = append(errs, err)
			}
		}(c)
	}
	wg.Wait()
	if len(tasks) != 2 || len(errs) > 0 || init.Load() != 1 {
		t.Fatal(len(tasks), errs, init.Load())
	}
	for _, task := range tasks {
		if err := task.Fail(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.Free(context.Background(), "three"); err != nil {
		t.Fatal(err)
	}
}

func TestManagedResourceOwnerRecoveryIsFencedByExecutionLease(t *testing.T) {
	c := newLeaseTestStrategy(t)
	c.managedOwner = "old"
	r := &JobResource{Name: "gpu", Capacity: 1, IdleTimeout: time.Hour}
	managedEnqueue(t, c, "job", 0, r)
	action, err := c.resourceStep(context.Background(), r, false)
	if err != nil || action != "initializing" {
		t.Fatal(action, err)
	}
	if err := c.finishResource(context.Background(), r, action, nil); err != nil {
		t.Fatal(err)
	}
	task := dequeueLeaseTest(t, c)
	if _, err := c.db.Exec("UPDATE job_managed_resources SET owner_until=?", time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	recovery := &callSQLStrategy{db: c.db, managedOwner: "new"}
	action, err = recovery.resourceStep(context.Background(), r, false)
	if err != nil || action != "" {
		t.Fatal("recovered with a live execution", action, err)
	}
	expireLeaseTest(t, c, task.Key())
	action, err = recovery.resourceStep(context.Background(), r, false)
	if err != nil || action != "releasing" {
		t.Fatal("missing crash cleanup", action, err)
	}
	if err := c.finishResource(context.Background(), r, action, nil); !errors.Is(err, ErrJobLeaseLost) {
		t.Fatal("stale lifecycle owner accepted", err)
	}
	if err := recovery.finishResource(context.Background(), r, action, nil); err != nil {
		t.Fatal(err)
	}
	action, err = recovery.resourceStep(context.Background(), r, false)
	if err != nil || action != "initializing" {
		t.Fatal(action, err)
	}
}

func TestManagedResourceReleaseTimeoutAndShutdown(t *testing.T) {
	c := newLeaseTestStrategy(t)
	var releases atomic.Int32
	r := &JobResource{Name: "gpu", Capacity: 1, Timeout: 20 * time.Millisecond, IdleTimeout: time.Hour,
		Release: func(ctx context.Context) error {
			if releases.Add(1) == 1 {
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		}}
	cancel := managedStart(t, c, r)
	managedEnqueue(t, c, "job", 0, r)
	managedReady(t, c, r)
	if err := c.Free(context.Background(), "job"); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := c.waitManagedResources(); err != nil {
		t.Fatal(err)
	}
	if releases.Load() != 2 || managedState(c, "gpu") != "cold" {
		t.Fatal("shutdown cleanup was not retried", releases.Load())
	}
}

func TestManagedResourceMigrationRetainsQueuedJobs(t *testing.T) {
	c := newLeaseTestStrategy(t)
	enqueueLeaseTest(t, c, "existing")
	for _, query := range []string{"DROP TRIGGER executions_resource_delete", "DROP TRIGGER executions_resource_demand", "DROP TABLE execution_resources", "DROP TABLE job_managed_resources"} {
		if _, err := c.db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Init(context.Background(), false); err == nil {
		t.Fatal("missing schema accepted")
	}
	if err := c.Init(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	if err := c.Init(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	task := dequeueLeaseTest(t, c)
	if task.Key() != "existing" {
		t.Fatal(task.Key())
	}
}

func TestManagedResourceCompletionReleasesCapacity(t *testing.T) {
	for _, mode := range []string{"success", "cached", "failure", "delete"} {
		t.Run(mode, func(t *testing.T) {
			c := newLeaseTestStrategy(t)
			r := &JobResource{Name: "gpu", Capacity: 1, IdleTimeout: time.Hour}
			managedStart(t, c, r)
			managedEnqueue(t, c, "first", 1, r)
			managedEnqueue(t, c, "second", 0, r)
			managedReady(t, c, r)
			first := dequeueLeaseTest(t, c)
			var err error
			switch mode {
			case "success", "cached":
				if mode == "cached" {
					ttl := time.Now().Add(time.Minute)
					first.Meta().TTL = &ttl
				}
				err = first.Success(context.Background(), "test", first.Meta(), first.Key(), first.Input(), []byte(`"done"`), nil)
			case "failure":
				err = first.Fail(context.Background())
			case "delete":
				err = c.Free(context.Background(), first.Key())
			}
			if err != nil {
				t.Fatal(err)
			}
			second := dequeueLeaseTest(t, c)
			if second.Key() != "second" {
				t.Fatal(second.Key())
			}
			if err := second.Fail(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestManagedResourceCallsCannotBypassQueue(t *testing.T) {
	for _, mode := range []string{JOBMODE_ASYNC, JOBMODE_DISPATCH} {
		t.Run(mode, func(t *testing.T) {
			c := newLeaseTestStrategy(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			oldLen := len(FunctionList)
			name := "managed-entry-" + mode
			defer func() { FunctionList = FunctionList[:oldLen]; delete(handlerMarshalMap, name) }()
			called := false
			resource := &JobResource{Name: "gpu", Capacity: 1, IdleTimeout: time.Hour}
			fn := NewFunction(Option{Name: name, Version: "1.0.0", JobMode: mode, Job: JobOption{Resources: []*JobResource{resource}}}, func(*Runtime, resourceTestInput) (string, error) { called = true; return "unexpected", nil })
			opt := fn.Options()
			sv := managerTestServer(ctx, 1, opt)
			sv.SQL = c.db
			sv.callSQLStrategy = c
			sv.jobManager = newJobManager()
			sv.Validator = validator.New()
			if err := callSQLInit(sv, opt); err != nil {
				t.Fatal(err)
			}
			defer func() {
				cancel()
				if err := c.waitManagedResources(); err != nil {
					t.Error(err)
				}
			}()
			_, err := fn.call_internal(NewRuntime(sv, nil), resourceTestInput{Value: "http"}, false)
			var pending *JobPendingError
			if !errors.As(err, &pending) || called {
				t.Fatal("HTTP call bypassed queue", err, called)
			}
			_, _, errJSON, systemErr := fn.invokeFunctionJSON(NewRuntime(sv, nil), name, "1.0.0", []byte(`{"Value":"cli"}`), true, nil)
			if systemErr != nil || errJSON == nil || called {
				t.Fatal("direct call bypassed queue", systemErr, called)
			}
			var count int
			if err := c.db.QueryRow("SELECT COUNT(*) FROM execution_resources WHERE resource_key='gpu'").Scan(&count); err != nil || count != 2 {
				t.Fatal("requirements not persisted", count, err)
			}
		})
	}
}

func TestManagedResourceBriefDemandResetsIdleDeadline(t *testing.T) {
	c := newLeaseTestStrategy(t)
	c.managedOwner = "owner"
	r := &JobResource{Name: "gpu", Capacity: 1, IdleTimeout: time.Minute}
	managedEnqueue(t, c, "first", 0, r)
	action, err := c.resourceStep(context.Background(), r, false)
	if err != nil || action != "initializing" {
		t.Fatal(action, err)
	}
	if err := c.finishResource(context.Background(), r, action, nil); err != nil {
		t.Fatal(err)
	}
	if err := dequeueLeaseTest(t, c).Fail(context.Background()); err != nil {
		t.Fatal(err)
	}
	// No lifecycle pass occurs between this new demand and job completion.
	if _, err := c.db.Exec("UPDATE job_managed_resources SET idle_since=?", time.Now().Add(-2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	managedEnqueue(t, c, "brief", 0, r)
	if err := dequeueLeaseTest(t, c).Fail(context.Background()); err != nil {
		t.Fatal(err)
	}
	action, err = c.resourceStep(context.Background(), r, false)
	if err != nil || action != "" || managedState(c, "gpu") != "ready" {
		t.Fatal("brief demand did not restart idle timeout", action, err)
	}
}
