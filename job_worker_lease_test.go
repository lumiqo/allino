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

	"github.com/wh-kuromai/allino/internal/ema"
	"go.uber.org/zap"
	_ "modernc.org/sqlite"
)

func newLeaseTestStrategy(t *testing.T) *callSQLStrategy {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "jobs.db"))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	c := &callSQLStrategy{db: db, maxretry: 2, logger: zap.NewNop()}
	if err = c.Init(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	return c
}
func enqueueLeaseTest(t *testing.T, c *callSQLStrategy, key string) {
	t.Helper()
	ok, err := c.Enqueue(context.Background(), "test", &JobMeta{Version: "1.0.0", Status: statusQueued}, key, []byte(`{"value":1}`), 0)
	if err != nil || !ok {
		t.Fatalf("enqueue: %v %v", ok, err)
	}
}
func dequeueLeaseTest(t *testing.T, c *callSQLStrategy) *sqlJobTask {
	t.Helper()
	task, err := c.Dequeue(context.Background(), []string{"test"}, time.Minute, ema.NewEMACalculator(0.3))
	if err != nil {
		t.Fatal(err)
	}
	return task.(*sqlJobTask)
}
func expireLeaseTest(t *testing.T, c *callSQLStrategy, key string) {
	t.Helper()
	if _, err := c.db.Exec("UPDATE executions SET leased_until=? WHERE key=?", time.Now().Add(-time.Minute), key); err != nil {
		t.Fatal(err)
	}
}
func TestWorkerLeaseRejectsStaleMutations(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		t.Run(map[bool]string{false: "ttl", true: "persistent"}[persistent], func(t *testing.T) {
			c := newLeaseTestStrategy(t)
			ctx := context.Background()
			enqueueLeaseTest(t, c, "job")
			old := dequeueLeaseTest(t, c)
			expireLeaseTest(t, c, "job")
			if err := old.HeartBeat(ctx, time.Minute); !errors.Is(err, ErrJobLeaseLost) {
				t.Fatalf("expired heartbeat: %v", err)
			}
			if err := c.Reaping(ctx); err != nil {
				t.Fatal(err)
			}
			current := dequeueLeaseTest(t, c)
			if current.leaseToken == "" || current.leaseToken == old.leaseToken {
				t.Fatal("lease token not rotated")
			}
			for name, fn := range map[string]func() error{
				"heartbeat": func() error { return old.HeartBeat(ctx, time.Minute) },
				"delete":    func() error { return old.Fail(ctx) },
				"requeue":   func() error { return old.Requeue(ctx, 0) },
				"success":   func() error { return old.Success(ctx, "test", old.Meta(), "job", old.Input(), []byte(`"old"`), nil) },
			} {
				if err := fn(); !errors.Is(err, ErrJobLeaseLost) {
					t.Fatalf("stale %s: %v", name, err)
				}
			}
			if !persistent {
				ttl := time.Now().Add(time.Hour)
				current.Meta().TTL = &ttl
			}
			if err := current.HeartBeat(ctx, time.Minute); err != nil {
				t.Fatal(err)
			}
			if err := current.Success(ctx, "test", current.Meta(), "job", current.Input(), []byte(`"current"`), nil); err != nil {
				t.Fatal(err)
			}
			if err := old.Success(ctx, "test", old.Meta(), "job", old.Input(), []byte(`"stale-overwrite"`), nil); !errors.Is(err, ErrJobLeaseLost) {
				t.Fatal(err)
			}
			_, out, _, err := c.Result(ctx, "job", !persistent)
			if err != nil || string(out) != `"current"` {
				t.Fatalf("result=%s err=%v", out, err)
			}
			if err = current.HeartBeat(ctx, time.Minute); !errors.Is(err, ErrJobLeaseLost) {
				t.Fatalf("completed task heartbeat: %v", err)
			}
		})
	}
}
func TestWorkerLeaseRejectsExpiredUnreapedCompletion(t *testing.T) {
	c := newLeaseTestStrategy(t)
	enqueueLeaseTest(t, c, "job")
	task := dequeueLeaseTest(t, c)
	expireLeaseTest(t, c, "job")
	if err := task.Success(context.Background(), "test", task.Meta(), "job", task.Input(), []byte(`"expired"`), nil); !errors.Is(err, ErrJobLeaseLost) {
		t.Fatal(err)
	}
	var count int
	if err := c.db.QueryRow("SELECT COUNT(*) FROM executions_results").Scan(&count); err != nil || count != 0 {
		t.Fatalf("stale result inserted: %d %v", count, err)
	}
}
func TestWorkerLeaseRequeueAndKeyReuse(t *testing.T) {
	c := newLeaseTestStrategy(t)
	ctx := context.Background()
	enqueueLeaseTest(t, c, "job")
	old := dequeueLeaseTest(t, c)
	if err := old.Requeue(ctx, 0); err != nil {
		t.Fatal(err)
	}
	current := dequeueLeaseTest(t, c)
	if err := old.Fail(ctx); !errors.Is(err, ErrJobLeaseLost) {
		t.Fatal(err)
	}
	if err := current.Fail(ctx); err != nil {
		t.Fatal(err)
	}
	enqueueLeaseTest(t, c, "job")
	replacement := dequeueLeaseTest(t, c)
	if err := current.Requeue(ctx, 0); !errors.Is(err, ErrJobLeaseLost) {
		t.Fatal(err)
	}
	if err := replacement.HeartBeat(ctx, time.Minute); err != nil {
		t.Fatal(err)
	}
}
func TestWorkerLeaseMigrationPreservesQueuedJobs(t *testing.T) {
	c := newLeaseTestStrategy(t)
	ctx := context.Background()
	enqueueLeaseTest(t, c, "queued-before-upgrade")
	if _, err := c.db.Exec("ALTER TABLE executions DROP COLUMN lease_token"); err != nil {
		t.Fatal(err)
	}
	if err := c.Init(ctx, false); err == nil {
		t.Fatal("old schema accepted without migration")
	}
	for i := 0; i < 2; i++ {
		if err := c.Init(ctx, true); err != nil {
			t.Fatal(err)
		}
	}
	task := dequeueLeaseTest(t, c)
	if task.Key() != "queued-before-upgrade" || task.leaseToken == "" {
		t.Fatal(task)
	}
}
func TestWorkerLeaseReapingHonorsRetryLimit(t *testing.T) {
	c := newLeaseTestStrategy(t)
	c.maxretry = 0
	enqueueLeaseTest(t, c, "job")
	dequeueLeaseTest(t, c)
	expireLeaseTest(t, c, "job")
	if err := c.Reaping(context.Background()); err != nil {
		t.Fatal(err)
	}
	var status int
	if err := c.db.QueryRow("SELECT status FROM executions WHERE key='job'").Scan(&status); err != nil || status != statusError {
		t.Fatalf("status=%d err=%v", status, err)
	}
}

// Only Dequeue is needed by these manager tests. The embedded strategy ensures
// other operations cannot silently succeed if the worker unexpectedly uses one.
type capacityTestStrategy struct {
	callStrategy
	mu       sync.Mutex
	tasks    []JobTask
	dequeued chan string
}

func (s *capacityTestStrategy) Dequeue(ctx context.Context, _ []string, _ time.Duration, average *ema.EMACalculator) (JobTask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if len(s.tasks) == 0 {
		return nil, ErrJobNotFound
	}
	task := s.tasks[0]
	s.tasks = s.tasks[1:]
	average.Update(1)
	s.dequeued <- task.Key()
	return task, nil
}

type capacityTestTask struct {
	handler      string
	key          string
	meta         JobMeta
	heartbeatErr error
	finished     chan struct{}
	once         sync.Once
	mutations    atomic.Int32
}

func (t *capacityTestTask) Key() string { return t.key }
func (t *capacityTestTask) Handler() string {
	if t.handler != "" {
		return t.handler
	}
	return "test"
}
func (t *capacityTestTask) Meta() *JobMeta                                 { return &t.meta }
func (t *capacityTestTask) Input() []byte                                  { return nil }
func (t *capacityTestTask) HeartBeat(context.Context, time.Duration) error { return t.heartbeatErr }
func (t *capacityTestTask) complete() error {
	t.mutations.Add(1)
	if t.finished != nil {
		t.once.Do(func() { close(t.finished) })
	}
	return nil
}
func (t *capacityTestTask) Fail(context.Context) error         { return t.complete() }
func (t *capacityTestTask) Requeue(context.Context, int) error { return t.complete() }
func (t *capacityTestTask) Success(context.Context, string, *JobMeta, string, []byte, []byte, []byte) error {
	return t.complete()
}
func managerTestServer(ctx context.Context, concurrency int, opt *Option) *Server {
	config := &Config{JobConfig: JobConfig{Concurrency: concurrency, LeaseDuration: time.Second, IdleInterval: 10 * time.Millisecond}}
	config.Log.Silent = true
	return &Server{Config: config, appctx: ctx, Logger: zap.NewNop(), handlerOptMap: map[string]*Option{"test": opt}}
}
func awaitManagerSignal(t *testing.T, ch <-chan string) string {
	t.Helper()
	select {
	case s := <-ch:
		return s
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not advance")
		return ""
	}
}
func TestWorkerReservesCapacityBeforeDequeue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	release := make(chan struct{})
	opt := &Option{invoker: func(r *Runtime, _ string, _ string, _ []byte, _ bool, _ func(any) error) (string, []byte, []byte, error) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
		return "", nil, nil, nil
	}}
	sv := managerTestServer(ctx, 1, opt)
	jm := newJobManager()
	jm.registerHandler("test", opt)
	first := &capacityTestTask{key: "first", finished: make(chan struct{})}
	second := &capacityTestTask{key: "second", finished: make(chan struct{})}
	strategy := &capacityTestStrategy{tasks: []JobTask{first, second}, dequeued: make(chan string, 2)}
	jm.WorkerInit(strategy, sv)
	if key := awaitManagerSignal(t, strategy.dequeued); key != "first" {
		t.Fatal(key)
	}
	select {
	case key := <-strategy.dequeued:
		t.Fatalf("leased %s before execution capacity was available", key)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	if key := awaitManagerSignal(t, strategy.dequeued); key != "second" {
		t.Fatal(key)
	}
	select {
	case <-second.finished:
	case <-time.After(3 * time.Second):
		t.Fatal("second job did not finish")
	}
}
func TestWorkerCancelsHandlerOnLeaseLoss(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handlerStopped := make(chan string, 1)
	opt := &Option{invoker: func(r *Runtime, _ string, _ string, _ []byte, _ bool, _ func(any) error) (string, []byte, []byte, error) {
		<-r.Context().Done()
		handlerStopped <- "stopped"
		return "", []byte(`"late"`), nil, nil
	}}
	sv := managerTestServer(ctx, 1, opt)
	sv.Config.JobConfig.LeaseDuration = 20 * time.Millisecond
	task := &capacityTestTask{key: "lost", heartbeatErr: ErrJobLeaseLost}
	strategy := &capacityTestStrategy{tasks: []JobTask{task}, dequeued: make(chan string, 1)}
	jm := newJobManager()
	jm.registerHandler("test", opt)
	jm.WorkerInit(strategy, sv)
	awaitManagerSignal(t, handlerStopped)
	if task.mutations.Load() != 0 {
		t.Fatal("lost task attempted completion")
	}
}
func TestWorkerBlockedDispatchStopsOnShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	jm := newJobManager()
	sv := managerTestServer(ctx, 1, &Option{})
	jm.Init(sv)
	started := make(chan struct{})
	if !jm.Do(func() { close(started); <-ctx.Done() }) {
		t.Fatal("dispatch failed")
	}
	<-started
	returned := make(chan bool, 1)
	go func() { returned <- jm.Do(func() {}) }()
	cancel()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("dispatch blocked after shutdown")
	}
}

func TestWorkerWaitsForSharedPoolCapacity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := &Option{invoker: func(*Runtime, string, string, []byte, bool, func(any) error) (string, []byte, []byte, error) {
		return "", nil, nil, nil
	}}
	sv := managerTestServer(ctx, 1, opt)
	jm := newJobManager()
	jm.Init(sv)
	jm.registerHandler("test", opt)
	release := make(chan struct{})
	started := make(chan struct{})
	jm.Do(func() {
		close(started)
		select {
		case <-release:
		case <-ctx.Done():
		}
	})
	<-started
	task := &capacityTestTask{key: "after-shared-task", finished: make(chan struct{})}
	strategy := &capacityTestStrategy{tasks: []JobTask{task}, dequeued: make(chan string, 1)}
	jm.WorkerInit(strategy, sv)
	select {
	case <-strategy.dequeued:
		t.Fatal("leased SQL work while the shared execution pool was full")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	awaitManagerSignal(t, strategy.dequeued)
	select {
	case <-task.finished:
	case <-time.After(3 * time.Second):
		t.Fatal("job did not complete")
	}
}

func TestWorkerIntervalWaitKeepsExecutionSlot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan string, 2)
	invoker := func(_ *Runtime, handler string, _ string, _ []byte, _ bool, _ func(any) error) (string, []byte, []byte, error) {
		started <- handler
		return "", nil, nil, nil
	}
	previous := time.Now()
	delayed := &Option{Job: JobOption{Interval: 300 * time.Millisecond}, lastRun: &previous, invoker: invoker}
	other := &Option{invoker: invoker}
	sv := managerTestServer(ctx, 1, delayed)
	jm := newJobManager()
	jm.registerHandler("test", delayed)
	jm.registerHandler("other", other)
	first := &capacityTestTask{key: "delayed"}
	second := &capacityTestTask{key: "other", handler: "other", finished: make(chan struct{})}
	strategy := &capacityTestStrategy{tasks: []JobTask{first, second}, dequeued: make(chan string, 2)}
	jm.WorkerInit(strategy, sv)
	awaitManagerSignal(t, strategy.dequeued)
	select {
	case <-started:
		t.Fatal("interval was not respected")
	case <-time.After(75 * time.Millisecond):
	}
	select {
	case <-strategy.dequeued:
		t.Fatal("interval wait released its execution slot")
	default:
	}
	if got := awaitManagerSignal(t, started); got != "test" {
		t.Fatal(got)
	}
	if got := awaitManagerSignal(t, started); got != "other" {
		t.Fatal(got)
	}
}
