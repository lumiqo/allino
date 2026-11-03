package allino

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCallRefreshDispatch(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		t.Run(map[bool]string{false: "ttl", true: "persistent"}[persistent], func(t *testing.T) {
			c := newLeaseTestStrategy(t)
			ctx := context.Background()
			fn := NewFunction(Option{Name: "test", Version: "1.0.0", JobMode: JOBMODE_DISPATCH, Job: JobOption{Cache: true, Async: true}}, func(*Runtime, struct{}) (string, error) { t.Fatal("dispatch ran synchronously"); return "", nil })
			if !persistent {
				fn.options.Job.CacheExpire = time.Hour
			}
			sv := managerTestServer(ctx, 1, fn.Options())
			sv.Config.System.DisableValidator = true
			sv.callSQLStrategy = c
			r := NewRuntime(sv, nil)
			key := encodeJobID("test", struct{}{}, []byte(`{}`), "")
			expires := time.Now().Add(time.Hour)
			meta := &JobMeta{Version: "1.0.0"}
			if !persistent {
				meta.TTL = &expires
			}
			if err := c.Done(ctx, "test", meta, key, []byte(`{}`), []byte(`"old"`), nil); err != nil {
				t.Fatal(err)
			}
			if out, err := fn.Call(r, struct{}{}); err != nil || out != "old" {
				t.Fatalf("normal: %q %v", out, err)
			}
			pending := func() {
				t.Helper()
				out, err := fn.CallRefresh(r, struct{}{})
				var p *JobPendingError
				if !errors.As(err, &p) || p.JobID != key || out != "" {
					t.Fatalf("refresh: %q %v", out, err)
				}
			}
			pending()
			pending()
			var count int
			if err := c.db.QueryRow("SELECT COUNT(*) FROM executions WHERE key=? AND status=0", key).Scan(&count); err != nil || count != 1 {
				t.Fatalf("queue: %d %v", count, err)
			}
			task := dequeueLeaseTest(t, c)
			pending()
			if out, info, err := fn.CallCache(r, struct{}{}); err != nil || out != "old" || info.State != CacheFresh {
				t.Fatalf("previous: %q %+v %v", out, info, err)
			}
			task.meta.TTL = meta.TTL
			if err := task.Success(ctx, "test", task.meta, key, task.input, nil, []byte(`"failed"`)); err != nil {
				t.Fatal(err)
			}
			if out, _, err := fn.CallCache(r, struct{}{}); err != nil || out != "old" {
				t.Fatalf("failed refresh lost cache: %q %v", out, err)
			}
			pending() // Failed executions can be explicitly retried.
			task = dequeueLeaseTest(t, c)
			task.meta.TTL = meta.TTL
			if err := task.Success(ctx, "test", task.meta, key, task.input, []byte(`"new"`), nil); err != nil {
				t.Fatal(err)
			}
			if out, err := fn.Call(r, struct{}{}); err != nil || out != "new" {
				t.Fatalf("updated: %q %v", out, err)
			}
		})
	}
}

func TestCallRefreshCache(t *testing.T) {
	c := newLeaseTestStrategy(t)
	sv := managerTestServer(context.Background(), 1, &Option{})
	sv.Config.System.DisableValidator = true
	sv.callSQLStrategy = c
	r := NewRuntime(sv, nil)
	fail := false
	calls := 0
	var fn *GenericFunction[struct{}, string, error]
	fn = NewFunction(Option{Name: "test", Version: "1.0.0", JobMode: JOBMODE_CACHE, Job: JobOption{Cache: true}}, func(r *Runtime, input struct{}) (string, error) {
		calls++
		if calls > 1 {
			out, _, err := fn.CallCache(r, input)
			if err != nil || out != "old" {
				t.Fatalf("previous %q %v", out, err)
			}
		}
		if fail {
			return "", errors.New("failed")
		}
		return "old", nil
	})
	if _, err := fn.Call(r, struct{}{}); err != nil {
		t.Fatal(err)
	}
	fail = true
	if _, err := fn.CallRefresh(r, struct{}{}); err == nil {
		t.Fatal("expected failure")
	}
	if out, err := fn.Call(r, struct{}{}); err != nil || out != "old" || calls != 2 {
		t.Fatalf("normal call: %q %v calls=%d", out, err, calls)
	}
	fail = false
	if _, err := fn.CallRefresh(r, struct{}{}); err != nil || calls != 3 {
		t.Fatalf("refresh: %v calls=%d", err, calls)
	}
}

func TestCallRefreshMemoizedJoins(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		t.Run(map[bool]string{false: "ttl", true: "persistent"}[persistent], func(t *testing.T) {
			c := newLeaseTestStrategy(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			sv := managerTestServer(ctx, 1, &Option{})
			sv.Config.System.DisableValidator = true
			sv.callSQLStrategy = c
			sv.TimeWheel = (&TimeWheelConfig{Slot: 32, TickInterval: time.Millisecond}).setup(ctx)
			c.waitInterval = time.Millisecond
			c.waitTimeout = time.Second
			entered := make(chan struct{}, 2)
			release := make(chan struct{})
			fn := NewFunction(Option{Name: "test", Version: "1.0.0", JobMode: JOBMODE_MEMOIZED, Job: JobOption{Cache: true, Dedupe: true}}, func(*Runtime, struct{}) (string, error) { entered <- struct{}{}; <-release; return "new", nil })
			if !persistent {
				fn.options.Job.CacheExpire = time.Hour
			}
			expires := time.Now().Add(time.Hour)
			meta := &JobMeta{Version: "1.0.0"}
			if !persistent {
				meta.TTL = &expires
			}
			key := encodeJobID("test", struct{}{}, []byte(`{}`), "")
			if err := c.Done(ctx, "test", meta, key, []byte(`{}`), []byte(`"old"`), nil); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 2)
			invoke := func() {
				out, err := fn.CallRefresh(NewRuntime(sv, nil), struct{}{})
				if err == nil && out != "new" {
					err = errors.New("refresh returned old output")
				}
				done <- err
			}
			go invoke()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("refresh did not start")
			}
			go invoke()
			// A join must not immediately return the old persistent cache.
			select {
			case err := <-done:
				t.Fatalf("returned before refresh completed: %v", err)
			case <-entered:
				t.Fatal("duplicate execution")
			case <-time.After(30 * time.Millisecond):
			}
			close(release)
			for i := 0; i < 2; i++ {
				select {
				case err := <-done:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("refresh did not finish")
				}
			}
		})
	}
}
