package allino

import (
	"context"
	"testing"
	"time"
)

func TestCallCacheRetainsExpiredSuccess(t *testing.T) {
	c := newLeaseTestStrategy(t)
	ctx := context.Background()
	calls := 0
	fn := NewFunction(Option{Name: "test", Version: "1.0.0", JobMode: JOBMODE_DISPATCH, Job: JobOption{Cache: true, CacheExpire: time.Hour}}, func(*Runtime, struct{}) (string, error) { calls++; return "unexpected", nil })
	sv := managerTestServer(ctx, 1, fn.Options())
	sv.Config.System.DisableValidator = true
	sv.callSQLStrategy = c
	r := NewRuntime(sv, nil)
	input := struct{}{}
	key := encodeJobID("test", input, []byte(`{}`), "")
	read := func(want CacheState, value string) {
		t.Helper()
		out, info, err := fn.CallCache(r, input)
		if err != nil || info.State != want || out != value {
			t.Fatalf("read: %q %+v %v", out, info, err)
		}
		if calls != 0 {
			t.Fatal("CallCache executed function")
		}
	}
	read(CacheMiss, "")
	expires := time.Now().Add(time.Hour)
	meta := &JobMeta{Version: "1.0.0", TTL: &expires}
	if err := c.Done(ctx, "test", meta, key, []byte(`{}`), []byte(`"old"`), nil); err != nil {
		t.Fatal(err)
	}
	read(CacheFresh, "old")
	// Expire both the active result and the snapshot without wall-clock sleeps.
	expires = time.Now().Add(-time.Hour)
	for _, table := range []string{"executions", "executions_results"} {
		if _, err := c.db.Exec("UPDATE "+table+" SET ttl=? WHERE key=?", expires, key); err != nil {
			t.Fatal(err)
		}
	}
	read(CacheExpired, "old")
	// Enqueue changes version/TTL/status of the active row, not the snapshot.
	ok, err := c.Enqueue(ctx, "test", &JobMeta{Version: "1.0.0", Status: statusQueued, TTL: &expires}, key, []byte(`{}`), 0)
	if err != nil || !ok {
		t.Fatalf("enqueue: %v %v", ok, err)
	}
	read(CacheExpired, "old")
	task := dequeueLeaseTest(t, c)
	task.meta.TTL = &expires
	if err := task.Success(ctx, task.handler, task.meta, task.key, task.input, []byte(`"failed"`), []byte(`"error"`)); err != nil {
		t.Fatal(err)
	}
	read(CacheExpired, "old")
	if err := c.Reaping(ctx); err != nil {
		t.Fatal(err)
	}
	read(CacheExpired, "old")
	expires = time.Now().Add(time.Hour)
	if err := c.Done(ctx, "test", meta, key, []byte(`{}`), []byte(`"new"`), nil); err != nil {
		t.Fatal(err)
	}
	read(CacheFresh, "new")
	fn.options.Version = "2.0.0"
	read(CacheVersionMismatch, "")
	fn.options.Version = "1.0.0"
	c.db.Close()
	if _, _, err := fn.CallCache(r, input); err == nil {
		t.Fatal("storage failure reported as miss")
	}
}

func TestCallCacheWorkerSnapshot(t *testing.T) {
	c := newLeaseTestStrategy(t)
	ctx := context.Background()
	expires := time.Now().Add(time.Hour)
	if ok, err := c.Enqueue(ctx, "test", &JobMeta{Version: "1.0.0", Status: statusQueued, TTL: &expires}, "job", []byte(`{}`), 0); err != nil || !ok {
		t.Fatal(err)
	}
	task := dequeueLeaseTest(t, c)
	task.meta.TTL = &expires
	if err := task.Success(ctx, task.handler, task.meta, task.key, task.input, []byte(`"success"`), nil); err != nil {
		t.Fatal(err)
	}
	var output string
	if err := c.db.QueryRow("SELECT output FROM executions_results WHERE key='job'").Scan(&output); err != nil || output != `"success"` {
		t.Fatalf("snapshot: %q %v", output, err)
	}
}

func TestCallCacheInsideRefreshingFunction(t *testing.T) {
	c := newLeaseTestStrategy(t)
	sv := managerTestServer(context.Background(), 1, &Option{})
	sv.Config.System.DisableValidator = true
	sv.callSQLStrategy = c
	r := NewRuntime(sv, nil)
	var fn *GenericFunction[struct{}, string, error]
	var previous string
	var state CacheState
	fn = NewFunction(Option{Name: "refresh", Version: "1.0.0", JobMode: JOBMODE_CACHE, Job: JobOption{Cache: true, CacheExpire: time.Hour}}, func(r *Runtime, input struct{}) (string, error) {
		var err error
		var info CacheInfo
		previous, info, err = fn.CallCache(r, input)
		state = info.State
		return "new", err
	})
	key := encodeJobID("refresh", struct{}{}, []byte(`{}`), "")
	expired := time.Now().Add(-time.Hour)
	if err := c.Done(context.Background(), "refresh", &JobMeta{Version: "1.0.0", TTL: &expired}, key, []byte(`{}`), []byte(`"old"`), nil); err != nil {
		t.Fatal(err)
	}
	out, err := fn.Call(r, struct{}{})
	if err != nil || out != "new" || previous != "old" || state != CacheExpired {
		t.Fatalf("refresh: %q previous=%q state=%s err=%v", out, previous, state, err)
	}
	_, info, err := fn.CallCache(r, struct{}{})
	if err != nil || info.State != CacheFresh {
		t.Fatalf("updated cache: %+v %v", info, err)
	}
}

func TestCallCacheDistinguishesFailedAndPending(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		t.Run(map[bool]string{false: "ttl", true: "persistent"}[persistent], func(t *testing.T) {
			c := newLeaseTestStrategy(t)
			ctx := context.Background()
			fn := NewFunction(Option{Name: "test", Version: "1.0.0", Job: JobOption{Cache: true}}, func(*Runtime, struct{}) (string, error) { t.Fatal("executed"); return "", nil })
			sv := managerTestServer(ctx, 1, fn.Options())
			sv.Config.System.DisableValidator = true
			sv.callSQLStrategy = c
			r := NewRuntime(sv, nil)
			read := func(want CacheState) {
				t.Helper()
				out, info, err := fn.CallCache(r, struct{}{})
				if err != nil || info.State != want || out != "" {
					t.Fatalf("got %q %+v %v", out, info, err)
				}
			}
			read(CacheMiss)
			key := encodeJobID("test", struct{}{}, []byte(`{}`), "")
			expires := time.Now().Add(-time.Hour)
			meta := &JobMeta{Version: "1.0.0", Status: statusQueued}
			if !persistent {
				meta.TTL = &expires
			}
			if ok, err := c.Enqueue(ctx, "test", meta, key, []byte(`{}`), 0); err != nil || !ok {
				t.Fatalf("enqueue: %v %v", ok, err)
			}
			read(CachePending)
			if err := c.Done(ctx, "test", meta, key, []byte(`{}`), []byte(`"partial"`), []byte(`"failed"`)); err != nil {
				t.Fatal(err)
			}
			read(CacheFailed)
		})
	}
}
