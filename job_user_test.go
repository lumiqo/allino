package allino

import (
	"context"
	"errors"
	"testing"
)

func TestJobUserQueuePropagation(t *testing.T) {
	for _, authenticated := range []bool{false, true} {
		name := "anonymous"
		if authenticated {
			name = "authenticated"
		}
		t.Run(name, func(t *testing.T) {
			c := newLeaseTestStrategy(t)
			ctx := context.Background()
			server := &Server{Config: &Config{}}
			caller := NewRuntime(server, nil)
			if authenticated {
				caller.AssumeUser("alice", "Alice", false)
			}
			execution := &jobExecutionContext{r: caller, opt: &Option{}, key: "job"}
			meta := execution.JobMeta(statusQueued)
			ok, err := c.Enqueue(ctx, "test", meta, "job", []byte(`{}`), 0)
			if err != nil || !ok {
				t.Fatalf("enqueue: %v %v", ok, err)
			}
			for attempt := 0; attempt < 2; attempt++ {
				task := dequeueLeaseTest(t, c)
				worker := NewRuntime(server, nil)
				worker.restoreJobUser(task.Meta().user)
				uid, display, writable, err := worker.User()
				if authenticated {
					if err != nil || uid != "alice" || display != "Alice" || writable || worker.AuthorizedBy() != "simulate" {
						t.Fatalf("restored user: %q %q %v %v", uid, display, writable, err)
					}
					child := (&jobExecutionContext{r: worker, opt: &Option{}, key: "job"}).JobMeta(statusQueued)
					if child.user == nil || *child.user != *meta.user {
						t.Fatal("child job lost user")
					}
				} else if !errors.Is(err, ErrNotLoggedIn) {
					t.Fatalf("anonymous user: %v", err)
				}
				if err := task.Requeue(ctx, 0); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestJobUserMigration(t *testing.T) {
	c := newLeaseTestStrategy(t)
	ctx := context.Background()
	enqueueLeaseTest(t, c, "legacy")
	if _, err := c.db.Exec("ALTER TABLE executions DROP COLUMN user_context"); err != nil {
		t.Fatal(err)
	}
	if err := c.Init(ctx, false); err == nil {
		t.Fatal("missing user_context should require migration")
	}
	if err := c.Init(ctx, true); err != nil {
		t.Fatal(err)
	}
	if err := c.Init(ctx, false); err != nil {
		t.Fatal(err)
	}
	if task := dequeueLeaseTest(t, c); task.Meta().user != nil {
		t.Fatal("legacy job should be anonymous")
	}
}

func TestWorkerRestoresQueuedUser(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := newLeaseTestStrategy(t)
	meta := &JobMeta{Version: "1.0.0", Status: statusQueued, user: &jobUser{UID: "alice", Name: "Alice", Writable: true, AuthorizedBy: "oauth"}}
	if ok, err := c.Enqueue(ctx, "test", meta, "job", []byte(`{}`), 0); err != nil || !ok {
		t.Fatalf("enqueue: %v %v", ok, err)
	}
	observed := make(chan string, 1)
	opt := &Option{invoker: func(r *Runtime, _ string, _ string, _ []byte, _ bool, _ func(any) error) (string, []byte, []byte, error) {
		uid, name, writable, err := r.User()
		if err != nil || name != "Alice" || !writable || r.AuthorizedBy() != "oauth" {
			observed <- "invalid user"
		} else {
			observed <- uid
		}
		return "job", []byte(`{}`), nil, nil
	}}
	server := managerTestServer(ctx, 1, opt)
	manager := newJobManager()
	manager.registerHandler("test", opt)
	manager.WorkerInit(c, server)
	if got := awaitManagerSignal(t, observed); got != "alice" {
		t.Fatalf("worker user: %s", got)
	}
}
