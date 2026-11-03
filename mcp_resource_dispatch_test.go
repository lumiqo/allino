package allino

import (
	"context"
	"encoding/json"
	"github.com/go-playground/validator/v10"
	"testing"
	"time"
)

func TestPublicMCPResourceJob(t *testing.T) {
	c := newLeaseTestStrategy(t)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	starts := make(chan time.Time, 2)
	count := len(FunctionList)
	name := "public-mcp-resource-test"
	defer func() { FunctionList = FunctionList[:count]; delete(handlerMarshalMap, name) }()
	fn := NewFunction(Option{Name: name, Version: "1.0.0", JobMode: JOBMODE_DISPATCH, Job: JobOption{MinInterval: 150 * time.Millisecond, MaxConcurrentPerResource: 1}}, func(r *Runtime, p resourceTestInput) (string, error) { starts <- time.Now(); return p.Value, nil })
	sv := managerTestServer(ctx, 2, fn.Options())
	sv.SQL = c.db
	sv.Validator = validator.New()
	sv.callSQLStrategy = c
	sv.jobManager = newJobManager()
	if e := callSQLInit(sv, fn.Options()); e != nil {
		t.Fatal(e)
	}
	sv.jobManager.WorkerInit(c, sv)
	for _, value := range []string{"one", "two"} {
		b, _ := json.Marshal(resourceTestInput{Group: "domain:example.com", Value: value})
		out, e := fn.CallJSON(NewRuntime(sv, nil).WithContext(ctx), b)
		if e != nil || out != value {
			t.Fatalf("output=%v error=%v", out, e)
		}
	}
	first, second := <-starts, <-starts
	if second.Sub(first) < 140*time.Millisecond {
		t.Fatalf("MCP bypassed resource gate: %v", second.Sub(first))
	}
}
