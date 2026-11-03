package allino

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestErrorWithDoesNotMutateSentinel(t *testing.T) {
	base := NewError("base")
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cause := fmt.Errorf("cause %d", i)
			wrapped := base.With(cause)
			if !errors.Is(wrapped, base) || !errors.Is(wrapped, cause) || wrapped.Unwrap() != cause {
				t.Error("error identity or cause lost")
			}
		}(i)
	}
	wg.Wait()
	if base.Err != nil {
		t.Fatal("sentinel was mutated")
	}
}
func TestCachedInterfaceErrorKeepsMessage(t *testing.T) {
	_, _, err, system := unmarshalOutputSet(JobInfo{}, NewReflectPool[string](), NewReflectPool[error](), nil, []byte(`{"msg":"browser launch failed"}`), nil)
	if system != nil || err == nil || err.Error() != "browser launch failed" {
		t.Fatalf("error=%v system=%v", err, system)
	}
}
