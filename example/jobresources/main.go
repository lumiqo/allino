// This example expects an adapter service on localhost:9000. /load and /unload
// must be idempotent; /llm and /image receive a JSON prompt and return a result.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/wh-kuromai/allino"
	_ "modernc.org/sqlite"
)

func post(ctx context.Context, path string, input any) (string, error) {
	body, err := json.Marshal(input)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost:9000"+path, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return "", err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("adapter %s: HTTP %d", path, resp.StatusCode)
	}
	return string(data), nil
}

var gpu = &allino.JobResource{
	Name: "ai-server:localhost:gpu0", Capacity: 1, IdleTimeout: 5 * time.Minute, Timeout: time.Minute,
	Init:    func(ctx context.Context) error { _, err := post(ctx, "/load", nil); return err },
	Release: func(ctx context.Context) error { _, err := post(ctx, "/unload", nil); return err },
}

type Prompt struct {
	Text string `json:"text"`
}

var llm = allino.NewFunction(allino.Option{
	Name: "llm", Version: "1.0.0", Path: "/llm", JobMode: "dispatch",
	Job: allino.JobOption{Resources: []*allino.JobResource{gpu}},
}, func(r *allino.Runtime, in Prompt) (string, error) { return post(r.Context(), "/llm", in) })

var image = allino.NewFunction(allino.Option{
	Name: "image", Version: "1.0.0", Path: "/image", JobMode: "dispatch",
	Job: allino.JobOption{Resources: []*allino.JobResource{gpu}},
}, func(r *allino.Runtime, in Prompt) (string, error) { return post(r.Context(), "/image", in) })

var cpu = allino.NewFunction(allino.Option{
	Name: "uppercase", Version: "1.0.0", Path: "/uppercase", JobMode: "dispatch",
}, func(_ *allino.Runtime, in Prompt) (string, error) { return strings.ToUpper(in.Text), nil })

func main() {
	allow := true
	allino.RunCLI(&allino.Config{SQL: allino.SQLConfig{Driver: "sqlite", DSN: "jobs.db?_pragma=busy_timeout(5000)", AllowMigrate: &allow}})
}
