package allino

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// MCPRequest and MCPResponse are transport-independent JSON-RPC envelopes.
type MCPRequest = mcpJSONRPCRequest
type MCPResponse = mcpJSONRPCResponse
type MCPError = mcpRPCError

// WithContext creates a runtime copy with the transport's cancellation context.
// It preserves authentication and request metadata already present in the runtime.
func (r *Runtime) WithContext(ctx context.Context) *Runtime { out := *r; out.ctx = ctx; return &out }

// DispatchMCP reuses the MCP registry and dispatcher without an HTTP round trip.
// Transports must authenticate/authorize their callers before calling this API.
func (s *Server) DispatchMCP(r *Runtime, req MCPRequest) MCPResponse {
	id := req.ID
	if len(id) == 0 {
		id = json.RawMessage("null")
	}
	if req.JSONRPC != "2.0" || req.Method == "" {
		return mcpError(id, -32600, "invalid JSON-RPC request")
	}
	if r == nil {
		r = NewRuntime(s, nil)
	}
	switch req.Method {
	case "ping", "initialize", "notifications/initialized", "tools/list", "tools/call", "resources/list", "resources/read", "prompts/list", "prompts/get":
	default:
		return mcpError(id, -32601, "method not found")
	}
	result, err := dispatchMCPRequest(s, r, &req)
	if err != nil {
		return mcpError(id, -32602, err.Error())
	}
	return MCPResponse{JSONRPC: "2.0", ID: id, Result: result}
}

// CallJSON invokes the typed Call path, retaining validation, ACL and job gates.
// Async results are awaited with the runtime context instead of a fixed MCP timeout.
func (rw *GenericFunction[T, U, E]) CallJSON(r *Runtime, args json.RawMessage) (any, error) {
	if e := r.Context().Err(); e != nil {
		return nil, e
	}
	if len(args) == 0 || string(args) == "null" {
		args = json.RawMessage(`{}`)
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(args, &obj); err != nil || obj == nil {
		return nil, fmt.Errorf("arguments must be a JSON object")
	}
	in := rw.NewInputWithDefault()
	if err := json.Unmarshal(args, &in); err != nil {
		return nil, err
	}
	if rw.options.Job.Async && !rw.options.Job.Cache {
		return nil, fmt.Errorf("queued MCP calls that return results require dispatch job mode")
	}
	output, err := rw.Call(r, in)
	var pending *JobPendingError
	if !errors.As(err, &pending) {
		return output, err
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return nil, r.Context().Err()
		case <-ticker.C:
			var status int
			if e := r.server.SQL.QueryRowContext(r.Context(), "SELECT status FROM executions WHERE key=?", pending.JobID).Scan(&status); e != nil {
				return nil, e
			}
			if status == statusDone || status == statusError {
				return rw.JobResult(r, pending.JobID)
			}
		}
	}
}
