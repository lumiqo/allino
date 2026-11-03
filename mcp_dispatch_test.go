package allino_test

import (
	"context"
	"encoding/json"
	"github.com/wh-kuromai/allino"
	"testing"
)

func TestPublicMCPDispatch(t *testing.T) {
	r := allino.NewRuntime(s, nil).WithContext(context.Background())
	response := s.DispatchMCP(r, allino.MCPRequest{JSONRPC: "2.0", ID: json.RawMessage(`"request"`), Method: "tools/call", Params: json.RawMessage(`{"name":"mcp_echo","arguments":{"message":"public"}}`)})
	if response.Error != nil {
		t.Fatal(response.Error)
	}
	b, e := json.Marshal(response)
	if e != nil {
		t.Fatal(e)
	}
	var v map[string]any
	_ = json.Unmarshal(b, &v)
	if v["id"] != "request" || v["result"].(map[string]any)["structuredContent"].(map[string]any)["echo"] != "public" {
		t.Fatal(string(b))
	}
	invalid := s.DispatchMCP(r, allino.MCPRequest{JSONRPC: "2.0", ID: json.RawMessage("1"), Method: "missing"})
	if invalid.Error == nil || invalid.Error.Code != -32601 {
		t.Fatal(invalid)
	}
	response = s.DispatchMCP(r, allino.MCPRequest{JSONRPC: "2.0", ID: json.RawMessage("2"), Method: "tools/call", Params: json.RawMessage(`{"name":"mcp_echo","arguments":{}}`)})
	if response.Error != nil {
		t.Fatal(response.Error)
	}
	v = response.Result.(map[string]any)
	if v["isError"] != true {
		t.Fatal("validation was bypassed", v)
	}
}

func TestPublicMCPDoesNotRunCancelledTool(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	response := s.DispatchMCP(allino.NewRuntime(s, nil).WithContext(ctx), allino.MCPRequest{JSONRPC: "2.0", ID: json.RawMessage("1"), Method: "tools/call", Params: json.RawMessage(`{"name":"mcp_echo","arguments":{"message":"cancelled"}}`)})
	if response.Error != nil {
		t.Fatal(response.Error)
	}
	if response.Result.(map[string]any)["isError"] != true {
		t.Fatal("cancelled call ran", response)
	}
}
