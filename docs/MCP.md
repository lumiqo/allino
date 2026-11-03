# allino - MCP

allino can expose selected `Function`s as an MCP-compatible HTTP JSON-RPC endpoint.

Set `Option.MCP` on a function, then start the server normally. During `OnFunctionInit`, allino registers the MCP HTTP handler when at least one MCP-enabled function exists.

The default endpoint is:

```text
POST /mcp
```

You can change the endpoint with config:

```yaml
mcp:
  endpoint: /my_mcp
```

## Supported Function Types

`Option.MCP` accepts these values:

| Value | MCP method support | Use case |
| --- | --- | --- |
| `tool` | `tools/list`, `tools/call` | Callable tools for LLM agents |
| `resource` | `resources/list`, `resources/read` | Readable resources |
| `prompt` | `prompts/list`, `prompts/get` | Prompt templates |

## Tool Example

```go
type EchoToolInput struct {
	Message string `json:"message" validate:"required"`
}

type EchoToolOutput struct {
	Echo string `json:"echo"`
}

var EchoToolFunction = allino.NewFunction(
	allino.Option{
		Name:        "echo",
		Description: "Echoes a message.",
		ContentType: allino.JSON,
		MCP:         "tool",
	},
	func(r *allino.Runtime, input *EchoToolInput) (*EchoToolOutput, error) {
		return &EchoToolOutput{Echo: input.Message}, nil
	},
)
```

Start your app:

```sh
go run main.go serve
```

List tools:

```sh
curl -s http://localhost:8000/mcp \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
```

Call the tool:

```sh
curl -s http://localhost:8000/mcp \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"echo","arguments":{"message":"hello"}}}'
```

The result contains both MCP text content and structured JSON output:

```json
{
  "jsonrpc": "2.0",
  "id": 2,
  "result": {
    "structuredContent": {
      "echo": "hello"
    },
    "content": [
      {
        "type": "text",
        "text": "{\"echo\":\"hello\"}"
      }
    ]
  }
}
```

## Naming

MCP names are resolved from:

1. `Option.Name`
2. `Option.Path`, when `Name` is empty

Names are normalized to MCP-friendly characters: letters, numbers, `_`, and `-`.

For stable MCP clients, prefer setting `Option.Name` explicitly.

## Schema Generation

allino uses the same typed function metadata used for OpenAPI generation:

- Input JSON schema is generated from `Option.InputType()`.
- Output JSON schema is generated from `Option.OutputType()`.
- `Description` is used as the MCP tool/resource/prompt description.

For tools, `tools/list` includes both `inputSchema` and `outputSchema`.

## Function Execution

MCP calls execute the function through the internal `Option` invoker. This means the MCP path shares the same typed JSON input/output path used by job workers:

- JSON `arguments` are decoded into the function input type.
- `go-playground/validator` validation is applied unless disabled.
- The function output is encoded as JSON.
- Function errors are returned as MCP tool errors for `tools/call`.

MCP calls pass only JSON-RPC `arguments` as input. HTTP query and form values are not merged into the function input.

## Resources

Resource functions are exposed with generated URIs:

```text
allino://<name>
```

Example request:

```json
{
  "jsonrpc": "2.0",
  "id": 3,
  "method": "resources/read",
  "params": {
    "uri": "allino://my_resource",
    "arguments": {}
  }
}
```

## Mounted Local Resources

You can also mount local directories as MCP resources.

```yaml
mcp:
  resourceScheme: allino
  resourceHost: resource
  resourceDirs:
    - ./resources
```

allino recursively scans every file in the configured directories and exposes each file from `resources/list`.

Mounted file resource URIs use this form:

```text
allino://resource/<relative-path>
```

The `allino` scheme and `resource` host can be changed with `mcp.resourceScheme` and `mcp.resourceHost`.

Example list item:

```json
{
  "uri": "allino://resource/docs/guide.md",
  "name": "docs/guide.md",
  "description": "Local file: docs/guide.md",
  "mimeType": "text/markdown; charset=utf-8"
}
```

`resources/read` returns UTF-8 files as `text`. Non-UTF-8 files are returned as base64 `blob`.

## Prompts

Prompt functions use their input schema to generate MCP prompt arguments. `prompts/get` calls the function and returns the output as a user prompt message.

Example request:

```json
{
  "jsonrpc": "2.0",
  "id": 4,
  "method": "prompts/get",
  "params": {
    "name": "my_prompt",
    "arguments": {}
  }
}
```

## Mounted Markdown Prompts

You can also mount local directories that contain Markdown prompt files.

Configure `mcp.promptDirs`:

```yaml
mcp:
  endpoint: /mcp
  promptDirs:
    - ./prompts
```

Relative paths are resolved from `ConfigDir`. If `ConfigDir` is empty, paths are resolved from the current working directory.

allino recursively scans every `.md` file in the configured directories. Each file is added to `prompts/list` and can be read with `prompts/get`.

Markdown file format:

```md
---
name: code_review
description: Review code and find correctness issues.
---
Review the following code for bugs, regressions, and missing tests.
```

The Markdown body is returned as a MCP prompt message:

```json
{
  "role": "user",
  "content": {
    "type": "text",
    "text": "Review the following code for bugs, regressions, and missing tests."
  }
}
```

Frontmatter fields:

| Field | Required | Behavior |
| --- | --- | --- |
| `name` | No | Used as the MCP prompt name. If omitted, allino uses the filename without `.md`. |
| `description` | No | Returned by `prompts/list` and `prompts/get`. |

If a mounted Markdown prompt has the same name as a Function prompt, the Function prompt takes precedence.

## Custom transports

Applications can reuse the MCP registry and dispatcher without routing through HTTP:

```go
s.RegisterAllFunction()
if err := s.InitializeFunctions(); err != nil {
    return err
}
r := allino.NewRuntime(s, nil).WithContext(ctx)
response := s.DispatchMCP(r, allino.MCPRequest{
    JSONRPC: "2.0",
    ID: json.RawMessage(`"request-1"`),
    Method: "tools/list",
})
```

The application transport must authenticate and authorize callers. `WithContext`
preserves an existing runtime's identity and metadata; it does not impersonate a
user. Register all functions before calling `InitializeFunctions`, which initializes
backend extensions once and returns initialization errors without starting an HTTP
listener. It does not run `OnServe` hooks.

Calls use the typed `CallJSON` / `Call` path, including validation, ACL and job
resource gates. Queued handlers returning MCP results must use `dispatch` mode;
result-less `async` calls are rejected before enqueueing. Dispatch results wait
with the runtime's context. Cancelling the waiter does not promise to cancel a
shared running job.

`Option.MCPInputSchema` and `Option.MCPOutputSchema` accept explicit JSON Schema
objects for dynamic payloads such as `map[string]any` or `any` that the reflection
schema generator cannot describe. These override catalog schemas, not runtime
validation; retain typed validation in the handler.
