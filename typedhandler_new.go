package allino

import (
	"html/template"
	"reflect"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
)

type Option struct {
	// HTTP
	Path               string
	Method             string
	SubMethod          []string
	ContentType        string
	CORS               bool
	CORSCustomHeader   map[string]string
	RequestHandler     func(r *Runtime, input any) (consumed bool, err error)
	ResponseHandler    func(r *Runtime, output any) (consumed bool)
	ResponseStatusCode int
	ErrorHandler       func(r *Runtime, err error) (consumed bool)
	ErrorStatusCode    int
	RedirectStatusCode int
	NoWrapJSON         bool
	HTMLTemplate       string

	// Session
	Session SessionOption

	// ACL
	ACLResource string
	ACLAction   string

	// Job
	Cron    string
	JobMode string
	Job     JobOption

	// Pipeline (experimental)
	Next []Function

	// AI (experimental)
	SystemPrompt    string
	Tools           []Function
	MCP             string         // "tool", "resource", "prompt"
	MCPInputSchema  map[string]any // Optional explicit schema for dynamic JSON inputs.
	MCPOutputSchema map[string]any // Optional explicit schema for dynamic JSON outputs.

	// Semantics
	Package     string // optional: override auto package detection, used for route printing,
	Name        string // required: job
	Version     string // required: job
	Summary     string // optional: openapi
	Description string // optional: openapi, tools, mcp
	Class       string // experimental
	Extra       any    // optional

	// Reflection Hints
	InputTypeHint  any
	OutputTypeHint any
	ErrorTypeHint  any

	// Events
	OnInit     func(s *Server, r *Runtime) error
	OnShutdown func(s *Server, r *Runtime) error

	// cache
	parsedTemplate *template.Template
	invoker        functionInvoker

	inputType        reflect.Type
	outputType       reflect.Type
	errorType        reflect.Type
	eiserror         bool
	hasSelfDiscovery bool
	inputReflectPlan *reflectPlan

	lastRun *time.Time
	exts    *sync.Map
	cronid  cron.EntryID
}

func (h Option) InputType() reflect.Type {
	return h.inputType
}

func (h Option) OutputType() reflect.Type {
	return h.outputType
}

func (h Option) ErrorType() reflect.Type {
	return h.errorType
}

type handlerExtEntry struct {
	value     any  // 実際の *F
	isUserSet bool // Handler 作成時にユーザーが明示設定したか
}

// 追加オプション
func (h Option) WithExt(v any) Option {
	if h.exts == nil {
		h.exts = &sync.Map{}
	}
	t := reflect.ValueOf(v).Type()
	if t.Kind() == reflect.Ptr {
		setDefault(v)
	} else {
		setDefault(&v)
	}

	h.exts.Store(t, handlerExtEntry{v, true})
	return h
}

func NewTypedAPI[T, U any, E error](path string, handler func(*Runtime, T) (U, E)) *GenericFunction[T, U, E] {
	return NewFunction(
		Option{
			Path:        path,
			Method:      "GET",
			SubMethod:   []string{"POST"},
			ContentType: JSON,
		},
		handler,
	)
}

func NewTypedUI[T, U any, E error](path string, handler func(*Runtime, T) (U, E)) *GenericFunction[T, U, E] {
	return NewFunction(
		Option{
			Path:        path,
			Method:      "GET",
			SubMethod:   []string{"POST"},
			ContentType: HTML,
		},
		handler,
	)
}

func GetClassHandlers(class string) []Function {
	list := make([]Function, 0, len(FunctionList))
	for _, th := range FunctionList {
		if th.Options().Class == class {
			list = append(list, th)
		}
	}
	return list
}
