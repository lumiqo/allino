package allino

import (
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"reflect"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"

	"github.com/wh-kuromai/jsonino"
	"go.uber.org/zap"
)

type Function interface {
	Options() *Option
	Copy() Function
	HandleRequest(r *Runtime)
	Handlefunc(r *Runtime, input any) (output any, err error)

	InputSchema() (*jsonino.Schema, error)
	OutputSchema() (*jsonino.Schema, error)
	ErrorSchema() (*jsonino.Schema, error)

	UnmarshalInput(buf []byte) (input any, err error)
	UnmarshalOutput(buf []byte) (output any, err error)
	UnmarshalError(buf []byte) (error error, err error)
}

var FunctionList []Function

type idxhandler struct {
	i int
	h Function
}

// パスの優先度を判定するロジック
func comparePaths(p1, p2 string) bool {
	s1 := strings.Split(strings.Trim(p1, "/"), "/")
	s2 := strings.Split(strings.Trim(p2, "/"), "/")

	for k := 0; k < len(s1) && k < len(s2); k++ {
		char1 := s1[k]
		char2 := s2[k]

		// スコア付け (低いほど優先度が高い)
		// 静的: 0, パラメータ(:): 1, ワイルドカード(*): 2
		score1 := getPriority(char1)
		score2 := getPriority(char2)

		if score1 != score2 {
			return score1 < score2
		}
	}

	// セグメントが同じ場合は、より長いパス（深い階層）を先に持ってくる
	return len(s1) > len(s2)
}

func getPriority(segment string) int {
	if strings.HasPrefix(segment, ":") {
		return 1
	}
	if strings.Contains(segment, "*") {
		return 2
	}
	return 0 // 静的パス
}

func (s *Server) RegisterAllFunction() {
	list := make([]*idxhandler, len(FunctionList))
	for i, h := range FunctionList {
		list[i] = &idxhandler{i, h}
	}

	sort.Slice(list, func(i, j int) bool {
		return comparePaths(list[i].h.Options().Path, list[j].h.Options().Path)
	})

	for _, l := range list {
		s.TypedHandle(l.h)
	}
}

var ErrServerError = NewCodeError(500, "server_error", "server error")

func NewFunction[T, U any, E error](option Option, handlefunc func(r *Runtime, input T) (output U, err E)) *GenericFunction[T, U, E] {
	options := &option
	if options.Package == "" {
		options.Package, _ = findExternalCaller([]string{"github.com/wh-kuromai/allino"})
	}

	if options.Method == "" {
		options.Method = "GET"
	}

	if options.ResponseStatusCode == 0 {
		options.ResponseStatusCode = 200
	}
	if options.ErrorStatusCode == 0 {
		options.ErrorStatusCode = 400
	}
	if options.RedirectStatusCode == 0 {
		options.RedirectStatusCode = 302
	}

	tpool := NewReflectPool[T]()
	upool := NewReflectPool[U]()
	epool := NewReflectPool[E]()

	var tDefault T
	var err error
	tDefault, err = tpool.New(func(a any) error {
		return setDefault(a)
	})
	if err != nil {
		panic("set default error: " + err.Error())
	}

	var t *T
	var u *U
	var k *E
	var newParamFn func() T
	//var inputReflectPlan *reflectPlan

	if options.NoWrapJSON || options.ContentType != JSON {
		options.outputType = reflect.TypeOf(u).Elem()
		options.errorType = reflect.TypeOf(k).Elem()
	} else {
		var uw *APIResponse[U]
		var ew *APIError[E]
		options.outputType = reflect.TypeOf(uw).Elem()
		options.errorType = reflect.TypeOf(ew).Elem()
	}
	options.inputType = reflect.TypeOf(t).Elem()
	options.inputReflectPlan = buildPlan(option.inputType)

	if reflect.TypeOf(k).Elem() == reflect.TypeOf((*error)(nil)).Elem() {
		options.eiserror = true
	}

	if options.InputTypeHint != nil {
		options.inputType = reflect.TypeOf(options.InputTypeHint)
	}

	if options.OutputTypeHint != nil {
		options.outputType = reflect.TypeOf(options.OutputTypeHint)
	}

	if options.ErrorTypeHint != nil {
		options.errorType = reflect.TypeOf(options.ErrorTypeHint)
	}

	if options.inputType.Kind() == reflect.Ptr {
		t := options.inputType.Elem()
		newParamFn = func() T {
			cp := reflect.New(t)
			cp.Elem().Set(reflect.ValueOf(tDefault).Elem())
			return cp.Interface().(T)
		}
	}

	var rw *GenericFunction[T, U, E]
	rw = &GenericFunction[T, U, E]{
		options: options,
		tpool:   tpool,
		upool:   upool,
		epool:   epool,
		handlefunc: func(r *Runtime, input T) (output U, err error) {
			defer func() {
				// recover needed.
				if rcv := recover(); rcv != nil {
					r.logger.Error("panic on handler", zap.Any("panic", rcv), zap.ByteString("stack", debug.Stack()))
					var zeroU U
					output = zeroU
					err = ErrServerError
				}
			}()
			return handlefunc(r, input)
		},
		handler: func(r *Runtime) {
			if options.ContentType != "" {
				r.fiber.Set("Content-Type", options.ContentType)
			}

			var param T
			var err error
			// instantiate param if it is a pointer type
			if IsAny[T]() {
			} else if newParamFn != nil {
				param = newParamFn()
				err = r.getAll(param, options.inputReflectPlan)
			} else {
				param = tDefault
				err = r.getAll(&param, options.inputReflectPlan)
			}

			var resp U
			if err == nil {

				var consumed bool
				if options.RequestHandler != nil {
					consumed, err = options.RequestHandler(r, param)
				}

				if err == nil && !consumed {
					for _, ext := range r.server.extopts {
						if err == nil && !consumed && ext.RequestHandler != nil {
							consumed, err = ext.RequestHandler(r, options, param)
						}
					}
				}

				if err == nil && !consumed {
					resp, err = rw.call_internal(r, param, false)
				}

			}

			// Response
			if !isReallyNil(err) {
				for _, ext := range r.server.extopts {
					if ext.ErrorHandler != nil {
						ok := ext.ErrorHandler(r, options, err)
						if ok {
							return
						}
					}
				}

				if options.ErrorHandler != nil {
					ok := options.ErrorHandler(r, err)
					if ok {
						return
					}
				}

				h := contentTypeHandlerMap[options.ContentType]
				if h != nil {
					h.ErrorHandler(r, options, err)
					return
				}

				h = contentTypeHandlerMap[HTML]
				if h != nil {
					h.ErrorHandler(r, options, err)
					return
				}
				return
			}

			for _, ext := range r.server.extopts {
				if ext.ResponseHandler != nil {
					ok := ext.ResponseHandler(r, options, resp)
					if ok {
						return
					}
				}
			}

			if options.ResponseHandler != nil {
				ok := options.ResponseHandler(r, resp)
				if ok {
					return
				}
			}

			h := contentTypeHandlerMap[options.ContentType]
			if h != nil {
				h.ResponseHandler(r, options, resp)
				return
			}

			h = contentTypeHandlerMap[HTML]
			if h != nil {
				h.ResponseHandler(r, options, resp)
				return
			}
		},
	}

	if options.Name != "" {
		handlerMarshalMap[encodeHandlerName(options)] = rw
	}
	options.hasSelfDiscovery = hasSelfDiscovery(reflect.TypeOf(t).Elem())
	options.invoker = rw.invokeFunctionJSON

	FunctionList = append(FunctionList, rw)
	return rw
}

func IsAny[T any]() bool {
	var t *T
	var a *any
	tType := reflect.TypeOf(t).Elem()
	anyType := reflect.TypeOf(a).Elem()
	return tType == anyType
}

type GenericFunction[T, U any, E error] struct {
	options    *Option
	handlefunc func(r *Runtime, input T) (output U, err error)
	handler    func(r *Runtime)

	tpool *ReflectPool[T]
	upool *ReflectPool[U]
	epool *ReflectPool[E]

	// for SelfDiscovery
	Handler string `json:"handler"`
}

func (rw *GenericFunction[T, U, E]) Options() *Option {
	return rw.options
}

func (rw *GenericFunction[T, U, E]) UnmarshalInput(buf []byte) (input any, err error) {
	return rw.tpool.AcquireUnmarshalJSON(buf)
}

func (rw *GenericFunction[T, U, E]) UnmarshalOutput(buf []byte) (output any, err error) {
	return rw.upool.AcquireUnmarshalJSON(buf)
}

func (rw *GenericFunction[T, U, E]) UnmarshalError(buf []byte) (outerr error, err error) {
	return rw.epool.AcquireUnmarshalJSON(buf)
}

func (rw *GenericFunction[T, U, E]) InputSchema() (*jsonino.Schema, error) {
	if rw.options.inputType == nil {
		return nil, errors.New("no inputType")
	}

	return jsonino.SchemaFrom(rw.options.inputType)
}

func (rw *GenericFunction[T, U, E]) OutputSchema() (*jsonino.Schema, error) {
	if rw.options.outputType == nil {
		return nil, errors.New("no outputType")
	}

	return jsonino.SchemaFrom(rw.options.outputType)
}

func (rw *GenericFunction[T, U, E]) ErrorSchema() (*jsonino.Schema, error) {
	if rw.options.errorType == nil {
		return nil, errors.New("no errorType")
	}

	return jsonino.SchemaFrom(rw.options.errorType)
}

func (rw *GenericFunction[T, U, E]) Copy() Function {
	opt := *rw.options
	cp := *rw
	cp.options = &opt
	opt.lastRun = nil
	opt.invoker = cp.invokeFunctionJSON
	return &cp
}

func (rw *GenericFunction[T, U, E]) Call(r *Runtime, input T) (output U, err error) {
	return rw.callWithRefresh(r, input, false)
}

// CallRefresh bypasses cached results and follows the function's JobMode.
// Dispatch returns JobPendingError; memoized joins an existing execution.
func (rw *GenericFunction[T, U, E]) CallRefresh(r *Runtime, input T) (output U, err error) {
	if !rw.options.Job.Cache || rw.options.Session.Type != "" || rw.options.Job.CacheErrOnHit {
		return output, fmt.Errorf("CallRefresh requires cache, memoized, or dispatch JobMode")
	}
	if r.server.callSQLStrategy == nil {
		return output, fmt.Errorf("CallRefresh requires a configured job store")
	}
	return rw.callWithRefresh(r, input, true)
}

func (rw *GenericFunction[T, U, E]) callWithRefresh(r *Runtime, input T, refresh bool) (output U, err error) {
	newR := *r // shallow copy (rは構造体)
	newR.memo = requestMemo{jobRefresh: refresh}
	//opt := rw.options
	//if opt != nil && opt.Path != "" {
	//	newR.loggerWith = r.Logger().With(zap.String("caller", opt.Path)) // 区別しやすく
	//}

	if !r.config.System.DisableValidator {
		if err := r.server.Validator.Struct(input); err != nil {
			var zeroU U
			return zeroU, err
		}
	}

	output, err = rw.call_internal(&newR, input, true)
	return
}

func (rw *GenericFunction[T, U, E]) call_internal(r *Runtime, input T, fromcall bool) (output U, err error) {
	//var zeroU U
	if err := r.enforceACL(rw.options, input); err != nil {
		var zeroU U
		return zeroU, err
	}

	if rw.options.Session.Type != "" {
		return rw.call_session(r, input, fromcall)
	}

	if r.server.callRedisStrategy != nil && r.server.callRedisStrategy.IsTarget(rw.options) {
		return rw.call_stream(r, input, fromcall)
	}

	if rw.options != nil &&
		((fromcall && rw.options.Job.Async) ||
			resourceLimitsEnabled(rw.options.Job) ||
			rw.options.Job.Dedupe ||
			rw.options.Job.Cache ||
			r.memo.jobabortctrl != "") {

		return rw.call_job(r, input, fromcall)
	}

	//for _, exopt := range r.server.extopts {
	//	if exopt.IsCallTarget != nil && exopt.IsCallTarget(rw.options) && exopt.OnCall != nil {
	//		outputa, err := exopt.OnCall(r, input, fromcall)
	//		var ok bool
	//		output, ok = outputa.(U)
	//		if ok {
	//			return output, err
	//		} else {
	//			return zeroU, err
	//		}
	//	}
	//}

	return rw.handlefunc(r, input)
}

func (rw *GenericFunction[T, U, E]) NewInputWithDefault() T {
	t, err := rw.tpool.New(func(a any) error {
		return setDefault(a)
	})
	if err != nil {
		panic("set default error: " + err.Error())
	}
	//var t T
	//t = NewRefOf[T](func(a any) {
	//	setDefault(a)
	//})
	return t
}

func (rw *GenericFunction[T, U, E]) HandleRequest(r *Runtime) {
	rw.handler(r)
}

func (rw *GenericFunction[T, U, E]) Handlefunc(r *Runtime, input any) (output any, err error) {
	var inT T
	inT, _ = input.(T)
	return rw.handlefunc(r, inT)
}

var contentTypeHandlerMap map[string]*contentTypeHandler

type contentTypeHandler struct {
	responseHandler func(r *Runtime, options *Option, output any)
	errorHandler    func(r *Runtime, options *Option, err error)
}

func (c *contentTypeHandler) ResponseHandler(r *Runtime, options *Option, output any) {
	c.responseHandler(r, options, output)
}

func (c *contentTypeHandler) ErrorHandler(r *Runtime, options *Option, err error) {
	c.errorHandler(r, options, err)
}

func init() {
	contentTypeHandlerMap = make(map[string]*contentTypeHandler)
	contentTypeHandlerMap[HTML] = &contentTypeHandler{
		responseHandler: func(r *Runtime, options *Option, output any) {
			r.fiber.Status(options.ResponseStatusCode)
			switch v := output.(type) {
			case []byte:
				r.fiber.Write(v)
			case string:
				r.fiber.Write([]byte(v))
			default:
				if options.HTMLTemplate != "" {
					if options.parsedTemplate == nil {
						tmpl, err := template.New("html").Parse(options.HTMLTemplate)
						if err != nil {
							r.errorRedirect(options.ErrorStatusCode, err)
							return
						}
						options.parsedTemplate = tmpl
					}

					err := options.parsedTemplate.Execute(r.fiber, v)
					if err != nil {
						r.errorRedirect(options.ErrorStatusCode, err)
						return
					}
					return
				}

				if !isReallyNil(v) {
					r.fiber.Write([]byte(fmt.Sprint(v)))
				}
			}

		},
		errorHandler: func(r *Runtime, options *Option, err error) {
			if redir, ok := err.(FiberHandler); ok {
				redir.HandleFiber(r.fiber)
				return
			}
			r.errorRedirect(options.RedirectStatusCode, err)
		},
	}

	contentTypeHandlerMap[JSON] = &contentTypeHandler{
		responseHandler: func(r *Runtime, options *Option, output any) {
			if !options.NoWrapJSON {
				output = &APIResponse[any]{output}
			}
			buf, err := json.MarshalIndent(output, "", "  ")
			if err != nil {
				r.errorJSON(options.ErrorStatusCode, options.NoWrapJSON, options.eiserror, err)
				return
			}

			r.fiber.Status(options.ResponseStatusCode)
			_ = r.fiber.Send(buf)
		},
		errorHandler: func(r *Runtime, options *Option, err error) {
			if redir, ok := err.(FiberHandler); ok {
				redir.HandleFiber(r.fiber)
				return
			}

			r.errorJSON(options.ErrorStatusCode, options.NoWrapJSON, options.eiserror, err)
		},
	}
}

func findExternalCaller(excludePrefixes []string) (string, string) {
	pcs := make([]uintptr, 16)

	// skip=2:
	// 0: runtime.Callers
	// 1: FindExternalCaller
	// 2: 呼び出し元からスタート
	n := runtime.Callers(2, pcs)

	frames := runtime.CallersFrames(pcs[:n])

	for {
		frame, more := frames.Next()

		fn := frame.Function // フルパス付き関数名

		if fn == "" {
			if !more {
				break
			}
			continue
		}

		// prefixチェック
		skip := false
		for _, p := range excludePrefixes {
			if strings.HasPrefix(fn, p) {
				skip = true
				break
			}
		}

		if !skip {
			return cleanPkg(fn), frame.File
		}

		if !more {
			break
		}
	}

	return "", ""
}

func cleanPkg(pkg string) string {

	idx := strings.Index(pkg, ".")
	if idx > 0 {
		pkg = pkg[:idx]
	}
	return pkg
}
