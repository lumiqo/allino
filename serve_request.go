package allino

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"encoding/xml"
	"errors"
	"mime/multipart"
	"net/http"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gofiber/fiber/v2"
	"github.com/openai/openai-go"
	"github.com/redis/go-redis/v9"
	"github.com/rs/xid"
	"go.uber.org/zap"
)

var (
	ErrNoRedisConfig = errors.New("config login.redis has no valid setting")
	ErrNotStruct     = errors.New("get need pointer for struct")
)

type Origin int

const (
	REQUEST_VIRTUAL Origin = iota
	REQUEST_HTTP
	REQUEST_WS
	REQUEST_CRON
	REQUEST_JOB
	REQUEST_CLI
	REQUEST_STREAM
)

var requestTypeStr = []string{"virtual", "http", "ws", "cron", "job", "cli", "stream"}

func (r Origin) String() string {
	return requestTypeStr[int(r)]
}

type Runtime struct {
	ctx        context.Context // per-execution cancellation (for leased jobs)
	config     *Config
	fiber      *fiber.Ctx
	logger     *zap.Logger
	loggerWith *zap.Logger
	redis      redis.UniversalClient
	sql        *sql.DB

	server *Server
	memo   requestMemo
	cache  *requestCache
}

type requestMemo struct {
	jobid          string
	jobabortctrl   string
	jobrequeuewait int
}

type requestCache struct {
	req_type  Origin
	requestid string
	clientip  string
	//endpoint  string

	//taskwheel        *twWheel
	//validator        *validator.Validate
	//input          any
	sessiontype    string
	sessionid      string
	sessionname    string
	sessionversion string
	sessionredis   *redisSession
	//guestcookiefound  bool
	//ctx              context.Context

	cachedLogin    bool
	cachedUid      string
	cachedName     string
	cachedWritable bool
	cachedJWTBody  []byte
	cachedErr      error
	authorizedBy   string

	parentjobid string
	rootjobid   string

	jwtdecodedbyclaims map[string]string
	jwtdecodedbytag    map[string]json.RawMessage

	//extopts []*ExtOption
	body    []byte
	deferfn []func() error
}

func NewRuntime(s *Server, w *fiber.Ctx) *Runtime {
	req := &Runtime{
		config: s.Config,
		fiber:  w,
		logger: s.Logger,
		redis:  s.Redis,
		sql:    s.SQL,
		server: s,

		cache: &requestCache{
			//ctx:       s.appctx,
			//extopts:   s.extopts,
			//taskwheel: s.taskwheel,
			//validator: s.Validator,
			//endpoint:  s.endpoint,
		},
	}
	return req
}

func (r *Runtime) Config() *Config {
	return r.config
}

func (r *Runtime) Fiber() *fiber.Ctx {
	return r.fiber
}

func (r *Runtime) Server() *Server {
	return r.server
}

func (r *Runtime) Redis() redis.UniversalClient {
	return r.redis
}

func (r *Runtime) SQL() *sql.DB {
	return r.sql
}

func (r *Runtime) S3() *s3.Client {
	return r.server.S3
}

func (r *Runtime) ChatGPT() *openai.Client {
	return r.server.ChatGPT
}

func (r *Runtime) AI(model ...string) AI {
	return r.config.AI.Select(model...)
}

func (r *Runtime) HttpClient() *http.Client {
	return r.server.HttpClient
}

func (r *Runtime) do_defer() {
	for _, fn := range r.cache.deferfn {
		fn()
	}
}

func (r *Runtime) Logger() *zap.Logger {
	if r.loggerWith != nil {
		return r.loggerWith
	}

	if !r.config.Log.NoRequestID {
		r.loggerWith = r.logger.With(zap.String("request_id", r.RequestID()), zap.String("from", r.cache.req_type.String()))
	} else {
		r.loggerWith = r.logger
	}
	return r.loggerWith
}

func (r *Runtime) Defer(fn func() error) {
	if r.cache.deferfn == nil {
		r.cache.deferfn = make([]func() error, 0, 5)
	}
	r.cache.deferfn = append(r.cache.deferfn, fn)
}

func (r *Runtime) RequestID() string {
	if r.cache.requestid != "" {
		return r.cache.requestid
	}

	var rid string
	if r.config.TrustedProxy.TrustXRequestID {
		rid = r.fiber.Get("X-Request-ID")
	}

	if rid == "" {
		rid = xid.New().String()
	}

	r.cache.requestid = rid
	return r.cache.requestid
}

func (r *Runtime) ClientIP() string {
	if r.cache.clientip != "" {
		return r.cache.clientip
	}

	if r.config.TrustedProxy.TrustXForwardedFor {
		xff := r.fiber.Get("X-Forwarded-For")
		if xff != "" {
			r.cache.clientip = xff
			return xff
		}
	}

	ra := r.fiber.Context().RemoteAddr()
	if ra == nil {
		r.cache.clientip = "unknown"
		return r.cache.clientip
	}

	r.cache.clientip = ra.String()
	return r.cache.clientip
}

var cachedRx sync.Map // map[string]*regexp.Regexp

func cachedRegex(rx string) (*regexp.Regexp, error) {
	if v, ok := cachedRx.Load(rx); ok {
		return v.(*regexp.Regexp), nil
	}
	re, err := regexp.Compile(rx) // MustCompileはpanicるので避ける
	if err != nil {
		return nil, err
	}
	cachedRx.Store(rx, re)
	return re, nil
}

func (r *Runtime) Context() context.Context {
	if r.ctx != nil {
		return r.ctx
	}
	if r.fiber != nil {
		return r.fiber.UserContext()
	}

	if r.server.appctx == nil {
		r.server.appctx = context.Background()
	}
	return r.server.appctx
}

func (r *Runtime) jwtdecodedbytag(jwtbody []byte) (map[string]json.RawMessage, error) {
	if r.cache.jwtdecodedbytag == nil {
		var out map[string]json.RawMessage
		err := json.Unmarshal(jwtbody, &out)
		if err != nil {
			return nil, err
		}
		r.cache.jwtdecodedbytag = out
	}
	return r.cache.jwtdecodedbytag, nil
}

func (r *Runtime) jwtdecodedbyclaims(jwtbody []byte) (map[string]string, error) {
	if r.cache.jwtdecodedbyclaims == nil {

		dec := json.NewDecoder(bytes.NewReader(jwtbody))
		dec.UseNumber() // 53bit超の整数でも文字列として保持できる
		var m map[string]any
		if err := dec.Decode(&m); err != nil {
			return nil, err
		}

		out := make(map[string]string, len(m))
		for k, v := range m {
			switch t := v.(type) {
			case string:
				out[k] = t
			case json.Number:
				out[k] = t.String() // そのままの表記を維持
			case bool:
				out[k] = strconv.FormatBool(t)
				// null / []any / map[string]any は捨てる
			}
		}
		r.cache.jwtdecodedbyclaims = out
	}
	return r.cache.jwtdecodedbyclaims, nil
}

func (r *Runtime) bodyBytes() []byte {
	if r.cache.body == nil {
		r.cache.body = r.fiber.Body() // Fiber側のバッファ参照（必要ならcopy）
	}
	return r.cache.body
}

func (r *Runtime) getByStructField(rpf *fieldPlan, fieldVal reflect.Value) (string, bool, error) {
	//qx, ok := field.Tag.Lookup("path")
	qx := rpf.tags[tagPath]
	ok := rpf.tagoks[tagPath]
	if ok {
		qv := r.fiber.Params(qx)
		if qv != "" {
			return qv, true, nil
		} else {
			return "", false, nil
		}
	}

	//qx, ok = field.Tag.Lookup("query")
	qx = rpf.tags[tagQuery]
	ok = rpf.tagoks[tagQuery]
	if ok {
		qv := r.fiber.Query(qx)
		if qv != "" {
			return qv, true, nil
		} else {
			return "", false, nil
		}
	}

	//qx, ok = field.Tag.Lookup("form")
	qx = rpf.tags[tagForm]
	ok = rpf.tagoks[tagForm]
	if ok {
		ct := strings.ToLower(r.fiber.Get("Content-Type"))
		if strings.HasPrefix(ct, "application/x-www-form-urlencoded") ||
			strings.HasPrefix(ct, "multipart/form-data") {
			return r.fiber.FormValue(qx), true, nil
		} else {
			return "", false, nil
		}
	}

	//qx, ok = field.Tag.Lookup("post")
	qx = rpf.tags[tagPost]
	ok = rpf.tagoks[tagPost]
	if ok {
		buf := r.bodyBytes()
		if len(buf) > 0 {
			target := fieldVal.Addr().Interface()
			if fieldVal.Kind() == reflect.Ptr {
				if fieldVal.IsNil() {
					fieldVal.Set(reflect.New(fieldVal.Type().Elem()))
				}
				target = fieldVal.Interface()
			}

			ct := strings.ToLower(r.fiber.Get("Content-Type"))
			switch qx {
			case "json":
				if !strings.HasPrefix(ct, "application/json") &&
					!strings.HasPrefix(ct, "text/") {
					return "", false, nil
				}

				err := json.Unmarshal(buf, target)
				if err != nil {
					return "", false, errors.New("failed to unmarshal JSON body: " + err.Error())
				}
				return "", false, nil
			case "xml":
				if !strings.HasPrefix(ct, "application/xml") &&
					!strings.HasPrefix(ct, "text/") {
					return "", false, nil
				}
				err := xml.Unmarshal(buf, target)
				if err != nil {
					return "", false, errors.New("failed to unmarshal XML body: " + err.Error())
				}
				return "", false, nil
			case "raw":
				if fieldVal.Kind() == reflect.Slice && fieldVal.Type().Elem().Kind() == reflect.Uint8 {
					fieldVal.SetBytes(buf)
					return "", false, nil
				}
			}
		}
	}

	//qx, ok = field.Tag.Lookup("jwt")
	qx = rpf.tags[tagJWT]
	ok = rpf.tagoks[tagJWT]
	if ok {
		_, _, _, jwtbody, err := r.userWithJWT()
		if err != nil {
			return "", false, nil
		}

		m, err := r.jwtdecodedbytag(jwtbody)
		if err == nil {
			if v, ok := m[qx]; ok {
				var target any
				if fieldVal.Kind() == reflect.Ptr {
					if fieldVal.IsNil() {
						fieldVal.Set(reflect.New(fieldVal.Type().Elem()))
					}
					target = fieldVal.Interface()
				} else {
					target = fieldVal.Addr().Interface()
				}

				json.Unmarshal(v, target)
			}

			return "", false, nil
		}
	}

	//qx, ok = field.Tag.Lookup("cookie")
	qx = rpf.tags[tagCookie]
	ok = rpf.tagoks[tagCookie]
	if ok {
		qx := r.fiber.Cookies(qx)
		if qx != "" {
			return qx, true, nil
		} else {
			return "", false, nil
		}
	}

	//qx, ok = field.Tag.Lookup("header")
	qx = rpf.tags[tagHeader]
	ok = rpf.tagoks[tagHeader]
	if ok {
		qx := r.fiber.Get(qx)
		if qx != "" {
			return qx, true, nil
		} else {
			return "", false, nil
		}
	}

	//qx, ok = field.Tag.Lookup("cli")
	qx = rpf.tags[tagCli]
	ok = rpf.tagoks[tagCli]
	if ok {
		qx := r.config.CliVar[qx]
		if qx != "" {
			return qx, true, nil
		} else {
			return "", false, nil
		}
	}

	return "", false, nil
}

var (
	tFileHeaderPtr = reflect.TypeOf((*multipart.FileHeader)(nil)) // *multipart.FileHeader
	tSliceFHPtr    = reflect.SliceOf(tFileHeaderPtr)              // []*multipart.FileHeader
	tTime          = reflect.TypeOf((*time.Time)(nil)).Elem()     // time.Time
)

func (r *Runtime) getAll(params interface{}, rp *reflectPlan) error {
	if params == nil {
		return ErrNotStruct
	}

	if rp == nil {
		return ErrNotStruct
	}

	pv := reflect.ValueOf(params)
	if pv.Kind() == reflect.Pointer {
		pv = pv.Elem()
	}
	pt := pv.Type()

	injectTarget := make([]*InjectionTarget, 0, 10)

	for i := 0; i < pt.NumField(); i++ {
		ptf := pt.Field(i)
		// 非公開フィールドはスキップ（タグ付けされていても set できないため）
		if ptf.PkgPath != "" && !ptf.Anonymous {
			continue
		}
		rpf := rp.fields[i]
		ptv := pv.Field(i)

		// if *multipart.FileHeader
		if rpf.typ == tFileHeaderPtr {
			qx := rpf.tags[tagForm]   //.Tag.Lookup("form")
			ok := rpf.tagoks[tagForm] //.Tag.Lookup("form")
			if ok {
				fh, err := r.fiber.FormFile(qx)
				if err != nil {
					continue
					//return errors.New("form file not found: " + qx)
				}
				ptv.Set(reflect.ValueOf(fh))
			}
			continue
		}
		// if []*multipart.FileHeader or *[]*multipart.FileHeader
		if rpf.typ == tSliceFHPtr || (rpf.kind == reflect.Ptr && rpf.typ.Elem() == tSliceFHPtr) {
			qx := rpf.tags[tagForm]   //.Tag.Lookup("form")
			ok := rpf.tagoks[tagForm] //.Tag.Lookup("form")
			if ok {
				mf, err := r.fiber.MultipartForm()
				if err == nil && mf != nil {
					files := mf.File[qx]
					if len(files) > 0 {
						// make slice []*multipart.FileHeader
						slv := reflect.MakeSlice(tSliceFHPtr, len(files), len(files))
						for i, fh := range files {
							slv.Index(i).Set(reflect.ValueOf(fh))
						}
						// set to field (supports both slice and pointer-to-slice)
						if rpf.kind == reflect.Ptr {
							p := reflect.New(tSliceFHPtr)
							p.Elem().Set(slv)
							ptv.Set(p)
						} else {
							ptv.Set(slv)
						}
					}
				}
			}
			continue
		}

		pfval, ok, err := r.getByStructField(rpf, ptv)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}

		if rpf.kind == reflect.Struct && pt.Field(i).Anonymous && rpf.typ != tTime {
			r.getAll(ptv.Interface(), rpf.child)
			continue
		}

		rx := rpf.tags[tagRegex]  //.Tag.Lookup("regex")
		ok = rpf.tagoks[tagRegex] //.Tag.Lookup("regex")
		if ok {
			cregex, err := cachedRegex(rx)
			if err != nil {
				//r.Logger().Warn("regex compile failed",
				//	zap.String("field", rpf.name),
				//	zap.String("pattern", rx),
				//	zap.String("value", pfval),
				//)
				//continue
				return ErrRegexCompileFailed
			}
			if !cregex.MatchString(pfval) {
				//r.Logger().Debug("regex mismatch",
				//	zap.String("field", rpf.name),
				//	zap.String("pattern", rx),
				//	zap.String("value", pfval),
				//)
				//continue
				return ErrRegexMismatch
			}
		}

		//--- inject="extensionName:action"
		if rpf.tagoks[tagResource] {
			target := ptv.Addr().Interface()
			if ptv.Kind() == reflect.Ptr {
				if ptv.IsNil() {
					ptv.Set(reflect.New(ptv.Type().Elem()))
				}
				target = ptv.Interface()
			}

			extName := rpf.tags[tagResource]
			action := ""
			extNameIdx := strings.Index(extName, ":")
			if extNameIdx > 0 {
				action = extName[extNameIdx+1:]
				extName = extName[:extNameIdx]
			}

			injectTarget = append(injectTarget, &InjectionTarget{
				Input:     pfval,
				Extension: extName,
				Action:    action,
				Reference: target,
			})

			continue
		}
		//---

		setByReflect(pfval, rpf.ispointer, rpf.basetyp, ptv)
	}

	if !r.config.System.DisableValidator {
		if err := r.server.Validator.Struct(params); err != nil {
			return ErrValidationFailed
		}
	}

	// Injection Extension
	if len(injectTarget) > 0 {
		for _, opt := range r.server.extopts {
			if opt.OnInjection == nil {
				continue
			}

			extInjectTarget := make([]*InjectionTarget, 0, len(injectTarget))
			for _, v := range injectTarget {
				if v.Extension == opt.Name {
					extInjectTarget = append(extInjectTarget, v)
				}
			}

			if len(extInjectTarget) > 0 {
				err := opt.OnInjection(r, extInjectTarget)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

var ErrValidationFailed = NewCodeError(401, "validation_failed", "validation failed")
var ErrRegexMismatch = NewError("regex mismatch")
var ErrRegexCompileFailed = NewError("regex micompile failedsmatch")

func setByReflect(value string, ispointer bool, basetyp reflect.Type, ptv reflect.Value) {
	// if time.Time
	if basetyp == tTime {
		tval, err := parseTimeSafe(value, time.UTC)
		if err != nil {
			return
		}

		if ispointer {
			ptr := reflect.New(basetyp)
			ptr.Elem().Set(reflect.ValueOf(tval))
			ptv.Set(ptr)
		} else {
			ptv.Set(reflect.ValueOf(tval))
		}
		return
	}

	if basetyp == tDuration {
		tval, err := time.ParseDuration(value)
		if err != nil {
			return
		}

		if ispointer {
			ptr := reflect.New(basetyp)
			ptr.Elem().Set(reflect.ValueOf(tval))
			ptv.Set(ptr)
		} else {
			ptv.Set(reflect.ValueOf(tval))
		}
	}

	switch basetyp.Kind() {
	case reflect.Bool:
		pfvalbool, err := strconv.ParseBool(value)
		if err != nil {
			break
		}

		if ispointer {
			ptr := reflect.New(basetyp)
			ptr.Elem().Set(reflect.ValueOf(pfvalbool))
			ptv.Set(ptr)
		} else {
			ptv.SetBool(pfvalbool)
		}
	case reflect.Float32:
		fallthrough
	case reflect.Float64:
		pfvalfloat, err := strconv.ParseFloat(value, 64)
		if err != nil {
			break
		}

		if ispointer {
			ptr := reflect.New(basetyp)
			ptr.Elem().SetFloat(pfvalfloat)
			ptv.Set(ptr)
		} else {
			ptv.SetFloat(pfvalfloat)
		}
	case reflect.Int8:
		fallthrough
	case reflect.Int16:
		fallthrough
	case reflect.Int32:
		fallthrough
	case reflect.Int64:
		fallthrough
	case reflect.Int:
		pfvalint, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			break
		}

		if ispointer {
			ptr := reflect.New(basetyp)
			ptr.Elem().SetInt(pfvalint)
			ptv.Set(ptr)
		} else {
			ptv.SetInt(pfvalint)
		}
	case reflect.Uint8:
		fallthrough
	case reflect.Uint16:
		fallthrough
	case reflect.Uint32:
		fallthrough
	case reflect.Uint64:
		fallthrough
	case reflect.Uint:
		pfvaluint, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			break
		}
		if ispointer {
			ptr := reflect.New(basetyp)
			ptr.Elem().SetUint(pfvaluint)
			ptv.Set(ptr)
		} else {
			ptv.SetUint(pfvaluint)
		}
	case reflect.Slice:
		// []byte
		if basetyp.Elem().Kind() == reflect.Uint8 { // []byte
			if ispointer {
				p := reflect.New(basetyp)
				p.Elem().SetBytes([]byte(value))
				ptv.Set(p)
			} else {
				ptv.SetBytes([]byte(value))
			}
		}

	case reflect.String:
		if ispointer {
			p := reflect.New(basetyp)
			p.Elem().SetString(value)
			ptv.Set(p)
		} else {
			ptv.SetString(value)
		}
	}
}
