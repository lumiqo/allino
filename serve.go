package allino

import (
	"bytes"
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/base64"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/casbin/casbin/v2"
	"github.com/go-playground/validator/v10"
	"github.com/goccy/go-yaml"
	"github.com/gofiber/fiber/v2"
	"github.com/openai/openai-go"
	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"
	"github.com/wh-kuromai/allino/internal/timewheel"
	"github.com/wh-kuromai/cryptino"
	"go.uber.org/zap"

	_ "embed"
)

type Config struct {
	ConfigBytes []byte `json:"-"`
	ConfigDir   string `json:"-"`
	ConfigFS    fs.FS  `json:"-"`
	AbsWorkDir  string `json:"-"`

	OnInit     func(s *Server) error       `json:"-"`
	OnServe    func(s *Server) error       `json:"-"`
	OnListen   func(s *Server) error       `json:"-"`
	OnShutdown func(s *Server) error       `json:"-"`
	OnError    func(msg string, err error) `json:"-"`

	AppName     string `json:"appName"`
	Description string `json:"description"`
	Version     string `json:"version"`
	Bind        string `json:"bind"`
	//Endpoint    string `json:"endpoint"`
	NoWelcome  bool `json:"nowelcome"`
	NoWrapJSON bool `json:"nowrapjson"`

	Routing      RoutingConfig      `json:"routing"`
	Login        LoginConfig        `json:"login"`
	Session      SessionConfig      `json:"session"`
	Redis        RedisConfig        `json:"redis"`
	SQL          SQLConfig          `json:"sql"`
	Log          LogConfig          `json:"log"`
	Fiber        fiber.Config       `json:"fiber"`
	WebSocket    WebSocketConfig    `json:"websocket"`
	Https        HttpsConfig        `json:"https"`
	System       SystemConfig       `json:"system"`
	TrustedProxy TrustedProxyConfig `json:"trustedproxy"`
	JobConfig    JobConfig          `json:"job"`
	HttpClient   HttpClientConfig   `json:"httpclient"`
	TimeWheel    TimeWheelConfig    `json:"timewheel"`
	Sqids        SqidsConfig        `json:"sqids"`
	S3           S3Config           `json:"s3"`
	AI           AIConfig           `json:"ai"`
	Casbin       CasbinConfig       `json:"casbin"`

	Debug            bool              `json:"debug"`
	DisabledCommands []string          `json:"-"`
	Prefix           string            `json:"-"`
	StartAt          time.Time         `json:"-"`
	CliVar           map[string]string `json:"-"`
}

type SystemConfig struct {
	DisableValidator bool `json:"disable_validator"`
	DisableExtension bool `json:"disable_extension"`
}

type TrustedProxyConfig struct {
	TrustXForwardedFor bool `json:"trustXForwardedFor"`
	TrustXRequestID    bool `json:"trustXRequestID"`
}

type RoutingConfig struct {
	FallbackPaths []string `json:"fallbacks"`
	ErrorPath     string   `json:"error"`
	Err404Path    string   `json:"404error"`
}

type Server struct {
	functionsInitOnce sync.Once
	functionsInitErr  error

	Config     *Config
	Fiber      *fiber.App
	Logger     *zap.Logger
	Redis      redis.UniversalClient
	SQL        *sql.DB
	Cron       *cron.Cron
	S3         *s3.Client
	Validator  *validator.Validate
	HttpClient *http.Client
	Env        map[string]string
	Sqids      *Sqids
	TimeWheel  *timewheel.TimeWheel
	ChatGPT    *openai.Client
	Casbin     *casbin.SyncedEnforcer

	nodeip     string
	extensions []extendable
	extopts    []*ExtOption

	FunctionCache        []Function
	internalHandlerCache []Function
	optionsCache         []*Option

	appctx      context.Context
	appcancel   context.CancelFunc
	forcectx    context.Context
	forcecancel context.CancelFunc
	//runAsPlugin bool

	handlerOptMap     map[string]*Option
	session           *sessionManager
	jobManager        *jobManager
	callSQLStrategy   *callSQLStrategy
	callRedisStrategy *callRedisStrategy
}

var yamlDecodeOption = NewYAMLCustomDecodeOption()
var yamlEncodeOption = NewYAMLCustomEncodeOption()

//go:embed appsetting_default.yaml
var settingDefault []byte

func NewServer(config *Config, extconfig ...map[string]any) (*Server, error) {

	s := &Server{
		Config: &Config{},
		//Router: httprouter.New(),
		Cron:          cron.New(),
		Validator:     validator.New(),
		handlerOptMap: map[string]*Option{},
	}

	ctxb := context.Background()
	appctx, appcancel := context.WithCancel(ctxb)
	s.appctx = appctx
	s.appcancel = appcancel

	forcectx, forcecancel := context.WithCancel(ctxb)
	s.forcectx = forcectx
	s.forcecancel = forcecancel

	//s.TypedRouter = &TypedRouter{
	//	//Router: s.Router,
	//	server: s,
	//}

	if !s.Config.System.DisableExtension {
		s.extensions = extensionList
		for _, ext := range s.extensions {
			s.extopts = append(s.extopts, ext.ExtOption())

			if len(extconfig) > 0 {
				extc, ok := extconfig[0][ext.ExtOption().Name]
				if ok {
					ext.initConfig(extc)
				}
			}
		}
	}

	s.Config.StartAt = time.Now()
	if s.Config.Prefix == "" {
		s.Config.Prefix = "allino"
	}

	err := s.Update(settingDefault, false)
	if err != nil {
		return nil, err
	}

	err = mergeStruct(s.Config, config)
	if err != nil {
		return nil, err
	}

	if s.Config.ConfigBytes != nil {
		err = s.Update(s.Config.ConfigBytes, false)
		if err != nil {
			return nil, err
		}
	}

	config_files := []string{
		s.filePrefix() + ".config.yaml",
		s.filePrefix() + ".config.enc",
	}

	secrets_files := []string{
		"secrets.config.json",
		"secrets.config.enc",
	}

	for _, config_file := range config_files {
		err = s.updateFS(config_file, false)
		if err != nil {
			return nil, err
		}
	}

	for _, secrets_file := range secrets_files {
		err = s.updateFS(secrets_file, true)
		if err != nil {
			return nil, err
		}
	}

	for _, config_file := range config_files {
		err = s.updateFile(config_file, false)
		if err != nil {
			return nil, err
		}
	}

	for _, secrets_file := range secrets_files {
		err = s.updateFile(secrets_file, true)
		if err != nil {
			return nil, err
		}
	}

	err = s.generateKeyIfNotExist()
	if err != nil {
		return nil, err
	}

	s.Config.Fiber.DisableStartupMessage = true
	s.Fiber = fiber.New(s.Config.Fiber)
	//s.Server, err = s.Config.Server.Setup()
	//if err != nil {
	//	return nil, err
	//}
	//
	var logmiddle fiber.Handler
	s.Logger, logmiddle, err = s.Config.Log.Setup(s.Cron, s.Config.Debug)
	if err != nil {
		return nil, err
	}

	if logmiddle != nil {
		s.Fiber.Use(logmiddle)
	}

	s.TimeWheel = s.Config.TimeWheel.setup(s.appctx)

	err = s.Config.Login.setup()
	if err != nil {
		return nil, err
	}

	s.Redis, err = s.Config.Redis.connect()
	if err != nil {
		return nil, err
	}

	s.SQL, err = s.Config.SQL.connect()
	if err != nil {
		return nil, err
	}

	s.HttpClient, err = s.Config.HttpClient.setup()
	if err != nil {
		return nil, err
	}

	s.Sqids, err = s.Config.Sqids.setup()
	if err != nil {
		return nil, err
	}

	s.S3, err = s.Config.S3.setup(appctx)
	if err != nil {
		return nil, err
	}

	s.Casbin, err = s.Config.Casbin.setup(s)
	if err != nil {
		return nil, err
	}

	s.Env = environMap()

	if len(s.Cron.Entries()) > 0 {
		s.Cron.Start()
	}

	if s.Config.OnInit != nil {
		err = s.Config.OnInit(s)
		if err != nil {
			return nil, fmt.Errorf("OnInit error: %w", err)
		}
	}

	for _, ext := range s.extopts {
		if ext.OnInit != nil {
			err = ext.OnInit(s, NewRuntime(s, nil))
			if err != nil {
				return nil, fmt.Errorf("OnInit error for Extension %s: %w", ext.Name, err)
			}
		}
	}

	if s.SQL != nil && (s.Config.SQL.AllowMigrate == nil || *s.Config.SQL.AllowMigrate) {
		for _, ext := range s.extopts {
			if ext.SQLSchema != nil {
				schema := ext.SQLSchema(s.Config.SQL.Driver)
				if schema != "" {
					_, err = s.SQL.ExecContext(s.appctx, schema)
					if err != nil {
						return nil, fmt.Errorf("SQL schema error for Extension %s: %w", ext.Name, err)
					}
				}
			}
		}
	}

	for _, th := range s.FunctionCache {
		opt := th.Options()
		if opt.OnInit != nil {
			err = opt.OnInit(s, NewRuntime(s, nil))
			if err != nil {
				return nil, fmt.Errorf("OnInit error for Function %s: %w", opt.Path, err)
			}
		}
	}

	return s, nil
}

func (s *Server) filePrefix() string {
	return strings.ToLower(s.Config.Prefix)
}

func (s *Server) envPrefix() string {
	return strings.ToUpper(s.Config.Prefix)
}

func NewTestServer(config *Config, extconfig ...map[string]any) *Server {
	s, err := NewServer(config, extconfig...)
	if err != nil {
		panic(fmt.Sprintf("NewTestServer error: %s", err))
	}
	s.RegisterAllFunction()
	s.serveInitOnly()
	return s
}

func (s *Server) updateFS(filename string, secure bool) error {
	if s.Config.ConfigFS == nil {
		return nil
	}

	file, err := s.Config.ConfigFS.Open(filepath.Join(s.Config.ConfigDir, filename))
	if err != nil {
		return nil
	}

	buf, err := io.ReadAll(file)
	if err != nil {
		return err
	}

	return s.updateBuf(buf, filename, secure)
}

func (s *Server) updateFile(filename string, secure bool) error {
	buf, err := os.ReadFile(filepath.Join(s.Config.ConfigDir, filename))
	if err != nil {
		return nil
	}
	return s.updateBuf(buf, filename, secure)
}

func (s *Server) updateBuf(buf []byte, filename string, secure bool) (err error) {
	if strings.HasSuffix(filename, ".enc") {
		key := os.Getenv(s.envPrefix() + "_SECRET")
		if key == "" {
			return fmt.Errorf("encrypted config `%s` found but "+s.envPrefix()+"_SECRET not set", filename)
		}

		keybuf, err := base64.RawURLEncoding.DecodeString(key)
		if err != nil {
			return err
		}
		if len(keybuf) != 32 {
			return fmt.Errorf("invalid key "+s.envPrefix()+"_SECRET: %s", key)
		}

		buf, err = cryptino.DecryptByGCM(keybuf, buf)
		if err != nil {
			return err
		}

	}
	return s.Update(buf, secure)
}

func (s *Server) Update(setting []byte, secure bool) error {
	decoder := yaml.NewDecoder(bytes.NewBuffer(setting), yamlDecodeOption...)
	err := decoder.Decode(s.Config)
	if err != nil {
		return err
	}

	if !secure {
		for _, configext := range s.extensions {
			err = configext.Update(setting)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Server) generateKeyIfNotExist() error {
	if !isReallyNil(s.Config.Login.PrivateKey) || !isReallyNil(s.Config.Login.PublicKey) {
		return nil
	}

	priv, err := cryptino.GenerateKey("ES256")
	if err != nil {
		return err
	}

	s.Config.Login.PrivateKey = priv
	return nil
}

func (s *Server) errorPrintln(msg string, err error) {
	if s.Config.OnError != nil {
		s.Config.OnError(msg, err)
	} else {
		fmt.Println(msg, err)
	}
}

func (s *Server) Context() context.Context {
	return s.appctx
}

func (s *Server) Quit() {
	s.appcancel()
}

func (s *Server) Serve() {
	var listener net.Listener
	var err error
	var isUnix bool

	addr := s.Config.Bind
	if strings.HasPrefix(addr, "unix:") {
		isUnix = true
		socketPath := strings.TrimPrefix(addr, "unix:")

		// 古いソケットがあったら削除（忘れると "address already in use"）
		os.Remove(socketPath)

		listener, err = net.Listen("unix", socketPath)
		if err != nil {
			s.errorPrintln("Failed to listen on unix socket: ", err)
			return
		}
	} else {
		if s.Config.Https.Enabled() {
			var cert tls.Certificate
			cert, err = tls.LoadX509KeyPair(s.Config.Https.CertFile, s.Config.Https.KeyFile)
			if err != nil {
				log.Fatalf("failed to load cert: %v", err)
			}

			listener, err = tls.Listen("tcp", ":443", &tls.Config{
				Certificates: []tls.Certificate{cert},
			})
			if err != nil {
				log.Fatalf("Failed to listen on TCP:433: %v", err)
			}
		} else {
			listener, err = net.Listen("tcp", addr)
			if err != nil {
				s.errorPrintln("Failed to listen on TCP:"+addr+": ", err)
				return
			}
		}
	}

	protocol := "http"
	if s.Config.Https.Enabled() {
		protocol = "https"
	}
	bindURL := s.Config.Bind
	if strings.HasPrefix(bindURL, ":") {
		bindURL = protocol + "://localhost" + bindURL
	} else {
		bindURL = protocol + "://" + bindURL
	}

	if isUnix {
		protocol = "unix"
		bindURL = s.Config.Bind
	}

	if !s.Config.Log.Silent && !s.Config.NoWelcome {
		s.showWelcome(bindURL)
	}

	if s.Config.Debug {
		fmt.Println(strings.TrimSpace(`
WARNING: debug=true
  DO NOT use this in production.
  - All endpoints allow cross-origin access.
  - Add '.user=USERID' to the query for a fake login.
`))
		if s.Config.Log.Silent {
			fmt.Println("         log.silent was overridden to false due to debug mode")
			s.Config.Log.Silent = false
		}
		fmt.Println("")
	}

	if !s.Config.Log.Silent {
		//fmt.Println("Starting " + strings.ToUpper(protocol) + " server: '" + bindURL + "'")
		fmt.Println("")
		s.Logger.Info("server starting",
			zap.String("protocol", protocol),
			zap.String("bind", s.Config.Bind),
			zap.String("url", bindURL),
			zap.Time("startAt", s.Config.StartAt),
		)
	}

	s.serveInitOnly()

	/*
		for _, ext := range s.extopts {
			s.Logger.Info("extension init", zap.String("name", ext.Name))
			if ext.OnHandlerInit != nil {
				opts := s.RegisteredFunctions()
				for _, opt := range opts {
					err = ext.OnHandlerInit(s, NewRequest(s, nil), opt)
					if err != nil {
						s.errorPrintln(fmt.Sprintf("OnHandlerInit error for Extension `%s` to path `%s`: ", ext.Name, opt.Path), err)
					}
				}

				for _, h := range s.internalHandlerCache {
					opt := h.Options()
					err := ext.OnHandlerInit(s, NewRequest(s, nil), opt)
					if err != nil {
						s.errorPrintln(fmt.Sprintf("OnHandlerInit error for Extension `%s` to path `%s`: ", ext.Name, opt.Path), err)
					}
				}
			}
		}

		if s.Config.OnServe != nil {
			err := s.Config.OnServe(s)
			if err != nil {
				s.errorPrintln("OnServe error: ", err)
			}
		}

		for _, ext := range s.extopts {
			if ext.OnServe != nil {
				err = ext.OnServe(s, NewRequest(s, nil))
				if err != nil {
					s.errorPrintln(fmt.Sprintf("OnServe error for Extension `%s`: ", ext.Name), err)
				}
			}
		}
	*/

	// Serve
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-quit
		s.appcancel()
	}()

	go func() {
		time.Sleep(2 * time.Second)
		if s.Config.OnListen != nil {
			err := s.Config.OnListen(s)
			if err != nil {
				s.errorPrintln("OnListen error: ", err)
			}
		}
	}()

	go func() {
		err = s.Fiber.Listener(listener)
		if err != nil && err != http.ErrServerClosed {
			s.errorPrintln("ListenAndServe error: ", err)
		}
	}()

	<-s.appctx.Done()

	if !s.Config.Log.Silent {
		//fmt.Println("Shutting down server...")
		s.Logger.Info("server shutting down")
	}

	// 二段階終了検知用のセカンドチャンネル
	forceQuit := make(chan os.Signal, 1)
	signal.Notify(forceQuit, os.Interrupt)

	go func() {
		<-forceQuit // 2回目のCtrl+Cで強制終了

		if !s.Config.Log.Silent {
			s.Logger.Info("force quitting in 3sec...")
		}
		s.forcecancel()

		time.AfterFunc(3*time.Second, func() {
			os.Exit(1)
		})
	}()

	//ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	//defer cancel()
	s.appcancel()
	if err := s.Fiber.ShutdownWithContext(s.forcectx); err != nil {
		s.errorPrintln("Shutdown error: ", err)
	}

	for _, th := range s.FunctionCache {
		opt := th.Options()
		if opt.OnShutdown != nil {
			err = opt.OnShutdown(s, NewRuntime(s, nil))
			if err != nil {
				s.errorPrintln(fmt.Sprintf("OnShutdown error for Function `%s`: ", opt.Path), err)
			}
		}
	}

	for _, ext := range s.extopts {
		if ext.OnShutdown != nil {
			err = ext.OnShutdown(s, NewRuntime(s, nil))
			if err != nil {
				s.errorPrintln(fmt.Sprintf("OnShutdown error for Extension `%s`: ", ext.Name), err)
			}
		}
	}

	if s.Config.OnShutdown != nil {
		err := s.Config.OnShutdown(s)
		if err != nil {
			s.errorPrintln("OnShutdown error: ", err)
		}
	}

	if !s.Config.Log.Silent {
		//fmt.Println("Server gracefully stopped")
		s.Logger.Info("server shutdown complete")
	}
}

func (s *Server) initializeFunctions() error {
	if !s.Config.Log.Silent {
		opts := s.RegisteredFunctions()
		for _, opt := range opts {
			s.Logger.Info("handler init", zap.String("path", opt.Path))
		}

		for _, h := range s.internalHandlerCache {
			opt := h.Options()
			s.Logger.Info("handler init", zap.String("name", opt.Name))
		}
	}

	for _, ext := range s.extopts {
		if !s.Config.Log.Silent {
			s.Logger.Info("extension init", zap.String("name", ext.Name))
		}
		if ext.OnFunctionInit != nil {
			opts := s.RegisteredFunctions()
			for _, opt := range opts {
				err := ext.OnFunctionInit(s, NewRuntime(s, nil), opt)
				if err != nil {
					return fmt.Errorf("initialize extension %s for %s: %w", ext.Name, opt.Name, err)
				}
			}

			for _, h := range s.internalHandlerCache {
				opt := h.Options()
				err := ext.OnFunctionInit(s, NewRuntime(s, nil), opt)
				if err != nil {
					return fmt.Errorf("initialize extension %s for %s: %w", ext.Name, opt.Name, err)
				}
			}
		}
	}

	return nil
}

// InitializeFunctions starts registered function backends without an HTTP listener.
// Register functions before calling it. Initialization runs once per server.
func (s *Server) InitializeFunctions() error {
	s.functionsInitOnce.Do(func() { s.functionsInitErr = s.initializeFunctions() })
	return s.functionsInitErr
}

func (s *Server) serveInitOnly() {
	if err := s.InitializeFunctions(); err != nil {
		s.errorPrintln("InitializeFunctions error: ", err)
		return
	}

	if s.Config.OnServe != nil {
		err := s.Config.OnServe(s)
		if err != nil {
			s.errorPrintln("OnServe error: ", err)
		}
	}

	for _, ext := range s.extopts {
		if ext.OnServe != nil {
			err := ext.OnServe(s, NewRuntime(s, nil))
			if err != nil {
				s.errorPrintln(fmt.Sprintf("OnServe error for Extension `%s`: ", ext.Name), err)
			}
		}
	}

}
