package allino

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"time"

	"github.com/wh-kuromai/allino/internal/ema"
	"github.com/wh-kuromai/allino/internal/timewheel"
	"go.uber.org/zap"
)

const (
	JOBMODE_NORMAL   = ""
	JOBMODE_ASYNC    = "async"
	JOBMODE_CACHE    = "cache"
	JOBMODE_DEDUPE   = "dedupe"
	JOBMODE_ONCE     = "once"
	JOBMODE_MEMOIZED = "memoized"
	JOBMODE_DISPATCH = "dispatch"

	JOBMODE_FANOUT    = "fanout"
	JOBMODE_REPLAY    = "replay"
	JOBMODE_REPLAYALL = "replayall"

	//JOB_BACKEND_SQL = "sql"
	//JOB_BACKEND_REDIS = "backend_redis"
)

type JobConfig struct {
	//Backend         string        `json:"backend"`
	MaxRetry        int           `json:"max_retry"`
	IdleInterval    time.Duration `json:"idle_interval"`
	Concurrency     int           `json:"concurrency"`
	LeaseDuration   time.Duration `json:"lease_duration"`
	RequeueInterval time.Duration `json:"requeue_interval"`
	WaitTimeout     time.Duration `json:"wait_timeout"`
	WaitInterval    time.Duration `json:"wait_interval"`

	RedisKeyPrefix            string `json:"redis_key_prefix"`
	RedisStreamGroupPrefix    string `json:"redis_stream_group_prefix"`
	RedisStreamConsumerPrefix string `json:"redis_stream_consumer_prefix"`
}

type JobOption struct {
	Async         bool
	Cache         bool
	CacheErrOnHit bool // used by JOBMODE_ONCE
	Dedupe        bool
	Priority      int
	CacheExpire   time.Duration
	Interval      time.Duration

	OnInputUpgrade  func(version string, old_input_at time.Time, old_input []byte) (bool, any)                     `json:"-"`
	OnOutputUpgrade func(version string, old_output_at time.Time, old_output, old_error []byte) (bool, any, error) `json:"-"`

	StreamTTL    time.Duration
	OnStreamInit func() (start string, err error) `json:"-"`
	//callSQLStrategy   *callSQLStrategy
	//callRedisStrategy *callRedisStrategy
}

var JobExtension = NewExtension[any, any](
	"job",
	&ExtOption{
		OnFunctionInit: func(s *Server, virtual *Runtime, opt *Option) (err error) {
			err = callSQLInit(s, opt)
			if err != nil && !s.Config.Log.Silent {
				s.Logger.Warn(
					"job OnHandlerInit failed",
					zap.Error(err),
				)
			}
			if err != nil {
				return err
			}
			err = callRedisInit(s, opt)
			if err != nil && !s.Config.Log.Silent {
				s.Logger.Warn(
					"redis stream OnHandlerInit failed",
					zap.Error(err),
				)
			}
			s.handlerOptMap[encodeHandlerName(opt)] = opt
			return nil
		},

		OnServe: func(s *Server, virtual *Runtime) (err error) {
			err = callRedisInitEnd(s)
			if err != nil && !s.Config.Log.Silent {
				s.Logger.Warn(
					"redis stream OnServe failed",
					zap.Error(err),
				)
			}
			return
		},

		OnShutdown: func(s *Server, virtual *Runtime) error {
			return nil
		},
	},
)

var ErrHandlerNotFound = NewError("handler not found")

func find_handler(sv *Server, handler string) (string, error) {
	_, ok := sv.handlerOptMap[handler]
	if ok {
		return handler, nil
	}

	for k, _ := range sv.handlerOptMap {
		idx := strings.Index(k, "@")
		if idx > 0 && k[:idx] == handler {
			return k, nil
		}
	}

	return "", ErrHandlerNotFound
}

// called from CLI
func call_direct(sv *Server, r *Runtime, handler string, injson []byte, infunc func(input any) error) (key string, outjson []byte, err []byte, syserr error) {

	opt := sv.handlerOptMap[handler]

	var jec = jobExecutionContext{
		r:               r,
		opt:             opt,
		inJSON:          injson,
		inJSONMarshaled: true,
		fromcall:        true,
	}

	r.cache.req_type = REQUEST_CLI
	r.cache.parentjobid = jec.JobID()
	r.cache.rootjobid = jec.JobID()

	return opt.invoker(r, encodeHandlerName(opt), handlerVersion(opt), injson, true, infunc)
}

func unmarshalfnMake[U any, E error](r *Runtime, opt *Option, upool *ReflectPool[U], epool *ReflectPool[E], handler string) func(ji JobInfo, outjson []byte, errjson []byte, serr error) (output U, err error, syserr error) {
	return func(ji JobInfo, outjson []byte, errjson []byte, serr error) (output U, err error, syserr error) {

		var zeroU U
		now := time.Now()

		// cache expired or version mismatch
		if hasMajorOrMinorVersionDiff(ji.Meta.Version, handlerVersion(opt)) ||
			(ji.Meta.TTL != nil && now.After(*ji.Meta.TTL)) {

			// try cache transform
			if opt.Job.OnOutputUpgrade != nil && outjson != nil && errjson == nil {

				ok, newout, newerr := opt.Job.OnOutputUpgrade(ji.Meta.Version, ji.UpdatedAt, outjson, errjson)
				if ok {
					newoutput, ok := newout.(U)
					if ok {
						ji.Meta.Version = handlerVersion(opt)
						ji.UpdatedAt = now
						newoutjson, newerrjson, newsyserr := marshalOutputSet[U, E](newoutput, newerr)
						if newsyserr != nil {
							if !r.config.Log.Silent {
								r.logger.Error("cache transform failed: marshal error", zap.String("handler", handler), zap.Error(newsyserr))
							}
							return zeroU, FatalTransformFailed, nil
						}
						// update cache and return new output
						syserr = r.server.callSQLStrategy.Done(r.Context(), encodeHandlerName(opt), &ji.Meta, ji.JobID, nil, newoutjson, newerrjson)
						if syserr != nil {
							if !r.config.Log.Silent {
								r.logger.Error("cache transform failed: done error", zap.String("handler", handler), zap.Error(newsyserr))
							}
							return zeroU, FatalTransformFailed, nil
						}
						return newoutput, newerr, nil
					}
					if !r.config.Log.Silent {
						r.logger.Error("cache transform failed: output type not match", zap.String("handler", handler))
					}
					return zeroU, FatalTransformFailed, nil

				}

				// return syserr -> cache hit failed -> fallback to execute.
				return zeroU, nil, ErrJobNotFound
			}

			// fallback if no transformer set.
			return zeroU, nil, FatalInvalidCacheError
		}

		_, output, err, syserr = unmarshalOutputSet[U, E](ji, upool, epool, outjson, errjson, serr)
		if syserr != nil {
			return zeroU, nil, syserr
		}

		if opt.Job.CacheErrOnHit {
			return zeroU, ErrJobDuplicated, nil
		}

		if !r.config.Log.Silent {
			r.logger.Debug("job cache hit", zap.String("handler", handler))
		}
		return output, err, nil
	}
}

// called from HTTP or Handler.Call
func (rw *GenericFunction[T, U, E]) call_job(r *Runtime, input T, fromcall bool) (output U, err error) {

	var zeroU U
	opt := rw.options
	asyncExec := fromcall && rw.options.Job.Async
	dedupeExec := rw.options.Job.Dedupe
	cacheExec := rw.options.Job.Cache

	c := r.server.callSQLStrategy

	var jec = jobExecutionContext{
		r:        r,
		opt:      rw.options,
		input:    input,
		fromcall: fromcall,
	}

	if err := jec.MarshalCheck(); err != nil {
		return zeroU, ErrJobInputEncodeFailed.With(err)
	}

	r.cache.req_type = REQUEST_CLI
	r.cache.parentjobid = jec.JobID()
	r.cache.rootjobid = jec.JobID()

	var syserr error

	unmarshalfn := unmarshalfnMake[U, E](r, opt, rw.upool, rw.epool, jec.Handler())
	if cacheExec {
		output, err, syserr = unmarshalfn(c.Hit(r.Context(), jec.Handler(), opt.Job.CacheExpire != 0, jec.JobID(), jec.InputJSON()))

		if syserr == nil {
			return output, err
		}
	}

	var aquiredLock bool
	if asyncExec {
		var enqueued bool
		enqueued, err = c.Enqueue(
			r.Context(),
			jec.Handler(),
			jec.JobMeta(statusQueued),
			jec.JobID(),
			jec.InputJSON(),
			0)
		if err != nil {
			if !r.config.Log.Silent {
				r.logger.Error("job system error", zap.String("component", "requeue.enqueue"), zap.Error(err))
			}
			// db error etc.
			return zeroU, FatalBackendError.With(err)
		}

		if enqueued {
			if !r.config.Log.Silent {
				r.logger.Debug("job queued", zap.String("handler", jec.Handler()))
			}
			return zeroU, NewJobPendingError(jec.JobID(), "job accepted")
		}
		return zeroU, NewJobPendingError(jec.JobID(), "job not finished yet")

	} else if dedupeExec {
		aquiredLock, err = c.Enqueue(
			r.Context(),
			jec.Handler(),
			jec.JobMeta(statusLeased),
			jec.JobID(),
			jec.InputJSON(),
			0)
		if err != nil {
			if !r.config.Log.Silent {
				r.logger.Error("job system error", zap.String("component", "requeue.enqueue"), zap.Error(err))
			}
			// db error etc.
			return zeroU, FatalBackendError.With(err)
		}

		if !aquiredLock {
			if cacheExec {
				output, err, syserr = unmarshalfn(c.Hit(r.Context(), jec.Handler(), opt.Job.CacheExpire != 0, jec.JobID(), jec.InputJSON()))
				if syserr == nil {
					return output, err
				}

				// if job is not finished, wait it
				jnferr, ok := syserr.(*JobPendingError)
				if ok {
					output, err, syserr = unmarshalfn(c.Wait(r.Context(), jnferr.JobID, rw.options.Job.CacheExpire != 0, r.server.TimeWheel))
					if syserr == nil {
						return output, err
					}
				}

				if !r.config.Log.Silent {
					r.logger.Error("job lost", zap.String("handler", jec.Handler()), zap.Error(syserr))
				}
				return zeroU, FatalBackendError.With(syserr)
				// cache not found or expired
			}
			// if enqueued failed & dedupe, exec declined.
			if !r.config.Log.Silent {
				r.logger.Info("job duplicated", zap.String("handler", jec.Handler()))
			}
			return zeroU, ErrJobDuplicated
		}

		// Lock aquired
		leaset := r.config.JobConfig.LeaseDuration
		task := r.server.TimeWheel.Add(time.Duration(leaset/2), func() bool {
			err := c.LeaseUpdate(r.Context(), jec.JobID(), leaset)
			//err := s.LeaseUpdate(sv.appctx, jobn.key, leaset)
			if err != nil && !r.Config().Log.Silent {
				r.logger.Error("job system error", zap.String("component", "leaseupdate"), zap.Error(err))
			}
			return true
		})
		defer task.Cancel()
	}

	output, err = rw.handlefunc(r, input)
	if cacheExec {
		// only cache job is not aborted
		if r.memo.jobabortctrl == "" {
			outJSON, errJSON, syserr := marshalOutputSet[U, E](output, err)
			if syserr == nil {

				err := c.Done(
					r.Context(),
					jec.Handler(),
					jec.JobMeta(statusDone),
					jec.JobID(),
					jec.InputJSON(),
					outJSON,
					errJSON)
				if err != nil && !r.config.Log.Silent {
					r.logger.Error("job system error", zap.String("component", "done"), zap.Error(err))
				}
			}
		}
	} else if dedupeExec {
		err := c.Free(r.Context(), jec.JobID())
		if err != nil && !r.config.Log.Silent {
			r.logger.Error("job system error", zap.String("component", "free"), zap.Error(err))
		}
	}

	// mark requeue
	if r.memo.jobabortctrl == JOB_ABORT_REQUEUE || r.memo.jobabortctrl == JOB_ABORT_REQUEUE_AT {
		_, err = c.Enqueue(
			r.Context(),
			jec.Handler(),
			jec.JobMeta(jec.EnqueueStatus()),
			jec.JobID(),
			jec.InputJSON(),
			requeueDelay(r))
		if err != nil {
			if !r.config.Log.Silent {
				r.logger.Error("job system error", zap.String("component", "markrequeue"), zap.Error(err))
			}
		} else {
			if !r.config.Log.Silent {
				r.logger.Debug("job requeued", zap.String("handler", jec.Handler()), zap.String("requestid", jec.JobID()))
			}
		}

	}

	return output, err
}

// called from CLI or Worker
func (rw *GenericFunction[T, U, E]) invokeFunctionJSON(r *Runtime, handler, version string, injson []byte, direct bool, infunc func(input any) error) (key string, outputz []byte, errjsonz []byte, syserr error) {

	var input T
	var innererr error
	updated := false
	if hasMajorOrMinorVersionDiff(version, handlerVersion(rw.options)) {
		var updatein any
		updated, updatein = rw.options.Job.OnInputUpgrade(version, time.Now(), injson)
		if updated {
			input2, ok2 := updatein.(T)
			if ok2 {
				input = input2
			}
		}
	}

	if !updated {
		input, innererr = rw.tpool.New(func(a any) error {
			return json.Unmarshal(injson, a)
		})
	}

	//var innererr error
	//input := NewRefOf[T](func(a any) {
	//	innererr = json.Unmarshal(injson, a)
	//})
	if innererr != nil {
		return key, nil, nil, ErrJobInputDecodeFailed.With(innererr)
	}

	key = encodeJobID(handler, input, injson, "")
	if r.cache.requestid == "" {
		r.cache.requestid = key
	}

	// fill SelfDiscovery.
	if rw.options.hasSelfDiscovery {
		ferr := fillSelfDiscovery(input)
		if ferr != nil {
			return key, nil, nil, ErrJobInputDecodeFailed.With(ferr)
		}
	}

	if direct {
		var t *T
		if reflect.TypeOf(t).Elem().Kind() == reflect.Ptr {
			err := r.getAll(input, rw.options.inputReflectPlan)
			if err != nil {
				return "", nil, nil, err
			}
		} else {
			err := r.getAll(&input, rw.options.inputReflectPlan)
			if err != nil {
				return "", nil, nil, err
			}
		}
	}

	if infunc != nil {
		err2 := infunc(input)
		if err2 != nil {
			return "", nil, nil, err2
		}
	}

	outJSON, errJSON, syserr := marshalOutputSet[U, E](rw.handlefunc(r, input))
	return key, outJSON, errJSON, syserr
}

func (rw *GenericFunction[T, U, E]) JobResult(r *Runtime, jobid string) (output U, err error) {
	var zeroU U
	var syserr error

	jid, err := decodeJobID(jobid)
	if err != nil {
		return zeroU, err
	}

	if jid.Handler != encodeHandlerName(rw.options) {
		return zeroU, ErrJobHandlerMismatch
	}

	unmarshalfn := unmarshalfnMake[U, E](r, rw.options, rw.upool, rw.epool, jid.Handler)
	output, err, syserr = unmarshalfn(r.server.callSQLStrategy.Result(r.Context(), jobid, rw.options.Job.CacheExpire != 0))

	if syserr == nil {
		return output, err
	}

	if !r.config.Log.Silent {
		r.logger.Error("job system error", zap.String("component", "jobresult"), zap.Error(syserr))
	}
	return zeroU, syserr
}

type callStrategy interface {
	Init(ctx context.Context, allow_migrate bool) error

	// Push job to queue / Aquire Lock
	Enqueue(ctx context.Context, handler string, meta *JobMeta, key string, injson []byte, delay_sec int) (enqueud bool, err error)

	// Pull queued job.
	Dequeue(ctx context.Context, handlers []string, lease_dur time.Duration, ema *ema.EMACalculator) (jt JobTask, err error)

	// Search lease expired job and requeue.
	Reaping(ctx context.Context) (err error)

	// Free Lock
	Free(ctx context.Context, key string) (err error)

	//
	LeaseUpdate(ctx context.Context, key string, lease_dur time.Duration) (err error)

	// Func is called synchronously and Hit checks if its cached already.
	Hit(ctx context.Context, handler string, volatile bool, key string, injson []byte) (meta JobInfo, outjson []byte, errjson []byte, err error)

	// Function is called synchronously and then cache its result.
	Done(ctx context.Context, handler string, meta *JobMeta, key string, injson []byte, outjson []byte, errjson []byte) (err error)

	// Find completed job. (blocking)
	Wait(ctx context.Context, key string, volatile bool, tw *timewheel.TimeWheel) (meta JobInfo, outjson []byte, errjson []byte, err error)

	// Find completed job. (non-blocking)
	Result(ctx context.Context, key string, volatile bool) (meta JobInfo, outjson []byte, errjson []byte, err error)

	// List jobs
	List(ctx context.Context, statuses []int, offset, limit int) ([]JobInfo, error)

	// Count jobs
	Total(ctx context.Context, jobid ...string) (map[string]int, error)
}

type JobTask interface {
	Key() string
	Handler() string
	Meta() *JobMeta
	Input() []byte

	Success(ctx context.Context, handler string, meta *JobMeta, key string, injson []byte, outjson []byte, errjson []byte) (err error)
	Fail(ctx context.Context) (err error)
	HeartBeat(ctx context.Context, lease_dur time.Duration) (err error)
	Requeue(ctx context.Context, delay_sec int) error
}
