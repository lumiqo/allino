package allino

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wh-kuromai/allino/internal/ema"
	"go.uber.org/zap"
)

type jobProgress struct{ Current, Total int }

type jobManager struct {
	handlers               *jobset
	handlerOptions         sync.Map
	lockedHandlers         *jobset
	resourcelockedHandlers *jobset
	dequeueThroughputEMA   *ema.EMACalculator
	// Serialize dequeue metadata and per-handler interval state across worker slots.
	dequeueMu  sync.Mutex
	activeJobs int64
	attempt    int64
	waiting    atomic.Bool
	doneCh     chan struct{}
	inited     sync.Once
	queueCh    chan func()
	ctx        context.Context
}

func newJobManager() *jobManager {
	return &jobManager{handlers: newJobset(), lockedHandlers: newJobset(), resourcelockedHandlers: newJobset(), dequeueThroughputEMA: ema.NewEMACalculator(0.3), doneCh: make(chan struct{}), queueCh: make(chan func())}
}

// Register the options before making a handler available to SQL workers.
func (jobm *jobManager) registerHandler(name string, opt *Option) {
	jobm.handlerOptions.Store(name, opt)
	jobm.handlers.Add(name)
}

func (jobm *jobManager) Init(sv *Server) {
	jobm.inited.Do(func() {
		jobm.ctx = sv.appctx
		concurrency := sv.Config.JobConfig.Concurrency
		if concurrency < 1 {
			concurrency = 1
		}
		for i := 0; i < concurrency; i++ {
			go func() {
				for {
					select {
					case <-sv.appctx.Done():
						return
					case fn := <-jobm.queueCh:
						fn()
					}
				}
			}()
		}
	})
}

// Do waits for an execution slot, without leaving a sender blocked on shutdown.
func (jobm *jobManager) Do(fn func()) bool {
	select {
	case <-jobm.ctx.Done():
		return false
	default:
	}
	select {
	case <-jobm.ctx.Done():
		return false
	case jobm.queueCh <- fn:
		return true
	}
}
func waitJobContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
func (jobm *jobManager) WorkerInit(s callStrategy, sv *Server) {
	jobm.Init(sv)
	backoff := NewBackoff(100*time.Millisecond, sv.Config.JobConfig.IdleInterval)
	go func() {
		// Dequeue runs inside the shared worker pool: a queued job is never leased
		// while waiting behind another SQL or Redis task for an execution slot.
		for jobm.Do(func() { jobm.pollAndRun(s, sv, backoff) }) {
		}
	}()
}
func (jobm *jobManager) pollAndRun(s callStrategy, sv *Server, backoff *Backoff) {
	if sv.appctx.Err() != nil {
		return
	}
	jobm.dequeueMu.Lock()
	hs := jobm.handlers.Diff(jobm.lockedHandlers, jobm.resourcelockedHandlers)
	jtask, err := s.Dequeue(sv.appctx, hs, sv.Config.JobConfig.LeaseDuration, jobm.dequeueThroughputEMA)
	if err != nil {
		attempts := atomic.AddInt64(&jobm.attempt, 1)
		jobm.dequeueMu.Unlock()
		if !errors.Is(err, ErrJobNotFound) && sv.appctx.Err() == nil {
			sv.Logger.Error("job system error", zap.String("component", "dequeue"), zap.Error(err))
		}
		if jobm.waiting.Load() {
			jobm.tryFinish()
		}
		waitJobContext(sv.appctx, backoff.Duration(int(attempts)))
		return
	}
	jtask.Meta().ParentID = jtask.Key()
	if jtask.Meta().RootID == "" {
		jtask.Meta().RootID = jtask.Key()
	}
	value, _ := jobm.handlerOptions.Load(jtask.Handler())
	opt := value.(*Option)
	var delay time.Duration
	if opt.Job.Interval != 0 {
		jobm.lockedHandlers.Add(jtask.Handler())
		if opt.lastRun != nil {
			delay = time.Until(opt.lastRun.Add(time.Duration(float64(opt.Job.Interval) * jobm.dequeueThroughputEMA.CurrentAverage)))
		}
	}
	atomic.StoreInt64(&jobm.attempt, 0)
	atomic.AddInt64(&jobm.activeJobs, 1)
	jobm.dequeueMu.Unlock()
	jobm.runTask(sv, opt, jtask, delay)
}
func (jobm *jobManager) runTask(sv *Server, opt *Option, jtask JobTask, delay time.Duration) {
	scheduledAt := time.Now()
	ctx, cancel := context.WithCancel(sv.appctx)
	var leaseLost atomic.Bool
	heartbeatDone := make(chan struct{})
	heartbeatInterval := sv.Config.JobConfig.LeaseDuration / 2
	if heartbeatInterval <= 0 {
		heartbeatInterval = time.Millisecond
	}
	go func() {
		defer close(heartbeatDone)
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := jtask.HeartBeat(ctx, sv.Config.JobConfig.LeaseDuration); err != nil {
					if !sv.Config.Log.Silent {
						sv.Logger.Error("job system error", zap.String("component", "leaseupdate"), zap.Error(err))
					}
					if errors.Is(err, ErrJobLeaseLost) {
						leaseLost.Store(true)
						cancel()
						return
					}
				}
			}
		}
	}()
	var started time.Time
	defer func() {
		cancel()
		<-heartbeatDone
		jobm.dequeueMu.Lock()
		if opt.Job.Interval != 0 {
			if !started.IsZero() {
				opt.lastRun = &started
			}
			jobm.lockedHandlers.Remove(jtask.Handler())
		}
		atomic.AddInt64(&jobm.activeJobs, -1)
		jobm.dequeueMu.Unlock()
		if jobm.waiting.Load() {
			jobm.tryFinish()
		}
	}()
	// Keep the execution slot and heartbeat while respecting the handler interval.
	// A delayed callback must not later run outside the configured worker limit.
	if delay > 0 && !waitJobContext(ctx, delay) {
		return
	}
	if ctx.Err() != nil {
		return
	}
	// Preserve interval accounting from when this slot accepted the task.
	started = scheduledAt
	r := NewRuntime(sv, nil)
	r.ctx = ctx
	defer r.do_defer()
	r.cache.requestid = jtask.Key()
	r.cache.req_type = REQUEST_JOB
	r.cache.parentjobid = jtask.Meta().ParentID
	r.cache.rootjobid = jtask.Meta().RootID
	if !r.config.Log.Silent {
		r.logger.Info("job started", zap.String("handler", jtask.Handler()), zap.String("requestid", jtask.Key()))
	}
	_, outjson, errjson, syserr := opt.invoker(r, jtask.Handler(), jtask.Meta().Version, jtask.Input(), false, nil)
	if leaseLost.Load() || ctx.Err() != nil {
		return
	}
	var err error
	event := "job completed"
	switch {
	case syserr != nil:
		event = "job failed"
		if !r.config.Log.Silent {
			r.logger.Error("job failed", zap.String("handler", jtask.Handler()), zap.Error(syserr))
		}
		err = jtask.Fail(ctx)
	case r.memo.jobabortctrl == JOB_ABORT_REQUEUE || r.memo.jobabortctrl == JOB_ABORT_REQUEUE_AT:
		event = "job requeued"
		err = jtask.Requeue(ctx, requeueDelay(r))
	case r.memo.jobabortctrl == JOB_ABORT:
		event = "job aborted"
		err = jtask.Fail(ctx)
	case opt.Job.Cache:
		event = "job completed/cached"
		if opt.Job.CacheExpire != 0 {
			ttl := time.Now().Add(opt.Job.CacheExpire)
			jtask.Meta().TTL = &ttl
		}
		err = jtask.Success(ctx, jtask.Handler(), jtask.Meta(), jtask.Key(), jtask.Input(), outjson, errjson)
	default:
		err = jtask.Fail(ctx)
	}
	if !r.config.Log.Silent {
		if err != nil {
			r.logger.Error("job system error", zap.String("component", "completion"), zap.Error(err))
		} else {
			r.logger.Info(event, zap.String("handler", jtask.Handler()), zap.String("requestid", jtask.Key()))
		}
	}
}

func (jobm *jobManager) tryFinish() {
	if atomic.LoadInt64(&jobm.activeJobs) == 0 && atomic.LoadInt64(&jobm.attempt) >= 5 {
		select {
		case jobm.doneCh <- struct{}{}:
		default:
		}
	}
}
func (jobm *jobManager) WaitForJob(ctx context.Context, c callStrategy, jobid string, progressf func(doneCount, errCount, total int)) error {
	jobm.waiting.Store(true)
	defer jobm.waiting.Store(false)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	tickerfn := func() {
		counts, err := c.Total(ctx, "*")
		if err == nil && progressf != nil {
			total := 0
			for _, v := range counts {
				total += v
			}
			progressf(counts["done"], counts["error"], total)
		}
	}
	for {
		select {
		case <-jobm.doneCh:
			tickerfn()
			return nil
		case <-ctx.Done():
			tickerfn()
			return ctx.Err()
		case <-ticker.C:
			tickerfn()
		}
	}
}
