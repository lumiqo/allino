package allino

import "go.uber.org/zap"

func callSQLInit(s *Server, opt *Option) error {
	sqlneed := false
	switch opt.JobMode {
	case JOBMODE_ASYNC:
		opt.Job.Async = true
		sqlneed = true
	case JOBMODE_CACHE:
		opt.Job.Cache = true
		sqlneed = true
	case JOBMODE_DEDUPE:
		opt.Job.Dedupe = true
		sqlneed = true
	case JOBMODE_ONCE:
		opt.Job.Dedupe = true
		opt.Job.Cache = true
		opt.Job.CacheErrOnHit = true
		sqlneed = true
	case JOBMODE_MEMOIZED:
		opt.Job.Dedupe = true
		opt.Job.Cache = true
		sqlneed = true
	case JOBMODE_DISPATCH:
		opt.Job.Async = true
		opt.Job.Cache = true
		sqlneed = true
	}

	if sqlneed {
		//hbackend := mergeSingle(s.Config.JobConfig.Backend, opt.Job.Backend)
		if s.jobManager == nil {
			s.jobManager = newJobManager()
		}

		if s.callSQLStrategy == nil {
			s.callSQLStrategy = newcallSQLStrategy(s)
			err := s.callSQLStrategy.Init(s.appctx, (s.Config.SQL.AllowMigrate != nil && *s.Config.SQL.AllowMigrate))
			if err != nil {
				s.callSQLStrategy = nil
				return err
			}
			s.jobManager.WorkerInit(s.callSQLStrategy, s)

			// Reaping
			s.TimeWheel.Add(s.Config.JobConfig.LeaseDuration, func() bool {
				err := s.callSQLStrategy.Reaping(s.appctx)
				if err != nil && !s.Config.Log.Silent {
					s.Logger.Error("job system error", zap.String("component", "reaping"), zap.Error(err))
				}
				return true
			})
		}

		s.jobManager.registerHandler(encodeHandlerName(opt), opt)
	}

	return nil
}
