package allino

import (
	"encoding/json"
	"fmt"
	"time"
)

type jobExecutionContext struct {
	r        *Runtime
	opt      *Option
	input    any
	fromcall bool

	handler         string
	key             string
	inJSON          []byte
	inJSONMarshaled bool
}

func (c *jobExecutionContext) JobMeta(status int) *JobMeta {
	meta := JobMeta{
		refresh:   c.r.memo.jobRefresh,
		resources: c.opt.Job.Resources,
		Version:   handlerVersion(c.opt),
		Status:    status,
		ParentID:  c.JobID(),
		RootID:    c.r.cache.rootjobid,
		Priority:  c.opt.Job.Priority,
	}

	if resourceLimitsEnabled(c.opt.Job) {
		meta.ResourceKey, _ = getResourceKey(c.input)
		meta.resourceMinInterval = c.opt.Job.MinInterval
		meta.resourceMaxConcurrent = c.opt.Job.MaxConcurrentPerResource
	}
	if c.opt.Job.CacheExpire != 0 {
		ttl := time.Now().Add(c.opt.Job.CacheExpire)
		meta.TTL = &ttl
	}
	return &meta
}

func (c *jobExecutionContext) MarshalCheck() error {
	if c.inJSONMarshaled {
		return nil
	}

	if resourceLimitsEnabled(c.opt.Job) {
		if _, ok := c.input.(ResourceRequest); !ok {
			return fmt.Errorf("job input must implement ResourceRequest.ResourceKey() string")
		}
	}
	inJSON, err := json.Marshal(c.input)
	c.inJSON = inJSON
	c.inJSONMarshaled = true
	return err
}

func (c *jobExecutionContext) InputJSON() []byte {
	//if c.inJSONMarshaled {
	//	return c.inJSON
	//}
	//
	//inJSON, _ := json.Marshal(c.input)
	//c.inJSON = inJSON
	//c.inJSONMarshaled = true
	//return inJSON

	if !c.inJSONMarshaled {
		panic("jobExecutionContext: InputJSON called before MarshalCheck")
	}
	return c.inJSON
}

func (c *jobExecutionContext) Handler() string {
	if c.handler != "" {
		return c.handler
	}

	c.handler = encodeHandlerName(c.opt)
	return c.handler
}

func (c *jobExecutionContext) JobID() string {
	if c.key != "" {
		return c.key
	}

	marker := ""
	if !c.opt.Job.Dedupe && !c.opt.Job.Cache {
		marker = c.r.RequestID()
	}

	c.key = encodeJobID(c.Handler(), c.input, c.InputJSON(), marker)
	return c.key
}

func (c *jobExecutionContext) EnqueueStatus() int {
	status := statusLeased
	if c.fromcall && c.opt.Job.Async {
		status = statusQueued
	}
	return status
}
