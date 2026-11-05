package allino

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type IdempotentRequest interface {
	IdempotencyKey() string
}

const (
	JOB_ABORT_REQUEUE    = "requeue"
	JOB_ABORT_REQUEUE_AT = "requeue_at"
	JOB_ABORT            = "abort"
)

func (r *Runtime) MarkRequeue() {
	r.memo.jobabortctrl = JOB_ABORT_REQUEUE
}

func (r *Runtime) MarkRequeueAt(waitsec int) {
	r.memo.jobabortctrl = JOB_ABORT_REQUEUE_AT
	r.memo.jobrequeuewait = waitsec
}

func (r *Runtime) MarkAbort() {
	r.memo.jobabortctrl = JOB_ABORT
}

type JobInfo struct {
	JobID       string     `json:"jobid"`
	Handler     string     `json:"handler"`
	Meta        JobMeta    `json:"meta"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
	LeasedUntil *time.Time `json:"leased_until,omitempty"`
	RetryCount  *int       `json:"retry_count,omitempty"`
}

type JobMeta struct {
	refresh               bool
	resources             []*JobResource
	ResourceKey           string `json:"resource_key,omitempty"`
	resourceMinInterval   time.Duration
	resourceMaxConcurrent int
	Version               string
	Status                int `json:"status"` // queued / leased / done / error
	ParentID              string
	RootID                string
	TTL                   *time.Time
	Priority              int
}

type jobid struct {
	Version string
	Backend string
	Handler string
	ID      string
}

const (
	jobIDPrefix  = "job"
	jobIDVersion = "v1"
	jobIDSep     = ":"
)

func requeueDelay(r *Runtime) int {
	switch r.memo.jobabortctrl {
	case JOB_ABORT_REQUEUE:
		return int(r.Config().JobConfig.RequeueInterval.Seconds())
	case JOB_ABORT_REQUEUE_AT:
		return r.memo.jobrequeuewait
	}
	return int(r.Config().JobConfig.RequeueInterval.Seconds())
}

func handlerVersion(opt *Option) string {
	v := opt.Version
	if opt.Version == "" {
		v = "0.0.1"
	}
	return v
}

func getIdempotentKey(input any) string {
	ir, ok := input.(IdempotentRequest)
	if ok {
		return ir.IdempotencyKey()
	}
	return ""
}

func encodeHandlerName(opt *Option) string {
	return opt.Name //+ "@" + handlerVersion(opt)
}

func encodeJobKey(handler string, injson []byte) string {
	h := sha256.New()
	h.Write([]byte(handler))
	h.Write(injson)
	return hex.EncodeToString(h.Sum(nil))
}

func encodeJobID(handler string, input any, injson []byte, marker string) string {
	if marker != "" {
		marker = "." + marker
	}

	jobid := getIdempotentKey(input)
	if jobid == "" {
		jobid = encodeJobKey(handler, injson)
	}

	return strings.Join([]string{
		jobIDPrefix,
		jobIDVersion,
		handler,
		jobid + marker,
	}, jobIDSep)
}

func decodeJobID(s string) (*jobid, error) {
	parts := strings.Split(s, jobIDSep)
	if len(parts) != 4 {
		return nil, errors.New("invalid jobid format")
	}

	if parts[0] != jobIDPrefix {
		return nil, errors.New("invalid jobid prefix")
	}
	if !strings.HasPrefix(parts[1], jobIDVersion) {
		return nil, errors.New("unsupported jobid version")
	}

	return &jobid{
		Version: parts[1],
		Handler: parts[2],
		ID:      parts[3],
	}, nil
}

var ErrDequeueFailed = NewError("job dequeue failed")

var FatalInvalidCacheError = NewError("fatal: invalid cache error")
var FatalBackendError = NewError("fatal: backend error")
var FatalTransformFailed = NewError("fatal: transform failed.")

var ErrJobDuplicated = NewError("job already executed")

var ErrJobNotFound = NewError("job not found")
var ErrJobNotFinished = NewError("job not finished")
var ErrJobExpired = NewError("job has beed expired")

var ErrJobHandlerMismatch = NewError("job does not belong to this handler")
var ErrJobResultEncodeFailed = NewError("failed to encode job result")
var ErrJobResultDecodeFailed = NewError("failed to decode job result")
var ErrJobErrorEncodeFailed = NewError("failed to encode job error")
var ErrJobErrorDecodeFailed = NewError("failed to decode job error")
var ErrJobInputEncodeFailed = NewError("failed to encode job input")
var ErrJobInputDecodeFailed = NewError("failed to decode job input")

var ErrStreamInputDecodeFailed = NewError("failed to decode stream input")

type JobPendingError struct {
	Status int    `json:"-"`
	JobID  string `json:"jobid,omitempty"`
	Msg    string `json:"msg,omitempty"`
}

func (e JobPendingError) StatusCode() int {
	return e.Status
}

func (e JobPendingError) Error() string {
	return e.Msg
}

func NewJobPendingError(key, msg string) *JobPendingError {
	return &JobPendingError{
		Status: 202,
		JobID:  key,
		Msg:    msg,
	}
}

func unmarshalOutputSet[U any, E error](version JobInfo, upool *ReflectPool[U], epool *ReflectPool[E], outJSON []byte, errJSON []byte, err error) (ver JobInfo, output U, erra error, errb error) {
	var zeroU U
	if err != nil {
		return version, zeroU, nil, err
	}

	//var innererr error
	// output
	if outJSON != nil {

		output, innererr := upool.New(func(a any) error {
			return json.Unmarshal(outJSON, a)
		})
		//output = NewRefOf[U](func(a any) {
		//	innererr = json.Unmarshal(outJSON, a)
		//})
		if innererr != nil {
			return version, zeroU, nil, ErrJobResultDecodeFailed.With(innererr)
		}

		return version, output, nil, nil
	}

	// error
	if errJSON != nil {
		// Interface errors cannot be directly instantiated by encoding/json.
		if epool.ttype == reflect.TypeFor[error]() {
			var decoded Error
			if e := json.Unmarshal(errJSON, &decoded); e != nil {
				return version, zeroU, nil, ErrJobErrorDecodeFailed.With(e)
			}
			if decoded.Msg == "" {
				decoded.Msg = string(errJSON)
			}
			return version, zeroU, &decoded, nil
		}

		err, innererr := epool.New(func(a any) error {
			return json.Unmarshal(errJSON, a)
		})
		//err = NewRefOf[E](func(a any) {
		//	innererr = json.Unmarshal(errJSON, a)
		//})
		if innererr != nil {
			return version, zeroU, nil, ErrJobErrorDecodeFailed.With(innererr)
		}

		return version, zeroU, err, nil
	}

	return version, zeroU, nil, ErrJobErrorDecodeFailed
}

func marshalOutputSet[U any, E error](output U, err error) (outJSON []byte, errJSON []byte, syserr error) {
	var errb error
	if !isReallyNil(err) {
		errJSON, errb = json.Marshal(normalizeError(err))
		if errb != nil {
			return nil, nil, ErrJobResultEncodeFailed.With(errb)
		}
		return nil, errJSON, nil
	}

	outJSON, errb = json.Marshal(any(output))
	if errb != nil {
		return nil, nil, ErrJobErrorEncodeFailed.With(errb)
	}

	return outJSON, nil, nil
}

func hasMajorOrMinorVersionDiff(a, b string) bool {
	ma, mi, err := parseMajorMinor(a)
	if err != nil {
		return false
	}
	mb, mj, err := parseMajorMinor(b)
	if err != nil {
		return false
	}

	return ma != mb || mi != mj
}

func parseMajorMinor(v string) (major int, minor int, err error) {
	parts := strings.Split(v, ".")
	if len(parts) < 2 {
		return 0, 0, fmt.Errorf("invalid semver: %s", v)
	}

	major, err = strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, err
	}
	minor, err = strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, err
	}

	return
}

func normalizeError(err error) any {
	b, _ := json.Marshal(err)
	if string(b) == "{}" || string(b) == "" {
		return NewError(err.Error())
	}
	return err
}

type Backoff struct {
	Min    time.Duration // 最小待ち時間 (例: 100ms)
	Max    time.Duration // 最大待ち時間 (例: 30s)
	Factor float64       // 倍率 (通常 2.0)
	jitter bool
}

func NewBackoff(min, max time.Duration) *Backoff {
	return &Backoff{
		Min:    min,
		Max:    max,
		Factor: 2.0,
		jitter: true,
	}
}

// Duration は現在の試行回数に応じた待ち時間を返す
func (b *Backoff) Duration(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}

	// 指数計算: min * (factor ^ attempt)
	dur := float64(b.Min) * math.Pow(b.Factor, float64(attempt))

	d := time.Duration(dur)

	// 最大値でキャップ
	if d > b.Max {
		d = b.Max
	}

	// Jitter を加える (0 ～ d の間でランダム)
	if b.jitter && d > 0 {
		d = time.Duration(rand.Int63n(int64(d)))
	}

	// 最小値を下回らないようにする
	if d < b.Min {
		return b.Min
	}

	return d
}
