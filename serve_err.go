package allino

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gofiber/fiber/v2"
)

type FiberHandler interface {
	HandleFiber(w *fiber.Ctx)
}

var ErrNoOp error = &noOpError{}

type noOpError struct {
}

func (e *noOpError) Error() string {
	return ""
}

func (e *noOpError) HandleFiber(w *fiber.Ctx) {
}

type RedirectError struct {
	StatusCode int
	Location   string
	Err        error
}

func (e *RedirectError) Error() string {
	if e.Err != nil {
		return e.Err.Error()
	}
	return http.StatusText(e.StatusCode) + " redirect to " + e.Location
}

func (e *RedirectError) HandleFiber(w *fiber.Ctx) {
	w.Set("Location", e.Location)
	w.Status(e.StatusCode)
}

func NewRedirectError(status int, location string) *RedirectError {
	return &RedirectError{
		StatusCode: status,
		Location:   location,
	}
}

type StatusError struct {
	StatusCode int
}

func (e *StatusError) Error() string {
	return http.StatusText(e.StatusCode) + " error"
}

func (e *StatusError) HandleFiber(w *fiber.Ctx) {
	w.Status(e.StatusCode)
}

func NewStatusError(status int) *StatusError {
	return &StatusError{
		StatusCode: status,
	}
}

type HttpError interface {
	error
	StatusCode() int
}

type errorcodeError interface {
	error
	ErrorCode() string
}

func NewError(msg string) *Error {
	return &Error{
		Msg: msg,
	}
}

func NewCodeError(status int, code, msg string) *Error {
	return &Error{
		Status: status,
		Code:   code,
		Msg:    msg,
	}
}

func NewCodeErrorf(status int, code, format string, a ...any) *Error {
	e := &Error{
		Status: status,
		Code:   code,
	}
	e.Err = fmt.Errorf(format, a...)
	e.Msg = e.Err.Error()
	return e
}

type Error struct {
	base *Error

	Status int    `json:"-"`
	Code   string `json:"code,omitempty"`
	Msg    string `json:"msg,omitempty"`
	Err    error  `json:"-"`
}

func (e *Error) StatusCode() int {
	return e.Status
}

func (e *Error) ErrorCode() string {
	return e.Code
}

func (e *Error) Error() string {
	return e.Msg
}

func (e *Error) With(err error) *Error {
	copy := *e
	copy.Err = err
	copy.base = e
	return &copy
}

// Is preserves sentinel matching for immutable With wrappers.
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	if !ok {
		return false
	}
	for parent := e; parent != nil; parent = parent.base {
		if parent == t {
			return true
		}
	}
	return false
}

func (e *Error) Unwrap() error {
	return e.Err
}

func (e *Error) JSON() []byte {
	js, err := json.Marshal(e)
	if err != nil {
		return []byte("{}")
	}
	return js
}

func (r *Runtime) errorRedirect(statusCode int, err error) {
	ecerr, ok2 := err.(errorcodeError)
	if ok2 {
		r.fiber.Set("Location", r.config.Routing.ErrorPath+ecerr.ErrorCode())
	} else {
		r.fiber.Set("Location", r.config.Routing.ErrorPath)
	}

	cerr, ok := err.(HttpError)
	if ok {
		if cerr.StatusCode() != 0 {
			r.fiber.Status(cerr.StatusCode())
		} else {
			r.fiber.Status(statusCode)
		}
	}
}

func (r *Runtime) errorJSON(statusCode int, nowrap bool, eiserror bool, errz error) {
	var err error
	//r.logger.Info("errorJSON", zap.Error(errz))
	cerr, ok := errz.(HttpError)
	if ok && cerr.StatusCode() != 0 {
		r.fiber.Status(cerr.StatusCode())
	} else {
		r.fiber.Status(statusCode)
	}

	if nowrap {
		if eiserror {
			buf, erry := json.Marshal(errz)
			if erry != nil || string(buf) == "{}" {
				err = &Error{Msg: errz.Error()}
			} else {
				err = errz
			}
		} else {
			err = errz
		}
	} else {
		if eiserror {
			buf, erry := json.Marshal(errz)
			if erry != nil || string(buf) == "{}" {
				err = &APIError[*Error]{Err: &Error{Msg: errz.Error()}}
			} else {
				err = &APIError[error]{Err: errz}
			}
		} else {
			err = &APIError[error]{Err: errz}
		}
	}
	jerrbuf, err := json.MarshalIndent(err, "", "  ")
	if err == nil {
		_ = r.fiber.Send(jerrbuf)
		return
	}
}
