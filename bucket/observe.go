package bucket

import (
	"context"
	"errors"
	"net"
	"time"
)

// Read kinds that Observe reports.
const (
	OpGet      = "get"
	OpGetRange = "get_range"
)

// Error kinds of a failed read, as ErrorKind names them.
const (
	KindTimeout = "timeout"
	Kind5xx     = "5xx"
	KindOther   = "other"
)

// WithStatus marks err as a failure that the service answered with the HTTP status code.
func WithStatus(err error, code int) error { return statusError{err, code} }

type statusError struct {
	error
	code int
}

func (e statusError) Unwrap() error   { return e.error }
func (e statusError) HTTPStatus() int { return e.code }

// ErrorKind names the kind of a failed read: a timeout, a server error that WithStatus marked, or another error. A
// read that the caller cancelled, and a missing key, are not failures and have no kind (ok is false).
func ErrorKind(err error) (kind string, ok bool) {
	var ne net.Error
	var se interface{ HTTPStatus() int }
	switch {
	case err == nil, errors.Is(err, ErrNotFound), errors.Is(err, context.Canceled):
		return "", false
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &ne) && ne.Timeout():
		return KindTimeout, true
	case errors.As(err, &se) && se.HTTPStatus() >= 500:
		return Kind5xx, true
	}
	return KindOther, true
}

// Observe returns a Store that calls fn after each Get and GetRange with the read kind, the time it took and its
// error. Other methods pass through. fn runs on the goroutine of the read, before the read returns, and concurrent reads
// call it concurrently.
func Observe(s Store, fn func(op string, d time.Duration, err error)) Store { return observed{s, fn} }

type observed struct {
	Store
	fn func(op string, d time.Duration, err error)
}

func (o observed) Unwrap() Store { return o.Store }

func (o observed) Get(ctx context.Context, key string) ([]byte, error) {
	start := time.Now()
	b, err := o.Store.Get(ctx, key)
	o.fn(OpGet, time.Since(start), err)
	return b, err
}

func (o observed) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	start := time.Now()
	b, err := o.Store.GetRange(ctx, key, off, n)
	o.fn(OpGetRange, time.Since(start), err)
	return b, err
}
