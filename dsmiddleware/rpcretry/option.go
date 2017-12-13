package rpcretry

import (
	"context"
	"time"
)

func WithRetryLimit(limit int) RetryOption {
	return &withRetryLimit{limit}
}

type withRetryLimit struct{ retryLimit int }

func (w *withRetryLimit) Apply(rh *retryHandler) {
	rh.retryLimit = w.retryLimit
}

func WithMinBackoffDuration(d time.Duration) RetryOption {
	return &withMinBackoffDuration{d}
}

type withMinBackoffDuration struct{ d time.Duration }

func (w *withMinBackoffDuration) Apply(rh *retryHandler) {
	rh.minBackoffDuration = w.d
}

func WithMaxBackoffDuration(d time.Duration) RetryOption {
	return &withMaxBackoffDuration{d}
}

type withMaxBackoffDuration struct{ d time.Duration }

func (w *withMaxBackoffDuration) Apply(rh *retryHandler) {
	rh.maxBackoffDuration = w.d
}

func WithMaxDoublings(maxDoublings int) RetryOption {
	return &withMaxDoublings{maxDoublings}
}

type withMaxDoublings struct{ maxDoublings int }

func (w *withMaxDoublings) Apply(rh *retryHandler) {
	rh.maxDoublings = w.maxDoublings
}

func WithLogf(logf func(ctx context.Context, format string, args ...interface{})) RetryOption {
	return &withLogf{logf}
}

type withLogf struct {
	logf func(ctx context.Context, format string, args ...interface{})
}

func (w *withLogf) Apply(rh *retryHandler) {
	rh.logf = w.logf
}
