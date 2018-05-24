package rpcretry

import (
	"context"
	"time"
)

// WithRetryLimit provides retry limit when RPC failed.
func WithRetryLimit(limit int) RetryOption {
	return &withRetryLimit{limit}
}

type withRetryLimit struct{ retryLimit int }

func (w *withRetryLimit) Apply(rh *retryHandler) {
	rh.retryLimit = w.retryLimit
}

// WithMinBackoffDuration specified minimal duration of retry backoff.
func WithMinBackoffDuration(d time.Duration) RetryOption {
	return &withMinBackoffDuration{d}
}

type withMinBackoffDuration struct{ d time.Duration }

func (w *withMinBackoffDuration) Apply(rh *retryHandler) {
	rh.minBackoffDuration = w.d
}

// WithMaxBackoffDuration specified maximum duratiuon of retry backoff.
func WithMaxBackoffDuration(d time.Duration) RetryOption {
	return &withMaxBackoffDuration{d}
}

type withMaxBackoffDuration struct{ d time.Duration }

func (w *withMaxBackoffDuration) Apply(rh *retryHandler) {
	rh.maxBackoffDuration = w.d
}

// WithMaxDoublings specifies how many times the waiting time should be doubled.
func WithMaxDoublings(maxDoublings int) RetryOption {
	return &withMaxDoublings{maxDoublings}
}

type withMaxDoublings struct{ maxDoublings int }

func (w *withMaxDoublings) Apply(rh *retryHandler) {
	rh.maxDoublings = w.maxDoublings
}

// WithLogf creates a ClientOption that uses the specified logger.
// TODO rename to WithLogger
func WithLogf(logf func(ctx context.Context, format string, args ...interface{})) RetryOption {
	return &withLogf{logf}
}

type withLogf struct {
	logf func(ctx context.Context, format string, args ...interface{})
}

func (w *withLogf) Apply(rh *retryHandler) {
	rh.logf = w.logf
}
