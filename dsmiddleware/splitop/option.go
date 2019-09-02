package splitop

import "context"

// WithSplitThreshold set operation split threshold.
func WithSplitThreshold(threshold int) Option {
	return &splitThreshold{threshold}
}

type splitThreshold struct{ splitThreshold int }

func (w *splitThreshold) Apply(o *splitHandler) {
	o.splitThreshold = w.splitThreshold
}

// WithLogger creates a Option that uses the specified logger.
func WithLogger(logf func(ctx context.Context, format string, args ...interface{})) Option {
	return &withLogger{logf}
}

type withLogger struct {
	logf func(ctx context.Context, format string, args ...interface{})
}

func (w *withLogger) Apply(o *splitHandler) {
	o.logf = w.logf
}
