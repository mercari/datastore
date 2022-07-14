package splitop

import "context"

// WithSplitThreshold set operation split threshold.
// Deprecated: use WithGetSplitThreshold instead.
func WithSplitThreshold(threshold int) Option {
	return &getSplitThreshold{threshold}
}

// WithGetSplitThreshold set get operation split threshold.
func WithGetSplitThreshold(threshold int) Option {
	return &getSplitThreshold{threshold}
}

type getSplitThreshold struct{ splitThreshold int }

func (w *getSplitThreshold) Apply(o *splitHandler) {
	o.getSplitThreshold = w.splitThreshold
}

// WithPutSplitThreshold set put operation split threshold.
func WithPutSplitThreshold(threshold int) Option {
	return &putSplitThreshold{threshold}
}

type putSplitThreshold struct{ splitThreshold int }

func (w *putSplitThreshold) Apply(o *splitHandler) {
	o.putSplitThreshold = w.splitThreshold
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
