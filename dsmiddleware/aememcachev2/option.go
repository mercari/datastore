package aememcachev2

import (
	"context"
	"time"

	"go.mercari.io/datastore"
	"go.mercari.io/datastore/dsmiddleware/storagecache"
)

// WithIncludeKinds creates a ClientOption that selects the Kind specified as the cache target.
func WithIncludeKinds(kinds ...string) CacheOption {
	return &withIncludeKinds{kinds}
}

type withIncludeKinds struct{ kinds []string }

func (w *withIncludeKinds) Apply(o *cacheHandler) {
	o.stOpts.Filters = append(o.stOpts.Filters, func(ctx context.Context, key datastore.Key) bool {
		for _, incKind := range w.kinds {
			if key.Kind() == incKind {
				return true
			}
		}

		return false
	})
}

// WithExcludeKinds creates a ClientOption that selects the Kind unspecified as the cache target.
func WithExcludeKinds(kinds ...string) CacheOption {
	return &withExcludeKinds{kinds}
}

type withExcludeKinds struct{ kinds []string }

func (w *withExcludeKinds) Apply(o *cacheHandler) {
	o.stOpts.Filters = append(o.stOpts.Filters, func(ctx context.Context, key datastore.Key) bool {
		for _, excKind := range w.kinds {
			if key.Kind() == excKind {
				return false
			}
		}

		return true
	})
}

// WithKeyFilter creates a ClientOption that selects the Keys specified as the cache target.
func WithKeyFilter(f storagecache.KeyFilter) CacheOption {
	return &withKeyFilter{f}
}

type withKeyFilter struct{ f storagecache.KeyFilter }

func (w *withKeyFilter) Apply(o *cacheHandler) {
	o.stOpts.Filters = append(o.stOpts.Filters, func(ctx context.Context, key datastore.Key) bool {
		return w.f(ctx, key)
	})
}

// WithLogger creates a ClientOption that uses the specified logger.
func WithLogger(logf func(ctx context.Context, format string, args ...interface{})) CacheOption {
	return &withLogger{logf}
}

type withLogger struct {
	logf func(ctx context.Context, format string, args ...interface{})
}

func (w *withLogger) Apply(o *cacheHandler) {
	o.logf = w.logf
}

// WithExpireDuration creates a ClientOption to expire at a specified time.
func WithExpireDuration(d time.Duration) CacheOption {
	return &withExpireDuration{d}
}

type withExpireDuration struct{ d time.Duration }

func (w *withExpireDuration) Apply(o *cacheHandler) {
	o.expireDuration = w.d
}

// WithCacheKey creates a ClientOption that specifies how to generate a cache key from datastore.Key.
func WithCacheKey(f func(key datastore.Key) string) CacheOption {
	return &withCacheKey{f}
}

type withCacheKey struct {
	cacheKey func(key datastore.Key) string
}

func (w *withCacheKey) Apply(o *cacheHandler) {
	o.cacheKey = w.cacheKey
}
