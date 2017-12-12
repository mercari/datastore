package storagecache

import "go.mercari.io/datastore"

func WithIncludeKinds(kinds ...string) CacheOption {
	return &withIncludeKinds{kinds}
}

type withIncludeKinds struct{ kinds []string }

func (w *withIncludeKinds) Apply(o *cacheHandler) {
	o.filters = append(o.filters, func(key datastore.Key) bool {
		for _, incKind := range w.kinds {
			if key.Kind() == incKind {
				return true
			}
		}

		return false
	})
}

func WithExcludeKinds(kinds ...string) CacheOption {
	return &withExcludeKinds{kinds}
}

type withExcludeKinds struct{ kinds []string }

func (w *withExcludeKinds) Apply(o *cacheHandler) {
	o.filters = append(o.filters, func(key datastore.Key) bool {
		for _, excKind := range w.kinds {
			if key.Kind() == excKind {
				return false
			}
		}

		return true
	})
}

func WithKeyFilter(f func(key datastore.Key) bool) CacheOption {
	return &withKeyFilter{f}
}

type withKeyFilter struct{ f func(key datastore.Key) bool }

func (w *withKeyFilter) Apply(o *cacheHandler) {
	o.filters = append(o.filters, func(key datastore.Key) bool {
		return w.f(key)
	})
}
