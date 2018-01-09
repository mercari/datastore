package localcache

import (
	"context"
	"sync"
	"time"

	"go.mercari.io/datastore"
	"go.mercari.io/datastore/dsmiddleware/storagecache"
)

var _ storagecache.Storage = &cacheHandler{}
var _ datastore.Middleware = &cacheHandler{}

const defaultExpiration = 3 * time.Minute

func New(opts ...CacheOption) CacheHandler {
	ch := &cacheHandler{
		cache:  make(map[string]cacheItem),
		stOpts: &storagecache.Options{},
	}

	for _, opt := range opts {
		opt.Apply(ch)
	}

	s := storagecache.New(ch, ch.stOpts)
	ch.Middleware = s

	if ch.expireDuration == 0 {
		ch.expireDuration = defaultExpiration
	}
	if ch.logf == nil {
		ch.logf = func(ctx context.Context, format string, args ...interface{}) {}
	}

	return ch
}

type CacheHandler interface {
	datastore.Middleware
	storagecache.Storage

	HasCache(key datastore.Key) bool
	DeleteCache(ctx context.Context, key datastore.Key)
	CacheKeys() []string
	CacheLen() int
	FlushLocalCache()
}

type cacheHandler struct {
	datastore.Middleware
	stOpts *storagecache.Options

	cache          map[string]cacheItem
	m              sync.Mutex
	expireDuration time.Duration
	logf           func(ctx context.Context, format string, args ...interface{})
}

type CacheOption interface {
	Apply(*cacheHandler)
}

type cacheItem struct {
	Key          datastore.Key
	PropertyList datastore.PropertyList
	setAt        time.Time
	expiration   time.Duration
}

func (ch *cacheHandler) HasCache(key datastore.Key) bool {
	_, ok := ch.cache[key.Encode()]
	return ok
}

func (ch *cacheHandler) DeleteCache(ctx context.Context, key datastore.Key) {
	ch.m.Lock()
	defer ch.m.Unlock()
	ch.logf(ctx, "dsmiddleware/localcache.DeleteCache: key=%s", key.String())
	delete(ch.cache, key.Encode())
}

func (ch *cacheHandler) CacheKeys() []string {
	list := make([]string, 0, len(ch.cache))
	for keyStr := range ch.cache {
		list = append(list, keyStr)
	}

	return list
}

func (ch *cacheHandler) CacheLen() int {
	return len(ch.cache)
}

func (ch *cacheHandler) FlushLocalCache() {
	ch.m.Lock()
	defer ch.m.Unlock()
	ch.cache = make(map[string]cacheItem)
}

func (ch *cacheHandler) SetMulti(ctx context.Context, cis []*storagecache.CacheItem) error {
	ch.m.Lock()
	defer ch.m.Unlock()

	ch.logf(ctx, "dsmiddleware/localcache.SetMulti: len=%d", len(cis))
	for idx, ci := range cis {
		ch.logf(ctx, "dsmiddleware/localcache.SetMulti: idx=%d key=%s len(ps)=%d", idx, ci.Key.String(), len(ci.PropertyList))
	}

	now := time.Now()
	for _, ci := range cis {
		if ci.Key.Incomplete() {
			continue
		}
		ch.cache[ci.Key.Encode()] = cacheItem{
			Key:          ci.Key,
			PropertyList: ci.PropertyList,
			setAt:        now,
			expiration:   ch.expireDuration,
		}
	}

	return nil
}

func (ch *cacheHandler) GetMulti(ctx context.Context, keys []datastore.Key) ([]*storagecache.CacheItem, error) {
	ch.m.Lock()
	defer ch.m.Unlock()

	now := time.Now()

	ch.logf(ctx, "dsmiddleware/localcache.GetMulti: len=%d", len(keys))
	for idx, key := range keys {
		ch.logf(ctx, "dsmiddleware/localcache.GetMulti: idx=%d key=%s", idx, key.String())
	}

	resultList := make([]*storagecache.CacheItem, len(keys))
	for idx, key := range keys {
		if key.Incomplete() {
			ch.logf(ctx, "dsmiddleware/localcache.GetMulti: idx=%d, incomplete key=%s", idx, key.String())
			continue
		}
		cItem, ok := ch.cache[key.Encode()]
		if !ok {
			ch.logf(ctx, "dsmiddleware/localcache.GetMulti: idx=%d, missed key=%s", idx, key.String())
			continue
		}

		if cItem.setAt.Add(cItem.expiration).After(now) {
			ch.logf(ctx, "dsmiddleware/localcache.GetMulti: idx=%d, hit key=%s len(ps)=%d", idx, key.String(), len(cItem.PropertyList))
			resultList[idx] = &storagecache.CacheItem{
				Key:          key,
				PropertyList: cItem.PropertyList,
			}
		} else {
			ch.logf(ctx, "dsmiddleware/localcache.GetMulti: idx=%d, expired key=%s", idx, key.String())
			delete(ch.cache, key.Encode())
		}
	}

	return resultList, nil
}

func (ch *cacheHandler) DeleteMulti(ctx context.Context, keys []datastore.Key) error {
	ch.m.Lock()
	defer ch.m.Unlock()

	ch.logf(ctx, "dsmiddleware/localcache.DeleteMulti: len=%d", len(keys))
	for idx, key := range keys {
		ch.logf(ctx, "dsmiddleware/localcache.DeleteMulti: idx=%d key=%s", idx, key.String())
	}

	for _, key := range keys {
		delete(ch.cache, key.Encode())
	}

	return nil
}
