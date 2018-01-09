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
	ch.st = s

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
	cache          map[string]cacheItem
	m              sync.Mutex
	st             datastore.Middleware
	stOpts         *storagecache.Options
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

// storagecache.Storage implementation

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

// datastore.Middleware implementations

func (ch *cacheHandler) AllocateIDs(info *datastore.MiddlewareInfo, keys []datastore.Key) ([]datastore.Key, error) {
	return ch.st.AllocateIDs(info, keys)
}

func (ch *cacheHandler) PutMultiWithoutTx(info *datastore.MiddlewareInfo, keys []datastore.Key, psList []datastore.PropertyList) ([]datastore.Key, error) {
	return ch.st.PutMultiWithoutTx(info, keys, psList)
}

func (ch *cacheHandler) PutMultiWithTx(info *datastore.MiddlewareInfo, keys []datastore.Key, psList []datastore.PropertyList) ([]datastore.PendingKey, error) {
	return ch.st.PutMultiWithTx(info, keys, psList)
}

func (ch *cacheHandler) GetMultiWithoutTx(info *datastore.MiddlewareInfo, keys []datastore.Key, psList []datastore.PropertyList) error {
	return ch.st.GetMultiWithoutTx(info, keys, psList)
}

func (ch *cacheHandler) GetMultiWithTx(info *datastore.MiddlewareInfo, keys []datastore.Key, psList []datastore.PropertyList) error {
	return ch.st.GetMultiWithTx(info, keys, psList)
}

func (ch *cacheHandler) DeleteMultiWithoutTx(info *datastore.MiddlewareInfo, keys []datastore.Key) error {
	return ch.st.DeleteMultiWithoutTx(info, keys)
}

func (ch *cacheHandler) DeleteMultiWithTx(info *datastore.MiddlewareInfo, keys []datastore.Key) error {
	return ch.st.DeleteMultiWithTx(info, keys)
}

func (ch *cacheHandler) PostCommit(info *datastore.MiddlewareInfo, tx datastore.Transaction, commit datastore.Commit) error {
	return ch.st.PostCommit(info, tx, commit)
}

func (ch *cacheHandler) PostRollback(info *datastore.MiddlewareInfo, tx datastore.Transaction) error {
	return ch.st.PostRollback(info, tx)
}

func (ch *cacheHandler) Run(info *datastore.MiddlewareInfo, q datastore.Query, qDump *datastore.QueryDump) datastore.Iterator {
	return ch.st.Run(info, q, qDump)
}

func (ch *cacheHandler) GetAll(info *datastore.MiddlewareInfo, q datastore.Query, qDump *datastore.QueryDump, psList *[]datastore.PropertyList) ([]datastore.Key, error) {
	return ch.st.GetAll(info, q, qDump, psList)
}

func (ch *cacheHandler) Next(info *datastore.MiddlewareInfo, q datastore.Query, qDump *datastore.QueryDump, iter datastore.Iterator, ps *datastore.PropertyList) (datastore.Key, error) {
	return ch.st.Next(info, q, qDump, iter, ps)
}

func (ch *cacheHandler) Count(info *datastore.MiddlewareInfo, q datastore.Query, qDump *datastore.QueryDump) (int, error) {
	return ch.st.Count(info, q, qDump)
}
