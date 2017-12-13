package localcache

import (
	"context"
	"sync"
	"time"

	"go.mercari.io/datastore"
	"go.mercari.io/datastore/dsmiddleware/storagecache"
)

var _ storagecache.Storage = &CacheHandler{}
var _ storagecache.Logger = &CacheHandler{}
var _ datastore.Middleware = &CacheHandler{}

const defaultExpiration = 3 * time.Minute

func New(opts ...storagecache.CacheOption) *CacheHandler {
	// I want to make ch.dsmiddleware accessible from the test
	ch := &CacheHandler{
		cache:          make(map[string]cacheItem),
		ExpireDuration: defaultExpiration,
		Logf:           func(ctx context.Context, format string, args ...interface{}) {},
	}
	s := storagecache.New(ch, opts...)
	ch.st = s

	return ch
}

type CacheHandler struct {
	cache          map[string]cacheItem
	m              sync.Mutex
	st             datastore.Middleware
	ExpireDuration time.Duration
	Logf           func(ctx context.Context, format string, args ...interface{})
}

type cacheItem struct {
	Key          datastore.Key
	PropertyList datastore.PropertyList
	setAt        time.Time
	expiration   time.Duration
}

func (ch *CacheHandler) Has(key datastore.Key) bool {
	_, ok := ch.cache[key.Encode()]
	return ok
}

func (ch *CacheHandler) DeleteCache(ctx context.Context, key datastore.Key) {
	ch.m.Lock()
	defer ch.m.Unlock()
	ch.Logf(ctx, "dsmiddleware/localcache.DeleteCache: key=%s", key.String())
	delete(ch.cache, key.Encode())
}

func (ch *CacheHandler) Len() int {
	return len(ch.cache)
}

func (ch *CacheHandler) FlushLocalCache() {
	ch.m.Lock()
	defer ch.m.Unlock()
	ch.cache = make(map[string]cacheItem)
}

func (ch *CacheHandler) Printf(ctx context.Context, format string, args ...interface{}) {
	ch.Logf(ctx, format, args...)
}

// storagecache.Storage implementation

func (ch *CacheHandler) SetMulti(ctx context.Context, cis []*storagecache.CacheItem) error {
	ch.m.Lock()
	defer ch.m.Unlock()

	ch.Logf(ctx, "dsmiddleware/localcache.SetMulti: len=%d", len(cis))
	for idx, ci := range cis {
		ch.Logf(ctx, "dsmiddleware/localcache.SetMulti: idx=%d key=%s len(ps)=%d", idx, ci.Key.String(), len(ci.PropertyList))
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
			expiration:   ch.ExpireDuration,
		}
	}

	return nil
}

func (ch *CacheHandler) GetMulti(ctx context.Context, keys []datastore.Key) ([]*storagecache.CacheItem, error) {
	ch.m.Lock()
	defer ch.m.Unlock()

	now := time.Now()

	ch.Logf(ctx, "dsmiddleware/localcache.GetMulti: len=%d", len(keys))
	for idx, key := range keys {
		ch.Logf(ctx, "dsmiddleware/localcache.GetMulti: idx=%d key=%s", idx, key.String())
	}

	resultList := make([]*storagecache.CacheItem, len(keys))
	for idx, key := range keys {
		if key.Incomplete() {
			ch.Logf(ctx, "dsmiddleware/localcache.GetMulti: idx=%d, incomplete key=%s", idx, key.String())
			continue
		}
		cItem, ok := ch.cache[key.Encode()]
		if !ok {
			ch.Logf(ctx, "dsmiddleware/localcache.GetMulti: idx=%d, missed key=%s", idx, key.String())
			continue
		}

		if cItem.setAt.Add(cItem.expiration).After(now) {
			ch.Logf(ctx, "dsmiddleware/localcache.GetMulti: idx=%d, hit key=%s len(ps)=%d", idx, key.String(), len(cItem.PropertyList))
			resultList[idx] = &storagecache.CacheItem{
				Key:          key,
				PropertyList: cItem.PropertyList,
			}
		} else {
			ch.Logf(ctx, "dsmiddleware/localcache.GetMulti: idx=%d, expired key=%s", idx, key.String())
			delete(ch.cache, key.Encode())
		}
	}

	return resultList, nil
}

func (ch *CacheHandler) DeleteMulti(ctx context.Context, keys []datastore.Key) error {
	ch.m.Lock()
	defer ch.m.Unlock()

	ch.Logf(ctx, "dsmiddleware/localcache.DeleteMulti: len=%d", len(keys))
	for idx, key := range keys {
		ch.Logf(ctx, "dsmiddleware/localcache.DeleteMulti: idx=%d key=%s", idx, key.String())
	}

	for _, key := range keys {
		delete(ch.cache, key.Encode())
	}

	return nil
}

// datastore.Middleware implementations

func (ch *CacheHandler) PutMultiWithoutTx(info *datastore.MiddlewareInfo, keys []datastore.Key, psList []datastore.PropertyList) ([]datastore.Key, error) {
	return ch.st.PutMultiWithoutTx(info, keys, psList)
}

func (ch *CacheHandler) PutMultiWithTx(info *datastore.MiddlewareInfo, keys []datastore.Key, psList []datastore.PropertyList) ([]datastore.PendingKey, error) {
	return ch.st.PutMultiWithTx(info, keys, psList)
}

func (ch *CacheHandler) GetMultiWithoutTx(info *datastore.MiddlewareInfo, keys []datastore.Key, psList []datastore.PropertyList) error {
	return ch.st.GetMultiWithoutTx(info, keys, psList)
}

func (ch *CacheHandler) GetMultiWithTx(info *datastore.MiddlewareInfo, keys []datastore.Key, psList []datastore.PropertyList) error {
	return ch.st.GetMultiWithTx(info, keys, psList)
}

func (ch *CacheHandler) DeleteMultiWithoutTx(info *datastore.MiddlewareInfo, keys []datastore.Key) error {
	return ch.st.DeleteMultiWithoutTx(info, keys)
}

func (ch *CacheHandler) DeleteMultiWithTx(info *datastore.MiddlewareInfo, keys []datastore.Key) error {
	return ch.st.DeleteMultiWithTx(info, keys)
}

func (ch *CacheHandler) PostCommit(info *datastore.MiddlewareInfo, tx datastore.Transaction, commit datastore.Commit) error {
	return ch.st.PostCommit(info, tx, commit)
}

func (ch *CacheHandler) PostRollback(info *datastore.MiddlewareInfo, tx datastore.Transaction) error {
	return ch.st.PostRollback(info, tx)
}

func (ch *CacheHandler) Run(info *datastore.MiddlewareInfo, q datastore.Query, qDump *datastore.QueryDump) datastore.Iterator {
	return ch.st.Run(info, q, qDump)
}

func (ch *CacheHandler) GetAll(info *datastore.MiddlewareInfo, q datastore.Query, qDump *datastore.QueryDump, psList *[]datastore.PropertyList) ([]datastore.Key, error) {
	return ch.st.GetAll(info, q, qDump, psList)
}

func (ch *CacheHandler) Next(info *datastore.MiddlewareInfo, q datastore.Query, qDump *datastore.QueryDump, iter datastore.Iterator, ps *datastore.PropertyList) (datastore.Key, error) {
	return ch.st.Next(info, q, qDump, iter, ps)
}
