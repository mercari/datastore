package localcache

import (
	"context"
	"sync"
	"time"

	"go.mercari.io/datastore"
	"go.mercari.io/datastore/cache/storagecache"
)

var _ storagecache.Storage = &CacheHandler{}
var _ datastore.CacheStrategy = &CacheHandler{}

const defaultExpiration = 3 * time.Minute

func New() *CacheHandler {
	// I want to make ch.cache accessible from the test
	ch := &CacheHandler{
		cache:          make(map[string]cacheItem),
		ExpireDuration: defaultExpiration,
	}
	s := storagecache.New(ch)
	ch.st = s

	return ch
}

type CacheHandler struct {
	cache          map[string]cacheItem
	m              sync.Mutex
	st             datastore.CacheStrategy
	ExpireDuration time.Duration
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

func (ch *CacheHandler) Len() int {
	return len(ch.cache)
}

// storagecache.Storage implementation

func (ch *CacheHandler) SetMulti(ctx context.Context, is []*storagecache.CacheItem) error {
	ch.m.Lock()
	defer ch.m.Unlock()

	now := time.Now()
	for _, ci := range is {
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

	resultList := make([]*storagecache.CacheItem, len(keys))
	for idx, key := range keys {
		if key.Incomplete() {
			continue
		}
		cItem, ok := ch.cache[key.Encode()]
		if !ok {
			continue
		}

		if cItem.setAt.Add(cItem.expiration).After(now) {
			resultList[idx] = &storagecache.CacheItem{
				Key:          key,
				PropertyList: cItem.PropertyList,
			}
		} else {
			delete(ch.cache, key.Encode())
		}
	}

	return resultList, nil
}

func (ch *CacheHandler) DeleteMulti(ctx context.Context, keys []datastore.Key) error {
	ch.m.Lock()
	defer ch.m.Unlock()

	for _, key := range keys {
		delete(ch.cache, key.Encode())
	}

	return nil
}

// datastore.CacheStrategy implementations

func (ch *CacheHandler) PutMultiWithoutTx(info *datastore.CacheInfo, keys []datastore.Key, psList []datastore.PropertyList) ([]datastore.Key, error) {
	return ch.st.PutMultiWithoutTx(info, keys, psList)
}

func (ch *CacheHandler) PutMultiWithTx(info *datastore.CacheInfo, keys []datastore.Key, psList []datastore.PropertyList) ([]datastore.PendingKey, error) {
	return ch.st.PutMultiWithTx(info, keys, psList)
}

func (ch *CacheHandler) GetMultiWithoutTx(info *datastore.CacheInfo, keys []datastore.Key, psList []datastore.PropertyList) error {
	return ch.st.GetMultiWithoutTx(info, keys, psList)
}

func (ch *CacheHandler) GetMultiWithTx(info *datastore.CacheInfo, keys []datastore.Key, psList []datastore.PropertyList) error {
	return ch.st.GetMultiWithTx(info, keys, psList)
}

func (ch *CacheHandler) DeleteMultiWithoutTx(info *datastore.CacheInfo, keys []datastore.Key) error {
	return ch.st.DeleteMultiWithoutTx(info, keys)
}

func (ch *CacheHandler) DeleteMultiWithTx(info *datastore.CacheInfo, keys []datastore.Key) error {
	return ch.st.DeleteMultiWithTx(info, keys)
}

func (ch *CacheHandler) PostCommit(info *datastore.CacheInfo, tx datastore.Transaction, commit datastore.Commit) error {
	return ch.st.PostCommit(info, tx, commit)
}

func (ch *CacheHandler) PostRollback(info *datastore.CacheInfo, tx datastore.Transaction) error {
	return ch.st.PostRollback(info, tx)
}

func (ch *CacheHandler) Run(info *datastore.CacheInfo, q datastore.Query, qDump *datastore.QueryDump) datastore.Iterator {
	return ch.st.Run(info, q, qDump)
}

func (ch *CacheHandler) GetAll(info *datastore.CacheInfo, q datastore.Query, qDump *datastore.QueryDump, psList *[]datastore.PropertyList) ([]datastore.Key, error) {
	return ch.st.GetAll(info, q, qDump, psList)
}

func (ch *CacheHandler) Next(info *datastore.CacheInfo, q datastore.Query, qDump *datastore.QueryDump, iter datastore.Iterator, ps *datastore.PropertyList) (datastore.Key, error) {
	return ch.st.Next(info, q, qDump, iter, ps)
}
