package aememcache

import (
	"bytes"
	"context"
	"encoding/gob"
	"time"

	"go.mercari.io/datastore"
	"go.mercari.io/datastore/dsmiddleware/storagecache"
	"google.golang.org/appengine"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/memcache"
)

var _ storagecache.Storage = &CacheHandler{}
var _ storagecache.Logger = &CacheHandler{}
var _ datastore.Middleware = &CacheHandler{}

func New(opts ...storagecache.CacheOption) *CacheHandler {
	ch := &CacheHandler{
		KeyPrefix:      "mercari:aememcache:",
		ExpireDuration: 0,
		Logf: func(ctx context.Context, format string, args ...interface{}) {
			log.Debugf(ctx, format, args...)
		},
	}
	s := storagecache.New(ch, opts...)
	ch.st = s

	return ch
}

type CacheHandler struct {
	st                 datastore.Middleware
	raiseMemcacheError bool
	KeyPrefix          string
	ExpireDuration     time.Duration
	Logf               func(ctx context.Context, format string, args ...interface{})
}

// storagecache.Storage implementation

func (ch *CacheHandler) cacheKey(key datastore.Key) string {
	return ch.KeyPrefix + key.Encode()
}

func (ch *CacheHandler) Printf(ctx context.Context, format string, args ...interface{}) {
	ch.Logf(ctx, format, args...)
}

func (ch *CacheHandler) SetMulti(ctx context.Context, cis []*storagecache.CacheItem) error {

	ch.Logf(ctx, "dsmiddleware/aememcache.SetMulti: incoming len=%d", len(cis))

	itemList := make([]*memcache.Item, 0, len(cis))
	for _, ci := range cis {
		if ci.Key.Incomplete() {
			panic("incomplete key incoming")
		}
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		err := enc.Encode(ci.PropertyList)
		if err != nil {
			ch.Logf(ctx, "dsmiddleware/aememcache.SetMulti: gob.Encode error key=%s err=%s", ci.Key.String(), err.Error())
			continue
		}
		itemList = append(itemList, &memcache.Item{
			Key:        ch.cacheKey(ci.Key),
			Value:      buf.Bytes(),
			Expiration: ch.ExpireDuration,
		})
	}

	ch.Logf(ctx, "dsmiddleware/aememcache.SetMulti: len=%d", len(itemList))

	err := memcache.SetMulti(ctx, itemList)
	if err != nil {
		ch.Logf(ctx, "dsmiddleware/aememcache: error on memcache.SetMulti %s", err.Error())
		if ch.raiseMemcacheError {
			if merr, ok := err.(appengine.MultiError); ok {
				for _, err := range merr {
					if err == nil || err == memcache.ErrCacheMiss {
						continue
					}
					return merr
				}
			} else {
				return err
			}
		}

		keys := make([]string, 0, len(cis))
		for _, ci := range cis {
			keys = append(keys, ci.Key.Encode())
		}
		err = memcache.DeleteMulti(ctx, keys)
		if err != nil {
			ch.Logf(ctx, "dsmiddleware/aememcache: error on memcache.DeleteMulti %s", err.Error())
			if ch.raiseMemcacheError {
				if merr, ok := err.(appengine.MultiError); ok {
					for _, err := range merr {
						if err == nil || err == memcache.ErrCacheMiss {
							continue
						}
						return merr
					}
				} else {
					return err
				}
			}
		}
	}

	return nil
}

func (ch *CacheHandler) GetMulti(ctx context.Context, keys []datastore.Key) ([]*storagecache.CacheItem, error) {

	ch.Logf(ctx, "dsmiddleware/aememcache.GetMulti: incoming len=%d", len(keys))

	resultList := make([]*storagecache.CacheItem, len(keys))

	cacheKeys := make([]string, 0, len(keys))
	for _, key := range keys {
		cacheKeys = append(cacheKeys, ch.cacheKey(key))
	}

	itemMap, err := memcache.GetMulti(ctx, cacheKeys)

	if err != nil {
		ch.Logf(ctx, "dsmiddleware/aememcache: error on memcache.GetMulti %s", err.Error())
		if ch.raiseMemcacheError {
			if merr, ok := err.(appengine.MultiError); ok {
				for _, err := range merr {
					if err == nil || err == memcache.ErrCacheMiss {
						continue
					}
					return nil, datastore.MultiError(merr)
				}
			} else {
				return nil, err
			}
		}
		return resultList, nil
	}

	hit, miss := 0, 0
	for idx, key := range keys {
		item, ok := itemMap[ch.cacheKey(key)]
		if !ok {
			resultList[idx] = nil
			miss++
			continue
		}
		buf := bytes.NewBuffer(item.Value)
		dec := gob.NewDecoder(buf)
		var ps datastore.PropertyList
		err = dec.Decode(&ps)
		if err != nil {
			resultList[idx] = nil
			ch.Logf(ctx, "dsmiddleware/aememcache.GetMulti: gob.Decode error key=%s err=%s", key.String(), err.Error())
			miss++
			continue
		}

		resultList[idx] = &storagecache.CacheItem{
			Key:          key,
			PropertyList: ps,
		}
		hit++
	}

	ch.Logf(ctx, "dsmiddleware/aememcache.GetMulti: hit=%d miss=%d", hit, miss)

	return resultList, nil
}

func (ch *CacheHandler) DeleteMulti(ctx context.Context, keys []datastore.Key) error {
	ch.Logf(ctx, "dsmiddleware/aememcache.DeleteMulti: incoming len=%d", len(keys))

	cacheKeys := make([]string, 0, len(keys))
	for _, key := range keys {
		cacheKeys = append(cacheKeys, ch.cacheKey(key))
	}

	err := memcache.DeleteMulti(ctx, cacheKeys)
	if err != nil {
		ch.Logf(ctx, "dsmiddleware/aememcache: error on memcache.DeleteMulti %s", err.Error())
		if ch.raiseMemcacheError {
			if merr, ok := err.(appengine.MultiError); ok {
				for _, err := range merr {
					if err == nil || err == memcache.ErrCacheMiss {
						continue
					}
					return merr
				}
			} else {
				return err
			}
		}
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
