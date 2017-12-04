package aememcache

import (
	"bytes"
	"context"
	"encoding/gob"
	"time"

	"go.mercari.io/datastore"
	"go.mercari.io/datastore/cache/storagecache"
	"google.golang.org/appengine"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/memcache"
)

var _ storagecache.Storage = &CacheHandler{}
var _ datastore.CacheStrategy = &CacheHandler{}

func New() *CacheHandler {
	ch := &CacheHandler{
		KeyPrefix:      "mercari:aememcache:",
		ExpireDuration: 0,
	}
	s := storagecache.New(ch)
	ch.st = s

	return ch
}

type CacheHandler struct {
	st                 datastore.CacheStrategy
	raiseMemcacheError bool
	KeyPrefix          string
	ExpireDuration     time.Duration
}

// storagecache.Storage implementation

func (ch *CacheHandler) cacheKey(key datastore.Key) string {
	return ch.KeyPrefix + key.Encode()
}

func (ch *CacheHandler) SetMulti(ctx context.Context, is []*storagecache.CacheItem) error {

	itemList := make([]*memcache.Item, 0, len(is))
	for _, ci := range is {
		if ci.Key.Incomplete() {
			continue
		}
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		err := enc.Encode(ci.PropertyList)
		if err != nil {
			return err
		}
		itemList = append(itemList, &memcache.Item{
			Key:        ch.cacheKey(ci.Key),
			Value:      buf.Bytes(),
			Expiration: ch.ExpireDuration,
		})
	}

	err := memcache.SetMulti(ctx, itemList)
	if err != nil {
		log.Infof(ctx, "cache/aememcache: error on memcache.SetMulti %s", err.Error())
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

		keys := make([]string, 0, len(is))
		for _, ci := range is {
			keys = append(keys, ci.Key.Encode())
		}
		err = memcache.DeleteMulti(ctx, keys)
		if err != nil {
			log.Infof(ctx, "cache/aememcache: error on memcache.DeleteMulti %s", err.Error())
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

	resultList := make([]*storagecache.CacheItem, len(keys))

	cacheKeys := make([]string, 0, len(keys))
	for _, key := range keys {
		cacheKeys = append(cacheKeys, ch.cacheKey(key))
	}

	itemMap, err := memcache.GetMulti(ctx, cacheKeys)
	if err != nil {
		log.Infof(ctx, "cache/aememcache: error on memcache.GetMulti %s", err.Error())
		if ch.raiseMemcacheError {
			if merr, ok := err.(appengine.MultiError); ok {
				for _, err := range merr {
					if err == nil || err == memcache.ErrCacheMiss {
						continue
					}
					return nil, merr
				}
			} else {
				return nil, err
			}
		}
		return resultList, nil
	}

	for idx, key := range keys {
		item, ok := itemMap[ch.cacheKey(key)]
		if !ok {
			resultList[idx] = nil
			continue
		}
		buf := bytes.NewBuffer(item.Value)
		dec := gob.NewDecoder(buf)
		var ps datastore.PropertyList
		err = dec.Decode(&ps)
		if err != nil {
			return nil, err
		}
		resultList[idx] = &storagecache.CacheItem{
			Key:          key,
			PropertyList: ps,
		}
	}

	return resultList, nil
}

func (ch *CacheHandler) DeleteMulti(ctx context.Context, keys []datastore.Key) error {

	cacheKeys := make([]string, 0, len(keys))
	for _, key := range keys {
		cacheKeys = append(cacheKeys, ch.cacheKey(key))
	}

	err := memcache.DeleteMulti(ctx, cacheKeys)
	if err != nil {
		log.Infof(ctx, "cache/aememcache: error on memcache.DeleteMulti %s", err.Error())
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
