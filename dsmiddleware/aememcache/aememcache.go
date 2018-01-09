package aememcache

import (
	"bytes"
	"context"
	"encoding/gob"
	"time"

	"go.mercari.io/datastore"
	"go.mercari.io/datastore/dsmiddleware/storagecache"
	"google.golang.org/appengine"
	"google.golang.org/appengine/memcache"
)

var _ storagecache.Storage = &cacheHandler{}
var _ datastore.Middleware = &cacheHandler{}

func New(opts ...CacheOption) interface {
	datastore.Middleware
	storagecache.Storage
} {
	ch := &cacheHandler{
		stOpts: &storagecache.Options{},
	}

	for _, opt := range opts {
		opt.Apply(ch)
	}

	s := storagecache.New(ch, ch.stOpts)
	ch.Middleware = s

	if ch.logf == nil {
		ch.logf = func(ctx context.Context, format string, args ...interface{}) {}
	}
	if ch.cacheKey == nil {
		ch.cacheKey = func(key datastore.Key) string {
			return "mercari:aememcache:" + key.Encode()
		}
	}

	return ch
}

type cacheHandler struct {
	datastore.Middleware
	stOpts *storagecache.Options

	raiseMemcacheError bool
	expireDuration     time.Duration
	logf               func(ctx context.Context, format string, args ...interface{})
	cacheKey           func(key datastore.Key) string
}

type CacheOption interface {
	Apply(*cacheHandler)
}

func (ch *cacheHandler) SetMulti(ctx context.Context, cis []*storagecache.CacheItem) error {

	ch.logf(ctx, "dsmiddleware/aememcache.SetMulti: incoming len=%d", len(cis))

	itemList := make([]*memcache.Item, 0, len(cis))
	for _, ci := range cis {
		if ci.Key.Incomplete() {
			panic("incomplete key incoming")
		}
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		err := enc.Encode(ci.PropertyList)
		if err != nil {
			ch.logf(ctx, "dsmiddleware/aememcache.SetMulti: gob.Encode error key=%s err=%s", ci.Key.String(), err.Error())
			continue
		}
		itemList = append(itemList, &memcache.Item{
			Key:        ch.cacheKey(ci.Key),
			Value:      buf.Bytes(),
			Expiration: ch.expireDuration,
		})
	}

	ch.logf(ctx, "dsmiddleware/aememcache.SetMulti: len=%d", len(itemList))

	err := memcache.SetMulti(ctx, itemList)
	if err != nil {
		ch.logf(ctx, "dsmiddleware/aememcache: error on memcache.SetMulti %s", err.Error())
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
			ch.logf(ctx, "dsmiddleware/aememcache: error on memcache.DeleteMulti %s", err.Error())
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

func (ch *cacheHandler) GetMulti(ctx context.Context, keys []datastore.Key) ([]*storagecache.CacheItem, error) {

	ch.logf(ctx, "dsmiddleware/aememcache.GetMulti: incoming len=%d", len(keys))

	resultList := make([]*storagecache.CacheItem, len(keys))

	cacheKeys := make([]string, 0, len(keys))
	for _, key := range keys {
		cacheKeys = append(cacheKeys, ch.cacheKey(key))
	}

	itemMap, err := memcache.GetMulti(ctx, cacheKeys)

	if err != nil {
		ch.logf(ctx, "dsmiddleware/aememcache: error on memcache.GetMulti %s", err.Error())
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
			ch.logf(ctx, "dsmiddleware/aememcache.GetMulti: gob.Decode error key=%s err=%s", key.String(), err.Error())
			miss++
			continue
		}

		resultList[idx] = &storagecache.CacheItem{
			Key:          key,
			PropertyList: ps,
		}
		hit++
	}

	ch.logf(ctx, "dsmiddleware/aememcache.GetMulti: hit=%d miss=%d", hit, miss)

	return resultList, nil
}

func (ch *cacheHandler) DeleteMulti(ctx context.Context, keys []datastore.Key) error {
	ch.logf(ctx, "dsmiddleware/aememcache.DeleteMulti: incoming len=%d", len(keys))

	cacheKeys := make([]string, 0, len(keys))
	for _, key := range keys {
		cacheKeys = append(cacheKeys, ch.cacheKey(key))
	}

	err := memcache.DeleteMulti(ctx, cacheKeys)
	if err != nil {
		ch.logf(ctx, "dsmiddleware/aememcache: error on memcache.DeleteMulti %s", err.Error())
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
