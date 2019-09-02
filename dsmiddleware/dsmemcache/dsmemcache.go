package dsmemcache

import (
	"bytes"
	"context"
	"encoding/gob"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"go.mercari.io/datastore"
	"go.mercari.io/datastore/dsmiddleware/storagecache"
)

var _ storagecache.Storage = &cacheHandler{}
var _ datastore.Middleware = &cacheHandler{}

// New dsmemcache middleware creates & returns.
func New(client *memcache.Client, opts ...CacheOption) interface {
	datastore.Middleware
	storagecache.Storage
} {
	ch := &cacheHandler{
		client: client,
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
			return "mercari:dsmemcache:" + key.Encode()
		}
	}

	return ch
}

type cacheHandler struct {
	datastore.Middleware
	stOpts *storagecache.Options

	client         *memcache.Client
	expireDuration time.Duration
	logf           func(ctx context.Context, format string, args ...interface{})
	cacheKey       func(key datastore.Key) string
}

// A CacheOption is an cache option for a dsmemcache middleware.
type CacheOption interface {
	Apply(*cacheHandler)
}

func (ch *cacheHandler) SetMulti(ctx context.Context, cis []*storagecache.CacheItem) error {

	ch.logf(ctx, "dsmiddleware/dsmemcache.SetMulti: incoming len=%d", len(cis))

	for _, ci := range cis {
		if ci.Key.Incomplete() {
			panic("incomplete key incoming")
		}
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		if err := enc.Encode(ci.PropertyList); err != nil {
			ch.logf(ctx, "dsmiddleware/dsmemcache.SetMulti: gob.Encode error key=%s err=%s", ci.Key.String(), err.Error())
			continue
		}
		item := &memcache.Item{
			Key:        ch.cacheKey(ci.Key),
			Value:      buf.Bytes(),
			Expiration: int32(ch.expireDuration.Seconds()),
		}
		if err := ch.client.Set(item); err != nil {
			return err
		}
	}

	return nil
}

func (ch *cacheHandler) GetMulti(ctx context.Context, keys []datastore.Key) ([]*storagecache.CacheItem, error) {
	ch.logf(ctx, "dsmiddleware/dsmemcache.GetMulti: incoming len=%d", len(keys))

	resultList := make([]*storagecache.CacheItem, len(keys))

	cacheKeys := make([]string, 0, len(keys))
	for _, key := range keys {
		cacheKeys = append(cacheKeys, ch.cacheKey(key))
	}
	itemMap, err := ch.client.GetMulti(cacheKeys)

	if err != nil {
		ch.logf(ctx, "dsmiddleware/dsmemcache: error on dsmemcache.GetMulti %s", err.Error())
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
			ch.logf(ctx, "dsmiddleware/dsmemcache.GetMulti: gob.Decode error key=%s err=%s", key.String(), err.Error())
			miss++
			continue
		}

		resultList[idx] = &storagecache.CacheItem{
			Key:          key,
			PropertyList: ps,
		}
		hit++
	}

	ch.logf(ctx, "dsmiddleware/dsmemcache.GetMulti: hit=%d miss=%d", hit, miss)

	return resultList, nil
}

func (ch *cacheHandler) DeleteMulti(ctx context.Context, keys []datastore.Key) error {
	ch.logf(ctx, "dsmiddleware/dsmemcache.DeleteMulti: incoming len=%d", len(keys))
	for _, key := range keys {
		err := ch.client.Delete(ch.cacheKey(key))
		if err != nil {
			ch.logf(ctx, "dsmiddleware/dsmemcache: error on dsmemcache.DeleteMulti %s", err.Error())
		}
	}

	return nil
}
