package rediscache

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"time"

	"github.com/garyburd/redigo/redis"
	"go.mercari.io/datastore"
	"go.mercari.io/datastore/dsmiddleware/storagecache"
)

var _ storagecache.Storage = &cacheHandler{}
var _ datastore.Middleware = &cacheHandler{}

const defaultExpiration = 15 * time.Minute

func New(conn redis.Conn, opts ...CacheOption) interface {
	datastore.Middleware
	storagecache.Storage
} {

	// I want to make ch.dsmiddleware accessible from the test
	ch := &cacheHandler{
		conn:           conn,
		stOpts:         &storagecache.Options{},
		expireDuration: defaultExpiration,
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
			return "mercari:rediscache:" + key.Encode()
		}
	}

	return ch
}

type cacheHandler struct {
	datastore.Middleware
	stOpts *storagecache.Options

	conn           redis.Conn
	expireDuration time.Duration
	logf           func(ctx context.Context, format string, args ...interface{})
	cacheKey       func(key datastore.Key) string
}

type CacheOption interface {
	Apply(*cacheHandler)
}

func (ch *cacheHandler) SetMulti(ctx context.Context, cis []*storagecache.CacheItem) error {

	ch.logf(ctx, "dsmiddleware/rediscache.SetMulti: incoming len=%d", len(cis))

	err := ch.conn.Send("MULTI")
	if err != nil {
		ch.logf(ctx, `dsmiddleware/rediscache.SetMulti: conn.Send("MULTI") err=%s`, err.Error())
		return err
	}

	cnt := 0
	for _, ci := range cis {
		if ci.Key.Incomplete() {
			panic("incomplete key incoming")
		}
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		err := enc.Encode(ci.PropertyList)
		if err != nil {
			ch.logf(ctx, "dsmiddleware/rediscache.SetMulti: gob.Encode error key=%s err=%s", ci.Key.String(), err.Error())
			continue
		}
		cacheKey := ch.cacheKey(ci.Key)
		cacheValue := buf.Bytes()

		if ch.expireDuration <= 0 {
			err = ch.conn.Send("SET", cacheKey, cacheValue)
			if err != nil {
				ch.logf(ctx, `dsmiddleware/rediscache.SetMulti: conn.Send("SET", "%s", ...) err=%s`, cacheKey, err.Error())
				return err
			}
		} else {
			err = ch.conn.Send("SET", cacheKey, cacheValue, "PX", int64(ch.expireDuration/time.Millisecond))
			if err != nil {
				ch.logf(ctx, `dsmiddleware/rediscache.SetMulti: conn.Send("SET", "%s", ..., "PX", %d) err=%s`, cacheKey, ch.expireDuration/time.Millisecond, err.Error())
				return err
			}
		}
		cnt++
	}

	ch.logf(ctx, "dsmiddleware/rediscache.SetMulti: len=%d", cnt)

	_, err = ch.conn.Do("EXEC")
	if err != nil {
		ch.logf(ctx, `dsmiddleware/rediscache.SetMulti: conn.Send("EXEC") err=%s`, err.Error())
		return err
	}

	return nil
}

func (ch *cacheHandler) GetMulti(ctx context.Context, keys []datastore.Key) ([]*storagecache.CacheItem, error) {

	ch.logf(ctx, "dsmiddleware/rediscache.GetMulti: incoming len=%d", len(keys))

	err := ch.conn.Send("MULTI")
	if err != nil {
		ch.logf(ctx, `dsmiddleware/rediscache.GetMulti: conn.Send("MULTI") err=%s`, err.Error())
		return nil, err
	}

	resultList := make([]*storagecache.CacheItem, len(keys))

	for idx, key := range keys {
		cacheKey := ch.cacheKey(key)
		resultList[idx] = &storagecache.CacheItem{
			Key: key,
		}
		err := ch.conn.Send("GET", cacheKey)
		if err != nil {
			ch.logf(ctx, `dsmiddleware/rediscache.GetMulti: conn.Send("GET", "%s") err=%s`, cacheKey, err.Error())
			return nil, err
		}
	}

	resp, err := ch.conn.Do("EXEC")
	if err != nil {
		ch.logf(ctx, `dsmiddleware/rediscache.GetMulti: conn.Do("EXEC") err=%s`, err.Error())
		return nil, err
	}
	bs, err := redis.ByteSlices(resp, nil)
	if err != nil {
		ch.logf(ctx, `dsmiddleware/rediscache.GetMulti: redis.ByteSlices err=%s`, err.Error())
		return nil, err
	}

	hit := 0
	miss := 0
	for idx, b := range bs {
		if len(b) == 0 {
			resultList[idx] = nil
			miss++
			continue
		}
		buf := bytes.NewBuffer(b)
		dec := gob.NewDecoder(buf)
		var ps datastore.PropertyList
		err = dec.Decode(&ps)
		if err != nil {
			resultList[idx] = nil
			ch.logf(ctx, "dsmiddleware/rediscache.GetMulti: gob.Decode error key=%s err=%s", keys[idx].String(), err.Error())
			miss++
			continue
		}

		if !resultList[idx].Key.Equal(keys[idx]) {
			ch.logf(ctx, "dsmiddleware/rediscache.GetMulti: key equality check failed")
			return nil, errors.New("dsmiddleware/rediscache.GetMulti: key equality check failed")
		}

		resultList[idx].PropertyList = ps
		hit++
	}

	ch.logf(ctx, "dsmiddleware/rediscache.GetMulti: hit=%d miss=%d", hit, miss)

	return resultList, nil
}

func (ch *cacheHandler) DeleteMulti(ctx context.Context, keys []datastore.Key) error {
	ch.logf(ctx, "dsmiddleware/rediscache.DeleteMulti: incoming len=%d", len(keys))

	err := ch.conn.Send("MULTI")
	if err != nil {
		ch.logf(ctx, `dsmiddleware/rediscache.DeleteMulti: conn.Send("MULTI") err=%s`, err.Error())
		return err
	}

	for _, key := range keys {
		cacheKey := ch.cacheKey(key)

		err = ch.conn.Send("DEL", cacheKey)
		if err != nil {
			ch.logf(ctx, `dsmiddleware/rediscache.DeleteMulti: conn.Send("DEL", "%s") err=%s`, cacheKey, err.Error())
			return err
		}
	}

	_, err = ch.conn.Do("EXEC")
	if err != nil {
		ch.logf(ctx, `dsmiddleware/rediscache.DeleteMulti: conn.Send("EXEC") err=%s`, err.Error())
		return err
	}

	return nil
}
