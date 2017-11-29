package localcache

import (
	"context"
	"sync"
	"time"

	"go.mercari.io/datastore"
)

var _ datastore.CacheStrategy = &CacheHandler{}

const defaultExpiration = 3 * time.Minute

func New() *CacheHandler {
	return &CacheHandler{
		ExpireDuration: defaultExpiration,
	}
}

type contextTx struct{}

type CacheHandler struct {
	cache map[string]cacheItem
	m     sync.Mutex

	ExpireDuration time.Duration
}

type cacheItem struct {
	Key          datastore.Key
	PropertyList datastore.PropertyList
	setAt        time.Time
	expiration   time.Duration
}

type txOps int

const (
	txPutOp txOps = iota
	txGetOp
	txDeleteOp
)

type txOpLog struct {
	Ops          txOps
	Key          datastore.Key
	PendingKey   datastore.PendingKey
	PropertyList datastore.PropertyList
}

func (ch *CacheHandler) PutMultiWithoutTx(info *datastore.CacheInfo, keys []datastore.Key, psList []datastore.PropertyList) ([]datastore.Key, error) {
	keys, err := info.Next.PutMultiWithoutTx(info, keys, psList)
	if err != nil {
		return nil, err
	}

	ch.m.Lock()
	defer ch.m.Unlock()

	if ch.cache == nil {
		ch.cache = make(map[string]cacheItem)
	}

	now := time.Now()
	for idx, key := range keys {
		cItem := cacheItem{
			Key:          key,
			PropertyList: psList[idx],
			setAt:        now,
			expiration:   ch.ExpireDuration,
		}
		ch.cache[key.Encode()] = cItem
	}

	return keys, nil
}

func (ch *CacheHandler) PutMultiWithTx(info *datastore.CacheInfo, keys []datastore.Key, psList []datastore.PropertyList) ([]datastore.PendingKey, error) {
	pKeys, err := info.Next.PutMultiWithTx(info, keys, psList)

	ch.m.Lock()
	defer ch.m.Unlock()

	txOpMap, ok := info.Context.Value(contextTx{}).(map[datastore.Transaction][]*txOpLog)
	if !ok {
		txOpMap = make(map[datastore.Transaction][]*txOpLog)
		info.Context = context.WithValue(info.Context, contextTx{}, txOpMap)
	}

	logs := txOpMap[info.Transaction]
	for idx, key := range keys {
		log := &txOpLog{
			Ops:          txPutOp,
			PropertyList: psList[idx],
		}
		if key.Incomplete() {
			log.PendingKey = pKeys[idx]
		} else {
			log.Key = key
		}
		logs = append(logs, log)
	}
	txOpMap[info.Transaction] = logs

	return pKeys, err
}

func (ch *CacheHandler) GetMultiWithoutTx(info *datastore.CacheInfo, keys []datastore.Key, psList []datastore.PropertyList) error {

	ch.m.Lock()
	// don't defer Unlock(). avoid crossing call info.Next.*

	if ch.cache == nil {
		ch.cache = make(map[string]cacheItem)
	}

	// strategy summary
	//   When we have a cache, don't get it.
	//   When we don't have a cache, passes arguments to next strategy.

	foundKeys := make([]datastore.Key, len(keys))
	missingKeys := make([]datastore.Key, 0, len(keys))
	replaceLaters := make([]func(ps datastore.PropertyList), 0)
	resultPsList := make([]datastore.PropertyList, len(keys))
	for idx, key := range keys {
		keyStr := key.Encode()
		if cItem, ok := ch.cache[keyStr]; ok && !key.Incomplete() && cItem.valid() {
			foundKeys[idx] = key
			resultPsList[idx] = cItem.PropertyList
		} else {
			delete(ch.cache, keyStr)
			missingKeys = append(missingKeys, key)
			idx := idx
			replaceLaters = append(replaceLaters, func(ps datastore.PropertyList) {
				resultPsList[idx] = ps
			})
		}
	}

	ch.m.Unlock()

	if len(missingKeys) == 0 {
		copy(resultPsList, psList)

		return nil
	}

	missingPsList := make([]datastore.PropertyList, 0, len(missingKeys))
	err := info.Next.GetMultiWithoutTx(info, missingKeys, missingPsList)
	if err != nil {
		ch.m.Lock()
		defer ch.m.Unlock()

		for _, key := range foundKeys {
			delete(ch.cache, key.Encode())
		}
		return err
	}
	for idx, ps := range missingPsList {
		replaceLaters[idx](ps)
	}
	copy(resultPsList, psList)

	return nil
}

func (ch *CacheHandler) GetMultiWithTx(info *datastore.CacheInfo, keys []datastore.Key, psList []datastore.PropertyList) error {
	err := info.Next.GetMultiWithTx(info, keys, psList)

	// strategy summary
	//   don't add to cache in tx. It may be complicated and become a spot of bugs

	ch.m.Lock()
	defer ch.m.Unlock()

	txOpMap, ok := info.Context.Value(contextTx{}).(map[datastore.Transaction][]*txOpLog)
	if !ok {
		txOpMap = make(map[datastore.Transaction][]*txOpLog)
		info.Context = context.WithValue(info.Context, contextTx{}, txOpMap)
	}

	logs := txOpMap[info.Transaction]
	for _, key := range keys {
		log := &txOpLog{
			Ops: txGetOp,
			Key: key,
		}
		logs = append(logs, log)
	}
	txOpMap[info.Transaction] = logs

	return err
}

func (ch *CacheHandler) DeleteMultiWithoutTx(info *datastore.CacheInfo, keys []datastore.Key) error {
	err := info.Next.DeleteMultiWithoutTx(info, keys)

	ch.m.Lock()
	defer ch.m.Unlock()

	if ch.cache == nil {
		ch.cache = make(map[string]cacheItem)
	}

	for _, key := range keys {
		delete(ch.cache, key.Encode())
	}

	return err
}

func (ch *CacheHandler) DeleteMultiWithTx(info *datastore.CacheInfo, keys []datastore.Key) error {
	err := info.Next.DeleteMultiWithTx(info, keys)

	ch.m.Lock()
	defer ch.m.Unlock()

	txOpMap, ok := info.Context.Value(contextTx{}).(map[datastore.Transaction][]*txOpLog)
	if !ok {
		txOpMap = make(map[datastore.Transaction][]*txOpLog)
		info.Context = context.WithValue(info.Context, contextTx{}, txOpMap)
	}

	logs := txOpMap[info.Transaction]
	for _, key := range keys {
		log := &txOpLog{
			Ops: txDeleteOp,
			Key: key,
		}
		logs = append(logs, log)
	}
	txOpMap[info.Transaction] = logs

	return err
}

func (ch *CacheHandler) PostCommit(info *datastore.CacheInfo, tx datastore.Transaction, commit datastore.Commit) error {

	ch.m.Lock()
	defer ch.m.Unlock()

	txOpMap, ok := info.Context.Value(contextTx{}).(map[datastore.Transaction][]*txOpLog)
	if !ok {
		return info.Next.PostCommit(info, tx, commit)
	}

	logs := txOpMap[tx]

	for _, log := range logs {
		switch log.Ops {
		case txPutOp:
			key := log.Key
			if log.PendingKey != nil {
				key = commit.Key(log.PendingKey)
			}
			delete(ch.cache, key.Encode())

		case txGetOp:
			delete(ch.cache, log.Key.Encode())

		case txDeleteOp:
			delete(ch.cache, log.Key.Encode())

		}
	}

	delete(txOpMap, tx)

	return info.Next.PostCommit(info, tx, commit)
}

func (ch *CacheHandler) PostRollback(info *datastore.CacheInfo, tx datastore.Transaction) error {
	ch.m.Lock()
	defer ch.m.Unlock()

	txOpMap, ok := info.Context.Value(contextTx{}).(map[datastore.Transaction][]*txOpLog)
	if !ok {
		return info.Next.PostRollback(info, tx)
	}

	delete(txOpMap, tx)

	return info.Next.PostRollback(info, tx)
}

func (ch *CacheHandler) Run(info *datastore.CacheInfo, q datastore.Query, qDump *datastore.QueryDump) datastore.Iterator {
	return info.Next.Run(info, q, qDump)
}

func (ch *CacheHandler) GetAll(info *datastore.CacheInfo, q datastore.Query, qDump *datastore.QueryDump, psList *[]datastore.PropertyList) ([]datastore.Key, error) {
	return info.Next.GetAll(info, q, qDump, psList)
}

func (ch *CacheHandler) Next(info *datastore.CacheInfo, q datastore.Query, qDump *datastore.QueryDump, iter datastore.Iterator, ps *datastore.PropertyList) (datastore.Key, error) {
	return info.Next.Next(info, q, qDump, iter, ps)
}

func (cItem *cacheItem) valid() bool {
	now := time.Now()
	return cItem.setAt.Add(cItem.expiration).After(now)
}
