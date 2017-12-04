package storagecache

import (
	"context"
	"sync"

	"go.mercari.io/datastore"
)

var _ datastore.CacheStrategy = &cacheHandler{}

func New(s Storage, opts ...CacheOption) datastore.CacheStrategy {
	ch := &cacheHandler{
		s: s,
	}

	for _, opt := range opts {
		opt.Apply(ch)
	}

	return ch
}

type contextTx struct{}

type Storage interface {
	SetMulti(ctx context.Context, is []*CacheItem) error
	// GetMulti returns slice of CacheItem of the same length as Keys of the argument.
	// If not in the cache, the value of the corresponding element is nil.
	GetMulti(ctx context.Context, keys []datastore.Key) ([]*CacheItem, error)
	DeleteMulti(ctx context.Context, keys []datastore.Key) error
}

type CacheOption interface {
	Apply(*cacheHandler)
}

type CacheItem struct {
	Key          datastore.Key
	PropertyList datastore.PropertyList
}

type TxOps int

const (
	TxPutOp TxOps = iota
	TxGetOp
	TxDeleteOp
)

type TxOpLog struct {
	Ops          TxOps
	Key          datastore.Key
	PendingKey   datastore.PendingKey
	PropertyList datastore.PropertyList
}

type cacheHandler struct {
	s       Storage
	m       sync.Mutex
	filters []func(key datastore.Key) bool
}

func (ch *cacheHandler) target(key datastore.Key) bool {
	for _, f := range ch.filters {
		// If false comes back even once, it is not cached
		if !f(key) {
			return false
		}
	}

	return true
}

func (ch *cacheHandler) PutMultiWithoutTx(info *datastore.CacheInfo, keys []datastore.Key, psList []datastore.PropertyList) ([]datastore.Key, error) {
	keys, err := info.Next.PutMultiWithoutTx(info, keys, psList)
	if err != nil {
		return nil, err
	}

	ch.m.Lock()
	defer ch.m.Unlock()

	cis := make([]*CacheItem, 0, len(keys))
	for idx, key := range keys {
		if !ch.target(key) {
			continue
		}
		cis = append(cis, &CacheItem{
			Key:          key,
			PropertyList: psList[idx],
		})
	}
	if len(cis) == 0 {
		return keys, nil
	}
	err = ch.s.SetMulti(info.Context, cis)
	if err != nil {
		return nil, err
	}

	return keys, nil
}

func (ch *cacheHandler) PutMultiWithTx(info *datastore.CacheInfo, keys []datastore.Key, psList []datastore.PropertyList) ([]datastore.PendingKey, error) {
	pKeys, err := info.Next.PutMultiWithTx(info, keys, psList)

	ch.m.Lock()
	defer ch.m.Unlock()

	txOpMap, ok := info.Context.Value(contextTx{}).(map[datastore.Transaction][]*TxOpLog)
	if !ok {
		txOpMap = make(map[datastore.Transaction][]*TxOpLog)
		info.Context = context.WithValue(info.Context, contextTx{}, txOpMap)
	}

	logs := txOpMap[info.Transaction]
	for idx, key := range keys {
		if !ch.target(key) {
			continue
		}
		log := &TxOpLog{
			Ops:          TxPutOp,
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

func (ch *cacheHandler) GetMultiWithoutTx(info *datastore.CacheInfo, keys []datastore.Key, psList []datastore.PropertyList) error {

	ch.m.Lock()
	// don't use defer Unlock(). avoid crossing call info.Next.*

	// strategy summary
	//   When we have a cache, don't get it.
	//   When we don't have a cache, passes arguments to next strategy.

	// 1. ch.target で判定したキャッシュを参照しないでよいKeysをmissingKeysで後段の処理に回すことを確定
	// 2. キャッシュを参照するKeysについてストレージに問い合わせ
	// 3. キャッシュにあったら resultPsList に結果を突っ込む
	//    キャッシュになかったら missingKeys に合流
	// 4. missingKeys を後段の処理に回す
	// 5. 4の結果を resultPsList に合流させる

	foundKeys := make([]datastore.Key, len(keys))
	missingKeys := make([]datastore.Key, 0, len(keys))
	resultPsList := make([]datastore.PropertyList, len(keys))

	replaceLaters := make([]func(ps datastore.PropertyList), 0)

	filteredKey := make([]datastore.Key, 0, len(keys))
	filteredIndices := make([]int, 0, len(keys))
	filteredReplaceLaters := make([]func(ci *CacheItem), 0, len(keys))
	for idx, key := range keys {
		idx := idx

		if !ch.target(key) {
			missingKeys = append(missingKeys, key)
			replaceLaters = append(replaceLaters, func(ps datastore.PropertyList) {
				resultPsList[idx] = ps
			})

			continue
		}

		filteredKey = append(filteredKey, key)
		filteredIndices = append(filteredIndices, idx)
		filteredReplaceLaters = append(filteredReplaceLaters, func(ci *CacheItem) {
			foundKeys[idx] = ci.Key
			resultPsList[idx] = ci.PropertyList
		})
	}

	if len(filteredKey) != 0 {
		is, err := ch.s.GetMulti(info.Context, filteredKey)
		if err != nil {
			return err
		}
		for idx, ci := range is {
			if ci != nil {
				filteredReplaceLaters[idx](ci)
				continue
			}

			idx := idx
			missingKeys = append(missingKeys, filteredKey[idx])
			replaceLaters = append(replaceLaters, func(ps datastore.PropertyList) {
				resultPsList[filteredIndices[idx]] = ps
			})
		}
	}

	ch.m.Unlock()

	if len(missingKeys) == 0 {
		copy(psList, resultPsList)

		return nil
	}

	missingPsList := make([]datastore.PropertyList, len(missingKeys))
	err := info.Next.GetMultiWithoutTx(info, missingKeys, missingPsList)
	if err != nil {
		ch.m.Lock()
		defer ch.m.Unlock()

		// ignore returned error
		deleteKeys := make([]datastore.Key, 0, len(foundKeys))
		for _, key := range foundKeys {
			if key == nil {
				continue
			}
			deleteKeys = append(deleteKeys, key)
		}
		ch.s.DeleteMulti(info.Context, deleteKeys)

		return err
	}
	for idx, ps := range missingPsList {
		replaceLaters[idx](ps)
	}
	copy(psList, resultPsList)

	return nil
}

func (ch *cacheHandler) GetMultiWithTx(info *datastore.CacheInfo, keys []datastore.Key, psList []datastore.PropertyList) error {
	err := info.Next.GetMultiWithTx(info, keys, psList)

	// strategy summary
	//   don't add to cache in tx. It may be complicated and become a spot of bugs

	ch.m.Lock()
	defer ch.m.Unlock()

	txOpMap, ok := info.Context.Value(contextTx{}).(map[datastore.Transaction][]*TxOpLog)
	if !ok {
		txOpMap = make(map[datastore.Transaction][]*TxOpLog)
		info.Context = context.WithValue(info.Context, contextTx{}, txOpMap)
	}

	logs := txOpMap[info.Transaction]
	for _, key := range keys {
		if !ch.target(key) {
			continue
		}
		log := &TxOpLog{
			Ops: TxGetOp,
			Key: key,
		}
		logs = append(logs, log)
	}
	txOpMap[info.Transaction] = logs

	return err
}

func (ch *cacheHandler) DeleteMultiWithoutTx(info *datastore.CacheInfo, keys []datastore.Key) error {
	err := info.Next.DeleteMultiWithoutTx(info, keys)

	ch.m.Lock()
	defer ch.m.Unlock()

	filteredKeys := make([]datastore.Key, 0, len(keys))
	for _, key := range keys {
		if !ch.target(key) {
			continue
		}

		filteredKeys = append(filteredKeys, key)
	}
	if len(filteredKeys) == 0 {
		return err
	}

	sErr := ch.s.DeleteMulti(info.Context, filteredKeys)
	if sErr != nil {
		return sErr
	}

	return err
}

func (ch *cacheHandler) DeleteMultiWithTx(info *datastore.CacheInfo, keys []datastore.Key) error {
	err := info.Next.DeleteMultiWithTx(info, keys)

	ch.m.Lock()
	defer ch.m.Unlock()

	txOpMap, ok := info.Context.Value(contextTx{}).(map[datastore.Transaction][]*TxOpLog)
	if !ok {
		txOpMap = make(map[datastore.Transaction][]*TxOpLog)
		info.Context = context.WithValue(info.Context, contextTx{}, txOpMap)
	}

	logs := txOpMap[info.Transaction]
	for _, key := range keys {
		if !ch.target(key) {
			continue
		}

		log := &TxOpLog{
			Ops: TxDeleteOp,
			Key: key,
		}
		logs = append(logs, log)
	}
	txOpMap[info.Transaction] = logs

	return err
}

func (ch *cacheHandler) PostCommit(info *datastore.CacheInfo, tx datastore.Transaction, commit datastore.Commit) error {

	ch.m.Lock()
	defer ch.m.Unlock()

	txOpMap, ok := info.Context.Value(contextTx{}).(map[datastore.Transaction][]*TxOpLog)
	if !ok {
		return info.Next.PostCommit(info, tx, commit)
	}

	logs := txOpMap[tx]

	keys := make([]datastore.Key, len(logs))
	for idx, log := range logs {
		switch log.Ops {
		case TxPutOp:
			key := log.Key
			if log.PendingKey != nil {
				key = commit.Key(log.PendingKey)
			}
			keys[idx] = key

		case TxGetOp, TxDeleteOp:
			keys[idx] = log.Key
		}
	}

	filteredKeys := make([]datastore.Key, 0, len(keys))
	for _, key := range keys {
		if !ch.target(key) {
			continue
		}

		filteredKeys = append(filteredKeys, key)
	}
	if len(filteredKeys) == 0 {
		return info.Next.PostCommit(info, tx, commit)
	}

	// don't pass txCtx to appengine.APICall
	// otherwise, `transaction context has expired` will be occur
	baseCtx := info.Client.SwapContext(context.Background())
	info.Client.SwapContext(baseCtx)
	sErr := ch.s.DeleteMulti(baseCtx, filteredKeys)
	nErr := info.Next.PostCommit(info, tx, commit)
	if sErr != nil {
		return sErr
	}

	return nErr
}

func (ch *cacheHandler) PostRollback(info *datastore.CacheInfo, tx datastore.Transaction) error {
	ch.m.Lock()
	defer ch.m.Unlock()

	txOpMap, ok := info.Context.Value(contextTx{}).(map[datastore.Transaction][]*TxOpLog)
	if !ok {
		return info.Next.PostRollback(info, tx)
	}

	delete(txOpMap, tx)

	return info.Next.PostRollback(info, tx)
}

func (ch *cacheHandler) Run(info *datastore.CacheInfo, q datastore.Query, qDump *datastore.QueryDump) datastore.Iterator {
	return info.Next.Run(info, q, qDump)
}

func (ch *cacheHandler) GetAll(info *datastore.CacheInfo, q datastore.Query, qDump *datastore.QueryDump, psList *[]datastore.PropertyList) ([]datastore.Key, error) {
	return info.Next.GetAll(info, q, qDump, psList)
}

func (ch *cacheHandler) Next(info *datastore.CacheInfo, q datastore.Query, qDump *datastore.QueryDump, iter datastore.Iterator, ps *datastore.PropertyList) (datastore.Key, error) {
	return info.Next.Next(info, q, qDump, iter, ps)
}
