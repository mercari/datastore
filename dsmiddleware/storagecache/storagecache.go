package storagecache

import (
	"context"
	"sync"

	"go.mercari.io/datastore"
)

var _ datastore.Middleware = &cacheHandler{}

func New(s Storage, opts *Options) datastore.Middleware {
	ch := &cacheHandler{
		s: s,
	}
	if opts != nil {
		ch.logf = opts.Logf
		ch.filters = opts.Filters
	}

	if ch.logf == nil {
		ch.logf = func(ctx context.Context, format string, args ...interface{}) {}
	}

	return ch
}

type Options struct {
	Logf    func(ctx context.Context, format string, args ...interface{})
	Filters []KeyFilter
}

type Storage interface {
	SetMulti(ctx context.Context, is []*CacheItem) error
	// GetMulti returns slice of CacheItem of the same length as Keys of the argument.
	// If not in the dsmiddleware, the value of the corresponding element is nil.
	GetMulti(ctx context.Context, keys []datastore.Key) ([]*CacheItem, error)
	DeleteMulti(ctx context.Context, keys []datastore.Key) error
}

type KeyFilter func(ctx context.Context, key datastore.Key) bool

type contextTx struct{}

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
	logf    func(ctx context.Context, format string, args ...interface{})
	filters []KeyFilter
}

func (ch *cacheHandler) target(ctx context.Context, key datastore.Key) bool {
	for _, f := range ch.filters {
		// If false comes back even once, it is not cached
		if !f(ctx, key) {
			return false
		}
	}

	return true
}

func (ch *cacheHandler) AllocateIDs(info *datastore.MiddlewareInfo, keys []datastore.Key) ([]datastore.Key, error) {
	return info.Next.AllocateIDs(info, keys)
}

func (ch *cacheHandler) PutMultiWithoutTx(info *datastore.MiddlewareInfo, keys []datastore.Key, psList []datastore.PropertyList) ([]datastore.Key, error) {
	keys, err := info.Next.PutMultiWithoutTx(info, keys, psList)
	if err != nil {
		return nil, err
	}

	cis := make([]*CacheItem, 0, len(keys))
	for idx, key := range keys {
		if key.Incomplete() {
			// 発生し得ないはずだが他のMiddlewareがやらかすかもしれないので
			continue
		} else if !ch.target(info.Context, key) {
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
		ch.logf(info.Context, "dsmiddleware/storagecache.GetMultiWithoutTx: error on storage.SetMulti err=%s", err.Error())
	}

	return keys, nil
}

func (ch *cacheHandler) PutMultiWithTx(info *datastore.MiddlewareInfo, keys []datastore.Key, psList []datastore.PropertyList) ([]datastore.PendingKey, error) {
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
		if !ch.target(info.Context, key) {
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

func (ch *cacheHandler) GetMultiWithoutTx(info *datastore.MiddlewareInfo, keys []datastore.Key, psList []datastore.PropertyList) error {
	// strategy summary
	//   When we have a dsmiddleware, don't get it.
	//   When we don't have a dsmiddleware, passes arguments to next strategy.

	// 最終的に各所からかき集めてきたdatastore.PropertyListを統合してpsListにする
	// 1. psListをkeysと同じ長さまで伸長し、任意の場所にindexアクセスできるようにする
	// 2. 全てのtargetであるkeysについてキャッシュに問い合わせをし、結果があった場合psListに代入する
	// 3. キャッシュに無かったものを後段に問い合わせる 結果があった場合psListに代入し、次回のためにキャッシュにも入れる

	// step 1
	for len(psList) < len(keys) {
		psList = append(psList, nil)
	}

	{ // step 2
		filteredIdxList := make([]int, 0, len(keys))
		filteredKey := make([]datastore.Key, 0, len(keys))
		for idx, key := range keys {
			if ch.target(info.Context, key) {
				filteredIdxList = append(filteredIdxList, idx)
				filteredKey = append(filteredKey, key)
			}
		}

		if len(filteredKey) != 0 {
			cis, err := ch.s.GetMulti(info.Context, filteredKey)
			if err != nil {
				ch.logf(info.Context, "dsmiddleware/storagecache.GetMultiWithoutTx: error on storage.GetMulti err=%s", err.Error())

				return info.Next.GetMultiWithoutTx(info, keys, psList)
			}

			for idx, ci := range cis {
				if ci != nil {
					baseIdx := filteredIdxList[idx]
					psList[baseIdx] = ci.PropertyList
				}
			}
		}
	}

	var errs []error
	{ // step 3
		missingIdxList := make([]int, 0, len(keys))
		missingKey := make([]datastore.Key, 0, len(keys))
		for idx, ps := range psList {
			if ps == nil {
				missingIdxList = append(missingIdxList, idx)
				missingKey = append(missingKey, keys[idx])
			}
		}

		if len(missingKey) != 0 {
			cis := make([]*CacheItem, 0, len(missingKey))

			missingPsList := make([]datastore.PropertyList, len(missingKey))
			err := info.Next.GetMultiWithoutTx(info, missingKey, missingPsList)
			if merr, ok := err.(datastore.MultiError); ok {
				errs = make([]error, len(keys))
				for idx, err := range merr {
					baseIdx := missingIdxList[idx]
					if err != nil {
						errs[baseIdx] = err
						continue
					}
					psList[baseIdx] = missingPsList[idx]
					if ch.target(info.Context, missingKey[idx]) {
						cis = append(cis, &CacheItem{
							Key:          missingKey[idx],
							PropertyList: missingPsList[idx],
						})
					}
				}
			} else if err != nil {
				return err
			} else {
				for idx := range missingKey {
					baseIdx := missingIdxList[idx]
					psList[baseIdx] = missingPsList[idx]
					if ch.target(info.Context, missingKey[idx]) {
						cis = append(cis, &CacheItem{
							Key:          missingKey[idx],
							PropertyList: missingPsList[idx],
						})
					}
				}
			}

			if len(cis) != 0 {
				err := ch.s.SetMulti(info.Context, cis)
				if err != nil {
					ch.logf(info.Context, "dsmiddleware/storagecache.GetMultiWithoutTx: error on storage.SetMulti err=%s", err.Error())
				}
			}
		}
	}

	if len(errs) != 0 {
		return datastore.MultiError(errs)
	}

	return nil
}

func (ch *cacheHandler) GetMultiWithTx(info *datastore.MiddlewareInfo, keys []datastore.Key, psList []datastore.PropertyList) error {
	err := info.Next.GetMultiWithTx(info, keys, psList)

	// strategy summary
	//   don't add to dsmiddleware in tx. It may be complicated and become a spot of bugs

	ch.m.Lock()
	defer ch.m.Unlock()

	txOpMap, ok := info.Context.Value(contextTx{}).(map[datastore.Transaction][]*TxOpLog)
	if !ok {
		txOpMap = make(map[datastore.Transaction][]*TxOpLog)
		info.Context = context.WithValue(info.Context, contextTx{}, txOpMap)
	}

	logs := txOpMap[info.Transaction]
	for _, key := range keys {
		if !ch.target(info.Context, key) {
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

func (ch *cacheHandler) DeleteMultiWithoutTx(info *datastore.MiddlewareInfo, keys []datastore.Key) error {
	err := info.Next.DeleteMultiWithoutTx(info, keys)

	filteredKeys := make([]datastore.Key, 0, len(keys))
	for _, key := range keys {
		if !ch.target(info.Context, key) {
			continue
		}

		filteredKeys = append(filteredKeys, key)
	}
	if len(filteredKeys) == 0 {
		return err
	}

	sErr := ch.s.DeleteMulti(info.Context, filteredKeys)
	if sErr != nil {
		ch.logf(info.Context, "dsmiddleware/storagecache.GetMultiWithoutTx: error on storage.DeleteMulti err=%s", err.Error())
	}

	return err
}

func (ch *cacheHandler) DeleteMultiWithTx(info *datastore.MiddlewareInfo, keys []datastore.Key) error {
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
		if !ch.target(info.Context, key) {
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

func (ch *cacheHandler) PostCommit(info *datastore.MiddlewareInfo, tx datastore.Transaction, commit datastore.Commit) error {

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
		if !ch.target(info.Context, key) {
			continue
		}

		filteredKeys = append(filteredKeys, key)
	}
	if len(filteredKeys) == 0 {
		return info.Next.PostCommit(info, tx, commit)
	}

	// don't pass txCtx to appengine.APICall
	// otherwise, `transaction context has expired` will be occur
	baseCtx := info.Client.Context()
	sErr := ch.s.DeleteMulti(baseCtx, filteredKeys)
	nErr := info.Next.PostCommit(info, tx, commit)
	if sErr != nil {
		ch.logf(info.Context, "dsmiddleware/storagecache.GetMultiWithoutTx: error on storage.DeleteMulti err=%s", sErr.Error())
	}

	return nErr
}

func (ch *cacheHandler) PostRollback(info *datastore.MiddlewareInfo, tx datastore.Transaction) error {
	ch.m.Lock()
	defer ch.m.Unlock()

	txOpMap, ok := info.Context.Value(contextTx{}).(map[datastore.Transaction][]*TxOpLog)
	if !ok {
		return info.Next.PostRollback(info, tx)
	}

	delete(txOpMap, tx)

	return info.Next.PostRollback(info, tx)
}

func (ch *cacheHandler) Run(info *datastore.MiddlewareInfo, q datastore.Query, qDump *datastore.QueryDump) datastore.Iterator {
	return info.Next.Run(info, q, qDump)
}

func (ch *cacheHandler) GetAll(info *datastore.MiddlewareInfo, q datastore.Query, qDump *datastore.QueryDump, psList *[]datastore.PropertyList) ([]datastore.Key, error) {
	return info.Next.GetAll(info, q, qDump, psList)
}

func (ch *cacheHandler) Next(info *datastore.MiddlewareInfo, q datastore.Query, qDump *datastore.QueryDump, iter datastore.Iterator, ps *datastore.PropertyList) (datastore.Key, error) {
	return info.Next.Next(info, q, qDump, iter, ps)
}

func (ch *cacheHandler) Count(info *datastore.MiddlewareInfo, q datastore.Query, qDump *datastore.QueryDump) (int, error) {
	return info.Next.Count(info, q, qDump)
}
