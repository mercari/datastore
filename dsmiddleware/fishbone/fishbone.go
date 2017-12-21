package fishbone

import (
	"context"
	"errors"

	"go.mercari.io/datastore"
)

// fishbone?
// https://www.google.co.jp/search?q=%E9%AD%9A%E3%81%AE%E9%A3%9F%E3%81%B9%E6%96%B9+%E8%83%8C%E9%AA%A8&tbm=isch
//
// Recommend: don't use this strategy in production.
// You should implement KeysOnly + GetMulti strategy in YOUR application.
// Because, SingleGet is executed every time when Iterator#Next called. It's too slow even memcache access.

// TODO アプリケーション側で実装する場合のベストプラクティス的コードをどこかに作成してリンクを置く

var _ datastore.Middleware = &modifier{}

func New() datastore.Middleware {
	return &modifier{}
}

type contextQuery struct{}

type modifier struct {
}

func (m *modifier) AllocateIDs(info *datastore.MiddlewareInfo, keys []datastore.Key) ([]datastore.Key, error) {
	return info.Next.AllocateIDs(info, keys)
}

func (m *modifier) PutMultiWithoutTx(info *datastore.MiddlewareInfo, keys []datastore.Key, psList []datastore.PropertyList) ([]datastore.Key, error) {
	return info.Next.PutMultiWithoutTx(info, keys, psList)
}

func (m *modifier) PutMultiWithTx(info *datastore.MiddlewareInfo, keys []datastore.Key, psList []datastore.PropertyList) ([]datastore.PendingKey, error) {
	return info.Next.PutMultiWithTx(info, keys, psList)
}

func (m *modifier) GetMultiWithoutTx(info *datastore.MiddlewareInfo, keys []datastore.Key, psList []datastore.PropertyList) error {
	return info.Next.GetMultiWithoutTx(info, keys, psList)
}

func (m *modifier) GetMultiWithTx(info *datastore.MiddlewareInfo, keys []datastore.Key, psList []datastore.PropertyList) error {
	return info.Next.GetMultiWithTx(info, keys, psList)
}

func (m *modifier) DeleteMultiWithoutTx(info *datastore.MiddlewareInfo, keys []datastore.Key) error {
	return info.Next.DeleteMultiWithoutTx(info, keys)
}

func (m *modifier) DeleteMultiWithTx(info *datastore.MiddlewareInfo, keys []datastore.Key) error {
	return info.Next.DeleteMultiWithTx(info, keys)
}

func (m *modifier) PostCommit(info *datastore.MiddlewareInfo, tx datastore.Transaction, commit datastore.Commit) error {
	return info.Next.PostCommit(info, tx, commit)
}

func (m *modifier) PostRollback(info *datastore.MiddlewareInfo, tx datastore.Transaction) error {
	return info.Next.PostRollback(info, tx)
}

func (m *modifier) Run(info *datastore.MiddlewareInfo, q datastore.Query, qDump *datastore.QueryDump) datastore.Iterator {
	if qDump.KeysOnly {
		return info.Next.Run(info, q, qDump)
	}

	q = q.KeysOnly()
	qDump.KeysOnly = true
	defer func() {
		qDump.KeysOnly = false
	}()

	mQDumpMap, ok := info.Context.Value(contextQuery{}).(map[*modifier]map[*datastore.QueryDump]bool)
	if !ok {
		mQDumpMap = make(map[*modifier]map[*datastore.QueryDump]bool)
		info.Context = context.WithValue(info.Context, contextQuery{}, mQDumpMap)
	}
	_, ok = mQDumpMap[m]
	if !ok {
		mQDumpMap[m] = make(map[*datastore.QueryDump]bool)
	}
	mQDumpMap[m][qDump] = true

	return info.Next.Run(info, q, qDump)
}

func (m *modifier) GetAll(info *datastore.MiddlewareInfo, q datastore.Query, qDump *datastore.QueryDump, psList *[]datastore.PropertyList) ([]datastore.Key, error) {
	if qDump.KeysOnly {
		return info.Next.GetAll(info, q, qDump, psList)
	}

	q = q.KeysOnly()
	qDump.KeysOnly = true
	defer func() {
		qDump.KeysOnly = false
	}()

	keys, err := info.Next.GetAll(info, q, qDump, nil)
	if err != nil {
		return nil, err
	}

	if qDump.Transaction != nil {
		if info.Transaction == nil {
			return nil, errors.New("cacheInfo.Transaction is required")
		}

		altPsList := make([]datastore.PropertyList, len(keys))
		err := qDump.Transaction.GetMulti(keys, altPsList)
		if err != nil {
			return nil, err
		}
		*psList = altPsList

		return keys, nil
	}

	altPsList := make([]datastore.PropertyList, len(keys))
	err = info.Client.GetMulti(info.Context, keys, altPsList)
	if err != nil {
		return nil, err
	}
	*psList = altPsList

	return keys, nil
}

func (m *modifier) Next(info *datastore.MiddlewareInfo, q datastore.Query, qDump *datastore.QueryDump, iter datastore.Iterator, ps *datastore.PropertyList) (datastore.Key, error) {
	mQDumpMap, ok := info.Context.Value(contextQuery{}).(map[*modifier]map[*datastore.QueryDump]bool)
	if !ok {
		return info.Next.Next(info, q, qDump, iter, ps)
	}

	qDumpMap, ok := mQDumpMap[m]
	if !ok {
		return info.Next.Next(info, q, qDump, iter, ps)
	}

	_, ok = qDumpMap[qDump]
	if !ok {
		return info.Next.Next(info, q, qDump, iter, ps)
	}

	// NOTE: We can't delete(qDumpMap, qDump). q is reusable!

	key, err := info.Next.Next(info, q, qDump, iter, ps)

	if err != nil {
		return nil, err
	}

	if qDump.Transaction != nil {
		if info.Transaction == nil {
			return nil, errors.New("cacheInfo.Transaction is required")
		}

		altPs := datastore.PropertyList{}
		err := qDump.Transaction.Get(key, &altPs)
		if err != nil {
			return nil, err
		}
		*ps = altPs

		return key, nil
	}

	altPs := datastore.PropertyList{}
	err = info.Client.Get(info.Context, key, &altPs)
	if err != nil {
		return nil, err
	}
	*ps = altPs

	return key, nil
}

func (m *modifier) Count(info *datastore.MiddlewareInfo, q datastore.Query, qDump *datastore.QueryDump) (int, error) {
	return info.Next.Count(info, q, qDump)
}
