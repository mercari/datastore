package dslog

import (
	"context"
	"strings"
	"sync"

	"go.mercari.io/datastore"
)

var _ datastore.Middleware = &logger{}

func NewLogger(prefix string, logf func(ctx context.Context, format string, args ...interface{})) datastore.Middleware {
	return &logger{Prefix: prefix, Logf: logf, counter: 1}
}

type contextTx struct{}

type logger struct {
	Prefix string
	Logf   func(ctx context.Context, format string, args ...interface{})

	m       sync.Mutex
	counter int
}

type txPutEntity struct {
	Index      int
	Key        datastore.Key
	PendingKey datastore.PendingKey
}

func (l *logger) KeysToString(keys []datastore.Key) string {
	keyStrings := make([]string, 0, len(keys))
	for _, key := range keys {
		keyStrings = append(keyStrings, key.String())
	}

	return strings.Join(keyStrings, ", ")
}

func (l *logger) AllocateIDs(info *datastore.MiddlewareInfo, keys []datastore.Key) ([]datastore.Key, error) {
	l.m.Lock()
	cnt := l.counter
	l.counter += 1
	l.m.Unlock()

	l.Logf(info.Context, l.Prefix+"AllocateIDs #%d, len(keys)=%d, keys=[%s]", cnt, len(keys), l.KeysToString(keys))

	keys, err := info.Next.AllocateIDs(info, keys)

	if err == nil {
		l.Logf(info.Context, l.Prefix+"AllocateIDs #%d, keys=[%s]", cnt, l.KeysToString(keys))
	} else {
		l.Logf(info.Context, l.Prefix+"AllocateIDs #%d, err=%s", cnt, err.Error())
	}

	return keys, err
}

func (l *logger) PutMultiWithoutTx(info *datastore.MiddlewareInfo, keys []datastore.Key, psList []datastore.PropertyList) ([]datastore.Key, error) {
	l.m.Lock()
	cnt := l.counter
	l.counter += 1
	l.m.Unlock()

	l.Logf(info.Context, l.Prefix+"PutMultiWithoutTx #%d, len(keys)=%d, keys=[%s]", cnt, len(keys), l.KeysToString(keys))

	keys, err := info.Next.PutMultiWithoutTx(info, keys, psList)

	if err == nil {
		l.Logf(info.Context, l.Prefix+"PutMultiWithoutTx #%d, keys=[%s]", cnt, l.KeysToString(keys))
	} else {
		l.Logf(info.Context, l.Prefix+"PutMultiWithoutTx #%d, err=%s", cnt, err.Error())
	}

	return keys, err
}

func (l *logger) PutMultiWithTx(info *datastore.MiddlewareInfo, keys []datastore.Key, psList []datastore.PropertyList) ([]datastore.PendingKey, error) {
	l.m.Lock()
	cnt := l.counter
	l.counter += 1
	l.m.Unlock()

	l.Logf(info.Context, l.Prefix+"PutMultiWithTx #%d, len(keys)=%d, keys=[%s]", cnt, len(keys), l.KeysToString(keys))

	pKeys, err := info.Next.PutMultiWithTx(info, keys, psList)
	if err != nil {
		l.Logf(info.Context, l.Prefix+"PutMultiWithTx #%d, err=%s", cnt, err.Error())
	}
	if len(keys) != len(pKeys) {
		l.Logf(info.Context, l.Prefix+"PutMultiWithTx #%d, keys length mismatch len(keys)=%d, len(pKeys)=%d", cnt, len(keys), len(pKeys))
		return pKeys, err
	}

	lgTxPutMap, ok := info.Context.Value(contextTx{}).(map[*logger]map[datastore.Transaction][]*txPutEntity)
	if !ok {
		lgTxPutMap = make(map[*logger]map[datastore.Transaction][]*txPutEntity)
		info.Context = context.WithValue(info.Context, contextTx{}, lgTxPutMap)
	}
	txPutMap, ok := lgTxPutMap[l]
	if !ok {
		txPutMap = make(map[datastore.Transaction][]*txPutEntity)
		lgTxPutMap[l] = txPutMap
	}
	putLogs := txPutMap[info.Transaction]
	for idx, key := range keys {
		if key.Incomplete() {
			putLogs = append(putLogs, &txPutEntity{PendingKey: pKeys[idx]})
		} else {
			putLogs = append(putLogs, &txPutEntity{Key: key})
		}
	}
	lgTxPutMap[l][info.Transaction] = putLogs

	return pKeys, err
}

func (l *logger) GetMultiWithoutTx(info *datastore.MiddlewareInfo, keys []datastore.Key, psList []datastore.PropertyList) error {
	l.m.Lock()
	cnt := l.counter
	l.counter += 1
	l.m.Unlock()

	l.Logf(info.Context, l.Prefix+"GetMultiWithoutTx #%d, len(keys)=%d, keys=[%s]", cnt, len(keys), l.KeysToString(keys))

	err := info.Next.GetMultiWithoutTx(info, keys, psList)

	if err != nil {
		l.Logf(info.Context, l.Prefix+"GetMultiWithoutTx #%d, err=%s", cnt, err.Error())
	}

	return err
}

func (l *logger) GetMultiWithTx(info *datastore.MiddlewareInfo, keys []datastore.Key, psList []datastore.PropertyList) error {
	l.m.Lock()
	cnt := l.counter
	l.counter += 1
	l.m.Unlock()

	l.Logf(info.Context, l.Prefix+"GetMultiWithTx #%d, len(keys)=%d, keys=[%s]", cnt, len(keys), l.KeysToString(keys))

	err := info.Next.GetMultiWithTx(info, keys, psList)

	if err != nil {
		l.Logf(info.Context, l.Prefix+"GetMultiWithTx #%d, err=%s", cnt, err.Error())
	}

	return err
}

func (l *logger) DeleteMultiWithoutTx(info *datastore.MiddlewareInfo, keys []datastore.Key) error {
	l.m.Lock()
	cnt := l.counter
	l.counter += 1
	l.m.Unlock()

	l.Logf(info.Context, l.Prefix+"DeleteMultiWithoutTx #%d, len(keys)=%d, keys=[%s]", cnt, len(keys), l.KeysToString(keys))

	err := info.Next.DeleteMultiWithoutTx(info, keys)

	if err != nil {
		l.Logf(info.Context, l.Prefix+"DeleteMultiWithoutTx #%d, err=%s", cnt, err.Error())
	}

	return err
}

func (l *logger) DeleteMultiWithTx(info *datastore.MiddlewareInfo, keys []datastore.Key) error {
	l.m.Lock()
	cnt := l.counter
	l.counter += 1
	l.m.Unlock()

	l.Logf(info.Context, l.Prefix+"DeleteMultiWithTx #%d, len(keys)=%d, keys=[%s]", cnt, len(keys), l.KeysToString(keys))

	err := info.Next.DeleteMultiWithTx(info, keys)

	if err != nil {
		l.Logf(info.Context, l.Prefix+"DeleteMultiWithTx #%d, err=%s", cnt, err.Error())
	}

	return err
}

func (l *logger) PostCommit(info *datastore.MiddlewareInfo, tx datastore.Transaction, commit datastore.Commit) error {
	l.m.Lock()
	cnt := l.counter
	l.counter += 1
	l.m.Unlock()

	lgTxPutMap, ok := info.Context.Value(contextTx{}).(map[*logger]map[datastore.Transaction][]*txPutEntity)
	if ok {
		txPutMap, ok := lgTxPutMap[l]
		if ok {
			putLogs := txPutMap[info.Transaction]
			delete(txPutMap, info.Transaction)
			keys := make([]datastore.Key, 0, len(putLogs))
			for _, putLog := range putLogs {
				if putLog.Key != nil {
					keys = append(keys, putLog.Key)
					continue
				}

				key := commit.Key(putLog.PendingKey)
				keys = append(keys, key)
			}

			l.Logf(info.Context, l.Prefix+"PostCommit #%d Put keys=[%s]", cnt, l.KeysToString(keys))

			delete(txPutMap, info.Transaction)

		} else {
			l.Logf(info.Context, l.Prefix+"PostCommit #%d put log not contains in ctx", cnt)
		}

		if len(txPutMap) == 0 {
			delete(lgTxPutMap, l)
		}

	} else {
		l.Logf(info.Context, l.Prefix+"PostCommit #%d put log not contains in ctx", cnt)
	}

	return info.Next.PostCommit(info, tx, commit)
}

func (l *logger) PostRollback(info *datastore.MiddlewareInfo, tx datastore.Transaction) error {
	l.m.Lock()
	cnt := l.counter
	l.counter += 1
	l.m.Unlock()

	l.Logf(info.Context, l.Prefix+"PostRollback #%d", cnt)

	return info.Next.PostRollback(info, tx)
}

func (l *logger) Run(info *datastore.MiddlewareInfo, q datastore.Query, qDump *datastore.QueryDump) datastore.Iterator {
	l.m.Lock()
	cnt := l.counter
	l.counter += 1
	l.m.Unlock()

	l.Logf(info.Context, l.Prefix+"Run #%d, q=%s", cnt, qDump.String())

	return info.Next.Run(info, q, qDump)
}

func (l *logger) GetAll(info *datastore.MiddlewareInfo, q datastore.Query, qDump *datastore.QueryDump, psList *[]datastore.PropertyList) ([]datastore.Key, error) {
	l.m.Lock()
	cnt := l.counter
	l.counter += 1
	l.m.Unlock()

	l.Logf(info.Context, l.Prefix+"GetAll #%d, q=%s", cnt, qDump.String())

	keys, err := info.Next.GetAll(info, q, qDump, psList)

	if err == nil {
		l.Logf(info.Context, l.Prefix+"GetAll #%d, len(keys)=%d, keys=[%s]", cnt, len(keys), l.KeysToString(keys))
	} else {
		l.Logf(info.Context, l.Prefix+"GetAll #%d, err=%s", cnt, err.Error())
	}

	return keys, err
}

func (l *logger) Next(info *datastore.MiddlewareInfo, q datastore.Query, qDump *datastore.QueryDump, iter datastore.Iterator, ps *datastore.PropertyList) (datastore.Key, error) {
	l.m.Lock()
	cnt := l.counter
	l.counter += 1
	l.m.Unlock()

	l.Logf(info.Context, l.Prefix+"Next #%d, q=%s", cnt, qDump.String())

	key, err := info.Next.Next(info, q, qDump, iter, ps)

	if err == nil {
		l.Logf(info.Context, l.Prefix+"Next #%d, key=%s", cnt, key.String())
	} else {
		l.Logf(info.Context, l.Prefix+"Next #%d, err=%s", cnt, err.Error())
	}

	return key, err
}

func (l *logger) Count(info *datastore.MiddlewareInfo, q datastore.Query, qDump *datastore.QueryDump) (int, error) {
	l.m.Lock()
	cnt := l.counter
	l.counter += 1
	l.m.Unlock()

	l.Logf(info.Context, l.Prefix+"Count #%d, q=%s", cnt, qDump.String())

	ret, err := info.Next.Count(info, q, qDump)

	if err == nil {
		l.Logf(info.Context, l.Prefix+"Count #%d, ret=%d", cnt, ret)
	} else {
		l.Logf(info.Context, l.Prefix+"Count #%d, err=%s", cnt, err.Error())
	}

	return ret, err
}
