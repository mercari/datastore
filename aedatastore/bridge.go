package aedatastore

import (
	"context"
	"encoding/gob"
	"errors"

	w "go.mercari.io/datastore"
	"go.mercari.io/datastore/internal/shared"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
)

func init() {
	w.FromContext = FromContext

	gob.Register(&keyImpl{})
}

func FromContext(ctx context.Context, opts ...w.ClientOption) (w.Client, error) {
	if ctx == nil {
		panic("unexpected")
	}
	return &datastoreImpl{ctx: ctx}, nil
}

func IsAEDatastoreClient(client w.Client) bool {
	_, ok := client.(*datastoreImpl)
	return ok
}

var _ shared.OriginalClientBridge = &originalClientBridgeImpl{}
var _ shared.OriginalTransactionBridge = &originalTransactionBridgeImpl{}
var _ shared.OriginalIteratorBridge = &originalIteratorBridgeImpl{}

type originalClientBridgeImpl struct {
	d *datastoreImpl
}

func (ocb *originalClientBridgeImpl) AllocateIDs(ctx context.Context, keys []w.Key) ([]w.Key, error) {
	// TODO 可能な限りバッチ化する
	var resultKeys []w.Key
	for _, key := range keys {
		pK := toOriginalKey(key.ParentKey())
		low, _, err := datastore.AllocateIDs(ctx, key.Kind(), pK, 1)
		if err != nil {
			return nil, toWrapperError(err)
		}
		origKey := datastore.NewKey(ctx, key.Kind(), "", low, pK)
		resultKeys = append(resultKeys, toWrapperKey(ctx, origKey))
	}

	return resultKeys, nil
}

func (ocb *originalClientBridgeImpl) PutMulti(ctx context.Context, keys []w.Key, psList []w.PropertyList) ([]w.Key, error) {
	origKeys := toOriginalKeys(keys)
	origPss, err := toOriginalPropertyListList(psList)
	if err != nil {
		return nil, err
	}

	origKeys, err = datastore.PutMulti(ctx, origKeys, origPss)
	return toWrapperKeys(ctx, origKeys), toWrapperError(err)
}

func (ocb *originalClientBridgeImpl) GetMulti(ctx context.Context, keys []w.Key, psList []w.PropertyList) error {
	origKeys := toOriginalKeys(keys)
	origPss, err := toOriginalPropertyListList(psList)
	if err != nil {
		return err
	}

	err = datastore.GetMulti(ctx, origKeys, origPss)
	wPss := toWrapperPropertyListList(ctx, origPss)
	copy(psList, wPss)
	return toWrapperError(err)
}

func (ocb *originalClientBridgeImpl) DeleteMulti(ctx context.Context, keys []w.Key) error {
	origKeys := toOriginalKeys(keys)

	err := datastore.DeleteMulti(ctx, origKeys)
	return toWrapperError(err)
}

func (ocb *originalClientBridgeImpl) Run(ctx context.Context, q w.Query, qDump *w.QueryDump) w.Iterator {
	qImpl := q.(*queryImpl)

	baseCtx := ctx

	if qImpl.dump.Transaction != nil {
		// replace ctx to tx ctx
		ctx = TransactionContext(qImpl.dump.Transaction)
	}

	ctx, err := appengine.Namespace(ctx, qImpl.dump.Namespace)
	if err != nil && qImpl.firstError == nil {
		qImpl.firstError = err
	}

	iter := qImpl.q.Run(ctx)
	return &iteratorImpl{
		client: ocb.d,
		q:      qImpl,
		qDump:  qDump,
		t:      iter,
		cacheInfo: &w.MiddlewareInfo{
			Context:     baseCtx,
			Client:      ocb.d,
			Transaction: qDump.Transaction,
		},
		firstError: qImpl.firstError,
	}
}

func (ocb *originalClientBridgeImpl) GetAll(ctx context.Context, q w.Query, qDump *w.QueryDump, psList *[]w.PropertyList) ([]w.Key, error) {
	qImpl, ok := q.(*queryImpl)
	if !ok {
		return nil, errors.New("invalid query type")
	}

	if qImpl.firstError != nil {
		return nil, qImpl.firstError
	}

	if qDump.Transaction != nil {
		// replace ctx to tx ctx
		ctx = TransactionContext(qImpl.dump.Transaction)
	}

	var err error
	ctx, err = appengine.Namespace(ctx, qDump.Namespace)
	if err != nil {
		return nil, toWrapperError(err)
	}

	var origPss []datastore.PropertyList
	if !qDump.KeysOnly {
		origPss, err = toOriginalPropertyListList(*psList)
		if err != nil {
			return nil, err
		}
	}
	origKeys, err := qImpl.q.GetAll(ctx, &origPss)
	if err != nil {
		return nil, toWrapperError(err)
	}

	wKeys := toWrapperKeys(ctx, origKeys)

	if !qDump.KeysOnly {
		// TODO should be copy? not replace?
		*psList = toWrapperPropertyListList(ctx, origPss)
	}

	return wKeys, nil
}

func (ocb *originalClientBridgeImpl) Count(ctx context.Context, q w.Query, qDump *w.QueryDump) (int, error) {
	qImpl, ok := q.(*queryImpl)
	if !ok {
		return 0, errors.New("invalid query type")
	}
	if qImpl.firstError != nil {
		return 0, qImpl.firstError
	}

	if qImpl.dump.Transaction != nil {
		// replace ctx to tx ctx
		txImpl, ok := qImpl.dump.Transaction.(*transactionImpl)
		if !ok {
			return 0, errors.New("unexpected context")
		}
		ctx = txImpl.client.ctx
	}

	var err error
	ctx, err = appengine.Namespace(ctx, qImpl.dump.Namespace)
	if err != nil {
		return 0, toWrapperError(err)
	}
	count, err := qImpl.q.Count(ctx)
	if err != nil {
		return 0, toWrapperError(err)
	}

	return count, nil
}

type originalTransactionBridgeImpl struct {
	tx *transactionImpl
}

func (otb *originalTransactionBridgeImpl) PutMulti(keys []w.Key, psList []w.PropertyList) ([]w.PendingKey, error) {
	ext := getTxExtractor(otb.tx.client.ctx)
	if ext == nil {
		return nil, errors.New("unexpected context")
	}

	origKeys := toOriginalKeys(keys)
	origPss, err := toOriginalPropertyListList(psList)
	if err != nil {
		return nil, err
	}

	origKeys, err = datastore.PutMulti(ext.txCtx, origKeys, origPss)
	if err != nil {
		return nil, toWrapperError(err)
	}

	wPKeys := toWrapperPendingKeys(ext.txCtx, origKeys)

	return wPKeys, nil
}

func (otb *originalTransactionBridgeImpl) GetMulti(keys []w.Key, psList []w.PropertyList) error {
	ext := getTxExtractor(otb.tx.client.ctx)
	if ext == nil {
		return errors.New("unexpected context")
	}

	origKeys := toOriginalKeys(keys)
	origPss, err := toOriginalPropertyListList(psList)
	if err != nil {
		return err
	}

	err = datastore.GetMulti(ext.txCtx, origKeys, origPss)
	wPss := toWrapperPropertyListList(ext.txCtx, origPss)
	copy(psList, wPss)
	if err != nil {
		return toWrapperError(err)
	}

	return nil
}

func (otb *originalTransactionBridgeImpl) DeleteMulti(keys []w.Key) error {
	ext := getTxExtractor(otb.tx.client.ctx)
	if ext == nil {
		return errors.New("unexpected context")
	}

	origKeys := toOriginalKeys(keys)

	err := datastore.DeleteMulti(ext.txCtx, origKeys)
	return toWrapperError(err)
}

type originalIteratorBridgeImpl struct {
	qDump *w.QueryDump
}

func (oib *originalIteratorBridgeImpl) Next(iter w.Iterator, ps *w.PropertyList) (w.Key, error) {
	iterImpl := iter.(*iteratorImpl)

	var origPsPtr *datastore.PropertyList
	if !oib.qDump.KeysOnly {
		origPs, err := toOriginalPropertyList(*ps)
		if err != nil {
			return nil, err
		}
		origPsPtr = &origPs
	}

	origKey, err := iterImpl.t.Next(origPsPtr)
	if err != nil {
		return nil, toWrapperError(err)
	}

	if !oib.qDump.KeysOnly {
		*ps = toWrapperPropertyList(iterImpl.client.ctx, *origPsPtr)
	}

	return toWrapperKey(iterImpl.client.ctx, origKey), nil
}
