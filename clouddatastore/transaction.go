package clouddatastore

import (
	"context"

	"cloud.google.com/go/datastore"
	w "go.mercari.io/datastore"
	"go.mercari.io/datastore/internal/shared"
)

var _ w.Transaction = (*transactionImpl)(nil)
var _ w.Commit = (*commitImpl)(nil)

type contextTransaction struct{}

func getTx(ctx context.Context) *datastore.Transaction {
	tx := ctx.Value(contextTransaction{})
	if tx != nil {
		return tx.(*datastore.Transaction)
	}

	return nil
}

type transactionImpl struct {
	client    *datastoreImpl
	cacheInfo *w.MiddlewareInfo
}

type commitImpl struct {
	commit *datastore.Commit
}

func (tx *transactionImpl) Get(key w.Key, dst interface{}) error {
	err := tx.GetMulti([]w.Key{key}, []interface{}{dst})
	if merr, ok := err.(w.MultiError); ok {
		return merr[0]
	} else if err != nil {
		return err
	}

	return nil
}

func (tx *transactionImpl) GetMulti(keys []w.Key, dst interface{}) error {
	cb := shared.NewCacheBridge(tx.cacheInfo, &originalClientBridgeImpl{tx.client}, &originalTransactionBridgeImpl{tx: tx}, nil, tx.client.middlewares)

	err := shared.GetMultiOps(tx.client.ctx, keys, dst, func(keys []w.Key, dst []w.PropertyList) error {
		return cb.GetMultiWithTx(tx.cacheInfo, keys, dst)
	})

	return err
}

func (tx *transactionImpl) Put(key w.Key, src interface{}) (w.PendingKey, error) {
	pKeys, err := tx.PutMulti([]w.Key{key}, []interface{}{src})
	if merr, ok := err.(w.MultiError); ok {
		return nil, merr[0]
	} else if err != nil {
		return nil, err
	}

	return pKeys[0], nil
}

func (tx *transactionImpl) PutMulti(keys []w.Key, src interface{}) ([]w.PendingKey, error) {
	cb := shared.NewCacheBridge(tx.cacheInfo, &originalClientBridgeImpl{tx.client}, &originalTransactionBridgeImpl{tx: tx}, nil, tx.client.middlewares)

	_, pKeys, err := shared.PutMultiOps(tx.client.ctx, keys, src, func(keys []w.Key, src []w.PropertyList) ([]w.Key, []w.PendingKey, error) {
		pKeys, err := cb.PutMultiWithTx(tx.cacheInfo, keys, src)
		return nil, pKeys, err
	})

	if err != nil {
		return nil, err
	}

	return pKeys, nil
}

func (tx *transactionImpl) Delete(key w.Key) error {
	err := tx.DeleteMulti([]w.Key{key})
	if merr, ok := err.(w.MultiError); ok {
		return merr[0]
	} else if err != nil {
		return err
	}

	return nil
}

func (tx *transactionImpl) DeleteMulti(keys []w.Key) error {
	cb := shared.NewCacheBridge(tx.cacheInfo, &originalClientBridgeImpl{tx.client}, &originalTransactionBridgeImpl{tx: tx}, nil, tx.client.middlewares)

	err := shared.DeleteMultiOps(tx.client.ctx, keys, func(keys []w.Key) error {
		return cb.DeleteMultiWithTx(tx.cacheInfo, keys)
	})

	return err
}

func (tx *transactionImpl) Commit() (w.Commit, error) {
	baseTx := getTx(tx.client.ctx)
	if baseTx == nil {
		return nil, nil
	}

	commit, err := baseTx.Commit()
	if err != nil {
		return nil, toWrapperError(err)
	}

	cb := shared.NewCacheBridge(tx.cacheInfo, &originalClientBridgeImpl{tx.client}, &originalTransactionBridgeImpl{tx: tx}, nil, tx.client.middlewares)
	commitImpl := &commitImpl{commit}
	err = cb.PostCommit(tx.cacheInfo, tx, commitImpl)

	if err != nil {
		return nil, err
	}

	return commitImpl, nil
}

func (tx *transactionImpl) Rollback() error {
	baseTx := getTx(tx.client.ctx)
	if tx == nil {
		return nil
	}

	err := baseTx.Rollback()
	if err != nil {
		return toWrapperError(err)
	}

	cb := shared.NewCacheBridge(tx.cacheInfo, &originalClientBridgeImpl{tx.client}, &originalTransactionBridgeImpl{tx: tx}, nil, tx.client.middlewares)
	return cb.PostRollback(tx.cacheInfo, tx)
}

func (tx *transactionImpl) Batch() *w.TransactionBatch {
	return &w.TransactionBatch{Transaction: tx}
}

func (c *commitImpl) Key(p w.PendingKey) w.Key {
	pk := toOriginalPendingKey(p)
	key := c.commit.Key(pk)
	return toWrapperKey(key)
}
