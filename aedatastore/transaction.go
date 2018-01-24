package aedatastore

import (
	"context"
	"errors"

	w "go.mercari.io/datastore"
	"go.mercari.io/datastore/internal/shared"
	netcontext "golang.org/x/net/context"
	"google.golang.org/appengine/datastore"
)

var _ w.Transaction = (*transactionImpl)(nil)
var _ w.Commit = (*commitImpl)(nil)

type contextTransaction struct{}

type txExtractor struct {
	txCtx   context.Context
	finishC chan txResult
	resultC chan error
}

type txResult struct {
	commit   bool
	rollback bool
}

func TransactionContext(tx w.Transaction) context.Context {
	txImpl := tx.(*transactionImpl)
	return txImpl.client.ctx
}

func newTxExtractor(ctx context.Context) (*txExtractor, error) {
	ctxC := make(chan context.Context)

	ext := &txExtractor{
		finishC: make(chan txResult),
		resultC: make(chan error),
	}

	rollbackErr := errors.New("rollback requested")

	go func() {
		// NOTE RunInTransactionが自動的にリトライされるのは初心者殺しなのでリトライしたかったらアプリ側でループしてほしいという意思
		err := datastore.RunInTransaction(ctx, func(ctx netcontext.Context) error {
			ctxC <- ctx

			result, ok := <-ext.finishC
			if !ok {
				return errors.New("channel closed")
			}

			if result.commit {
				return nil
			} else if result.rollback {
				return rollbackErr
			}

			panic("unexpected tx state")

		}, &datastore.TransactionOptions{XG: true, Attempts: 1})
		if err == rollbackErr {
			// This is intended error
			err = nil
		}
		ext.resultC <- toWrapperError(err)
	}()

	select {
	case txCtx := <-ctxC:
		ext.txCtx = txCtx
	case err := <-ext.resultC:
		if err == nil {
			panic("unexpected state")
		}
		return nil, toWrapperError(err)
	}

	return ext, nil
}

func getTxExtractor(ctx context.Context) *txExtractor {
	tx := ctx.Value(contextTransaction{})
	if tx != nil {
		return tx.(*txExtractor)
	}

	return nil
}

type transactionImpl struct {
	client    *datastoreImpl
	cacheInfo *w.MiddlewareInfo
}

type commitImpl struct {
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
	ext := getTxExtractor(tx.client.ctx)
	if ext == nil {
		return nil, errors.New("unexpected context")
	}

	err := ext.commit()
	if err != nil {
		return nil, err
	}

	cb := shared.NewCacheBridge(tx.cacheInfo, &originalClientBridgeImpl{tx.client}, &originalTransactionBridgeImpl{tx: tx}, nil, tx.client.middlewares)
	commitImpl := &commitImpl{}
	err = cb.PostCommit(tx.cacheInfo, tx, commitImpl)

	if err != nil {
		return nil, err
	}

	return commitImpl, nil
}

func (tx *transactionImpl) Rollback() error {
	ext := getTxExtractor(tx.client.ctx)
	if ext == nil {
		return errors.New("unexpected context")
	}

	err := ext.rollback()
	if err != nil {
		return err
	}

	cb := shared.NewCacheBridge(tx.cacheInfo, &originalClientBridgeImpl{tx.client}, &originalTransactionBridgeImpl{tx: tx}, nil, tx.client.middlewares)
	return cb.PostRollback(tx.cacheInfo, tx)
}

func (tx *transactionImpl) Batch() *w.TransactionBatch {
	return &w.TransactionBatch{Transaction: tx}
}

func (c *commitImpl) Key(p w.PendingKey) w.Key {
	pk := toOriginalPendingKey(p)
	return toWrapperKey(p.StoredContext(), pk)
}

func (ext *txExtractor) commit() error {
	ext.finishC <- txResult{commit: true}
	return <-ext.resultC
}

func (ext *txExtractor) rollback() error {
	ext.finishC <- txResult{rollback: true}
	return <-ext.resultC
}
