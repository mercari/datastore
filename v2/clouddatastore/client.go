package clouddatastore

import (
	"context"
	"errors"

	"cloud.google.com/go/datastore"
	w "go.mercari.io/datastore/v2"
	"go.mercari.io/datastore/v2/internal/shared"
)

var _ w.Client = (*datastoreImpl)(nil)

type datastoreImpl struct {
	ctx         context.Context
	client      *datastore.Client
	middlewares []w.Middleware
}

func (d *datastoreImpl) Get(ctx context.Context, key w.Key, dst interface{}) error {
	err := d.GetMulti(ctx, []w.Key{key}, []interface{}{dst})
	if merr, ok := err.(w.MultiError); ok {
		return merr[0]
	} else if err != nil {
		return err
	}

	return nil
}

func (d *datastoreImpl) GetMulti(ctx context.Context, keys []w.Key, dst interface{}) error {
	cacheInfo := &w.MiddlewareInfo{
		Context: ctx,
		Client:  d,
	}
	cb := shared.NewCacheBridge(cacheInfo, &originalClientBridgeImpl{d}, nil, nil, d.middlewares)

	return shared.GetMultiOps(ctx, keys, dst, func(keys []w.Key, dst []w.PropertyList) error {
		return cb.GetMultiWithoutTx(cacheInfo, keys, dst)
	})
}

func (d *datastoreImpl) Put(ctx context.Context, key w.Key, src interface{}) (w.Key, error) {
	keys, err := d.PutMulti(ctx, []w.Key{key}, []interface{}{src})
	if merr, ok := err.(w.MultiError); ok {
		return nil, merr[0]
	} else if err != nil {
		return nil, err
	}

	return keys[0], nil
}

func (d *datastoreImpl) PutMulti(ctx context.Context, keys []w.Key, src interface{}) ([]w.Key, error) {
	cacheInfo := &w.MiddlewareInfo{
		Context: ctx,
		Client:  d,
	}
	cb := shared.NewCacheBridge(cacheInfo, &originalClientBridgeImpl{d}, nil, nil, d.middlewares)

	keys, _, err := shared.PutMultiOps(ctx, keys, src, func(keys []w.Key, src []w.PropertyList) ([]w.Key, []w.PendingKey, error) {
		keys, err := cb.PutMultiWithoutTx(cacheInfo, keys, src)
		return keys, nil, err
	})
	if err != nil {
		return nil, err
	}

	return keys, nil
}

func (d *datastoreImpl) Delete(ctx context.Context, key w.Key) error {
	err := d.DeleteMulti(ctx, []w.Key{key})
	if merr, ok := err.(w.MultiError); ok {
		return merr[0]
	} else if err != nil {
		return err
	}

	return nil
}

func (d *datastoreImpl) DeleteMulti(ctx context.Context, keys []w.Key) error {
	cacheInfo := &w.MiddlewareInfo{
		Context: ctx,
		Client:  d,
	}
	cb := shared.NewCacheBridge(cacheInfo, &originalClientBridgeImpl{d}, nil, nil, d.middlewares)

	return shared.DeleteMultiOps(ctx, keys, func(keys []w.Key) error {
		return cb.DeleteMultiWithoutTx(cacheInfo, keys)
	})
}

func (d *datastoreImpl) NewTransaction(ctx context.Context) (w.Transaction, error) {
	tx, err := d.client.NewTransaction(ctx)
	if err != nil {
		return nil, toWrapperError(err)
	}

	txCtx := context.WithValue(ctx, contextTransaction{}, tx)
	txImpl := &transactionImpl{
		client: &datastoreImpl{
			ctx:         txCtx,
			client:      d.client,
			middlewares: d.middlewares,
		},
	}
	txImpl.cacheInfo = &w.MiddlewareInfo{
		Context:     txCtx,
		Client:      d,
		Transaction: txImpl,
	}

	return txImpl, nil
}

func (d *datastoreImpl) RunInTransaction(ctx context.Context, f func(tx w.Transaction) error) (w.Commit, error) {
	var txImpl *transactionImpl
	commit, err := d.client.RunInTransaction(ctx, func(baseTx *datastore.Transaction) error {
		txCtx := context.WithValue(ctx, contextTransaction{}, baseTx)
		txImpl = &transactionImpl{
			client: &datastoreImpl{
				ctx:         txCtx,
				client:      d.client,
				middlewares: d.middlewares,
			},
		}
		txImpl.cacheInfo = &w.MiddlewareInfo{
			Context:     txCtx,
			Client:      d,
			Transaction: txImpl,
		}
		return f(txImpl)
	}, datastore.MaxAttempts(1))
	if err != nil {
		return nil, toWrapperError(err)
	}

	cb := shared.NewCacheBridge(txImpl.cacheInfo, &originalClientBridgeImpl{txImpl.client}, &originalTransactionBridgeImpl{tx: txImpl}, nil, txImpl.client.middlewares)
	commitImpl := &commitImpl{commit}
	err = cb.PostCommit(txImpl.cacheInfo, txImpl, commitImpl)

	if err != nil {
		return nil, err
	}

	return commitImpl, nil
}

func (d *datastoreImpl) Run(ctx context.Context, q w.Query) w.Iterator {
	cacheInfo := &w.MiddlewareInfo{
		Context: ctx,
		Client:  d,
	}
	cb := shared.NewCacheBridge(cacheInfo, &originalClientBridgeImpl{d}, nil, nil, d.middlewares)

	return cb.Run(cb.Info, q, q.Dump())
}

func (d *datastoreImpl) AllocateIDs(ctx context.Context, keys []w.Key) ([]w.Key, error) {
	cacheInfo := &w.MiddlewareInfo{
		Context: ctx,
		Client:  d,
	}
	cb := shared.NewCacheBridge(cacheInfo, &originalClientBridgeImpl{d}, nil, nil, d.middlewares)

	return cb.AllocateIDs(cb.Info, keys)
}

func (d *datastoreImpl) Count(ctx context.Context, q w.Query) (int, error) {
	cacheInfo := &w.MiddlewareInfo{
		Context: ctx,
		Client:  d,
	}
	cb := shared.NewCacheBridge(cacheInfo, &originalClientBridgeImpl{d}, nil, nil, d.middlewares)

	return cb.Count(cb.Info, q, q.Dump())
}

func (d *datastoreImpl) GetAll(ctx context.Context, q w.Query, dst interface{}) ([]w.Key, error) {
	qImpl, ok := q.(*queryImpl)
	if !ok {
		return nil, errors.New("invalid query type")
	}

	if qImpl.firstError != nil {
		return nil, qImpl.firstError
	}

	qDump := q.Dump()
	cacheInfo := &w.MiddlewareInfo{
		Context:     ctx,
		Client:      d,
		Transaction: qDump.Transaction,
	}
	cb := shared.NewCacheBridge(cacheInfo, &originalClientBridgeImpl{d}, nil, nil, d.middlewares)
	return shared.GetAllOps(ctx, qDump, dst, func(dst *[]w.PropertyList) ([]w.Key, error) {
		return cb.GetAll(cacheInfo, q, qDump, dst)
	})
}

func (d *datastoreImpl) IncompleteKey(kind string, parent w.Key) w.Key {
	key := &keyImpl{
		kind: kind,
		id:   0,
		name: "",
	}
	if parent != nil {
		parentImpl := parent.(*keyImpl)
		key.parent = parentImpl
	}

	return key
}

func (d *datastoreImpl) NameKey(kind, name string, parent w.Key) w.Key {
	key := &keyImpl{
		kind: kind,
		id:   0,
		name: name,
	}
	if parent != nil {
		parentImpl := parent.(*keyImpl)
		key.parent = parentImpl
	}

	return key
}

func (d *datastoreImpl) IDKey(kind string, id int64, parent w.Key) w.Key {
	key := &keyImpl{
		kind: kind,
		id:   id,
		name: "",
	}
	if parent != nil {
		parentImpl := parent.(*keyImpl)
		key.parent = parentImpl
	}

	return key
}

func (d *datastoreImpl) NewQuery(kind string) w.Query {
	q := datastore.NewQuery(kind)
	return &queryImpl{ctx: d.ctx, q: q, dump: &w.QueryDump{Kind: kind}}
}

func (d *datastoreImpl) Close() error {
	return d.client.Close()
}

func (d *datastoreImpl) DecodeKey(encoded string) (w.Key, error) {
	key, err := datastore.DecodeKey(encoded)
	if err != nil {
		return nil, toWrapperError(err)
	}

	return toWrapperKey(key), nil
}

func (d *datastoreImpl) DecodeCursor(s string) (w.Cursor, error) {
	cur, err := datastore.DecodeCursor(s)
	if err != nil {
		return nil, toWrapperError(err)
	}

	return &cursorImpl{cursor: cur}, nil
}

func (d *datastoreImpl) Batch() *w.Batch {
	return &w.Batch{Client: d}
}

func (d *datastoreImpl) AppendMiddleware(mw w.Middleware) {
	d.middlewares = append(d.middlewares, mw)
}

func (d *datastoreImpl) RemoveMiddleware(mw w.Middleware) bool {
	list := make([]w.Middleware, 0, len(d.middlewares))
	found := false
	for _, old := range d.middlewares {
		if old == mw {
			found = true
			continue
		}
		list = append(list, old)
	}
	d.middlewares = list

	return found
}

func (d *datastoreImpl) Context() context.Context {
	return d.ctx
}

func (d *datastoreImpl) SetContext(ctx context.Context) {
	if ctx == nil {
		panic("ctx can't be nil")
	}
	d.ctx = ctx
}
