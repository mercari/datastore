package aedatastore

import (
	"context"

	w "go.mercari.io/datastore"
	"go.mercari.io/datastore/internal/shared"
	"google.golang.org/appengine/datastore"
)

var _ w.Client = (*datastoreImpl)(nil)

type datastoreImpl struct {
	ctx         context.Context
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
	ext, err := newTxExtractor(ctx)
	if err != nil {
		return nil, err
	}

	txCtx := context.WithValue(ext.txCtx, contextTransaction{}, ext)
	txImpl := &transactionImpl{
		client: &datastoreImpl{
			ctx:         txCtx,
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
	ext, err := newTxExtractor(ctx)
	if err != nil {
		return nil, err
	}

	txCtx := context.WithValue(ext.txCtx, contextTransaction{}, ext)
	txImpl := &transactionImpl{
		client: &datastoreImpl{
			ctx:         txCtx,
			middlewares: d.middlewares,
		},
	}
	txImpl.cacheInfo = &w.MiddlewareInfo{
		Context:     txCtx,
		Client:      d,
		Transaction: txImpl,
	}

	err = f(txImpl)
	if err != nil {
		rollbackErr := txImpl.Rollback()
		if rollbackErr != nil {
			return nil, rollbackErr
		}
		return nil, err
	}

	commit, err := txImpl.Commit()
	if err != nil {
		return nil, err
	}
	return commit, nil
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
		ctx:  d.ctx,
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
		ctx:  d.ctx,
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
		ctx:  d.ctx,
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
	// TODO closeした後に呼んだら殺したほうが ae <-> cloud の移行が楽になりそう
	return nil
}

func (d *datastoreImpl) DecodeKey(encoded string) (w.Key, error) {
	key, err := datastore.DecodeKey(encoded)
	if err != nil {
		return nil, toWrapperError(err)
	}

	return toWrapperKey(d.ctx, key), nil
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

func (d *datastoreImpl) RemoveMiddleware(md w.Middleware) bool {
	list := make([]w.Middleware, 0, len(d.middlewares))
	found := false
	for _, old := range d.middlewares {
		if old == md {
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
