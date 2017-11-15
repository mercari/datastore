package aedatastore

import (
	"context"
	"errors"
	"reflect"

	w "go.mercari.io/datastore"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
)

var _ w.Client = (*datastoreImpl)(nil)

var typeOfPropertyLoadSaver = reflect.TypeOf((*w.PropertyLoadSaver)(nil)).Elem()
var typeOfPropertyList = reflect.TypeOf(w.PropertyList(nil))

type datastoreImpl struct {
	ctx context.Context
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
	return getMultiOps(ctx, keys, dst, func(keys []*datastore.Key, dst []datastore.PropertyList) error {
		return datastore.GetMulti(ctx, keys, dst)
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
	keys, _, err := putMultiOps(ctx, keys, src, func(keys []*datastore.Key, src []datastore.PropertyList) ([]w.Key, []w.PendingKey, error) {
		keys, err := datastore.PutMulti(ctx, keys, src)
		if err != nil {
			return nil, nil, err
		}

		wKeys := toWrapperKeys(ctx, keys)
		return wKeys, nil, nil
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
	return deleteMultiOps(ctx, keys, func(keys []*datastore.Key) error {
		return datastore.DeleteMulti(ctx, keys)
	})
}

func (d *datastoreImpl) NewTransaction(ctx context.Context) (w.Transaction, error) {
	ext, err := newTxExtractor(ctx)
	if err != nil {
		return nil, err
	}

	txCtx := context.WithValue(ext.txCtx, contextTransaction{}, ext)
	return &transactionImpl{client: &datastoreImpl{ctx: txCtx}}, nil
}

func (d *datastoreImpl) RunInTransaction(ctx context.Context, f func(tx w.Transaction) error) (w.Commit, error) {
	ext, err := newTxExtractor(ctx)
	if err != nil {
		return nil, err
	}

	// TODO この辺テストガッツリ

	txCtx := context.WithValue(ext.txCtx, contextTransaction{}, ext)
	tx := &transactionImpl{client: &datastoreImpl{ctx: txCtx}}
	err = f(tx)
	if err != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			return nil, rollbackErr
		}
		return nil, err
	}

	commit, err := tx.Commit()
	if err != nil {
		return nil, err
	}
	return commit, nil
}

func (d *datastoreImpl) Run(ctx context.Context, q w.Query) w.Iterator {
	qImpl := q.(*queryImpl)
	if qImpl.transaction != nil {
		// replace ctx to tx ctx
		ctx = qImpl.transaction.client.ctx
	}
	firstError := qImpl.firstError
	var err error
	ctx, err = appengine.Namespace(ctx, qImpl.namespace)
	if firstError == nil && err != nil {
		firstError = err
	}
	t := qImpl.q.Run(ctx)
	return &iteratorImpl{client: d, q: qImpl, t: t, firstError: firstError}
}

func (d *datastoreImpl) AllocatedIDs(ctx context.Context, keys []w.Key) ([]w.Key, error) {
	// TODO 可能な限りバッチ化する
	var resultKeys []w.Key
	for _, key := range keys {
		pK := toOriginalKey(key.ParentKey())
		low, _, err := datastore.AllocateIDs(ctx, key.Kind(), pK, 1)
		if err != nil {
			return nil, err
		}
		origKey := datastore.NewKey(ctx, key.Kind(), "", low, pK)
		resultKeys = append(resultKeys, toWrapperKey(ctx, origKey))
	}

	return resultKeys, nil
}

func (d *datastoreImpl) Count(ctx context.Context, q w.Query) (int, error) {
	qImpl, ok := q.(*queryImpl)
	if !ok {
		return 0, errors.New("invalid query type")
	}
	if qImpl.firstError != nil {
		return 0, qImpl.firstError
	}

	if qImpl.transaction != nil {
		// replace ctx to tx ctx
		ctx = qImpl.transaction.client.ctx
	}

	var err error
	ctx, err = appengine.Namespace(ctx, qImpl.namespace)
	if err != nil {
		return 0, toWrapperError(err)
	}
	count, err := qImpl.q.Count(ctx)
	if err != nil {
		return 0, toWrapperError(err)
	}

	return count, nil
}

func (d *datastoreImpl) GetAll(ctx context.Context, q w.Query, dst interface{}) ([]w.Key, error) {
	qImpl, ok := q.(*queryImpl)
	if !ok {
		return nil, errors.New("invalid query type")
	}

	if qImpl.firstError != nil {
		return nil, qImpl.firstError
	}

	if qImpl.transaction != nil {
		// replace ctx to tx ctx
		ctx = qImpl.transaction.client.ctx
	}

	var err error
	ctx, err = appengine.Namespace(ctx, qImpl.namespace)
	if err != nil {
		return nil, toWrapperError(err)
	}

	var dv reflect.Value
	var elemType reflect.Type
	var isPtrStruct bool
	if !qImpl.keysOnly {
		dv = reflect.ValueOf(dst)
		if dv.Kind() != reflect.Ptr || dv.IsNil() {
			return nil, w.ErrInvalidEntityType
		}
		dv = dv.Elem()
		if dv.Kind() != reflect.Slice {
			return nil, w.ErrInvalidEntityType
		}
		if dv.Type() == typeOfPropertyList {
			return nil, w.ErrInvalidEntityType
		}
		elemType = dv.Type().Elem()
		if reflect.PtrTo(elemType).Implements(typeOfPropertyLoadSaver) {
			// ok
		} else {
			switch elemType.Kind() {
			case reflect.Ptr:
				isPtrStruct = true
				elemType = elemType.Elem()
				if elemType.Kind() != reflect.Struct {
					return nil, w.ErrInvalidEntityType
				}
			}
		}
	}

	// TODO add reflect.Map support

	var origPss []datastore.PropertyList
	origKeys, err := qImpl.q.GetAll(ctx, &origPss)
	if err != nil {
		return nil, toWrapperError(err)
	}

	wKeys := toWrapperKeys(ctx, origKeys)

	if !qImpl.keysOnly {
		for idx, origPs := range origPss {
			ps := toWrapperPropertyList(ctx, origPs)

			elem := reflect.New(elemType)

			if err = w.LoadEntity(ctx, elem.Interface(), &w.Entity{Key: wKeys[idx], Properties: ps}); err != nil {
				return nil, err
			}

			if !isPtrStruct {
				elem = elem.Elem()
			}

			dv.Set(reflect.Append(dv, elem))
		}
	}

	return wKeys, nil
}

func (d *datastoreImpl) IncompleteKey(kind string, parent w.Key) w.Key {
	parentKey := toOriginalKey(parent)

	// TODO 名前空間の整合性がCloud Datastore側と取れてなさそう
	ctx, err := appengine.Namespace(d.ctx, "")
	if err != nil {
		panic(err)
	}
	key := datastore.NewIncompleteKey(ctx, kind, parentKey)
	return toWrapperKey(d.ctx, key)
}

func (d *datastoreImpl) NameKey(kind, name string, parent w.Key) w.Key {
	parentKey := toOriginalKey(parent)

	// TODO 名前空間の整合性がCloud Datastore側と取れてなさそう
	ctx, err := appengine.Namespace(d.ctx, "")
	if err != nil {
		panic(err)
	}
	key := datastore.NewKey(ctx, kind, name, 0, parentKey)
	return toWrapperKey(ctx, key)
}

func (d *datastoreImpl) IDKey(kind string, id int64, parent w.Key) w.Key {
	parentKey := toOriginalKey(parent)

	// TODO 名前空間の整合性がCloud Datastore側と取れてなさそう
	ctx, err := appengine.Namespace(d.ctx, "")
	if err != nil {
		panic(err)
	}
	key := datastore.NewKey(ctx, kind, "", id, parentKey)
	return toWrapperKey(d.ctx, key)
}

func (d *datastoreImpl) NewQuery(kind string) w.Query {
	q := datastore.NewQuery(kind)
	return &queryImpl{ctx: d.ctx, q: q}
}

func (d *datastoreImpl) Close() error {
	// TODO closeした後に呼んだら殺したほうが ae <-> cloud の移行が楽になりそう
	return nil
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

func (d *datastoreImpl) SwapContext(ctx context.Context) context.Context {
	origCtx := d.ctx
	d.ctx = ctx
	return origCtx
}
