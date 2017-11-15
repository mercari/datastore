package clouddatastore

import (
	"context"
	"errors"
	"reflect"

	"cloud.google.com/go/datastore"
	w "go.mercari.io/datastore"
)

var _ w.Client = (*datastoreImpl)(nil)

var typeOfPropertyLoadSaver = reflect.TypeOf((*w.PropertyLoadSaver)(nil)).Elem()
var typeOfPropertyList = reflect.TypeOf(w.PropertyList(nil))

type datastoreImpl struct {
	ctx    context.Context
	client *datastore.Client
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
		return d.client.GetMulti(ctx, keys, dst)
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
		keys, err := d.client.PutMulti(ctx, keys, src)
		if err != nil {
			return nil, nil, err
		}

		wKeys := toWrapperKeys(keys)
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
		return d.client.DeleteMulti(ctx, keys)
	})
}

func (d *datastoreImpl) NewTransaction(ctx context.Context) (w.Transaction, error) {
	tx, err := d.client.NewTransaction(ctx)
	if err != nil {
		return nil, toWrapperError(err)
	}

	txCtx := context.WithValue(ctx, contextTransaction{}, tx)
	return &transactionImpl{client: &datastoreImpl{ctx: txCtx, client: d.client}}, nil
}

func (d *datastoreImpl) RunInTransaction(ctx context.Context, f func(tx w.Transaction) error) (w.Commit, error) {
	commit, err := d.client.RunInTransaction(ctx, func(baseTx *datastore.Transaction) error {
		txCtx := context.WithValue(ctx, contextTransaction{}, baseTx)
		tx := &transactionImpl{client: &datastoreImpl{ctx: txCtx, client: d.client}}
		return f(tx)
	})
	if err != nil {
		return nil, toWrapperError(err)
	}

	return &commitImpl{commit}, nil
}

func (d *datastoreImpl) Run(ctx context.Context, q w.Query) w.Iterator {
	qImpl := q.(*queryImpl)
	t := d.client.Run(ctx, qImpl.q)
	return &iteratorImpl{client: d, q: qImpl, t: t, firstError: qImpl.firstError}
}

func (d *datastoreImpl) AllocatedIDs(ctx context.Context, keys []w.Key) ([]w.Key, error) {
	origKeys := toOriginalKeys(keys)
	respKeys, err := d.client.AllocateIDs(ctx, origKeys)
	if err != nil {
		return nil, toWrapperError(err)
	}

	wKeys := toWrapperKeys(respKeys)

	return wKeys, nil
}

func (d *datastoreImpl) Count(ctx context.Context, q w.Query) (int, error) {
	qImpl, ok := q.(*queryImpl)
	if !ok {
		return 0, errors.New("invalid query type")
	}
	if qImpl.firstError != nil {
		return 0, qImpl.firstError
	}

	count, err := d.client.Count(ctx, qImpl.q)
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
	origKeys, err := d.client.GetAll(ctx, qImpl.q, &origPss)
	if err != nil {
		return nil, toWrapperError(err)
	}

	wKeys := toWrapperKeys(origKeys)

	if !qImpl.keysOnly {
		for idx, origPs := range origPss {
			ps := toWrapperPropertyList(origPs)

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
	key := datastore.IncompleteKey(kind, parentKey)
	return toWrapperKey(key)
}

func (d *datastoreImpl) NameKey(kind, name string, parent w.Key) w.Key {
	parentKey := toOriginalKey(parent)
	key := datastore.NameKey(kind, name, parentKey)
	return toWrapperKey(key)
}

func (d *datastoreImpl) IDKey(kind string, id int64, parent w.Key) w.Key {
	parentKey := toOriginalKey(parent)
	key := datastore.IDKey(kind, id, parentKey)
	return toWrapperKey(key)
}

func (d *datastoreImpl) NewQuery(kind string) w.Query {
	q := datastore.NewQuery(kind)
	return &queryImpl{ctx: d.ctx, q: q}
}

func (d *datastoreImpl) Close() error {
	return d.client.Close()
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
