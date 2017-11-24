package clouddatastore

import (
	"context"
	"errors"
	"reflect"

	"cloud.google.com/go/datastore"
	w "go.mercari.io/datastore"
)

type getOps func(keys []*datastore.Key, dst []datastore.PropertyList) error
type putOps func(keys []*datastore.Key, src []datastore.PropertyList) ([]w.Key, []w.PendingKey, error)
type deleteOps func(keys []*datastore.Key) error

func getMultiOps(ctx context.Context, keys []w.Key, dst interface{}, ops getOps) error {
	v := reflect.ValueOf(dst)
	if v.Kind() != reflect.Slice {
		return errors.New("datastore: dst has invalid type")
	}
	if len(keys) != v.Len() {
		return errors.New("datastore: keys and dst slices have different length")
	}
	if len(keys) == 0 {
		return nil
	}

	origKeys := toOriginalKeys(keys)
	origPss := make([]datastore.PropertyList, len(keys))
	err := ops(origKeys, origPss)
	foundError := false

	var merr []error
	origMerr, catchMerr := err.(datastore.MultiError)
	if catchMerr || err == nil {
		merr = make([]error, len(keys))
	} else if err != nil {
		return toWrapperError(err)
	}

	elemType := v.Type().Elem()
	for idx := range keys {
		if catchMerr {
			err := origMerr[idx]
			if _, ok := err.(*datastore.ErrFieldMismatch); ok {
				merr[idx] = toWrapperError(err)
				foundError = true
			} else if err != nil {
				merr[idx] = toWrapperError(err)
				foundError = true
				continue
			}
		}
		ps := toWrapperPropertyList(origPss[idx])
		elem := v.Index(idx)

		if reflect.PtrTo(elemType).Implements(typeOfPropertyLoadSaver) {
			elem = elem.Addr()
		} else if elemType.Kind() == reflect.Struct {
			elem = elem.Addr()
		} else if elemType.Kind() == reflect.Ptr && elemType.Elem().Kind() == reflect.Struct {
			if elem.IsNil() {
				elem.Set(reflect.New(elem.Type().Elem()))
			}
		}

		if err = w.LoadEntity(ctx, elem.Interface(), &w.Entity{Key: keys[idx], Properties: ps}); err != nil {
			merr[idx] = err
			foundError = true
		}
	}

	if foundError {
		return w.MultiError(merr)
	}

	return nil
}

func putMultiOps(ctx context.Context, keys []w.Key, src interface{}, ops putOps) ([]w.Key, []w.PendingKey, error) {
	v := reflect.ValueOf(src)
	if v.Kind() != reflect.Slice {
		return nil, nil, errors.New("datastore: src has invalid type")
	}
	if len(keys) != v.Len() {
		return nil, nil, errors.New("datastore: key and src slices have different length")
	}
	if len(keys) == 0 {
		return nil, nil, nil
	}

	var origPss []datastore.PropertyList
	for idx, key := range keys {
		elem := v.Index(idx)
		if reflect.PtrTo(elem.Type()).Implements(typeOfPropertyLoadSaver) || elem.Type().Kind() == reflect.Struct {
			elem = elem.Addr()
		}
		src := elem.Interface()
		e, err := w.SaveEntity(ctx, key, src)
		if err != nil {
			return nil, nil, toWrapperError(err)
		}
		origPs := toOriginalPropertyList(e.Properties)
		origPss = append(origPss, origPs)
	}

	origKeys := toOriginalKeys(keys)
	wKeys, wPKeys, err := ops(origKeys, origPss)
	if err != nil {
		return nil, nil, toWrapperError(err)
	}

	return wKeys, wPKeys, nil
}

func deleteMultiOps(ctx context.Context, keys []w.Key, ops deleteOps) error {
	origKeys := toOriginalKeys(keys)

	err := ops(origKeys)
	if err != nil {
		return toWrapperError(err)
	}

	return nil
}
