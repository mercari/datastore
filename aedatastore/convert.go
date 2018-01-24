package aedatastore

import (
	"context"
	"fmt"
	"time"

	w "go.mercari.io/datastore"
	"google.golang.org/api/iterator"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
)

func namespaceFromContext(ctx context.Context) string {
	if ctx == nil {
		panic("ctx is nil")
	}
	return datastore.NewIncompleteKey(ctx, "FooBarTest", nil).Namespace()
}

func toOriginalKey(key w.Key) *datastore.Key {
	if key == nil {
		return nil
	}

	keyImpl := key.(*keyImpl)
	ctx := keyImpl.ctx

	// NOTE appengine.Namespace 呼ぶのは内部でregexpで値チェックしてるので遅い可能性がある…？(のでif文つけてる
	if namespaceFromContext(ctx) != key.Namespace() {
		var err error
		ctx, err = appengine.Namespace(ctx, key.Namespace())
		if err != nil {
			panic(err)
		}
	}

	origPK := toOriginalKey(key.ParentKey())
	return datastore.NewKey(ctx, key.Kind(), key.Name(), key.ID(), origPK)
}

func toOriginalKeys(keys []w.Key) []*datastore.Key {
	if keys == nil {
		return nil
	}

	origKeys := make([]*datastore.Key, len(keys))
	for idx, key := range keys {
		origKeys[idx] = toOriginalKey(key)
	}

	return origKeys
}

func toWrapperKey(ctx context.Context, key *datastore.Key) *keyImpl {
	if key == nil {
		return nil
	}

	return &keyImpl{
		ctx:       ctx,
		kind:      key.Kind(),
		id:        key.IntID(),
		name:      key.StringID(),
		parent:    toWrapperKey(ctx, key.Parent()),
		namespace: key.Namespace(),
	}
}

func toOriginalPendingKey(pKey w.PendingKey) *datastore.Key {
	if pKey == nil {
		return nil
	}
	pk, ok := pKey.StoredContext().Value(contextPendingKey{}).(*pendingKeyImpl)
	if !ok {
		return nil
	}

	if pk == nil || pk.key == nil {
		return nil
	}

	return pk.key
}

func toWrapperKeys(ctx context.Context, keys []*datastore.Key) []w.Key {
	if keys == nil {
		return nil
	}

	wKeys := make([]w.Key, len(keys))
	for idx, key := range keys {
		wKeys[idx] = toWrapperKey(ctx, key)
	}

	return wKeys
}

func toWrapperPendingKey(ctx context.Context, key *datastore.Key) *pendingKeyImpl {
	if key == nil {
		return nil
	}

	return &pendingKeyImpl{
		ctx: ctx,
		key: key,
	}
}

func toWrapperPendingKeys(ctx context.Context, keys []*datastore.Key) []w.PendingKey {
	if keys == nil {
		return nil
	}

	wKeys := make([]w.PendingKey, len(keys))
	for idx, key := range keys {
		wKeys[idx] = toWrapperPendingKey(ctx, key)
	}

	return wKeys
}

func toWrapperError(err error) error {
	if err == nil {
		return nil
	}

	switch {
	case err == datastore.ErrNoSuchEntity:
		return w.ErrNoSuchEntity

	case err == datastore.Done:
		// align to Cloud Datastore API.
		return iterator.Done

	case err == datastore.ErrConcurrentTransaction:
		return w.ErrConcurrentTransaction

	case err == datastore.ErrInvalidEntityType:
		return w.ErrInvalidEntityType

	case err == datastore.ErrInvalidKey:
		return w.ErrInvalidKey

	default:
		switch err := err.(type) {
		case *datastore.ErrFieldMismatch:
			return &w.ErrFieldMismatch{
				StructType: err.StructType,
				FieldName:  err.FieldName,
				Reason:     err.Reason,
			}

		case appengine.MultiError:
			merr := err
			newErr := make(w.MultiError, 0, len(merr))
			for _, err := range merr {
				if err != nil {
					newErr = append(newErr, toWrapperError(err))
					continue
				}

				newErr = append(newErr, nil)
			}
			return newErr
		}

		return err
	}
}

func toOriginalValue(v interface{}) (interface{}, error) {
	switch v := v.(type) {
	case []interface{}:
		vs := v
		origVs := make([]interface{}, 0, len(v))
		for _, v := range vs {
			origV, err := toOriginalValue(v)
			if err != nil {
				return nil, err
			}
			origVs = append(origVs, origV)
		}
		return origVs, nil

	case *w.Entity, []*w.Entity:
		return nil, w.ErrInvalidEntityType

	case w.Key:
		return toOriginalKey(v), nil
	case []w.Key:
		return toOriginalKeys(v), nil

	case w.GeoPoint:
		return appengine.GeoPoint{Lat: v.Lat, Lng: v.Lng}, nil
	case []w.GeoPoint:
		vs := v
		origVs := make([]appengine.GeoPoint, 0, len(v))
		for _, v := range vs {
			origV, err := toOriginalValue(v)
			if err != nil {
				return nil, err
			}
			origVs = append(origVs, origV.(appengine.GeoPoint))
		}
		return origVs, nil

	default:
		return v, nil
	}
}

func toWrapperValue(ctx context.Context, v interface{}) interface{} {
	switch v := v.(type) {
	case []interface{}:
		vs := v
		wVs := make([]interface{}, 0, len(v))
		for _, v := range vs {
			wVs = append(wVs, toWrapperValue(ctx, v))
		}
		return wVs

	case *datastore.Key:
		return toWrapperKey(ctx, v)
	case []*datastore.Key:
		return toWrapperKeys(ctx, v)

	case time.Time:
		// align to cloud datastore.
		// cloud datastore uses machine locale.
		return v.In(time.Local)

	case appengine.GeoPoint:
		return w.GeoPoint{Lat: v.Lat, Lng: v.Lng}
	case []appengine.GeoPoint:
		vs := v
		wVs := make([]w.GeoPoint, 0, len(v))
		for _, v := range vs {
			wVs = append(wVs, toWrapperValue(ctx, v).(w.GeoPoint))
		}
		return wVs

	default:
		return v
	}
}

func toOriginalProperty(p w.Property) (datastore.Property, error) {
	v, err := toOriginalValue(p.Value)
	if err != nil {
		return datastore.Property{}, err
	}
	origP := datastore.Property{
		Name:    p.Name,
		Value:   v,
		NoIndex: p.NoIndex,
	}
	return origP, nil
}

func toOriginalPropertyList(ps w.PropertyList) (datastore.PropertyList, error) {
	// NOTE Cloud Datastore側の仕様に寄せているため、PropertyのValueが[]interface{}の場合がある
	// その場合、1要素毎に分解してMultiple=trueをセットしてやらないといけない

	if ps == nil {
		return nil, nil
	}

	newPs := make([]datastore.Property, 0, len(ps))
	for _, p := range ps {
		switch v := p.Value.(type) {
		case []interface{}:
			newV := make([]datastore.Property, 0, len(v))
			for _, pV := range v {
				origV, err := toOriginalValue(pV)
				if err != nil {
					return nil, err
				}
				origP, err := toOriginalProperty(w.Property{
					Name:    p.Name,
					Value:   origV,
					NoIndex: p.NoIndex,
				})
				if err != nil {
					return nil, err
				}
				origP.Multiple = true
				newV = append(newV, origP)
			}
			newPs = append(newPs, newV...)

		case []*w.Entity:
			newV := make([]datastore.Property, 0, len(v))
			for _, pV := range v {
				origV, err := toOriginalValue(pV)
				if err != nil {
					return nil, err
				}
				origP, err := toOriginalProperty(w.Property{
					Name:    p.Name,
					Value:   origV,
					NoIndex: p.NoIndex,
				})
				if err != nil {
					return nil, err
				}
				origP.Multiple = true
				newV = append(newV, origP)
			}
			newPs = append(newPs, newV...)

		case []w.Key:
			newV := make([]datastore.Property, 0, len(v))
			for _, pV := range v {
				origV, err := toOriginalValue(pV)
				if err != nil {
					return nil, err
				}
				origP, err := toOriginalProperty(w.Property{
					Name:    p.Name,
					Value:   origV,
					NoIndex: p.NoIndex,
				})
				if err != nil {
					return nil, err
				}
				origP.Multiple = true
				newV = append(newV, origP)
			}
			newPs = append(newPs, newV...)

		case []w.GeoPoint:
			newV := make([]datastore.Property, 0, len(v))
			for _, pV := range v {
				origV, err := toOriginalValue(pV)
				if err != nil {
					return nil, err
				}
				origP, err := toOriginalProperty(w.Property{
					Name:    p.Name,
					Value:   origV,
					NoIndex: p.NoIndex,
				})
				if err != nil {
					return nil, err
				}
				origP.Multiple = true
				newV = append(newV, origP)
			}
			newPs = append(newPs, newV...)

		default:
			newP, err := toOriginalProperty(p)
			if err != nil {
				return nil, err
			}
			newPs = append(newPs, newP)
		}
	}

	return newPs, nil
}

func toOriginalPropertyListList(pss []w.PropertyList) ([]datastore.PropertyList, error) {
	if pss == nil {
		return nil, nil
	}

	newPss := make([]datastore.PropertyList, 0, len(pss))
	for _, ps := range pss {
		newPs, err := toOriginalPropertyList(ps)
		if err != nil {
			return nil, err
		}
		newPss = append(newPss, newPs)
	}

	return newPss, nil
}

func toWrapperProperty(ctx context.Context, p datastore.Property) w.Property {
	return w.Property{
		Name:    p.Name,
		Value:   toWrapperValue(ctx, p.Value),
		NoIndex: p.NoIndex,
	}
}

func toWrapperPropertyMulti(ctx context.Context, ps []datastore.Property) w.Property {
	var name string
	var noIndex bool
	vs := make([]interface{}, 0, len(ps))
	for _, p := range ps {
		if name == "" {
			name = p.Name
			noIndex = p.NoIndex
		} else if name != p.Name {
			panic(fmt.Sprintf("property name mismatch: %s - %s", name, p.Name))
		}
		vs = append(vs, toWrapperValue(ctx, p.Value))
	}
	return w.Property{
		Name:    name,
		Value:   vs,
		NoIndex: noIndex,
	}
}

func toWrapperPropertyList(ctx context.Context, ps datastore.PropertyList) w.PropertyList {
	// NOTE Cloud Datastore側の仕様に寄せているため、Multiple=trueの場合、
	// 同名の要素を集めて[]interface{}にしてやらないといけない
	// `datastore:",flatten" を使っていない場合のサポートは考えない

	if ps == nil {
		return nil
	}

	var multiMap map[string]bool
	newPs := make([]w.Property, 0, len(ps))
	for idx, p := range ps {
		if p.Multiple {
			if multiMap == nil {
				multiMap = make(map[string]bool)
			} else if multiMap[p.Name] {
				continue
			}

			subPs := make([]datastore.Property, 0)
			subPs = append(subPs, p)
			for _, p2 := range ps[idx+1:] {
				if p.Name == p2.Name {
					subPs = append(subPs, p2)
				}
			}
			newPs = append(newPs, toWrapperPropertyMulti(ctx, subPs))

			multiMap[p.Name] = true

		} else {
			newPs = append(newPs, toWrapperProperty(ctx, p))
		}
	}

	return newPs
}

func toWrapperPropertyListList(ctx context.Context, pss []datastore.PropertyList) []w.PropertyList {
	if pss == nil {
		return nil
	}

	newPss := make([]w.PropertyList, 0, len(pss))
	for _, ps := range pss {
		newPss = append(newPss, toWrapperPropertyList(ctx, ps))
	}

	return newPss
}
