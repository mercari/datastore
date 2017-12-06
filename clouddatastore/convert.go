package clouddatastore

import (
	"cloud.google.com/go/datastore"
	w "go.mercari.io/datastore"
)

func toOriginalKey(key w.Key) *datastore.Key {
	if key == nil {
		return nil
	}

	return &datastore.Key{
		Kind:      key.Kind(),
		ID:        key.ID(),
		Name:      key.Name(),
		Parent:    toOriginalKey(key.ParentKey()),
		Namespace: key.Namespace(),
	}
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

func toWrapperKey(key *datastore.Key) *keyImpl {
	if key == nil {
		return nil
	}

	return &keyImpl{
		kind:      key.Kind,
		id:        key.ID,
		name:      key.Name,
		parent:    toWrapperKey(key.Parent),
		namespace: key.Namespace,
	}
}

func toOriginalPendingKey(pKey w.PendingKey) *datastore.PendingKey {
	if pKey == nil {
		return nil
	}
	pk, ok := pKey.StoredContext().Value(contextPendingKey{}).(*pendingKeyImpl)
	if !ok {
		return nil
	}

	if pk == nil || pk.pendingKey == nil {
		return nil
	}

	return pk.pendingKey
}

func toWrapperKeys(keys []*datastore.Key) []w.Key {
	if keys == nil {
		return nil
	}

	wKeys := make([]w.Key, len(keys))
	for idx, key := range keys {
		wKeys[idx] = toWrapperKey(key)
	}

	return wKeys
}

func toWrapperPendingKey(pendingKey *datastore.PendingKey) *pendingKeyImpl {
	if pendingKey == nil {
		return nil
	}

	return &pendingKeyImpl{
		pendingKey: pendingKey,
	}
}

func toWrapperPendingKeys(keys []*datastore.PendingKey) []w.PendingKey {
	if keys == nil {
		return nil
	}

	wKeys := make([]w.PendingKey, len(keys))
	for idx, key := range keys {
		wKeys[idx] = toWrapperPendingKey(key)
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

		case datastore.MultiError:
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

func toOriginalEntity(entity *w.Entity) *datastore.Entity {
	if entity == nil {
		return nil
	}

	return &datastore.Entity{
		Key:        toOriginalKey(entity.Key),
		Properties: toOriginalPropertyList(entity.Properties),
	}
}

func toOriginalValue(v interface{}) interface{} {
	switch v := v.(type) {
	case []interface{}:
		vs := v
		origVs := make([]interface{}, 0, len(v))
		for _, v := range vs {
			origVs = append(origVs, toOriginalValue(v))
		}
		return origVs

	case *w.Entity:
		return toOriginalEntity(v)
	case []*w.Entity:
		vs := v
		origVs := make([]*datastore.Entity, 0, len(v))
		for _, v := range vs {
			origVs = append(origVs, toOriginalValue(v).(*datastore.Entity))
		}
		return origVs

	case w.Key:
		return toOriginalKey(v)
	case []w.Key:
		return toOriginalKeys(v)

	case w.GeoPoint:
		return datastore.GeoPoint{Lat: v.Lat, Lng: v.Lng}
	case []w.GeoPoint:
		vs := v
		origVs := make([]datastore.GeoPoint, 0, len(v))
		for _, v := range vs {
			origVs = append(origVs, toOriginalValue(v).(datastore.GeoPoint))
		}
		return origVs

	default:
		return v
	}
}

func toWrapperValue(v interface{}) interface{} {
	switch v := v.(type) {
	case []interface{}:
		vs := v
		wVs := make([]interface{}, 0, len(v))
		for _, v := range vs {
			wVs = append(wVs, toWrapperValue(v))
		}
		return wVs

	case *datastore.Entity:
		if v == nil {
			return nil
		}
		return toWrapperEntity(v)
	case []*datastore.Entity:
		vs := v
		wVs := make([]*w.Entity, 0, len(v))
		for _, v := range vs {
			wVs = append(wVs, toWrapperValue(v).(*w.Entity))
		}
		return wVs

	case *datastore.Key:
		return toWrapperKey(v)
	case []*datastore.Key:
		return toWrapperKeys(v)

	case datastore.GeoPoint:
		return w.GeoPoint{Lat: v.Lat, Lng: v.Lng}
	case []datastore.GeoPoint:
		vs := v
		wVs := make([]w.GeoPoint, 0, len(v))
		for _, v := range vs {
			wVs = append(wVs, toWrapperValue(v).(w.GeoPoint))
		}
		return wVs

	default:
		return v
	}
}

func toOriginalProperty(p w.Property) datastore.Property {
	return datastore.Property{
		Name:    p.Name,
		Value:   toOriginalValue(p.Value),
		NoIndex: p.NoIndex,
	}
}

func toOriginalPropertyList(ps w.PropertyList) datastore.PropertyList {
	if ps == nil {
		return nil
	}

	newPs := make([]datastore.Property, 0, len(ps))
	for _, p := range ps {
		newPs = append(newPs, toOriginalProperty(p))
	}

	return newPs
}

func toOriginalPropertyListList(pss []w.PropertyList) []datastore.PropertyList {
	if pss == nil {
		return nil
	}

	newPss := make([]datastore.PropertyList, 0, len(pss))
	for _, ps := range pss {
		newPss = append(newPss, toOriginalPropertyList(ps))
	}

	return newPss
}

func toWrapperEntity(entity *datastore.Entity) *w.Entity {
	if entity == nil {
		return nil
	}

	return &w.Entity{
		Key:        toWrapperKey(entity.Key),
		Properties: toWrapperPropertyList(entity.Properties),
	}
}

func toWrapperProperty(p datastore.Property) w.Property {
	return w.Property{
		Name:    p.Name,
		Value:   toWrapperValue(p.Value),
		NoIndex: p.NoIndex,
	}
}

func toWrapperPropertyList(ps datastore.PropertyList) w.PropertyList {
	if ps == nil {
		return nil
	}

	newPs := make([]w.Property, 0, len(ps))
	for _, p := range ps {
		newPs = append(newPs, toWrapperProperty(p))
	}

	return newPs
}

func toWrapperPropertyListList(pss []datastore.PropertyList) []w.PropertyList {
	if pss == nil {
		return nil
	}

	newPss := make([]w.PropertyList, 0, len(pss))
	for _, ps := range pss {
		newPss = append(newPss, toWrapperPropertyList(ps))
	}

	return newPss
}

func toOriginalTransaction(tx w.Transaction) *datastore.Transaction {
	baseTx := getTx(tx.(*transactionImpl).client.ctx)
	if tx == nil {
		panic("not in transaction")
	}

	return baseTx
}
