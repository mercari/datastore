package boom

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"go.mercari.io/datastore"
)

var typeOfKey = reflect.TypeOf((*datastore.Key)(nil)).Elem()

type Boom struct {
	Context context.Context
	Client  datastore.Client
}

func (bm *Boom) extractKeys(src interface{}) ([]datastore.Key, error) {
	v := reflect.Indirect(reflect.ValueOf(src))
	if v.Kind() != reflect.Slice {
		return nil, fmt.Errorf("boom: value must be a slice or pointer-to-slice or key-slice")
	}
	l := v.Len()

	keys := make([]datastore.Key, 0, l)
	for i := 0; i < l; i++ {
		v := v.Index(i)
		obj := v.Interface()

		key, ok := obj.(datastore.Key)
		if ok {
			keys = append(keys, key)
			continue
		}

		key, err := bm.KeyError(obj)
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}

func (bm *Boom) setStructKey(src interface{}, key datastore.Key) error {
	v := reflect.ValueOf(src)
	t := v.Type()
	k := t.Kind()

	if k != reflect.Ptr {
		return fmt.Errorf("boom: Expected pointer to struct, got instead: %v", k)
	}

	v = reflect.Indirect(v)
	t = v.Type()
	k = t.Kind()

	if k != reflect.Struct {
		return fmt.Errorf(fmt.Sprintf("boom: Expected struct, got instead: %v", k))
	}

	idSet := false
	kindSet := false
	parentSet := false
	for i := 0; i < v.NumField(); i++ {
		tf := t.Field(i)
		vf := v.Field(i)

		if !vf.CanSet() {
			continue
		}

		tag := tf.Tag.Get("boom")
		if tag == "" {
			tag = tf.Tag.Get("goon")
		}
		tagValues := strings.SplitN(tag, ",", 2)
		if len(tagValues) == 0 {
			continue
		}

		switch tagValues[0] {
		case "id":
			if idSet {
				return fmt.Errorf("boom: Only one field may be marked id")
			}

			pt, ok := vf.Interface().(datastore.PropertyTranslator)
			if ok {
				pv, err := pt.FromPropertyValue(bm.Context, datastore.Property{Value: key})
				if err != nil {
					return err
				}

				vf.Set(reflect.ValueOf(pv))

			} else {
				switch vf.Kind() {
				case reflect.Int64:
					vf.SetInt(key.ID())
				case reflect.String:
					vf.SetString(key.Name())
				}
			}

			idSet = true

		case "kind":
			if kindSet {
				return fmt.Errorf("boom: Only one field may be marked kind")
			}
			if vf.Kind() == reflect.String {
				if (len(tagValues) <= 1 || key.Kind() != tagValues[1]) && t.Name() != key.Kind() {
					vf.Set(reflect.ValueOf(key.Kind()))
				}
				kindSet = true
			}

		case "parent":
			if parentSet {
				return fmt.Errorf("boom: Only one field may be marked parent")
			}

			pt, ok := vf.Interface().(datastore.PropertyTranslator)
			if ok {
				pv, err := pt.FromPropertyValue(bm.Context, datastore.Property{Value: key.ParentKey()})
				if err != nil {
					return err
				}

				vf.Set(reflect.ValueOf(pv))
				parentSet = true

			} else {
				vfType := vf.Type()
				if vfType.ConvertibleTo(typeOfKey) {
					if key.ParentKey() != nil {
						vf.Set(reflect.ValueOf(key.ParentKey()).Convert(vfType))
					}
					parentSet = true
				}
			}
		}
	}

	if !idSet {
		return fmt.Errorf("boom: Could not set id field")
	}

	return nil
}

func (bm *Boom) Kind(src interface{}) string {
	key, err := bm.KeyError(src)
	if err != nil {
		return ""
	}

	return key.Kind()
}

func (bm *Boom) Key(src interface{}) datastore.Key {
	key, err := bm.KeyError(src)
	if err != nil {
		return nil
	}

	return key
}

func (bm *Boom) KeyError(src interface{}) (datastore.Key, error) {
	v := reflect.Indirect(reflect.ValueOf(src))
	t := v.Type()
	k := t.Kind()

	if k != reflect.Struct {
		return nil, fmt.Errorf("boom: Expected struct, got instead: %v", k)
	}

	var parent datastore.Key
	var keyName string
	var keyID int64
	var kind string

	for i := 0; i < v.NumField(); i++ {
		tf := t.Field(i)
		vf := v.Field(i)

		tag := tf.Tag.Get("boom")
		if tag == "" {
			tag = tf.Tag.Get("goon")
		}
		tagValues := strings.SplitN(tag, ",", 2)
		if len(tagValues) > 0 {
			switch tagValues[0] {
			case "id":

				pt, ok := vf.Interface().(datastore.PropertyTranslator)
				if ok {
					pv, err := pt.ToPropertyValue(bm.Context)
					if err != nil {
						return nil, err
					}
					if id, ok := pv.(int64); ok {
						if keyID != 0 || keyName != "" {
							return nil, fmt.Errorf("boom: Only one field may be marked id")
						}
						keyID = id
					} else if name, ok := pv.(string); ok {
						if keyID != 0 || keyName != "" {
							return nil, fmt.Errorf("boom: Only one field may be marked id")
						}
						keyName = name
					} else if key, ok := pv.(datastore.Key); ok {
						if keyID != 0 || keyName != "" {
							return nil, fmt.Errorf("boom: Only one field may be marked id")
						}
						if key.ID() != 0 {
							keyID = key.ID()
						} else if key.Name() != "" {
							keyName = key.Name()
						} else {
							return nil, fmt.Errorf("boom: ID field must be int64 or string in %v", t.Name())
						}
					}
				} else {
					switch vf.Kind() {
					case reflect.Int64:
						if keyID != 0 || keyName != "" {
							return nil, fmt.Errorf("boom: Only one field may be marked id")
						}
						keyID = vf.Int()
					case reflect.String:
						if keyID != 0 || keyName != "" {
							return nil, fmt.Errorf("boom: Only one field may be marked id")
						}
						keyName = vf.String()
					default:
						return nil, fmt.Errorf("boom: ID field must be int64 or string in %v", t.Name())
					}
				}

			case "kind":
				if vf.Kind() == reflect.String {
					if kind != "" {
						return nil, fmt.Errorf("boom: Only one field may be marked kind")
					}
					kind = vf.String()
					if kind == "" && len(tagValues) > 1 && tagValues[1] != "" {
						kind = tagValues[1]
					}
				}

			case "parent":
				pt, ok := vf.Interface().(datastore.PropertyTranslator)
				if ok {
					pv, err := pt.ToPropertyValue(bm.Context)
					if err != nil {
						return nil, err
					}
					if key, ok := pv.(datastore.Key); ok {
						if parent != nil {
							return nil, fmt.Errorf("boom: Only one field may be marked parent")
						}
						parent = key
					}
				} else {
					vfType := vf.Type()
					if !vf.IsNil() && vfType.ConvertibleTo(typeOfKey) {
						if parent != nil {
							return nil, fmt.Errorf("boom: Only one field may be marked parent")
						}
						parent = vf.Convert(typeOfKey).Interface().(datastore.Key)
					}
				}
			}
		}
	}

	if kind == "" {
		kind = t.Name()
	}

	if keyName != "" {
		return bm.Client.NameKey(kind, keyName, parent), nil
	}

	return bm.Client.IDKey(kind, keyID, parent), nil
}

func (bm *Boom) AllocateID(src interface{}) (datastore.Key, error) {
	srcs := []interface{}{src}
	keys, err := bm.AllocateIDs(srcs)
	if merr, ok := err.(datastore.MultiError); ok {
		return nil, merr[0]
	} else if err != nil {
		return nil, err
	}

	return keys[0], nil
}

func (bm *Boom) AllocateIDs(src interface{}) ([]datastore.Key, error) {
	v := reflect.Indirect(reflect.ValueOf(src))
	if v.Kind() != reflect.Slice {
		return nil, fmt.Errorf("boom: value must be a slice or pointer-to-slice or incompletekey-slice or string-slice")
	}
	l := v.Len()

	keys := make([]datastore.Key, 0, l)
	structIndex := make([]int, 0, l)
	for i := 0; i < l; i++ {
		v := v.Index(i)
		obj := v.Interface()

		key, ok := obj.(datastore.Key)
		if ok {
			keys = append(keys, key)
			continue
		}

		kind, ok := obj.(string)
		if ok {
			keys = append(keys, bm.Client.IncompleteKey(kind, nil))
			continue
		}

		key, err := bm.KeyError(obj)
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
		structIndex = append(structIndex, i)
	}

	keys, err := bm.Client.AllocateIDs(bm.Context, keys)
	if err != nil {
		return nil, err
	}

	for _, sIdx := range structIndex {
		v := v.Index(sIdx)
		obj := v.Interface()

		err = bm.setStructKey(obj, keys[sIdx])
		if err != nil {
			return nil, err
		}
	}

	return keys, nil
}

func (bm *Boom) Get(dst interface{}) error {
	dsts := []interface{}{dst}
	err := bm.GetMulti(dsts)
	if merr, ok := err.(datastore.MultiError); ok {
		return merr[0]
	} else if err != nil {
		return err
	}

	return nil
}

func (bm *Boom) GetMulti(dst interface{}) error {
	keys, err := bm.extractKeys(dst)
	if err != nil {
		return err
	}

	return bm.Client.GetMulti(bm.Context, keys, dst)
}

func (bm *Boom) Put(src interface{}) (datastore.Key, error) {
	srcs := []interface{}{src}
	keys, err := bm.PutMulti(srcs)
	if merr, ok := err.(datastore.MultiError); ok {
		return nil, merr[0]
	} else if err != nil {
		return nil, err
	}

	return keys[0], nil
}

func (bm *Boom) PutMulti(src interface{}) ([]datastore.Key, error) {
	keys, err := bm.extractKeys(src)
	if err != nil {
		return nil, err
	}

	keys, err = bm.Client.PutMulti(bm.Context, keys, src)
	if err != nil {
		return nil, err
	}

	v := reflect.Indirect(reflect.ValueOf(src))
	for idx, key := range keys {
		err = bm.setStructKey(v.Index(idx).Interface(), key)
		if err != nil {
			return nil, err
		}
	}

	return keys, nil
}

func (bm *Boom) Delete(src interface{}) error {
	srcs := []interface{}{src}
	err := bm.DeleteMulti(srcs)
	if merr, ok := err.(datastore.MultiError); ok {
		return merr[0]
	} else if err != nil {
		return err
	}

	return nil
}

func (bm *Boom) DeleteMulti(src interface{}) error {
	keys, err := bm.extractKeys(src)
	if err != nil {
		return err
	}

	return bm.Client.DeleteMulti(bm.Context, keys)
}

func (bm *Boom) NewTransaction() (*Transaction, error) {
	tx, err := bm.Client.NewTransaction(bm.Context)
	if err != nil {
		return nil, err
	}

	return &Transaction{bm: bm, tx: tx}, nil
}

func (bm *Boom) RunInTransaction(f func(tx *Transaction) error) (datastore.Commit, error) {
	var tx *Transaction
	commit, err := bm.Client.RunInTransaction(bm.Context, func(origTx datastore.Transaction) error {
		tx = &Transaction{bm: bm, tx: origTx}
		return f(tx)
	})
	if err != nil {
		return nil, err
	}

	for _, s := range tx.pendingKeysLater {
		key := commit.Key(s.pendingKey)
		err = tx.bm.setStructKey(s.src, key)
		if err != nil {
			return nil, err
		}
	}

	return commit, nil
}

func (bm *Boom) Run(q datastore.Query) *Iterator {
	it := bm.Client.Run(bm.Context, q)
	return &Iterator{bm: bm, it: it}
}

func (bm *Boom) Count(q datastore.Query) (int, error) {
	return bm.Client.Count(bm.Context, q)
}

func (bm *Boom) GetAll(q datastore.Query, dst interface{}) ([]datastore.Key, error) {
	keys, err := bm.Client.GetAll(bm.Context, q, dst)
	if err != nil {
		return nil, err
	}

	if dst == nil {
		return keys, nil
	}

	v := reflect.Indirect(reflect.ValueOf(dst))
	for idx, key := range keys {
		err = bm.setStructKey(v.Index(idx).Interface(), key)
		if err != nil {
			return nil, err
		}
	}

	return keys, nil
}

func (bm *Boom) Batch() *Batch {
	b := bm.Client.Batch()
	return &Batch{bm: bm, b: b}
}

func (bm *Boom) DecodeCursor(s string) (datastore.Cursor, error) {
	return bm.Client.DecodeCursor(s)
}

func (bm *Boom) NewQuery(k string) datastore.Query {
	return bm.Client.NewQuery(k)
}
