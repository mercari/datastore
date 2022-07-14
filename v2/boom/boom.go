package boom

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"go.mercari.io/datastore/v2"
)

var typeOfKey = reflect.TypeOf((*datastore.Key)(nil)).Elem()

// Boom is a datastore client wrapper to make it easy to understand the handling of Key.
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

// Kind retrieves kind name from struct.
func (bm *Boom) Kind(src interface{}) string {
	// bm.KeyError を使うと id が PropertyTranslator だった場合に無限再起する場合がある
	kind, err := bm.kindErr(src)
	if err != nil {
		return ""
	}
	return kind
}

func (bm *Boom) kindErr(src interface{}) (string, error) {
	v := reflect.Indirect(reflect.ValueOf(src))
	t := v.Type()
	k := t.Kind()

	if k != reflect.Struct {
		return "", fmt.Errorf("boom: Expected struct, got instead: %v", k)
	}

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
			case "kind":
				if vf.Kind() == reflect.String {
					if kind != "" {
						return "", fmt.Errorf("boom: Only one field may be marked kind")
					}
					kind = vf.String()
					if kind == "" && len(tagValues) > 1 && tagValues[1] != "" {
						kind = tagValues[1]
					}
				}
			}
		}
	}

	if kind == "" {
		kind = t.Name()
	}

	return kind, nil
}

// Key retrieves datastore key from struct without error occurred.
func (bm *Boom) Key(src interface{}) datastore.Key {
	key, err := bm.KeyError(src)
	if err != nil {
		return nil
	}

	return key
}

// KeyError retrieves datastore key from struct with error occurred.
func (bm *Boom) KeyError(src interface{}) (datastore.Key, error) {
	v := reflect.Indirect(reflect.ValueOf(src))
	t := v.Type()
	k := t.Kind()

	if k != reflect.Struct {
		return nil, fmt.Errorf("boom: Expected struct, got instead: %v", k)
	}

	var parent datastore.Key
	var key datastore.Key
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
						if key != nil || keyID != 0 || keyName != "" {
							return nil, fmt.Errorf("boom: Only one field may be marked id")
						}
						keyID = id
					} else if name, ok := pv.(string); ok {
						if key != nil || keyID != 0 || keyName != "" {
							return nil, fmt.Errorf("boom: Only one field may be marked id")
						}
						keyName = name
					} else if propertyKey, ok := pv.(datastore.Key); ok {
						if key != nil || keyID != 0 || keyName != "" {
							return nil, fmt.Errorf("boom: Only one field may be marked id")
						}
						key = propertyKey
					}
				} else {
					switch vf.Kind() {
					case reflect.Int64:
						if key != nil || keyID != 0 || keyName != "" {
							return nil, fmt.Errorf("boom: Only one field may be marked id")
						}
						keyID = vf.Int()
					case reflect.String:
						if key != nil || keyID != 0 || keyName != "" {
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

	if key != nil {
		if key.ParentKey() != nil && parent != nil {
			return nil, fmt.Errorf("boom: ID field returns key. don't use parent annotated field at same time")
		}
		if key.Kind() != kind {
			return nil, fmt.Errorf("boom: ID field returns key that contains unexpected kind")
		}

		if key.ParentKey() != nil {
			return key, nil
		}

		if keyName := key.Name(); keyName != "" {
			return bm.Client.NameKey(kind, keyName, parent), nil
		}

		return bm.Client.IDKey(kind, key.ID(), parent), nil
	}

	if keyName != "" {
		return bm.Client.NameKey(kind, keyName, parent), nil
	}

	return bm.Client.IDKey(kind, keyID, parent), nil
}

// AllocateID takes a struct whose key has not yet been set as an argument,
// allocates the Key of the relevant Kind, and sets it to a struct.
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

// AllocateIDs takes a slice of a struct whose key has not yet been set as an argument,
// secures the Key of the relevant Kind, and sets it to each struct.
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

// Get loads the entity stored for key into dst, which must be a struct pointer or implement PropertyLoadSaver.
// key will be extracted from dst.
//
// If there is no such entity for the key, Get returns ErrNoSuchEntity.
// The values of dst's unmatched struct fields are not modified, and matching slice-typed fields are not reset before appending to them.
// In particular, it is recommended to pass a pointer to a zero valued struct on each Get call.
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

// GetMulti is a batch version of Get.
// key will be extracted from each struct of dst.
//
// dst must be a []S, []*S, []I or []P, for some struct type S, some interface type I, or some non-interface non-pointer type P such that P or *P implements PropertyLoadSaver.
// If an []I, each element must be a valid dst for Get: it must be a struct pointer or implement PropertyLoadSaver.
func (bm *Boom) GetMulti(dst interface{}) error {
	keys, err := bm.extractKeys(dst)
	if err != nil {
		return err
	}

	return bm.Client.GetMulti(bm.Context, keys, dst)
}

// Put saves the entity src into the datastore.
// key will be extract from src struct.
// src must be a struct pointer or implement PropertyLoadSaver; if a struct pointer then any unexported fields of that struct will be skipped.
// If k is an incomplete key, the returned key will be a unique key generated by the datastore,
// and inject key to src struct.
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

// PutMulti is a batch version of Put.
//
// src must satisfy the same conditions as the dst argument to GetMulti.
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

// Delete deletes the entity.
// key will be extract from src struct.
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

// DeleteMulti is a batch version of Delete.
func (bm *Boom) DeleteMulti(src interface{}) error {
	keys, err := bm.extractKeys(src)
	if err != nil {
		return err
	}

	return bm.Client.DeleteMulti(bm.Context, keys)
}

// NewTransaction starts a new transaction.
func (bm *Boom) NewTransaction() (*Transaction, error) {
	tx, err := bm.Client.NewTransaction(bm.Context)
	if err != nil {
		return nil, err
	}

	return &Transaction{bm: bm, tx: tx}, nil
}

// RunInTransaction runs f in a transaction. f is invoked with a Transaction that f should use for all the transaction's datastore operations.
//
// f must not call Commit or Rollback on the provided Transaction.
//
// If f returns nil, RunInTransaction commits the transaction, returning the Commit and a nil error if it succeeds.
// If the commit fails due to a conflicting transaction, RunInTransaction gives up and returns ErrConcurrentTransaction immediately.
// If you want to retry operation, You have to retry by yourself.
//
// If f returns non-nil, then the transaction will be rolled back and RunInTransaction will return the same error.
//
// Note that when f returns, the transaction is not committed. Calling code must not assume that any of f's changes have been committed until RunInTransaction returns nil.
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

// Run runs the given query.
func (bm *Boom) Run(q datastore.Query) *Iterator {
	it := bm.Client.Run(bm.Context, q)
	return &Iterator{bm: bm, it: it}
}

// Count returns the number of results for the given query.
//
// The running time and number of API calls made by Count scale linearly with with the sum of the query's offset and limit.
// Unless the result count is expected to be small, it is best to specify a limit; otherwise Count will continue until it finishes counting or the provided context expires.
func (bm *Boom) Count(q datastore.Query) (int, error) {
	return bm.Client.Count(bm.Context, q)
}

// GetAll runs the provided query that it returns all entities that match that query, as well as appending the values to dst.
//
// dst must have type *[]S or *[]*S or *[]P, for some struct type S or some non-interface, non-pointer type P such that P or *P implements PropertyLoadSaver.
//
// As a special case, *PropertyList is an invalid type for dst, even though a PropertyList is a slice of structs.
// It is treated as invalid to avoid being mistakenly passed when *[]PropertyList was intended.
//
// The keys are injected to each dst struct.
//
// If q is a “keys-only” query, GetAll ignores dst and only returns the keys.
//
// The running time and number of API calls made by GetAll scale linearly with with the sum of the query's offset and limit.
// Unless the result count is expected to be small, it is best to specify a limit; otherwise GetAll will continue until it finishes collecting results or the provided context expires.
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

// Batch creates batch mode objects.
func (bm *Boom) Batch() *Batch {
	b := bm.Client.Batch()
	return &Batch{bm: bm, b: b}
}

// DecodeCursor from its base-64 string representation.
func (bm *Boom) DecodeCursor(s string) (datastore.Cursor, error) {
	return bm.Client.DecodeCursor(s)
}

// NewQuery creates a new Query for a specific entity kind.
//
// An empty kind means to return all entities, including entities created and managed by other App Engine features, and is called a kindless query.
// Kindless queries cannot include filters or sort orders on property values.
func (bm *Boom) NewQuery(k string) datastore.Query {
	return bm.Client.NewQuery(k)
}
