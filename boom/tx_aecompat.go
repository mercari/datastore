package boom

import (
	"reflect"

	"go.mercari.io/datastore"
)

// IMPORTANT NOTICE: You should use *boom.Transaction.

var _ AECompatibleOperations = &Boom{}
var _ AECompatibleOperations = &AECompatibleTransaction{}

type AECompatibleOperations interface {
	Kind(src interface{}) string
	Key(src interface{}) datastore.Key
	KeyError(src interface{}) (datastore.Key, error)
	Get(dst interface{}) error
	GetMulti(dst interface{}) error
	Put(src interface{}) (datastore.Key, error)
	PutMulti(src interface{}) ([]datastore.Key, error)
	Delete(src interface{}) error
	DeleteMulti(src interface{}) error
}

func ToAECompatibleTransaction(tx *Transaction) *AECompatibleTransaction {
	return &AECompatibleTransaction{bm: tx.bm, tx: tx.tx}
}

type AECompatibleTransaction struct {
	bm *Boom
	tx datastore.Transaction
}

func (tx *AECompatibleTransaction) Boom() *Boom {
	return tx.bm
}

func (tx *AECompatibleTransaction) Kind(src interface{}) string {
	return tx.bm.Kind(src)
}

func (tx *AECompatibleTransaction) Key(src interface{}) datastore.Key {
	return tx.bm.Key(src)
}

func (tx *AECompatibleTransaction) KeyError(src interface{}) (datastore.Key, error) {
	return tx.bm.KeyError(src)
}

func (tx *AECompatibleTransaction) Get(dst interface{}) error {
	dsts := []interface{}{dst}
	err := tx.GetMulti(dsts)
	if merr, ok := err.(datastore.MultiError); ok {
		return merr[0]
	} else if err != nil {
		return err
	}

	return nil
}

func (tx *AECompatibleTransaction) GetMulti(dst interface{}) error {
	keys, err := tx.bm.extractKeys(dst)
	if err != nil {
		return err
	}

	return tx.tx.GetMulti(keys, dst)
}

func (tx *AECompatibleTransaction) Put(src interface{}) (datastore.Key, error) {
	srcs := []interface{}{src}
	keys, err := tx.PutMulti(srcs)
	if merr, ok := err.(datastore.MultiError); ok {
		return nil, merr[0]
	} else if err != nil {
		return nil, err
	}

	return keys[0], nil
}

func (tx *AECompatibleTransaction) PutMulti(src interface{}) ([]datastore.Key, error) {
	keys, err := tx.bm.extractKeys(src)
	if err != nil {
		return nil, err
	}

	// This api should returns []datastore.Key.
	// Use AllocateIDs instead of []datastore.PendingKey.
	incompleteIndexes := make([]int, 0, len(keys))
	incompleteKeys := make([]datastore.Key, 0, len(keys))
	for idx, key := range keys {
		if key.Incomplete() {
			incompleteIndexes = append(incompleteIndexes, idx)
			incompleteKeys = append(incompleteKeys, key)
		}
	}
	incompleteKeys, err = tx.bm.AllocateIDs(incompleteKeys)
	if err != nil {
		return nil, err
	}
	for idx, inIdx := range incompleteIndexes {
		keys[inIdx] = incompleteKeys[idx]
	}

	_, err = tx.tx.PutMulti(keys, src)
	if err != nil {
		return nil, err
	}

	v := reflect.Indirect(reflect.ValueOf(src))
	for idx, key := range keys {
		err = tx.bm.setStructKey(v.Index(idx).Interface(), key)
		if err != nil {
			return nil, err
		}
	}

	return keys, nil
}

func (tx *AECompatibleTransaction) Delete(src interface{}) error {
	srcs := []interface{}{src}
	err := tx.DeleteMulti(srcs)
	if merr, ok := err.(datastore.MultiError); ok {
		return merr[0]
	} else if err != nil {
		return err
	}

	return nil
}

func (tx *AECompatibleTransaction) DeleteMulti(src interface{}) error {
	keys, err := tx.bm.extractKeys(src)
	if err != nil {
		return err
	}

	return tx.tx.DeleteMulti(keys)
}

func (tx *AECompatibleTransaction) Commit() (datastore.Commit, error) {
	commit, err := tx.tx.Commit()
	if err != nil {
		return nil, err
	}

	return commit, nil
}

func (tx *AECompatibleTransaction) Rollback() error {
	return tx.tx.Rollback()
}
