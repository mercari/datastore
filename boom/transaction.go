package boom

import (
	"reflect"
	"sync"

	"go.mercari.io/datastore"
)

type Transaction struct {
	m                sync.Mutex
	bm               *Boom
	tx               datastore.Transaction
	pendingKeysLater []*setKeyLater
}

type setKeyLater struct {
	pendingKey datastore.PendingKey
	src        interface{}
}

func (tx *Transaction) Boom() *Boom {
	return tx.bm
}

func (tx *Transaction) Kind(src interface{}) string {
	return tx.bm.Kind(src)
}

func (tx *Transaction) Key(src interface{}) datastore.Key {
	return tx.bm.Key(src)
}

func (tx *Transaction) KeyError(src interface{}) (datastore.Key, error) {
	return tx.bm.KeyError(src)
}

func (tx *Transaction) Get(dst interface{}) error {
	dsts := []interface{}{dst}
	err := tx.GetMulti(dsts)
	if merr, ok := err.(datastore.MultiError); ok {
		return merr[0]
	} else if err != nil {
		return err
	}

	return nil
}

func (tx *Transaction) GetMulti(dst interface{}) error {
	keys, err := tx.bm.extractKeys(dst)
	if err != nil {
		return err
	}

	return tx.tx.GetMulti(keys, dst)
}

func (tx *Transaction) Put(src interface{}) (datastore.PendingKey, error) {
	srcs := []interface{}{src}
	keys, err := tx.PutMulti(srcs)
	if merr, ok := err.(datastore.MultiError); ok {
		return nil, merr[0]
	} else if err != nil {
		return nil, err
	}

	return keys[0], nil
}

func (tx *Transaction) PutMulti(src interface{}) ([]datastore.PendingKey, error) {
	keys, err := tx.bm.extractKeys(src)
	if err != nil {
		return nil, err
	}

	pKeys, err := tx.tx.PutMulti(keys, src)
	if err != nil {
		return nil, err
	}

	v := reflect.Indirect(reflect.ValueOf(src))
	tx.m.Lock()
	defer tx.m.Unlock()
	for idx, pKey := range pKeys {
		if !keys[idx].Incomplete() {
			continue
		}
		tx.pendingKeysLater = append(tx.pendingKeysLater, &setKeyLater{
			pendingKey: pKey,
			src:        v.Index(idx).Interface(),
		})
	}

	return pKeys, nil
}

func (tx *Transaction) Delete(src interface{}) error {
	srcs := []interface{}{src}
	err := tx.DeleteMulti(srcs)
	if merr, ok := err.(datastore.MultiError); ok {
		return merr[0]
	} else if err != nil {
		return err
	}

	return nil
}

func (tx *Transaction) DeleteMulti(src interface{}) error {
	keys, err := tx.bm.extractKeys(src)
	if err != nil {
		return err
	}

	return tx.tx.DeleteMulti(keys)
}

func (tx *Transaction) Commit() (datastore.Commit, error) {
	commit, err := tx.tx.Commit()
	if err != nil {
		return nil, err
	}

	tx.m.Lock()
	defer tx.m.Unlock()

	for _, s := range tx.pendingKeysLater {
		key := commit.Key(s.pendingKey)
		err = tx.bm.setStructKey(s.src, key)
		if err != nil {
			return nil, err
		}
	}
	tx.pendingKeysLater = nil

	return commit, nil
}

func (tx *Transaction) Rollback() error {
	return tx.tx.Rollback()
}

func (tx *Transaction) Batch() *TransactionBatch {
	b := tx.tx.Batch()
	return &TransactionBatch{bm: tx.bm, tx: tx, b: b}
}
