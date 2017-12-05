package datastore

import (
	"sync"
)

type TransactionBatch struct {
	Transaction Transaction

	put    txBatchPut
	get    txBatchGet
	delete txBatchDelete
}

type TxBatchPutHandler func(pKey PendingKey, err error) error

type txBatchPut struct {
	m    sync.Mutex
	keys []Key
	srcs []interface{}
	hs   []TxBatchPutHandler
}

type txBatchGet struct {
	m    sync.Mutex
	keys []Key
	dsts []interface{}
	hs   []BatchErrHandler
}

type txBatchDelete struct {
	m    sync.Mutex
	keys []Key
	hs   []BatchErrHandler
}

func (b *TransactionBatch) Put(key Key, src interface{}, h TxBatchPutHandler) {
	b.put.Put(key, src, h)
}

func (b *TransactionBatch) Get(key Key, dst interface{}, h BatchErrHandler) {
	b.get.Get(key, dst, h)
}

func (b *TransactionBatch) Delete(key Key, h BatchErrHandler) {
	b.delete.Delete(key, h)
}

func (b *TransactionBatch) Exec() error {
	var wg sync.WaitGroup
	var errors []error
	var m sync.Mutex
	wg.Add(3)

	go func() {
		defer wg.Done()
		errs := b.put.Exec(b.Transaction)
		if len(errs) != 0 {
			m.Lock()
			errors = append(errors, errs...)
			m.Unlock()
		}
	}()
	go func() {
		defer wg.Done()
		errs := b.get.Exec(b.Transaction)
		if len(errs) != 0 {
			m.Lock()
			errors = append(errors, errs...)
			m.Unlock()
		}
	}()
	go func() {
		defer wg.Done()
		errs := b.delete.Exec(b.Transaction)
		if len(errs) != 0 {
			m.Lock()
			errors = append(errors, errs...)
			m.Unlock()
		}
	}()

	wg.Wait()

	if len(errors) != 0 {
		return MultiError(errors)
	}

	// Batch操作した後PropertyLoadSaverなどで追加のBatch操作が積まれたらそれがなくなるまで処理する
	if len(b.put.keys) != 0 || len(b.get.keys) != 0 || len(b.delete.keys) != 0 {
		return b.Exec()
	}

	return nil
}

func (b *txBatchPut) Put(key Key, src interface{}, h TxBatchPutHandler) {
	b.m.Lock()
	defer b.m.Unlock()

	b.keys = append(b.keys, key)
	b.srcs = append(b.srcs, src)
	b.hs = append(b.hs, h)
}

func (b *txBatchPut) Exec(tx Transaction) []error {
	if len(b.keys) == 0 {
		return nil
	}

	b.m.Lock()
	defer func() {
		b.keys = nil
		b.srcs = nil
		b.hs = nil
	}()
	defer b.m.Unlock()

	newPendingKeys, err := tx.PutMulti(b.keys, b.srcs)

	if merr, ok := err.(MultiError); ok {
		trimmedError := make([]error, 0, len(merr))
		for idx, err := range merr {
			h := b.hs[idx]
			if h != nil {
				err = h(newPendingKeys[idx], err)
			}
			if err != nil {
				trimmedError = append(trimmedError, err)
			}
		}
		return trimmedError
	} else if err != nil {
		for _, h := range b.hs {
			if h != nil {
				h(nil, err)
			}
		}
		return []error{err}
	}

	errs := make([]error, 0, len(newPendingKeys))
	for idx, newKey := range newPendingKeys {
		h := b.hs[idx]
		if h != nil {
			err := h(newKey, nil)
			if err != nil {
				errs = append(errs, err)
			}
		}
	}

	if len(errs) != 0 {
		return errs
	}

	return nil
}

func (b *txBatchGet) Get(key Key, dst interface{}, h BatchErrHandler) {
	b.m.Lock()
	defer b.m.Unlock()

	b.keys = append(b.keys, key)
	b.dsts = append(b.dsts, dst)
	b.hs = append(b.hs, h)
}

func (b *txBatchGet) Exec(tx Transaction) []error {
	if len(b.keys) == 0 {
		return nil
	}

	b.m.Lock()
	defer func() {
		b.keys = nil
		b.dsts = nil
		b.hs = nil
	}()
	defer b.m.Unlock()

	err := tx.GetMulti(b.keys, b.dsts)

	if merr, ok := err.(MultiError); ok {
		trimmedError := make([]error, 0, len(merr))
		for idx, err := range merr {
			h := b.hs[idx]
			if h != nil {
				err = h(err)
			}
			if err != nil {
				trimmedError = append(trimmedError, err)
			}
		}
		return trimmedError
	} else if err != nil {
		for _, h := range b.hs {
			if h != nil {
				h(err)
			}
		}
		return []error{err}
	}

	errs := make([]error, 0, len(b.hs))
	for _, h := range b.hs {
		if h != nil {
			err := h(nil)
			if err != nil {
				errs = append(errs, err)
			}
		}
	}

	if len(errs) != 0 {
		return errs
	}

	return nil
}

func (b *txBatchDelete) Delete(key Key, h BatchErrHandler) {
	b.m.Lock()
	defer b.m.Unlock()

	b.keys = append(b.keys, key)
	b.hs = append(b.hs, h)
}

func (b *txBatchDelete) Exec(tx Transaction) []error {
	if len(b.keys) == 0 {
		return nil
	}

	b.m.Lock()
	defer func() {
		b.keys = nil
		b.hs = nil
	}()
	defer b.m.Unlock()

	err := tx.DeleteMulti(b.keys)

	if merr, ok := err.(MultiError); ok {
		trimmedError := make([]error, 0, len(merr))
		for idx, err := range merr {
			h := b.hs[idx]
			if h != nil {
				err = h(err)
			}
			if err != nil {
				trimmedError = append(trimmedError, err)
			}
		}
		return trimmedError
	} else if err != nil {
		for _, h := range b.hs {
			if h != nil {
				h(err)
			}
		}
		return []error{err}
	}

	errs := make([]error, 0, len(b.hs))
	for _, h := range b.hs {
		if h != nil {
			err := h(nil)
			if err != nil {
				errs = append(errs, err)
			}
		}
	}

	if len(errs) != 0 {
		return errs
	}

	return nil
}
