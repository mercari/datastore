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

type txBatchPut struct {
	m    sync.Mutex
	keys []Key
	srcs []interface{}
	cs   []chan *TransactionPutResult
}

type TransactionPutResult struct {
	PendingKey PendingKey
	Err        error
}

type txBatchGet struct {
	m    sync.Mutex
	keys []Key
	dsts []interface{}
	cs   []chan error
}

type txBatchDelete struct {
	m    sync.Mutex
	keys []Key
	cs   []chan error
}

func (b *TransactionBatch) Put(key Key, src interface{}) chan *TransactionPutResult {
	return b.put.Put(key, src)
}

func (b *TransactionBatch) UnwrapPutResult(r *TransactionPutResult) (PendingKey, error) {
	if r.Err != nil {
		return nil, r.Err
	}

	return r.PendingKey, nil
}

func (b *TransactionBatch) Get(key Key, dst interface{}) chan error {
	return b.get.Get(key, dst)
}

func (b *TransactionBatch) Delete(key Key) chan error {
	return b.delete.Delete(key)
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

func (b *txBatchPut) Put(key Key, src interface{}) chan *TransactionPutResult {
	b.m.Lock()
	defer b.m.Unlock()

	c := make(chan *TransactionPutResult, 1)

	b.keys = append(b.keys, key)
	b.srcs = append(b.srcs, src)
	b.cs = append(b.cs, c)

	return c
}

func (b *txBatchPut) Exec(tx Transaction) []error {
	if len(b.keys) == 0 {
		return nil
	}

	b.m.Lock()
	defer func() {
		b.keys = nil
		b.srcs = nil
		b.cs = nil
	}()
	defer b.m.Unlock()

	newPendingKeys, err := tx.PutMulti(b.keys, b.srcs)

	if merr, ok := err.(MultiError); ok {
		trimmedError := make([]error, 0, len(merr))
		for idx, err := range merr {
			c := b.cs[idx]
			if err != nil {
				trimmedError = append(trimmedError, err)
				c <- &TransactionPutResult{Err: err}
			} else {
				c <- &TransactionPutResult{PendingKey: newPendingKeys[idx]}
			}
		}
		return trimmedError
	} else if err != nil {
		for _, c := range b.cs {
			c <- &TransactionPutResult{Err: err}
		}
		return []error{err}
	}

	for idx, newKey := range newPendingKeys {
		c := b.cs[idx]
		c <- &TransactionPutResult{PendingKey: newKey}
	}

	return nil
}

func (b *txBatchGet) Get(key Key, dst interface{}) chan error {
	b.m.Lock()
	defer b.m.Unlock()

	c := make(chan error, 1)

	b.keys = append(b.keys, key)
	b.dsts = append(b.dsts, dst)
	b.cs = append(b.cs, c)

	return c
}

func (b *txBatchGet) Exec(tx Transaction) []error {
	if len(b.keys) == 0 {
		return nil
	}

	b.m.Lock()
	defer func() {
		b.keys = nil
		b.dsts = nil
		b.cs = nil
	}()
	defer b.m.Unlock()

	err := tx.GetMulti(b.keys, b.dsts)

	if merr, ok := err.(MultiError); ok {
		trimmedError := make([]error, 0, len(merr))
		for idx, err := range merr {
			c := b.cs[idx]
			if err != nil {
				trimmedError = append(trimmedError, err)
				c <- err
			} else {
				c <- nil
			}
		}
		return trimmedError
	} else if err != nil {
		for _, c := range b.cs {
			c <- err
		}
		return []error{err}
	}

	for _, c := range b.cs {
		c <- nil
	}

	return nil
}

func (b *txBatchDelete) Delete(key Key) chan error {
	b.m.Lock()
	defer b.m.Unlock()

	c := make(chan error, 1)

	b.keys = append(b.keys, key)
	b.cs = append(b.cs, c)

	return c
}

func (b *txBatchDelete) Exec(tx Transaction) []error {
	if len(b.keys) == 0 {
		return nil
	}

	b.m.Lock()
	defer func() {
		b.keys = nil
		b.cs = nil
	}()
	defer b.m.Unlock()

	err := tx.DeleteMulti(b.keys)

	if merr, ok := err.(MultiError); ok {
		trimmedError := make([]error, 0, len(merr))
		for idx, err := range merr {
			c := b.cs[idx]
			if err != nil {
				trimmedError = append(trimmedError, err)
				c <- err
			} else {
				c <- nil
			}
		}
		return trimmedError
	} else if err != nil {
		for _, c := range b.cs {
			c <- err
		}
		return []error{err}
	}

	for _, c := range b.cs {
		c <- nil
	}

	return nil
}
