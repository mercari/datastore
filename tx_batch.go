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

func (b *TransactionBatch) Exec() {
	go b.put.Exec(b.Transaction)
	go b.get.Exec(b.Transaction)
	go b.delete.Exec(b.Transaction)
}

func (b *txBatchPut) Put(key Key, src interface{}) chan *TransactionPutResult {
	b.m.Lock()
	defer b.m.Unlock()

	c := make(chan *TransactionPutResult)

	b.keys = append(b.keys, key)
	b.srcs = append(b.srcs, src)
	b.cs = append(b.cs, c)

	return c
}

func (b *txBatchPut) Exec(tx Transaction) {
	if len(b.keys) == 0 {
		return
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
		for idx, err := range merr {
			c := b.cs[idx]
			if err != nil {
				c <- &TransactionPutResult{Err: err}
			} else {
				c <- &TransactionPutResult{PendingKey: newPendingKeys[idx]}
			}
		}
		return
	} else if err != nil {
		for _, c := range b.cs {
			c <- &TransactionPutResult{Err: err}
		}
		return
	}

	for idx, newKey := range newPendingKeys {
		c := b.cs[idx]
		c <- &TransactionPutResult{PendingKey: newKey}
	}
}

func (b *txBatchGet) Get(key Key, dst interface{}) chan error {
	b.m.Lock()
	defer b.m.Unlock()

	c := make(chan error)

	b.keys = append(b.keys, key)
	b.dsts = append(b.dsts, dst)
	b.cs = append(b.cs, c)

	return c
}

func (b *txBatchGet) Exec(tx Transaction) {
	if len(b.keys) == 0 {
		return
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
		for idx, err := range merr {
			c := b.cs[idx]
			if err != nil {
				c <- err
			} else {
				c <- nil
			}
		}
		return
	} else if err != nil {
		for _, c := range b.cs {
			c <- err
		}
		return
	}

	for _, c := range b.cs {
		c <- nil
	}
}

func (b *txBatchDelete) Delete(key Key) chan error {
	b.m.Lock()
	defer b.m.Unlock()

	c := make(chan error)

	b.keys = append(b.keys, key)
	b.cs = append(b.cs, c)

	return c
}

func (b *txBatchDelete) Exec(tx Transaction) {
	if len(b.keys) == 0 {
		return
	}

	b.m.Lock()
	defer func() {
		b.keys = nil
		b.cs = nil
	}()
	defer b.m.Unlock()

	err := tx.DeleteMulti(b.keys)

	if merr, ok := err.(MultiError); ok {
		for idx, err := range merr {
			c := b.cs[idx]
			if err != nil {
				c <- err
			} else {
				c <- nil
			}
		}
		return
	} else if err != nil {
		for _, c := range b.cs {
			c <- err
		}
		return
	}

	for _, c := range b.cs {
		c <- nil
	}
}
