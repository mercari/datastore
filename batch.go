package datastore

import (
	"context"
	"sync"
)

type Batch struct {
	Client Client

	put    batchPut
	get    batchGet
	delete batchDelete
}

type batchPut struct {
	m    sync.Mutex
	keys []Key
	srcs []interface{}
	cs   []chan *PutResult
}

type PutResult struct {
	Key Key
	Err error
}

type batchGet struct {
	m    sync.Mutex
	keys []Key
	dsts []interface{}
	cs   []chan error
}

type batchDelete struct {
	m    sync.Mutex
	keys []Key
	cs   []chan error
}

func (b *Batch) Put(key Key, src interface{}) chan *PutResult {
	return b.put.Put(key, src)
}

func (b *Batch) UnwrapPutResult(r *PutResult) (Key, error) {
	if r.Err != nil {
		return nil, r.Err
	}

	return r.Key, nil
}

func (b *Batch) Get(key Key, dst interface{}) chan error {
	return b.get.Get(key, dst)
}

func (b *Batch) Delete(key Key) chan error {
	return b.delete.Delete(key)
}

func (b *Batch) Exec(ctx context.Context) error {
	var wg sync.WaitGroup
	var errors []error
	var m sync.Mutex
	wg.Add(3)

	go func() {
		defer wg.Done()
		errs := b.put.Exec(ctx, b.Client)
		if len(errs) != 0 {
			m.Lock()
			errors = append(errors, errs...)
			m.Unlock()
		}
	}()
	go func() {
		defer wg.Done()
		errs := b.get.Exec(ctx, b.Client)
		if len(errs) != 0 {
			m.Lock()
			errors = append(errors, errs...)
			m.Unlock()
		}
	}()
	go func() {
		defer wg.Done()
		errs := b.delete.Exec(ctx, b.Client)
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
		return b.Exec(ctx)
	}

	return nil
}

func (b *batchPut) Put(key Key, src interface{}) chan *PutResult {
	b.m.Lock()
	defer b.m.Unlock()

	c := make(chan *PutResult, 1)

	b.keys = append(b.keys, key)
	b.srcs = append(b.srcs, src)
	b.cs = append(b.cs, c)

	return c
}

func (b *batchPut) Exec(ctx context.Context, client Client) []error {
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

	newKeys, err := client.PutMulti(ctx, b.keys, b.srcs)

	if merr, ok := err.(MultiError); ok {
		trimmedError := make([]error, 0, len(merr))
		for idx, err := range merr {
			c := b.cs[idx]
			if err != nil {
				trimmedError = append(trimmedError, err)
				c <- &PutResult{Err: err}
			} else {
				c <- &PutResult{Key: newKeys[idx]}
			}
		}
		return trimmedError
	} else if err != nil {
		for _, c := range b.cs {
			c <- &PutResult{Err: err}
		}
		return []error{err}
	}

	for idx, newKey := range newKeys {
		c := b.cs[idx]
		c <- &PutResult{Key: newKey}
	}

	return nil
}

func (b *batchGet) Get(key Key, dst interface{}) chan error {
	b.m.Lock()
	defer b.m.Unlock()

	c := make(chan error, 1)

	b.keys = append(b.keys, key)
	b.dsts = append(b.dsts, dst)
	b.cs = append(b.cs, c)

	return c
}

func (b *batchGet) Exec(ctx context.Context, client Client) []error {
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

	err := client.GetMulti(ctx, b.keys, b.dsts)

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

func (b *batchDelete) Delete(key Key) chan error {
	b.m.Lock()
	defer b.m.Unlock()

	c := make(chan error, 1)

	b.keys = append(b.keys, key)
	b.cs = append(b.cs, c)

	return c
}

func (b *batchDelete) Exec(ctx context.Context, client Client) []error {
	if len(b.keys) == 0 {
		return nil
	}

	b.m.Lock()
	defer func() {
		b.keys = nil
		b.cs = nil
	}()
	defer b.m.Unlock()

	err := client.DeleteMulti(ctx, b.keys)

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
