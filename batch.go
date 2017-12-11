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

type BatchPutHandler func(key Key, err error) error
type BatchErrHandler func(err error) error

type batchPut struct {
	m    sync.Mutex
	keys []Key
	srcs []interface{}
	hs   []BatchPutHandler
}

type batchGet struct {
	m    sync.Mutex
	keys []Key
	dsts []interface{}
	hs   []BatchErrHandler
}

type batchDelete struct {
	m    sync.Mutex
	keys []Key
	hs   []BatchErrHandler
}

func (b *Batch) Put(key Key, src interface{}, h BatchPutHandler) {
	b.put.Put(key, src, h)
}

func (b *Batch) Get(key Key, dst interface{}, h BatchErrHandler) {
	b.get.Get(key, dst, h)
}

func (b *Batch) Delete(key Key, h BatchErrHandler) {
	b.delete.Delete(key, h)
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

func (b *batchPut) Put(key Key, src interface{}, h BatchPutHandler) {
	b.m.Lock()
	defer b.m.Unlock()

	b.keys = append(b.keys, key)
	b.srcs = append(b.srcs, src)
	b.hs = append(b.hs, h)
}

func (b *batchPut) Exec(ctx context.Context, client Client) []error {
	if len(b.keys) == 0 {
		return nil
	}

	b.m.Lock()
	keys := b.keys
	srcs := b.srcs
	hs := b.hs
	b.keys = nil
	b.srcs = nil
	b.hs = nil
	b.m.Unlock()

	newKeys, err := client.PutMulti(ctx, keys, srcs)

	if merr, ok := err.(MultiError); ok {
		trimmedError := make([]error, 0, len(merr))
		for idx, err := range merr {
			h := hs[idx]
			if h != nil {
				err = h(newKeys[idx], err)
			}
			if err != nil {
				trimmedError = append(trimmedError, err)
			}
		}
		return trimmedError
	} else if err != nil {
		for _, h := range hs {
			if h != nil {
				h(nil, err)
			}
		}
		return []error{err}
	}

	errs := make([]error, 0, len(newKeys))
	for idx, newKey := range newKeys {
		h := hs[idx]
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

func (b *batchGet) Get(key Key, dst interface{}, h BatchErrHandler) {
	b.m.Lock()
	defer b.m.Unlock()

	b.keys = append(b.keys, key)
	b.dsts = append(b.dsts, dst)
	b.hs = append(b.hs, h)
}

func (b *batchGet) Exec(ctx context.Context, client Client) []error {
	if len(b.keys) == 0 {
		return nil
	}

	b.m.Lock()
	keys := b.keys
	dsts := b.dsts
	hs := b.hs
	b.keys = nil
	b.dsts = nil
	b.hs = nil
	b.m.Unlock()

	err := client.GetMulti(ctx, keys, dsts)

	if merr, ok := err.(MultiError); ok {
		trimmedError := make([]error, 0, len(merr))
		for idx, err := range merr {
			h := hs[idx]
			if h != nil {
				err = h(err)
			}
			if err != nil {
				trimmedError = append(trimmedError, err)
			}
		}
		return trimmedError
	} else if err != nil {
		for _, h := range hs {
			if h != nil {
				h(err)
			}
		}
		return []error{err}
	}

	errs := make([]error, 0, len(hs))
	for _, h := range hs {
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

func (b *batchDelete) Delete(key Key, h BatchErrHandler) {
	b.m.Lock()
	defer b.m.Unlock()

	b.keys = append(b.keys, key)
	b.hs = append(b.hs, h)
}

func (b *batchDelete) Exec(ctx context.Context, client Client) []error {
	if len(b.keys) == 0 {
		return nil
	}

	b.m.Lock()
	keys := b.keys
	hs := b.hs
	b.keys = nil
	b.hs = nil
	b.m.Unlock()

	err := client.DeleteMulti(ctx, keys)

	if merr, ok := err.(MultiError); ok {
		trimmedError := make([]error, 0, len(merr))
		for idx, err := range merr {
			h := hs[idx]
			if h != nil {
				err = h(err)
			}
			if err != nil {
				trimmedError = append(trimmedError, err)
			}
		}
		return trimmedError
	} else if err != nil {
		for _, h := range hs {
			if h != nil {
				h(err)
			}
		}
		return []error{err}
	}

	errs := make([]error, 0, len(hs))
	for _, h := range hs {
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
