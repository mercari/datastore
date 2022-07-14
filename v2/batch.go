package datastore

import (
	"context"
	"sync"
)

// Batch can queue operations on Datastore and process them in batch.
// Batch does nothing until you call Exec().
// This helps to reduce the number of RPCs.
type Batch struct {
	Client Client

	m      sync.Mutex
	put    batchPut
	get    batchGet
	delete batchDelete
}

// BatchPutHandler represents Entity's individual callback when batching Put processing.
type BatchPutHandler func(key Key, err error) error

// BatchErrHandler represents Entity's individual callback when batching non-Put processing.
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

// Put Entity operation into the queue.
// This operation doesn't Put to Datastore immediately.
// If a h is provided, it passes the processing result to the handler, and treats the return value as the value of the result of Putting.
func (b *Batch) Put(key Key, src interface{}, h BatchPutHandler) {
	b.put.Put(key, src, h)
}

// Get Entity operation into the queue.
func (b *Batch) Get(key Key, dst interface{}, h BatchErrHandler) {
	b.get.Get(key, dst, h)
}

// Delete Entity operation into the queue.
func (b *Batch) Delete(key Key, h BatchErrHandler) {
	b.delete.Delete(key, h)
}

// Exec will perform all the processing that was queued.
// This process is done recursively until the queue is empty.
// The return value may be MultiError, but the order of contents is not guaranteed.
func (b *Batch) Exec(ctx context.Context) error {
	// batch#Exec でロックを取る理由
	// 次のようなシチュエーションで問題になる… 可能性がある
	//
	// 同一 *Batch に対して並列に動くジョブがあるとする。
	// ジョブAがGet+error handlerを登録する
	// ジョブBがGet+error handlerを登録する
	// ジョブAがExecする 上記2つの操作が実行される 処理には少し時間がかかる
	// ジョブBがExecする キューには何もないので高速に終了する ジョブAのExecは終わっていない
	// ジョブBのGet+error handlerはまだ発火していないがジョブBはエラー無しとして処理を終了する
	//
	// 解決策は2種類ある
	//   1. ここで行われている実装のように、ジョブがExecしている時は別ジョブのExecを待たせる
	//   2. 呼び出し側でerror handlerが終わったことを sync.WaitGroup などを使って確定させる
	//
	// ここでは、 "Execしたら処理は全て終わっている" というモデルを維持するため 解決策1 を採用する
	// 弊害として、Execがエラーを返さなかったからといってジョブが成功したとは限らなくなるということである
	// 対策として、error handlerを使ったハンドリングを適切にやるか、同一の *Batch を使わない方法がある

	b.m.Lock()
	defer b.m.Unlock()

	return b.exec(ctx)
}

func (b *Batch) exec(ctx context.Context) error {
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
		return b.exec(ctx)
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
