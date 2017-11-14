package boom

import (
	"sync"

	"go.mercari.io/datastore"
)

type TransactionBatch struct {
	m  sync.Mutex
	bm *Boom
	tx *Transaction
	b  *datastore.TransactionBatch

	putWait     sync.WaitGroup
	earlyErrors []error
}

func (b *TransactionBatch) Get(dst interface{}) chan error {
	keys, err := b.bm.extractKeys([]interface{}{dst})
	if err != nil {
		b.m.Lock()
		b.earlyErrors = append(b.earlyErrors, err)
		b.m.Unlock()
		c := make(chan error, 1)
		c <- err
		return c
	}

	return b.b.Get(keys[0], dst)
}

func (b *TransactionBatch) Put(src interface{}) chan *datastore.TransactionPutResult {
	c := make(chan *datastore.TransactionPutResult, 1)

	keys, err := b.bm.extractKeys([]interface{}{src})
	if err != nil {
		b.m.Lock()
		b.earlyErrors = append(b.earlyErrors, err)
		b.m.Unlock()
		c <- &datastore.TransactionPutResult{Err: err}
		return c
	}

	res := b.b.Put(keys[0], src)
	b.putWait.Add(1)

	go func() {
		defer b.putWait.Done()

		putResult := <-res
		if putResult.Err != nil {
			c <- putResult
			return
		}

		b.tx.m.Lock()
		defer b.tx.m.Unlock()

		b.tx.pendingKeysLater = append(b.tx.pendingKeysLater, &setKeyLater{
			pendingKey: putResult.PendingKey,
			src:        src,
		})

		c <- putResult
	}()
	return c
}

func (b *TransactionBatch) Delete(dst interface{}) chan error {
	keys, err := b.bm.extractKeys([]interface{}{dst})
	if err != nil {
		b.m.Lock()
		b.earlyErrors = append(b.earlyErrors, err)
		b.m.Unlock()
		c := make(chan error, 1)
		c <- err
		return c
	}

	return b.b.Delete(keys[0])
}

func (b *TransactionBatch) Exec() error {
	b.m.Lock()
	defer b.m.Unlock()

	err := b.b.Exec()

	if merr, ok := err.(datastore.MultiError); ok {
		merr = append(merr, b.earlyErrors...)
		if len(merr) == 0 {
			return nil
		}
		return merr
	} else if err != nil {
		return err
	}

	b.putWait.Wait()
	b.earlyErrors = nil

	return nil
}
