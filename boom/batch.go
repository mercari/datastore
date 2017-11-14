package boom

import (
	"sync"

	"go.mercari.io/datastore"
)

type Batch struct {
	m  sync.Mutex
	bm *Boom
	b  *datastore.Batch

	putWait     sync.WaitGroup
	earlyErrors []error
}

func (b *Batch) Get(dst interface{}) chan error {
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

func (b *Batch) Put(src interface{}) chan *datastore.PutResult {
	c := make(chan *datastore.PutResult, 1)

	keys, err := b.bm.extractKeys([]interface{}{src})
	if err != nil {
		b.m.Lock()
		b.earlyErrors = append(b.earlyErrors, err)
		b.m.Unlock()
		c <- &datastore.PutResult{Err: err}
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

		err := b.bm.setStructKey(src, putResult.Key)
		if err != nil {
			b.m.Lock()
			defer b.m.Unlock()

			b.earlyErrors = append(b.earlyErrors, err)
			c <- &datastore.PutResult{Err: err}
			return
		}

		c <- putResult
	}()
	return c
}

func (b *Batch) Delete(dst interface{}) chan error {
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

func (b *Batch) Exec() error {
	err := b.b.Exec(b.bm.Context)

	b.putWait.Wait()

	b.m.Lock()
	defer b.m.Unlock()

	if merr, ok := err.(datastore.MultiError); ok {
		merr = append(merr, b.earlyErrors...)
		b.earlyErrors = nil
		if len(merr) == 0 {
			return nil
		}
		return merr
	} else if err != nil {
		return err
	} else if len(b.earlyErrors) != 0 {
		errs := b.earlyErrors
		b.earlyErrors = nil
		return datastore.MultiError(errs)
	}

	return nil
}
