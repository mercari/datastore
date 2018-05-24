package boom

import (
	"sync"

	"go.mercari.io/datastore"
)

// Batch can queue operations on Datastore and process them in batch.
// Batch does nothing until you call Exec().
// This helps to reduce the number of RPCs.
type Batch struct {
	m  sync.Mutex
	bm *Boom
	b  *datastore.Batch

	earlyErrors []error
}

// Boom object that is the source of the Batch object is returned.
func (b *Batch) Boom() *Boom {
	return b.bm
}

// Get puts Entity fetch processing into the queue of Get.
func (b *Batch) Get(dst interface{}, h datastore.BatchErrHandler) {
	keys, err := b.bm.extractKeys([]interface{}{dst})
	if err != nil {
		if h != nil {
			err = h(err)
		}
		if err != nil {
			b.m.Lock()
			b.earlyErrors = append(b.earlyErrors, err)
			b.m.Unlock()
		}
		return
	}

	b.b.Get(keys[0], dst, h)
}

// Put puts Entity into the queue of Put.
// This operation doesn't Put to Datastore immediatly.
// If a h is provided, it passes the processing result to the handler, and treats the return value as the value of the result of Putting.
// TODO move this method before Get method
func (b *Batch) Put(src interface{}, h datastore.BatchPutHandler) {
	keys, err := b.bm.extractKeys([]interface{}{src})
	if err != nil {
		if h != nil {
			err = h(nil, err)
		}
		if err != nil {
			b.m.Lock()
			b.earlyErrors = append(b.earlyErrors, err)
			b.m.Unlock()
		}
		return
	}

	b.b.Put(keys[0], src, func(key datastore.Key, err error) error {
		if err != nil {
			if h != nil {
				err = h(key, err)
			}
			return err
		}

		err = b.bm.setStructKey(src, key)
		if err != nil {
			if h != nil {
				err = h(key, err)
			}
			if err != nil {
				b.m.Lock()
				b.earlyErrors = append(b.earlyErrors, err)
				b.m.Unlock()
			}
			return err
		}

		if h != nil {
			return h(key, nil)
		}

		return nil
	})
}

// Delete puts Entity delete processing into the queue of Delete.
func (b *Batch) Delete(dst interface{}, h datastore.BatchErrHandler) {
	keys, err := b.bm.extractKeys([]interface{}{dst})
	if err != nil {
		if h != nil {
			err = h(err)
		}
		if err != nil {
			b.m.Lock()
			b.earlyErrors = append(b.earlyErrors, err)
			b.m.Unlock()
		}
		return
	}

	b.b.Delete(keys[0], h)
}

// Exec will perform all the processing that was queued.
// This process is done recursively until the queue is empty.
// The return value may be MultiError, but the order of contents is not guaranteed.
func (b *Batch) Exec() error {
	err := b.b.Exec(b.bm.Context)

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
