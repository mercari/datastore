package boom

import (
	"sync"

	"go.mercari.io/datastore"
)

type Batch struct {
	m  sync.Mutex
	bm *Boom
	b  *datastore.Batch

	earlyErrors []error
}

func (b *Batch) Boom() *Boom {
	return b.bm
}

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
