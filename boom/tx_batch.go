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

	earlyErrors []error
}

func (b *TransactionBatch) Boom() *Boom {
	return b.bm
}

func (b *TransactionBatch) Transaction() *Transaction {
	return b.tx
}

func (b *TransactionBatch) Get(dst interface{}, h datastore.BatchErrHandler) {
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

func (b *TransactionBatch) Put(src interface{}, h datastore.TxBatchPutHandler) {
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

	b.b.Put(keys[0], src, func(pKey datastore.PendingKey, err error) error {
		b.tx.m.Lock()
		defer b.tx.m.Unlock()
		if err != nil {
			if h != nil {
				err = h(pKey, err)
			}
			if err != nil {
				b.m.Lock()
				b.earlyErrors = append(b.earlyErrors, err)
				b.m.Unlock()
			}
			return err
		}

		if keys[0].Incomplete() {
			b.tx.pendingKeysLater = append(b.tx.pendingKeysLater, &setKeyLater{
				pendingKey: pKey,
				src:        src,
			})
		}

		if h != nil {
			return h(pKey, nil)
		}

		return nil
	})
}

func (b *TransactionBatch) Delete(dst interface{}, h datastore.BatchErrHandler) {
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

	b.earlyErrors = nil

	return nil
}
