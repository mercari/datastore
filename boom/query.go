package boom

import "go.mercari.io/datastore"

type Iterator struct {
	bm *Boom
	it datastore.Iterator
}

func (it *Iterator) Next(dst interface{}) (datastore.Key, error) {
	key, err := it.it.Next(dst)
	if err != nil {
		return nil, err
	}

	if dst != nil {
		err = it.bm.setStructKey(dst, key)
		if err != nil {
			return nil, err
		}
	}

	return key, nil
}

func (it *Iterator) Cursor() (datastore.Cursor, error) {
	return it.it.Cursor()
}
