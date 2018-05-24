package boom

import "go.mercari.io/datastore"

// Iterator is the result of running a query.
type Iterator struct {
	bm *Boom
	it datastore.Iterator
}

// Next returns the key of the next result. When there are no more results,
// iterator.Done is returned as the error.
//
// If the query is not keys only and dst is non-nil, it also loads the entity
// stored for that key into the struct pointer or PropertyLoadSaver dst, with
// the same semantics and possible errors as for the Get function.
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

// Cursor returns a cursor for the iterator's current location.
func (it *Iterator) Cursor() (datastore.Cursor, error) {
	return it.it.Cursor()
}
