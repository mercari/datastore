package aedatastore

import (
	"context"

	w "go.mercari.io/datastore"
	"google.golang.org/appengine/datastore"
)

var _ w.Query = (*queryImpl)(nil)
var _ w.Iterator = (*iteratorImpl)(nil)
var _ w.Cursor = (*cursorImpl)(nil)

type queryImpl struct {
	ctx context.Context
	q   *datastore.Query

	keysOnly    bool
	namespace   string
	transaction *transactionImpl

	firstError error
}

type iteratorImpl struct {
	client *datastoreImpl
	q      *queryImpl
	t      *datastore.Iterator

	firstError error
}

type cursorImpl struct {
	cursor datastore.Cursor
}

func (q *queryImpl) clone() *queryImpl {
	x := *q
	return &x
}

func (q *queryImpl) Ancestor(ancestor w.Key) w.Query {
	q = q.clone()
	q.q = q.q.Ancestor(toOriginalKey(ancestor))
	return q
}

func (q *queryImpl) EventualConsistency() w.Query {
	q = q.clone()
	q.q = q.q.EventualConsistency()
	return q
}

func (q *queryImpl) Namespace(ns string) w.Query {
	q = q.clone()
	q.namespace = ns
	return q
}

func (q *queryImpl) Transaction(t w.Transaction) w.Query {
	q = q.clone()
	q.transaction = t.(*transactionImpl)
	return q
}

func (q *queryImpl) Filter(filterStr string, value interface{}) w.Query {
	q = q.clone()
	var err error
	if pt, ok := value.(w.PropertyTranslator); ok {
		value, err = pt.ToPropertyValue(q.ctx)
		if err != nil {
			if q.firstError == nil {
				q.firstError = err
			}
			return q
		}
	}
	origV, err := toOriginalValue(value)
	if err != nil {
		if q.firstError == nil {
			q.firstError = err
		}
		return q
	}
	q.q = q.q.Filter(filterStr, origV)
	return q
}

func (q *queryImpl) Order(fieldName string) w.Query {
	q = q.clone()
	q.q = q.q.Order(fieldName)
	return q
}

func (q *queryImpl) Project(fieldNames ...string) w.Query {
	q = q.clone()
	q.q = q.q.Project(fieldNames...)
	return q
}

func (q *queryImpl) Distinct() w.Query {
	q = q.clone()
	q.q = q.q.Distinct()
	return q
}

func (q *queryImpl) KeysOnly() w.Query {
	q = q.clone()
	q.q = q.q.KeysOnly()
	q.keysOnly = true
	return q
}

func (q *queryImpl) Limit(limit int) w.Query {
	q = q.clone()
	q.q = q.q.Limit(limit)
	return q
}

func (q *queryImpl) Offset(offset int) w.Query {
	q = q.clone()
	q.q = q.q.Offset(offset)
	return q
}

func (q *queryImpl) Start(c w.Cursor) w.Query {
	q = q.clone()
	curImpl := c.(*cursorImpl)
	q.q = q.q.Start(curImpl.cursor)
	return q
}

func (q *queryImpl) End(c w.Cursor) w.Query {
	q = q.clone()
	curImpl := c.(*cursorImpl)
	q.q = q.q.End(curImpl.cursor)
	return q
}

func (t *iteratorImpl) Next(dst interface{}) (w.Key, error) {
	if t.firstError != nil {
		return nil, t.firstError
	}

	var key *datastore.Key
	var origPs datastore.PropertyList
	var err error
	if !t.q.keysOnly {
		key, err = t.t.Next(&origPs)
	} else {
		key, err = t.t.Next(nil)
	}
	if err != nil {
		return nil, toWrapperError(err)
	}

	wKey := toWrapperKey(t.client.ctx, key)
	if !t.q.keysOnly {
		ps := toWrapperPropertyList(t.client.ctx, origPs)

		if err = w.LoadEntity(t.client.ctx, dst, &w.Entity{Key: wKey, Properties: ps}); err != nil {
			return wKey, err
		}
	}

	return wKey, nil
}

func (t *iteratorImpl) Cursor() (w.Cursor, error) {
	if t.firstError != nil {
		return nil, t.firstError
	}
	cur, err := t.t.Cursor()
	if err != nil {
		return nil, toWrapperError(err)
	}

	return &cursorImpl{cursor: cur}, nil
}

func (cur *cursorImpl) String() string {
	if cur == nil {
		return ""
	}
	return cur.cursor.String()
}
