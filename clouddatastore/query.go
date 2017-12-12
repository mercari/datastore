package clouddatastore

import (
	"context"

	"cloud.google.com/go/datastore"
	w "go.mercari.io/datastore"
	"go.mercari.io/datastore/internal/shared"
)

var _ w.Query = (*queryImpl)(nil)
var _ w.Iterator = (*iteratorImpl)(nil)
var _ w.Cursor = (*cursorImpl)(nil)

type queryImpl struct {
	ctx context.Context
	q   *datastore.Query

	dump *w.QueryDump

	firstError error
}

type iteratorImpl struct {
	client    *datastoreImpl
	q         *queryImpl
	qDump     *w.QueryDump
	t         *datastore.Iterator
	cacheInfo *w.MiddlewareInfo

	firstError error
}

type cursorImpl struct {
	cursor datastore.Cursor
}

func (q *queryImpl) clone() *queryImpl {
	x := *q
	d := *q.dump
	d.Filter = d.Filter[:]
	d.Order = d.Order[:]
	d.Project = d.Project[:]
	x.dump = &d
	return &x
}

func (q *queryImpl) Ancestor(ancestor w.Key) w.Query {
	q = q.clone()
	q.q = q.q.Ancestor(toOriginalKey(ancestor))
	q.dump.Ancestor = ancestor
	return q
}

func (q *queryImpl) EventualConsistency() w.Query {
	q = q.clone()
	q.q = q.q.EventualConsistency()
	q.dump.EventualConsistency = true
	return q
}

func (q *queryImpl) Namespace(ns string) w.Query {
	q = q.clone()
	q.q = q.q.Namespace(ns)
	q.dump.Namespace = ns
	return q
}

func (q *queryImpl) Transaction(t w.Transaction) w.Query {
	q = q.clone()
	q.q = q.q.Transaction(toOriginalTransaction(t))
	q.dump.Transaction = t
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
	origV := toOriginalValue(value)
	q.q = q.q.Filter(filterStr, origV)
	q.dump.Filter = append(q.dump.Filter, &w.QueryFilterCondition{
		Filter: filterStr,
		Value:  value,
	})
	return q
}

func (q *queryImpl) Order(fieldName string) w.Query {
	q = q.clone()
	q.q = q.q.Order(fieldName)
	q.dump.Order = append(q.dump.Order, fieldName)
	return q
}

func (q *queryImpl) Project(fieldNames ...string) w.Query {
	q = q.clone()
	q.q = q.q.Project(fieldNames...)
	q.dump.Project = append([]string(nil), fieldNames...)
	return q
}

func (q *queryImpl) Distinct() w.Query {
	q = q.clone()
	q.q = q.q.Distinct()
	q.dump.Distinct = true
	return q
}

func (q *queryImpl) KeysOnly() w.Query {
	q = q.clone()
	q.q = q.q.KeysOnly()
	q.dump.KeysOnly = true
	return q
}

func (q *queryImpl) Limit(limit int) w.Query {
	q = q.clone()
	q.q = q.q.Limit(limit)
	q.dump.Limit = limit
	return q
}

func (q *queryImpl) Offset(offset int) w.Query {
	q = q.clone()
	q.q = q.q.Offset(offset)
	q.dump.Offset = offset
	return q
}

func (q *queryImpl) Start(c w.Cursor) w.Query {
	q = q.clone()
	curImpl := c.(*cursorImpl)
	q.q = q.q.Start(curImpl.cursor)
	q.dump.Start = c
	return q
}

func (q *queryImpl) End(c w.Cursor) w.Query {
	q = q.clone()
	curImpl := c.(*cursorImpl)
	q.q = q.q.End(curImpl.cursor)
	q.dump.End = c
	return q
}

func (q *queryImpl) Dump() *w.QueryDump {
	return q.dump
}

func (t *iteratorImpl) Next(dst interface{}) (w.Key, error) {
	if t.firstError != nil {
		return nil, t.firstError
	}

	cb := shared.NewCacheBridge(t.cacheInfo, &originalClientBridgeImpl{t.client}, nil, &originalIteratorBridgeImpl{t.qDump}, t.client.middlewares)
	return shared.NextOps(t.client.ctx, t.qDump, dst, func(dst *w.PropertyList) (w.Key, error) {
		return cb.Next(t.cacheInfo, t.q, t.qDump, t, dst)
	})
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
