package datastore

import (
	"context"
)

var FromContext ClientGenerator

type ClientGenerator func(ctx context.Context, opts ...ClientOption) (Client, error)

type Client interface {
	Get(ctx context.Context, key Key, dst interface{}) error
	GetMulti(ctx context.Context, keys []Key, dst interface{}) error
	Put(ctx context.Context, key Key, src interface{}) (Key, error)
	PutMulti(ctx context.Context, keys []Key, src interface{}) ([]Key, error)
	Delete(ctx context.Context, key Key) error
	DeleteMulti(ctx context.Context, keys []Key) error

	NewTransaction(ctx context.Context) (Transaction, error)
	RunInTransaction(ctx context.Context, f func(tx Transaction) error) (Commit, error)
	Run(ctx context.Context, q Query) Iterator
	AllocateIDs(ctx context.Context, keys []Key) ([]Key, error)
	Count(ctx context.Context, q Query) (int, error)
	GetAll(ctx context.Context, q Query, dst interface{}) ([]Key, error)

	IncompleteKey(kind string, parent Key) Key
	NameKey(kind, name string, parent Key) Key
	IDKey(kind string, id int64, parent Key) Key

	NewQuery(kind string) Query

	Close() error

	DecodeKey(encoded string) (Key, error)
	DecodeCursor(s string) (Cursor, error)

	Batch() *Batch
	AppendMiddleware(middleware Middleware) // NOTE First-In First-Apply
	RemoveMiddleware(middleware Middleware) bool
	Context() context.Context
	SetContext(ctx context.Context)
}

type Key interface {
	Kind() string
	ID() int64
	Name() string
	ParentKey() Key
	Namespace() string
	SetNamespace(namespace string)

	String() string
	GobEncode() ([]byte, error)
	GobDecode(buf []byte) error
	MarshalJSON() ([]byte, error)
	UnmarshalJSON(buf []byte) error
	Encode() string
	Equal(o Key) bool
	Incomplete() bool
}

type PendingKey interface {
	StoredContext() context.Context
}

type Transaction interface {
	Get(key Key, dst interface{}) error
	GetMulti(keys []Key, dst interface{}) error
	Put(key Key, src interface{}) (PendingKey, error)
	PutMulti(keys []Key, src interface{}) ([]PendingKey, error)
	Delete(key Key) error
	DeleteMulti(keys []Key) error

	Commit() (Commit, error)
	Rollback() error

	Batch() *TransactionBatch
}

type Commit interface {
	Key(p PendingKey) Key
}

type GeoPoint struct {
	Lat, Lng float64
}

type Query interface {
	Ancestor(ancestor Key) Query
	EventualConsistency() Query
	Namespace(ns string) Query
	Transaction(t Transaction) Query
	Filter(filterStr string, value interface{}) Query
	Order(fieldName string) Query
	Project(fieldNames ...string) Query
	Distinct() Query
	// NOT IMPLEMENTED ON APPENGINE DistinctOn(fieldNames ...string) *Query
	KeysOnly() Query
	Limit(limit int) Query
	Offset(offset int) Query
	Start(c Cursor) Query
	End(c Cursor) Query

	Dump() *QueryDump
}

type Iterator interface {
	Next(dst interface{}) (Key, error)
	Cursor() (Cursor, error)
}

type Cursor interface {
	String() string
}

type PropertyTranslator interface {
	ToPropertyValue(ctx context.Context) (interface{}, error)
	FromPropertyValue(ctx context.Context, p Property) (dst interface{}, err error)
}

// TODO ComplexPropertyTranslator e.g. ToProperties(ctx context.Context) ([]Property, error)
