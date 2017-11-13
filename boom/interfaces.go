package boom

import (
	"context"

	"go.mercari.io/datastore"
)

type Boom interface {
	Key(src interface{}) datastore.Key

	Get(ctx context.Context, dts interface{}) error
	GetMulti(ctx context.Context, dst interface{}) error
	Put(ctx context.Context, src interface{}) (datastore.Key, error)
	PutMulti(ctx context.Context, src interface{}) ([]datastore.Key, error)
	Delete(ctx context.Context, dst interface{}) error
	DeleteMulti(ctx context.Context, dst interface{}) error

	NewTransaction(ctx context.Context) (Transaction, error)
	RunInTransaction(ctx context.Context, f func(tx Transaction) error) (datastore.Commit, error)
	Run(ctx context.Context, q datastore.Query) Iterator
	Count(ctx context.Context, q datastore.Query) (int, error)
	GetAll(ctx context.Context, q datastore.Query, dst interface{}) ([]datastore.Key, error)

	Batch() Batch
}

type Iterator interface {
	Next(dst interface{}) (datastore.Key, error)
	Cursor() (datastore.Cursor, error)
}

type Transaction interface {
	Get(dts interface{}) error
	GetMulti(dst interface{}) error
	Put(src interface{}) (datastore.PendingKey, error)
	PutMulti(src interface{}) ([]datastore.PendingKey, error)
	Delete(src interface{}) error
	DeleteMulti(src interface{}) error

	Commit() (datastore.Commit, error)
	Rollback() error

	Batch() TransactionBatch
}

type Batch interface {
	Get(ctx context.Context, dts interface{}) chan error
	Put(ctx context.Context, src interface{}) chan *datastore.PutResult
	Delete(ctx context.Context, dst interface{}) chan error
	Exec(ctx context.Context) error
}

type TransactionBatch interface {
	Get(ctx context.Context, dts interface{}) chan error
	Put(ctx context.Context, src interface{}) chan *datastore.TransactionPutResult
	Delete(ctx context.Context, dst interface{}) chan error
	Exec(ctx context.Context) error
}
