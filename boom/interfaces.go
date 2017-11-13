package boom

import (
	"context"

	"go.mercari.io/datastore"
)

func FromContext(ctx context.Context) (*Boom, error) {
	client, err := datastore.FromContext(ctx)
	if err != nil {
		return nil, err
	}
	return &Boom{Context: ctx, Client: client}, nil
}

func FromClient(ctx context.Context, client datastore.Client) *Boom {
	return &Boom{Context: ctx, Client: client}
}

type Transaction interface {
	Get(dst interface{}) error
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
	Get(ctx context.Context, dst interface{}) chan error
	Put(ctx context.Context, src interface{}) chan *datastore.PutResult
	Delete(ctx context.Context, dst interface{}) chan error
	Exec(ctx context.Context) error
}

type TransactionBatch interface {
	Get(ctx context.Context, dst interface{}) chan error
	Put(ctx context.Context, src interface{}) chan *datastore.TransactionPutResult
	Delete(ctx context.Context, dst interface{}) chan error
	Exec(ctx context.Context) error
}
