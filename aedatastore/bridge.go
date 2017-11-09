package aedatastore

import (
	"context"

	w "go.mercari.io/datastore"
)

func init() {
	w.FromContext = FromContext
}

func FromContext(ctx context.Context, opts ...w.ClientOption) (w.Client, error) {
	if ctx == nil {
		panic("unexpected")
	}
	return &datastoreImpl{ctx: ctx}, nil
}

func IsAEDatastoreClient(client w.Client) bool {
	_, ok := client.(*datastoreImpl)
	return ok
}
