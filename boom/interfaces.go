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
