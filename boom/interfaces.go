package boom

import (
	"context"

	"go.mercari.io/datastore"
)

// FromContext make new Boom object with specified context.
func FromContext(ctx context.Context) (*Boom, error) {
	client, err := datastore.FromContext(ctx)
	if err != nil {
		return nil, err
	}
	return &Boom{Context: ctx, Client: client}, nil
}

// FromClient make new Boom object from specified datastore.Client.
func FromClient(ctx context.Context, client datastore.Client) *Boom {
	return &Boom{Context: ctx, Client: client}
}
