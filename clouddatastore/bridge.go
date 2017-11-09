package clouddatastore

import (
	"context"

	"cloud.google.com/go/compute/metadata"
	"cloud.google.com/go/datastore"
	w "go.mercari.io/datastore"
	"go.mercari.io/datastore/internal"
	"google.golang.org/api/option"
)

func init() {
	w.FromContext = FromContext
}

var projectID *string

func newClientSettings(opts ...w.ClientOption) *internal.ClientSettings {
	if projectID == nil {
		pID, err := metadata.ProjectID()
		if err != nil {
			// don't check again even if it was failed...
			pID = internal.GetProjectID()
		}
		projectID = &pID
	}
	settings := &internal.ClientSettings{
		ProjectID: *projectID,
	}
	for _, opt := range opts {
		opt.Apply(settings)
	}
	return settings
}

func FromContext(ctx context.Context, opts ...w.ClientOption) (w.Client, error) {
	settings := newClientSettings(opts...)
	origOpts := make([]option.ClientOption, 0, len(opts))
	if len(settings.Scopes) != 0 {
		origOpts = append(origOpts, option.WithScopes(settings.Scopes...))
	}
	if settings.TokenSource != nil {
		origOpts = append(origOpts, option.WithTokenSource(settings.TokenSource))
	}
	if settings.CredentialsFile != "" {
		origOpts = append(origOpts, option.WithCredentialsFile(settings.CredentialsFile))
	}
	if settings.HTTPClient != nil {
		origOpts = append(origOpts, option.WithHTTPClient(settings.HTTPClient))
	}

	client, err := datastore.NewClient(ctx, settings.ProjectID, origOpts...)
	if err != nil {
		return nil, err
	}

	return &datastoreImpl{ctx: ctx, client: client}, nil
}

func IsCloudDatastoreClient(client w.Client) bool {
	_, ok := client.(*datastoreImpl)
	return ok
}
