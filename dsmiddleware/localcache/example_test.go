package localcache_test

import (
	"context"

	"go.mercari.io/datastore/clouddatastore"
	"go.mercari.io/datastore/dsmiddleware/localcache"
	"go.mercari.io/datastore/internal/testutils"
)

func Example_howToUse() {
	ctx := context.Background()
	client, err := clouddatastore.FromContext(ctx)
	if err != nil {
		panic(err)
	}
	defer client.Close()
	defer testutils.CleanUpAllEntities(ctx, client)

	mw := localcache.New()
	client.AppendMiddleware(mw)
}
