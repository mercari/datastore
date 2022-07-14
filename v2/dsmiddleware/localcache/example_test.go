package localcache_test

import (
	"context"

	"go.mercari.io/datastore/v2/clouddatastore"
	"go.mercari.io/datastore/v2/dsmiddleware/localcache"
	"go.mercari.io/datastore/v2/internal/testutils"
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
