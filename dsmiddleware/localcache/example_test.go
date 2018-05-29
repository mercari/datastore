package localcache_test

import (
	"context"

	"go.mercari.io/datastore/clouddatastore"
	"go.mercari.io/datastore/dsmiddleware/localcache"
)

func Example_howToUse() {
	ctx := context.Background()
	client, err := clouddatastore.FromContext(ctx)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	mw := localcache.New()
	client.AppendMiddleware(mw)
}
