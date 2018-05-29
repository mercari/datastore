package aememcache_test

import (
	"context"

	"go.mercari.io/datastore/aedatastore"
	"go.mercari.io/datastore/dsmiddleware/aememcache"
	"google.golang.org/appengine"
	"google.golang.org/appengine/aetest"
)

func appengineContext() (ctx context.Context, cancelFn func() error) {
	inst, err := aetest.NewInstance(&aetest.Options{StronglyConsistentDatastore: true, SuppressDevAppServerLog: true})
	if err != nil {
		panic(err)
	}
	cancelFn = inst.Close
	r, err := inst.NewRequest("GET", "/", nil)
	if err != nil {
		panic(err)
	}
	ctx = appengine.NewContext(r)

	return
}

func Example_howToUse() {
	ctx, cancelFn := appengineContext()
	go cancelFn()

	client, err := aedatastore.FromContext(ctx)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	mw := aememcache.New()
	client.AppendMiddleware(mw)
}
