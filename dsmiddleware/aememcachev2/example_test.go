package aememcachev2_test

import (
	"context"

	"go.mercari.io/datastore/aedatastore"
	"go.mercari.io/datastore/dsmiddleware/aememcachev2"
	"google.golang.org/appengine/v2"
	"google.golang.org/appengine/v2/aetest"
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

	mw := aememcachev2.New()
	client.AppendMiddleware(mw)
}
