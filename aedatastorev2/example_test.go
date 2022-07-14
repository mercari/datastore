package aedatastorev2_test

import (
	"context"
	"fmt"
	"net/url"

	"go.mercari.io/datastore/aedatastorev2"
	"google.golang.org/appengine/v2"
	"google.golang.org/appengine/v2/aetest"
	"google.golang.org/appengine/v2/taskqueue"
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

func ExampleFromContext() {
	inst, err := aetest.NewInstance(&aetest.Options{StronglyConsistentDatastore: true, SuppressDevAppServerLog: true})
	if err != nil {
		panic(err)
	}
	defer inst.Close()
	r, err := inst.NewRequest("GET", "/", nil)
	if err != nil {
		panic(err)
	}
	ctx := appengine.NewContext(r)

	client, err := aedatastorev2.FromContext(
		ctx,
	)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	type Data struct {
		Name string
	}

	key := client.IncompleteKey("Data", nil)
	entity := &Data{Name: "mercari"}
	key, err = client.Put(ctx, key, entity)
	if err != nil {
		panic(err)
	}

	entity = &Data{}
	err = client.Get(ctx, key, entity)
	if err != nil {
		panic(err)
	}

	fmt.Println(entity.Name)
	// Output: mercari
}

func ExampleTransactionContext() {
	ctx, cancelFn := appengineContext()
	go cancelFn()

	client, err := aedatastorev2.FromContext(ctx)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	tx, err := client.NewTransaction(ctx)
	if err != nil {
		panic(err)
	}
	go tx.Commit()

	txCtx := aedatastorev2.TransactionContext(tx)

	// join task to Transaction!
	task := taskqueue.NewPOSTTask("/foobar", url.Values{})
	_, err = taskqueue.Add(txCtx, task, "")
	if err != nil {
		panic(err)
	}
}
