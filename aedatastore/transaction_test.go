package aedatastore

import (
	"testing"

	w "go.mercari.io/datastore"
	"google.golang.org/appengine"
	"google.golang.org/appengine/aetest"
	"google.golang.org/appengine/datastore"
)

func TestAEDatastore_TransactionContext(t *testing.T) {
	// for Transactional task enqueuing
	// https://cloud.google.com/appengine/docs/standard/go/datastore/transactions#transactional_task_enqueuing

	inst, err := aetest.NewInstance(&aetest.Options{StronglyConsistentDatastore: true})
	if err != nil {
		t.Fatal(err)
	}
	defer inst.Close()
	r, err := inst.NewRequest("GET", "/", nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := appengine.NewContext(r)

	type Data struct {
		Str string
	}

	client, err := FromContext(ctx)
	if err != nil {
		t.Fatal(err)
	}

	key := datastore.NewKey(ctx, "Data", "a", 0, nil)
	_, err = datastore.Put(ctx, key, &Data{})
	if err != nil {
		t.Fatal(err)
	}

	// ErrConcurrent will be occur

	tx1, err := client.NewTransaction(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tx2, err := client.NewTransaction(ctx)
	if err != nil {
		t.Fatal(err)
	}

	txCtx1 := TransactionContext(tx1)
	txCtx2 := TransactionContext(tx2)

	err = datastore.Get(txCtx1, key, &Data{})
	if err != nil {
		t.Fatal(err)
	}
	err = datastore.Get(txCtx2, key, &Data{})
	if err != nil {
		t.Fatal(err)
	}

	_, err = datastore.Put(txCtx1, key, &Data{Str: "#1"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = datastore.Put(txCtx2, key, &Data{Str: "#2"})
	if err != nil {
		t.Fatal(err)
	}

	_, err = tx1.Commit()
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx2.Commit()
	if err != w.ErrConcurrentTransaction {
		t.Fatal(err)
	}
}
