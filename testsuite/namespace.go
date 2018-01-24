package testsuite

import (
	"context"
	"testing"

	"go.mercari.io/datastore"
)

func Namespace_PutAndGet(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type Data struct {
		Name string
	}

	key := client.IDKey("Test", 1, nil)
	if v := key.Namespace(); v != "" {
		t.Fatalf("unexpected: %v", v)
	}

	key.SetNamespace("no1")
	if v := key.String(); v != "/Test,1" {
		t.Fatalf("unexpected: %v", v)
	}

	_, err := client.Put(ctx, key, &Data{"Name #1"})
	if err != nil {
		t.Fatal(err)
	}

	key.SetNamespace("")
	err = client.Get(ctx, key, &Data{})
	if err != datastore.ErrNoSuchEntity {
		t.Fatal(err)
	}

	key.SetNamespace("no1")
	err = client.Get(ctx, key, &Data{})
	if err != nil {
		t.Fatal(err)
	}
}

func Namespace_PutAndGetWithTx(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type Data struct {
		Name string
	}

	tx, err := client.NewTransaction(ctx)
	if err != nil {
		t.Fatal(err)
	}

	key := client.IDKey("Test", 1, nil)
	if v := key.Namespace(); v != "" {
		t.Fatalf("unexpected: %v", v)
	}

	key.SetNamespace("no1")
	if v := key.String(); v != "/Test,1" {
		t.Fatalf("unexpected: %v", v)
	}

	_, err = tx.Put(key, &Data{"Name #1"})
	if err != nil {
		t.Fatal(err)
	}

	_, err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	key.SetNamespace("")
	err = client.Get(ctx, key, &Data{})
	if err != datastore.ErrNoSuchEntity {
		t.Fatal(err)
	}

	key.SetNamespace("no1")
	err = client.Get(ctx, key, &Data{})
	if err != nil {
		t.Fatal(err)
	}
}

func Namespace_Query(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type Data struct {
		Name string
	}

	key := client.IDKey("Test", 1, nil)
	if v := key.Namespace(); v != "" {
		t.Fatalf("unexpected: %v", v)
	}

	key.SetNamespace("no1")
	if v := key.String(); v != "/Test,1" {
		t.Fatalf("unexpected: %v", v)
	}

	_, err := client.Put(ctx, key, &Data{"Name #1"})
	if err != nil {
		t.Fatal(err)
	}

	q := client.NewQuery("Test")
	q = q.KeysOnly()
	if v := q.Dump().String(); v != "v1:Test&k=t" {
		t.Fatalf("unexpected: %v", v)
	}

	var keys []datastore.Key

	keys, err = client.GetAll(ctx, q, nil)
	if err != nil {
		t.Fatal(err)
	}
	if v := len(keys); v != 0 {
		t.Fatalf("unexpected: %v", v)
	}

	q = q.Namespace("no1")
	if v := q.Dump().String(); v != "v1:Test&n=no1&k=t" {
		t.Fatalf("unexpected: %v", v)
	}
	keys, err = client.GetAll(ctx, q, nil)
	if err != nil {
		t.Fatal(err)
	}
	if v := len(keys); v != 1 {
		t.Fatalf("unexpected: %v", v)
	}
}
