package boom

import (
	"context"
	"testing"

	"go.mercari.io/datastore"
	"go.mercari.io/datastore/clouddatastore"
)

func TestBoom_BatchGet(t *testing.T) {
	defer cleanUp()

	ctx := context.Background()
	client, err := clouddatastore.FromContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type Data struct {
		ID int64 `datastore:"-" boom:"id"`
	}

	const size = 100

	bm := FromClient(ctx, client)

	var list []*Data
	for i := 0; i < size; i++ {
		list = append(list, &Data{})
	}
	keys, err := bm.PutMulti(ctx, list)
	if err != nil {
		t.Fatal(err)
	}

	list = nil
	b := bm.Batch()
	for _, key := range keys {
		obj := &Data{ID: key.ID()}
		b.Get(ctx, obj)
		list = append(list, obj)
	}

	err = b.Exec(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if v := len(list); v != size {
		t.Errorf("unexpected: %v", v)
	}
	for _, obj := range list {
		if v := obj.ID; v == 0 {
			t.Errorf("unexpected: %v", v)
		}
	}
}

func TestBoom_BatchPut(t *testing.T) {
	defer cleanUp()

	ctx := context.Background()
	client, err := clouddatastore.FromContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type Data struct {
		ID int64 `datastore:"-" boom:"id"`
	}

	const size = 100

	bm := FromClient(ctx, client)

	var list []*Data
	b := bm.Batch()
	for i := 0; i < size; i++ {
		obj := &Data{}
		b.Put(ctx, obj)
		list = append(list, obj)
	}

	err = b.Exec(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if v := len(list); v != size {
		t.Errorf("unexpected: %v", v)
	}
	for _, obj := range list {
		if v := obj.ID; v == 0 {
			t.Errorf("unexpected: %v", v)
		}
	}
}

func TestBoom_BatchDelete(t *testing.T) {
	defer cleanUp()

	ctx := context.Background()
	client, err := clouddatastore.FromContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type Data struct {
		ID int64 `datastore:"-" boom:"id"`
	}

	const size = 100

	bm := FromClient(ctx, client)

	var list []*Data
	for i := 0; i < size; i++ {
		list = append(list, &Data{})
	}
	keys, err := bm.PutMulti(ctx, list)
	if err != nil {
		t.Fatal(err)
	}

	b := bm.Batch()
	for _, key := range keys {
		obj := &Data{ID: key.ID()}
		b.Delete(ctx, obj)
	}

	err = b.Exec(ctx)
	if err != nil {
		t.Fatal(err)
	}

	err = bm.GetMulti(ctx, list)
	merr, ok := err.(datastore.MultiError)
	if !ok {
		t.Fatalf("unexpected: %v, %s", ok, err.Error())
	}

	for _, err := range merr {
		if v := err; err != datastore.ErrNoSuchEntity {
			t.Errorf("unexpected: %v", v)
		}
	}
}
