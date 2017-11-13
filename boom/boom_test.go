package boom

import (
	"context"
	"testing"

	"go.mercari.io/datastore"
	"go.mercari.io/datastore/clouddatastore"
	"google.golang.org/api/iterator"
)

func cleanUp() error {
	ctx := context.Background()
	client, err := clouddatastore.FromContext(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	q := client.NewQuery("__kind__").KeysOnly()
	iter := client.Run(ctx, q)
	var kinds []string
	for {
		key, err := iter.Next(nil)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		kinds = append(kinds, key.Name())
	}

	for _, kind := range kinds {
		q := client.NewQuery(kind).KeysOnly()
		keys, err := client.GetAll(ctx, q, nil)
		if err != nil {
			return err
		}
		err = client.DeleteMulti(ctx, keys)
		if err != nil {
			return err
		}
	}

	return nil
}

func TestBoom_Key(t *testing.T) {
	ctx := context.Background()
	client, err := clouddatastore.FromContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	defer cleanUp()

	type Data struct {
		ID int64 `datastore:"-" boom:"id"`
	}

	bm := FromClient(ctx, client)

	key := bm.Key(&Data{111})
	if v := key.Kind(); v != "Data" {
		t.Errorf("unexpected: %v", v)
	}
	if v := key.ID(); v != 111 {
		t.Errorf("unexpected: %v", v)
	}
}

func TestBoom_KeyWithParent(t *testing.T) {
	ctx := context.Background()
	client, err := clouddatastore.FromContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	defer cleanUp()

	type Data struct {
		ParentKey datastore.Key `datastore:"-" boom:"parent"`
		ID        int64         `datastore:"-" boom:"id"`
	}

	bm := FromClient(ctx, client)

	userKey := client.NameKey("User", "test", nil)
	key := bm.Key(&Data{userKey, 111})
	if v := key.ParentKey().Kind(); v != "User" {
		t.Errorf("unexpected: %v", v)
	}
	if v := key.ParentKey().Name(); v != "test" {
		t.Errorf("unexpected: %v", v)
	}
	if v := key.Kind(); v != "Data" {
		t.Errorf("unexpected: %v", v)
	}
	if v := key.ID(); v != 111 {
		t.Errorf("unexpected: %v", v)
	}
}

func TestBoom_Put(t *testing.T) {
	ctx := context.Background()
	client, err := clouddatastore.FromContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	defer cleanUp()

	type Data struct {
		ID  int64  `datastore:"-" boom:"id"`
		Str string ``
	}

	bm := FromClient(ctx, client)

	key, err := bm.Put(ctx, &Data{111, "Str"})
	if err != nil {
		t.Fatal(err)
	}

	if v := key.Kind(); v != "Data" {
		t.Errorf("unexpected: %v", v)
	}
	if v := key.ID(); v != 111 {
		t.Errorf("unexpected: %v", v)
	}

	obj := &Data{}
	err = client.Get(ctx, key, obj)
	if err != nil {
		t.Fatal(err)
	}

	if v := obj.Str; v != "Str" {
		t.Errorf("unexpected: %v", v)
	}
}

func TestBoom_PutWithIncomplete(t *testing.T) {
	ctx := context.Background()
	client, err := clouddatastore.FromContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	defer cleanUp()

	type Data struct {
		ID  int64  `datastore:"-" boom:"id"`
		Str string ``
	}

	bm := FromClient(ctx, client)

	obj := &Data{Str: "Str"}
	key, err := bm.Put(ctx, obj)
	if err != nil {
		t.Fatal(err)
	}

	if v := key.Kind(); v != "Data" {
		t.Errorf("unexpected: %v", v)
	}
	if v := key.ID(); v == 0 {
		t.Errorf("unexpected: %v", v)
	}
	if v := obj.ID; v != key.ID() {
		t.Errorf("unexpected: %v", v)
	}

	obj = &Data{}
	err = client.Get(ctx, key, obj)
	if err != nil {
		t.Fatal(err)
	}

	if v := obj.Str; v != "Str" {
		t.Errorf("unexpected: %v", v)
	}
}

func TestBoom_Get(t *testing.T) {
	ctx := context.Background()
	client, err := clouddatastore.FromContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	defer cleanUp()

	type Data struct {
		ID  int64  `datastore:"-" boom:"id"`
		Str string ``
	}

	bm := FromClient(ctx, client)

	key := client.IDKey("Data", 111, nil)
	_, err = client.Put(ctx, key, &Data{Str: "Str"})
	if err != nil {
		t.Fatal(err)
	}

	obj := &Data{ID: 111}
	err = bm.Get(ctx, obj)
	if err != nil {
		t.Fatal(err)
	}

	if v := obj.Str; v != "Str" {
		t.Errorf("unexpected: %v", v)
	}
}

func TestBoom_DeleteByStruct(t *testing.T) {
	ctx := context.Background()
	client, err := clouddatastore.FromContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	defer cleanUp()

	type Data struct {
		ID  int64  `datastore:"-" boom:"id"`
		Str string ``
	}

	bm := FromClient(ctx, client)

	key := client.IDKey("Data", 111, nil)
	_, err = client.Put(ctx, key, &Data{Str: "Str"})
	if err != nil {
		t.Fatal(err)
	}

	obj := &Data{ID: 111}
	err = bm.Delete(ctx, obj)
	if err != nil {
		t.Fatal(err)
	}

	err = client.Get(ctx, key, &Data{})
	if err != datastore.ErrNoSuchEntity {
		t.Fatal(err)
	}
}

func TestBoom_DeleteByKey(t *testing.T) {
	ctx := context.Background()
	client, err := clouddatastore.FromContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	defer cleanUp()

	type Data struct {
		ID  int64  `datastore:"-" boom:"id"`
		Str string ``
	}

	bm := FromClient(ctx, client)

	key := client.IDKey("Data", 111, nil)
	_, err = client.Put(ctx, key, &Data{Str: "Str"})
	if err != nil {
		t.Fatal(err)
	}

	err = bm.Delete(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	err = client.Get(ctx, key, &Data{})
	if err != datastore.ErrNoSuchEntity {
		t.Fatal(err)
	}
}
