package boom

import (
	"context"
	"testing"

	"go.mercari.io/datastore/clouddatastore"
)

func TestBoom_NewTransaction(t *testing.T) {
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
		ID  int64 `datastore:"-" boom:"id"`
		Str string
	}

	bm := FromClient(ctx, client)

	key, err := bm.Put(ctx, &Data{Str: "Str1"})
	if err != nil {
		t.Fatal(err)
	}

	tx, err := bm.NewTransaction(ctx)
	if err != nil {
		t.Fatal(err)
	}

	obj := &Data{ID: key.ID()}
	err = tx.Get(obj)
	if err != nil {
		t.Fatal(err)
	}
	if v := obj.Str; v != "Str1" {
		t.Errorf("unexpected: %v", v)
	}

	obj = &Data{Str: "Str2"}
	_, err = tx.Put(obj)
	if err != nil {
		t.Fatal(err)
	}
	// Key is PendingKey state still...
	if v := obj.ID; v != 0 {
		t.Errorf("unexpected: %v", v)
	}

	err = tx.Delete(key)
	if err != nil {
		t.Fatal(err)
	}

	_, err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if v := obj.ID; v == 0 {
		t.Errorf("unexpected: %v", v)
	}
}

func TestBoom_RunInTransaction(t *testing.T) {
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
		ID  int64 `datastore:"-" boom:"id"`
		Str string
	}

	bm := FromClient(ctx, client)

	key, err := bm.Put(ctx, &Data{Str: "Str1"})
	if err != nil {
		t.Fatal(err)
	}

	var pObj *Data
	_, err = bm.RunInTransaction(ctx, func(tx *Transaction) error {
		obj := &Data{ID: key.ID()}
		err = tx.Get(obj)
		if err != nil {
			t.Fatal(err)
		}
		if v := obj.Str; v != "Str1" {
			t.Errorf("unexpected: %v", v)
		}

		pObj = &Data{Str: "Str2"}
		_, err = tx.Put(pObj)
		if err != nil {
			t.Fatal(err)
		}
		// Key is PendingKey state still...
		if v := pObj.ID; v != 0 {
			t.Errorf("unexpected: %v", v)
		}

		err = tx.Delete(key)
		if err != nil {
			t.Fatal(err)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if v := pObj.ID; v == 0 {
		t.Errorf("unexpected: %v", v)
	}
}
