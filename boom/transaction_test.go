package boom

import (
	"testing"

	"go.mercari.io/datastore/internal/testutils"
)

func TestBoom_NewTransaction(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupCloudDatastore(t)
	defer cleanUp()

	type Data struct {
		ID  int64 `datastore:"-" boom:"id"`
		Str string
	}

	bm := FromClient(ctx, client)

	key, err := bm.Put(&Data{Str: "Str1"})
	if err != nil {
		t.Fatal(err)
	}

	tx, err := bm.NewTransaction()
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
	ctx, client, cleanUp := testutils.SetupCloudDatastore(t)
	defer cleanUp()

	type Data struct {
		ID  int64 `datastore:"-" boom:"id"`
		Str string
	}

	bm := FromClient(ctx, client)

	key, err := bm.Put(&Data{Str: "Str1"})
	if err != nil {
		t.Fatal(err)
	}

	var pObj *Data
	_, err = bm.RunInTransaction(func(tx *Transaction) error {
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

func TestBoom_TxRollback(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupCloudDatastore(t)
	defer cleanUp()

	type Data struct {
		ID  int64 `datastore:"-" boom:"id"`
		Str string
	}

	bm := FromClient(ctx, client)

	tx, err := bm.NewTransaction()
	if err != nil {
		t.Fatal(err)
	}

	_, err = tx.Put(&Data{Str: "Str1"})
	if err != nil {
		t.Fatal(err)
	}

	err = tx.Rollback()
	if err != nil {
		t.Fatal(err)
	}
}

func TestBoom_TxWithCompleteKey(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupCloudDatastore(t)
	defer cleanUp()

	bm := FromClient(ctx, client)

	type Data struct {
		ID string `boom:"id" datastore:"-"`
	}

	tx, err := bm.NewTransaction()
	if err != nil {
		t.Fatal(err)
	}

	_, err = tx.PutMulti([]*Data{{ID: "hoge"}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}
}
