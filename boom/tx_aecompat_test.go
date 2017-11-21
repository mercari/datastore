package boom

import (
	"testing"

	"go.mercari.io/datastore/internal/testutils"
)

func TestBoomAECompatTransaction_Put(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupCloudDatastore(t)
	defer cleanUp()

	type Data struct {
		ID  int64 `datastore:"-" boom:"id"`
		Str string
	}

	bm := FromClient(ctx, client)

	txOrig, err := bm.NewTransaction()
	if err != nil {
		t.Fatal(err)
	}

	tx := ToAECompatibleTransaction(txOrig)

	key, err := tx.Put(&Data{Str: "Str1"})
	if err != nil {
		t.Fatal(err)
	}
	if v := key.Kind(); v != "Data" {
		t.Errorf("unexpected: %v", v)
	}
	if v := key.ID(); v == 0 {
		t.Errorf("unexpected: %v", v)
	}
}

func TestBoomAECompatTransaction_PutMulti(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupCloudDatastore(t)
	defer cleanUp()

	type Data struct {
		ID  int64 `datastore:"-" boom:"id"`
		Str string
	}

	bm := FromClient(ctx, client)

	txOrig, err := bm.NewTransaction()
	if err != nil {
		t.Fatal(err)
	}

	tx := ToAECompatibleTransaction(txOrig)

	list := make([]*Data, 2)
	list[0] = &Data{Str: "Str1"}
	list[1] = &Data{ID: 2, Str: "Str2"}

	_, err = tx.PutMulti(list)
	if err != nil {
		t.Fatal(err)
	}
	if v := list[0].ID; v == 0 {
		t.Errorf("unexpected: %v", v)
	}
	if v := list[1].ID; v != 2 {
		t.Errorf("unexpected: %v", v)
	}
}

func TestBoomAECompatTransaction_Get(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupCloudDatastore(t)
	defer cleanUp()

	type Data struct {
		ID  int64 `datastore:"-" boom:"id"`
		Str string
	}

	bm := FromClient(ctx, client)

	_, err := bm.Put(&Data{ID: 3, Str: "A"})
	if err != nil {
		t.Fatal(err)
	}

	txOrig, err := bm.NewTransaction()
	if err != nil {
		t.Fatal(err)
	}

	tx := ToAECompatibleTransaction(txOrig)

	obj := &Data{ID: 3}
	err = tx.Get(obj)
	if err != nil {
		t.Fatal(err)
	}
	if v := obj.ID; v != 3 {
		t.Errorf("unexpected: %v", v)
	}
}

func TestBoomAECompatTransaction_GetMulti(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupCloudDatastore(t)
	defer cleanUp()

	type Data struct {
		ID  int64 `datastore:"-" boom:"id"`
		Str string
	}

	bm := FromClient(ctx, client)

	list := make([]*Data, 2)
	list[0] = &Data{ID: 3, Str: "A"}
	list[1] = &Data{ID: 4, Str: "B"}
	_, err := bm.PutMulti(list)
	if err != nil {
		t.Fatal(err)
	}

	txOrig, err := bm.NewTransaction()
	if err != nil {
		t.Fatal(err)
	}

	tx := ToAECompatibleTransaction(txOrig)

	list = make([]*Data, 2)
	list[0] = &Data{ID: 3}
	list[1] = &Data{ID: 4}
	err = tx.GetMulti(list)
	if err != nil {
		t.Fatal(err)
	}
	for _, obj := range list {
		if v := obj.Str; v == "" {
			t.Errorf("unexpected: %v", v)
		}
	}
}

func TestBoomAECompatTransaction_Delete(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupCloudDatastore(t)
	defer cleanUp()

	type Data struct {
		ID  int64 `datastore:"-" boom:"id"`
		Str string
	}

	bm := FromClient(ctx, client)

	_, err := bm.Put(&Data{ID: 3, Str: "A"})
	if err != nil {
		t.Fatal(err)
	}

	txOrig, err := bm.NewTransaction()
	if err != nil {
		t.Fatal(err)
	}

	tx := ToAECompatibleTransaction(txOrig)

	err = tx.Delete(&Data{ID: 3})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBoomAECompatTransaction_DeleteMulti(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupCloudDatastore(t)
	defer cleanUp()

	type Data struct {
		ID  int64 `datastore:"-" boom:"id"`
		Str string
	}

	bm := FromClient(ctx, client)

	list := make([]*Data, 2)
	list[0] = &Data{ID: 3, Str: "A"}
	list[1] = &Data{ID: 4, Str: "B"}
	_, err := bm.PutMulti(list)
	if err != nil {
		t.Fatal(err)
	}

	txOrig, err := bm.NewTransaction()
	if err != nil {
		t.Fatal(err)
	}

	tx := ToAECompatibleTransaction(txOrig)

	list = make([]*Data, 2)
	list[0] = &Data{ID: 3}
	list[1] = &Data{ID: 4}
	err = tx.DeleteMulti(list)
	if err != nil {
		t.Fatal(err)
	}
}
