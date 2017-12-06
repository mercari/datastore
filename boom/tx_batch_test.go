package boom

import (
	"testing"

	"go.mercari.io/datastore"
	"go.mercari.io/datastore/internal/testutils"
)

func TestBoom_TransactionBatchGet(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupCloudDatastore(t)
	defer cleanUp()

	type Data struct {
		ID int64 `datastore:"-" boom:"id"`
	}

	const size = 25

	bm := FromClient(ctx, client)

	var list []*Data
	for i := 0; i < size; i++ {
		list = append(list, &Data{})
	}
	keys, err := bm.PutMulti(list)
	if err != nil {
		t.Fatal(err)
	}

	list = nil
	tx, err := bm.NewTransaction()
	if err != nil {
		t.Fatal(err)
	}
	b := tx.Batch()
	for _, key := range keys {
		obj := &Data{ID: key.ID()}
		b.Get(obj, nil)
		list = append(list, obj)
	}

	err = b.Exec()
	if err != nil {
		t.Fatal(err)
	}

	_, err = tx.Commit()
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

func TestBoom_TransactionBatchPut(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupCloudDatastore(t)
	defer cleanUp()

	type Data struct {
		ID int64 `datastore:"-" boom:"id"`
	}

	const size = 25

	bm := FromClient(ctx, client)

	var list []*Data
	tx, err := bm.NewTransaction()
	if err != nil {
		t.Fatal(err)
	}
	b := tx.Batch()
	for i := 0; i < size; i++ {
		obj := &Data{}
		b.Put(obj, nil)
		list = append(list, obj)
	}

	err = b.Exec()
	if err != nil {
		t.Fatal(err)
	}

	if v := len(list); v != size {
		t.Errorf("unexpected: %v", v)
	}
	for _, obj := range list {
		if v := obj.ID; v != 0 {
			t.Errorf("unexpected: %v", v)
		}
	}

	_, err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	for _, obj := range list {
		if v := obj.ID; v == 0 {
			t.Errorf("unexpected: %v", v)
		}
	}
}

func TestBoom_TransactionBatchPutWithCompleteKey(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupCloudDatastore(t)
	defer cleanUp()

	type Data struct {
		ID int64 `datastore:"-" boom:"id"`
	}

	const size = 25

	bm := FromClient(ctx, client)

	var list []*Data
	tx, err := bm.NewTransaction()
	if err != nil {
		t.Fatal(err)
	}
	b := tx.Batch()
	for i := 0; i < size; i++ {
		obj := &Data{ID: int64(i + 1)}
		b.Put(obj, nil)
		list = append(list, obj)
	}

	err = b.Exec()
	if err != nil {
		t.Fatal(err)
	}

	if v := len(list); v != size {
		t.Errorf("unexpected: %v", v)
	}
	for idx, obj := range list {
		if v := obj.ID; v != int64(idx+1) {
			t.Errorf("unexpected: %v", v)
		}
	}

	_, err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	for idx, obj := range list {
		if v := obj.ID; v != int64(idx+1) {
			t.Errorf("unexpected: %v", v)
		}
	}
}

func TestBoom_TransactionBatchDelete(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupCloudDatastore(t)
	defer cleanUp()

	type Data struct {
		ID int64 `datastore:"-" boom:"id"`
	}

	const size = 25

	bm := FromClient(ctx, client)

	var list []*Data
	for i := 0; i < size; i++ {
		list = append(list, &Data{})
	}
	keys, err := bm.PutMulti(list)
	if err != nil {
		t.Fatal(err)
	}

	tx, err := bm.NewTransaction()
	if err != nil {
		t.Fatal(err)
	}
	b := tx.Batch()
	for _, key := range keys {
		obj := &Data{ID: key.ID()}
		b.Delete(obj, nil)
	}

	err = b.Exec()
	if err != nil {
		t.Fatal(err)
	}

	err = bm.GetMulti(list)
	if err != nil {
		t.Fatal(err)
	}

	_, err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	err = bm.GetMulti(list)
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
