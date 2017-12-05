package boom

import (
	"strings"
	"testing"

	"go.mercari.io/datastore"
	"go.mercari.io/datastore/internal/testutils"
)

func TestBoom_BatchGet(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupCloudDatastore(t)
	defer cleanUp()

	type Data struct {
		ID int64 `datastore:"-" boom:"id"`
	}

	const size = 100

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
	b := bm.Batch()
	for _, key := range keys {
		obj := &Data{ID: key.ID()}
		b.Get(obj, nil)
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
		if v := obj.ID; v == 0 {
			t.Errorf("unexpected: %v", v)
		}
	}
}

func TestBoom_BatchPutSingle(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupCloudDatastore(t)
	defer cleanUp()

	type Data struct {
		ID int64 `datastore:"-" boom:"id"`
	}

	bm := FromClient(ctx, client)

	b := bm.Batch()
	b.Put(&Data{}, nil)

	err := b.Exec()
	if err != nil {
		t.Fatal(err)
	}
}

func TestBoom_BatchPut(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupCloudDatastore(t)
	defer cleanUp()

	type Data struct {
		ID int64 `datastore:"-" boom:"id"`
	}

	const size = 100

	bm := FromClient(ctx, client)

	var list []*Data
	b := bm.Batch()
	for i := 0; i < size; i++ {
		obj := &Data{}
		b.Put(obj, nil)
		list = append(list, obj)
	}

	err := b.Exec()
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
	ctx, client, cleanUp := testutils.SetupCloudDatastore(t)
	defer cleanUp()

	type Data struct {
		ID int64 `datastore:"-" boom:"id"`
	}

	const size = 100

	bm := FromClient(ctx, client)

	var list []*Data
	for i := 0; i < size; i++ {
		list = append(list, &Data{})
	}
	keys, err := bm.PutMulti(list)
	if err != nil {
		t.Fatal(err)
	}

	b := bm.Batch()
	for _, key := range keys {
		obj := &Data{ID: key.ID()}
		b.Delete(obj, nil)
	}

	err = b.Exec()
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

func TestBoom_BatchEarlyError(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupCloudDatastore(t)
	defer cleanUp()

	bm := FromClient(ctx, client)

	b := bm.Batch()
	// invalid src
	b.Put(1, nil)

	err := b.Exec()
	if merr, ok := err.(datastore.MultiError); ok {
		if v := len(merr); v != 1 {
			t.Fatalf("unexpected: %v", v)
		}
		if v := merr[0].Error(); !strings.HasPrefix(v, "boom:") {
			t.Errorf("unexpected: %v", v)
		}
	} else {
		t.Fatalf("unexpected: %v", ok)
	}
}
