package boom

import (
	"testing"

	"go.mercari.io/datastore/internal/testutils"
	"google.golang.org/api/iterator"
)

func TestBoom_IteratorNext(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupCloudDatastore(t)
	defer cleanUp()

	type Data struct {
		ID int64 `datastore:"-" boom:"id"`
	}

	bm := FromClient(ctx, client)

	var list []*Data
	for i := 0; i < 100; i++ {
		list = append(list, &Data{})
	}
	_, err := bm.PutMulti(list)
	if err != nil {
		t.Fatal(err)
	}

	q := bm.NewQuery(bm.Kind(&Data{}))
	it := bm.Run(q)

	for {
		obj := &Data{}
		_, err = it.Next(obj)
		if err == iterator.Done {
			break
		} else if err != nil {
			t.Fatal(err)
		}

		if v := obj.ID; v == 0 {
			t.Errorf("unexpected: %v", v)
		}

		_, err := it.Cursor()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestBoom_IteratorNextKeysOnly(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupCloudDatastore(t)
	defer cleanUp()

	type Data struct {
		ID int64 `datastore:"-" boom:"id"`
	}

	bm := FromClient(ctx, client)

	var list []*Data
	for i := 0; i < 100; i++ {
		list = append(list, &Data{})
	}
	_, err := bm.PutMulti(list)
	if err != nil {
		t.Fatal(err)
	}

	q := bm.NewQuery(bm.Kind(&Data{})).KeysOnly()
	it := bm.Run(q)

	for {
		_, err = it.Next(nil)
		if err == iterator.Done {
			break
		} else if err != nil {
			t.Fatal(err)
		}
	}
}
