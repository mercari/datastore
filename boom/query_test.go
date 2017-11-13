package boom

import (
	"context"
	"testing"

	"go.mercari.io/datastore/clouddatastore"
	"google.golang.org/api/iterator"
)

func TestBoom_IteratorNext(t *testing.T) {
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

	var list []*Data
	for i := 0; i < 100; i++ {
		list = append(list, &Data{})
	}
	_, err = bm.PutMulti(ctx, list)
	if err != nil {
		t.Fatal(err)
	}

	q := client.NewQuery(bm.Kind(&Data{}))
	it := bm.Run(ctx, q)

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
	}
}
