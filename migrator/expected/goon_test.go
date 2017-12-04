package fixture

import (
	"context"
	"fmt"
	"testing"

	"go.mercari.io/datastore"
	"go.mercari.io/datastore/boom"
	"google.golang.org/api/iterator"
)

func TestGoon_KeyMethods(t *testing.T) {
	ctx, close, err := newContext()
	if err != nil {
		t.Fatal(err)
	}
	defer close()

	type Data struct {
		ID   int64 `datastore:"-" goon:"id"`
		Name string
	}
	bm, _ := boom.FromContext(ctx)

	key := bm.Key(&Data{})
	if v := key.Kind(); v != "Data" {
		t.Errorf("unexpected: %v", v)
	}

	key, err = bm.KeyError(&Data{})
	if err != nil {
		t.Fatal(err)
	}
	if v := key.Kind(); v != "Data" {
		t.Errorf("unexpected: %v", v)
	}

	kind := bm.Kind(&Data{})
	if v := kind; v != "Data" {
		t.Errorf("unexpected: %v", v)
	}
}

func TestGoon_PutGetDelete(t *testing.T) {
	ctx, close, err := newContext()
	if err != nil {
		t.Fatal(err)
	}
	defer close()

	type Data struct {
		ID   int64 `datastore:"-" goon:"id"`
		Name string
	}
	bm, _ := boom.FromContext(ctx)

	obj := &Data{Name: "foo"}

	key, err := bm.Put(obj)
	if err != nil {
		t.Fatal(err)
	}
	if v := key.Kind(); v != "Data" {
		t.Errorf("unexpected: %v", v)
	}
	if v := key.ID(); v == 0 {
		t.Errorf("unexpected: %v", v)
	}

	err = bm.Get(&Data{ID: obj.ID})
	if err != nil {
		t.Fatal(err)
	}

	err = bm.Delete(key)
	if err != nil {
		t.Fatal(err)
	}
}

func TestGoon_PutGetDeleteMulti(t *testing.T) {
	ctx, close, err := newContext()
	if err != nil {
		t.Fatal(err)
	}
	defer close()

	type Data struct {
		ID   int64 `datastore:"-" goon:"id"`
		Name string
	}
	bm, _ := boom.FromContext(ctx)

	list := make([]*Data, 2)
	list[0] = &Data{Name: "foo"}
	list[1] = &Data{Name: "bar"}

	keys, err := bm.PutMulti(list)
	if err != nil {
		t.Fatal(err)
	}
	if v := len(keys); v != 2 {
		t.Errorf("unexpected: %v", v)
	}

	list[0] = &Data{ID: list[0].ID}
	list[1] = &Data{ID: list[1].ID}
	err = bm.GetMulti(list)
	if err != nil {
		t.Fatal(err)
	}

	err = bm.DeleteMulti(keys)
	if err != nil {
		t.Fatal(err)
	}
}

func TestGoon_RunInTransaction(t *testing.T) {
	ctx, close, err := newContext()
	if err != nil {
		t.Fatal(err)
	}
	defer close()

	type Data struct {
		ID   int64 `datastore:"-" goon:"id"`
		Name string
	}
	bm, _ := boom.FromContext(ctx)
	commit, err = bm.RunInTransaction(func(tx *boom.Transaction) error {
		_, err := tx.Put(&Data{})
		if err != nil {
			return err
		}
		return nil

	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestGoon_GetAll(t *testing.T) {
	ctx, close, err := newContext()
	if err != nil {
		t.Fatal(err)
	}
	defer close()

	type Data struct {
		ID   int64 `datastore:"-" goon:"id"`
		Name string
	}
	bm, _ := boom.FromContext(ctx)

	_, err = bm.Put(&Data{})
	if err != nil {
		t.Fatal(err)
	}

	q := client.NewQuery("Data")

	var list []*Data
	keys, err := bm.GetAll(q, &list)
	if err != nil {
		t.Fatal(err)
	}

	if v := len(keys); v != 1 {
		t.Errorf("unexpected: %v", v)
	}
	if v := len(list); v != 1 {
		t.Errorf("unexpected: %v", v)
	}
}

func TestGoon_Count(t *testing.T) {
	ctx, close, err := newContext()
	if err != nil {
		t.Fatal(err)
	}
	defer close()

	type Data struct {
		ID   int64 `datastore:"-" goon:"id"`
		Name string
	}
	bm, _ := boom.FromContext(ctx)

	_, err = bm.Put(&Data{})
	if err != nil {
		t.Fatal(err)
	}

	q := client.NewQuery("Data")

	cnt, err := bm.Count(q)
	if err != nil {
		t.Fatal(err)
	}
	if v := cnt; v != 1 {
		t.Errorf("unexpected: %v", v)
	}
}

func TestGoon_RunNextCursor(t *testing.T) {
	ctx, close, err := newContext()
	if err != nil {
		t.Fatal(err)
	}
	defer close()

	type Data struct {
		ID   int64 `datastore:"-" goon:"id"`
		Name string
	}
	bm, _ := boom.FromContext(ctx)

	list := make([]*Data, 10)
	for idx := range list {
		list[idx] = &Data{Name: fmt.Sprintf("#%d", idx)}
	}
	_, err = bm.PutMulti(list)
	if err != nil {
		t.Fatal(err)
	}

	var cur datastore.Cursor
	var dataList []*Data
	const limit = 3
outer:
	for {
		q := client.NewQuery("Data").Limit(limit)
		if cur.String() != "" {
			q = q.Start(cur)
		}
		it := bm.Run(q)

		count := 0
		for {
			obj := &Data{}
			_, err := it.Next(obj)
			if err == iterator.Done {
				break
			} else if err != nil {
				t.Fatal(err)
			}

			dataList = append(dataList, obj)
			count++
		}
		if count != limit {
			break
		}

		cur, err = it.Cursor()
		if err != nil {
			t.Fatal(err)
		}
		if cur.String() == "" {
			break outer
		}
	}

	if v := len(dataList); v != 10 {
		t.Errorf("unexpected: %v", v)
	}
}
