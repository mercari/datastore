package fixture

import (
	"fmt"
	"testing"

	"github.com/mjibson/goon"
	"google.golang.org/appengine/datastore"
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

	g := goon.FromContext(ctx)

	key := g.Key(&Data{})
	if v := key.Kind(); v != "Data" {
		t.Errorf("unexpected: %v", v)
	}

	key, err = g.KeyError(&Data{})
	if err != nil {
		t.Fatal(err)
	}
	if v := key.Kind(); v != "Data" {
		t.Errorf("unexpected: %v", v)
	}

	kind := g.Kind(&Data{})
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

	g := goon.FromContext(ctx)

	obj := &Data{Name: "foo"}

	key, err := g.Put(obj)
	if err != nil {
		t.Fatal(err)
	}
	if v := key.Kind(); v != "Data" {
		t.Errorf("unexpected: %v", v)
	}
	if v := key.IntID(); v == 0 {
		t.Errorf("unexpected: %v", v)
	}

	err = g.Get(&Data{ID: obj.ID})
	if err != nil {
		t.Fatal(err)
	}

	err = g.Delete(key)
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

	g := goon.FromContext(ctx)

	list := make([]*Data, 2)
	list[0] = &Data{Name: "foo"}
	list[1] = &Data{Name: "bar"}

	keys, err := g.PutMulti(list)
	if err != nil {
		t.Fatal(err)
	}
	if v := len(keys); v != 2 {
		t.Errorf("unexpected: %v", v)
	}

	list[0] = &Data{ID: list[0].ID}
	list[1] = &Data{ID: list[1].ID}
	err = g.GetMulti(list)
	if err != nil {
		t.Fatal(err)
	}

	err = g.DeleteMulti(keys)
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

	g := goon.FromContext(ctx)

	err = g.RunInTransaction(func(tg *goon.Goon) error {
		_, err := tg.Put(&Data{})
		if err != nil {
			return err
		}
		return nil

	}, &datastore.TransactionOptions{XG: true})
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

	g := goon.FromContext(ctx)

	_, err = g.Put(&Data{})
	if err != nil {
		t.Fatal(err)
	}

	q := datastore.NewQuery("Data")

	var list []*Data
	keys, err := g.GetAll(q, &list)
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

	g := goon.FromContext(ctx)

	_, err = g.Put(&Data{})
	if err != nil {
		t.Fatal(err)
	}

	q := datastore.NewQuery("Data")

	cnt, err := g.Count(q)
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

	g := goon.FromContext(ctx)

	list := make([]*Data, 10)
	for idx := range list {
		list[idx] = &Data{Name: fmt.Sprintf("#%d", idx)}
	}
	_, err = g.PutMulti(list)
	if err != nil {
		t.Fatal(err)
	}

	var cur datastore.Cursor
	var dataList []*Data
	const limit = 3
outer:
	for {
		q := datastore.NewQuery("Data").Limit(limit)
		if cur.String() != "" {
			q = q.Start(cur)
		}
		it := g.Run(q)

		count := 0
		for {
			obj := &Data{}
			_, err := it.Next(obj)
			if err == datastore.Done {
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
