package testbed

import (
	"context"
	"errors"
	"fmt"
	"testing"

	netcontext "golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/aetest"
	"google.golang.org/appengine/datastore"
)

type AEDatastoreStruct struct {
	Test string
}

func newContext() (context.Context, func(), error) {
	inst, err := aetest.NewInstance(&aetest.Options{StronglyConsistentDatastore: true})
	if err != nil {
		return nil, nil, err
	}
	r, err := inst.NewRequest("GET", "/", nil)
	if err != nil {
		return nil, nil, err
	}
	ctx := appengine.NewContext(r)
	return ctx, func() { inst.Close() }, nil
}

func TestAEDatastore_Put(t *testing.T) {
	ctx, close, err := newContext()
	if err != nil {
		t.Fatal(err)
	}
	defer close()

	key := datastore.NewIncompleteKey(ctx, "AEDatastoreStruct", nil)
	key, err = datastore.Put(ctx, key, &AEDatastoreStruct{"Hi!"})
	if err != nil {
		t.Fatal(err.Error())
	}

	t.Logf("key: %s", key.String())
}

func TestAEDatastore_GetMulti(t *testing.T) {
	ctx, close, err := newContext()
	if err != nil {
		t.Fatal(err)
	}
	defer close()

	type Data struct {
		Str string
	}

	key1, err := datastore.Put(ctx, datastore.NewKey(ctx, "Data", "", 1, nil), &Data{"Data1"})
	if err != nil {
		t.Fatal(err.Error())
	}
	key2, err := datastore.Put(ctx, datastore.NewKey(ctx, "Data", "", 2, nil), &Data{"Data2"})
	if err != nil {
		t.Fatal(err.Error())
	}

	list := make([]*Data, 2)
	err = datastore.GetMulti(ctx, []*datastore.Key{key1, key2}, list)
	if err != nil {
		t.Fatal(err.Error())
	}

	if v := len(list); v != 2 {
		t.Fatalf("unexpected: %v", v)
	}
}

func TestAEDatastore_Transaction(t *testing.T) {
	ctx, close, err := newContext()
	if err != nil {
		t.Fatal(err)
	}
	defer close()

	key := datastore.NewIncompleteKey(ctx, "AEDatastoreStruct", nil)
	key, err = datastore.Put(ctx, key, &AEDatastoreStruct{"Hi!"})
	if err != nil {
		t.Fatal(err.Error())
	}

	newTransaction := func(ctx context.Context) (context.Context, func() error, func() error) {
		if ctx == nil {
			t.Fatal("context is not comming")
		}

		ctxC := make(chan context.Context)

		type transaction struct {
			commit   bool
			rollback bool
		}

		finishC := make(chan transaction)
		resultC := make(chan error)

		commit := func() error {
			finishC <- transaction{commit: true}
			return <-resultC
		}
		rollback := func() error {
			finishC <- transaction{rollback: true}
			return <-resultC
		}
		rollbackErr := errors.New("rollback requested")

		go func() {
			err := datastore.RunInTransaction(ctx, func(ctx netcontext.Context) error {
				t.Logf("into datastore.RunInTransaction")

				ctxC <- ctx

				t.Logf("send context")

				result, ok := <-finishC
				t.Logf("receive action: %v, %+v", ok, result)
				if !ok {
					return errors.New("channel closed")
				}
				if result.commit {
					return nil
				} else if result.rollback {
					return rollbackErr
				}

				panic("unexpected state")

			}, &datastore.TransactionOptions{XG: true})
			if err == rollbackErr {
				// This is intended error
				err = nil
			}
			resultC <- err
		}()

		ctx = <-ctxC

		return ctx, commit, rollback
	}

	{ // Commit
		txCtx, commit, _ := newTransaction(ctx)

		s := &AEDatastoreStruct{}
		err = datastore.Get(txCtx, key, s)
		if err != nil {
			t.Fatal(err.Error())
		}

		s.Test = "Updated 1"
		_, err := datastore.Put(txCtx, key, s)
		if err != nil {
			t.Fatal(err.Error())
		}

		err = commit()
		if err != nil {
			t.Fatal(err.Error())
		}

		// should updated
		newS := &AEDatastoreStruct{}
		err = datastore.Get(ctx, key, newS)
		if err != nil {
			t.Fatal(err.Error())
		}

		if v := newS.Test; v != "Updated 1" {
			t.Fatalf("unexpected: %+v", v)
		}
	}
	{ // Rollback
		txCtx, _, rollback := newTransaction(ctx)

		s := &AEDatastoreStruct{}
		err = datastore.Get(txCtx, key, s)
		if err != nil {
			t.Fatal(err.Error())
		}

		s.Test = "Updated 2"
		_, err := datastore.Put(txCtx, key, s)
		if err != nil {
			t.Fatal(err.Error())
		}

		err = rollback()
		if err != nil {
			t.Fatal(err.Error())
		}

		// should not updated
		newS := &CloudDatastoreStruct{}
		err = datastore.Get(ctx, key, newS)
		if err != nil {
			t.Fatal(err.Error())
		}

		if v := newS.Test; v != "Updated 1" {
			t.Fatalf("unexpected: %+v", v)
		}
	}

	t.Logf("key: %s", key.String())
}

func TestAEDatastore_TransactionDeleteAndGet(t *testing.T) {
	ctx, close, err := newContext()
	if err != nil {
		t.Fatal(err)
	}
	defer close()

	type Data struct {
		Str string
	}

	key, err := datastore.Put(ctx, datastore.NewIncompleteKey(ctx, "Data", nil), &Data{"Data"})
	if err != nil {
		t.Fatal(err.Error())
	}

	err = datastore.RunInTransaction(ctx, func(ctx netcontext.Context) error {
		err := datastore.Delete(ctx, key)
		if err != nil {
			return err
		}

		obj := &Data{}
		err = datastore.Get(ctx, key, obj)
		if err != nil {
			t.Fatalf("unexpected: %v", err)
		}

		return nil
	}, nil)

	if err != nil {
		t.Fatal(err.Error())
	}
}

func TestAEDatastore_Query(t *testing.T) {
	ctx, close, err := newContext()
	if err != nil {
		t.Fatal(err)
	}
	defer close()

	type Data struct {
		Str string
	}

	_, err = datastore.Put(ctx, datastore.NewIncompleteKey(ctx, "Data", nil), &Data{"Data1"})
	if err != nil {
		t.Fatal(err.Error())
	}
	_, err = datastore.Put(ctx, datastore.NewIncompleteKey(ctx, "Data", nil), &Data{"Data2"})
	if err != nil {
		t.Fatal(err.Error())
	}

	q := datastore.NewQuery("Data").Filter("Str =", "Data2")
	var list []*Data
	_, err = q.GetAll(ctx, &list)
	if err != nil {
		t.Fatal(err.Error())
	}

	if v := len(list); v != 1 {
		t.Fatalf("unexpected: %v", v)
	}
}

func TestAEDatastore_QueryCursor(t *testing.T) {
	ctx, close, err := newContext()
	if err != nil {
		t.Fatal(err)
	}
	defer close()

	type Data struct {
		Str string
	}

	{
		var keys []*datastore.Key
		var entities []*Data
		for i := 0; i < 100; i++ {
			keys = append(keys, datastore.NewIncompleteKey(ctx, "Data", nil))
			entities = append(entities, &Data{Str: fmt.Sprintf("#%d", i+1)})
		}
		_, err = datastore.PutMulti(ctx, keys, entities)
		if err != nil {
			t.Fatal(err)
		}
	}

	var cur datastore.Cursor
	var dataList []*Data
	const limit = 3
outer:
	for {
		q := datastore.NewQuery("Data").Order("Str").Limit(limit)
		if cur.String() != "" {
			q = q.Start(cur)
		}
		it := q.Run(ctx)

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

	if v := len(dataList); v != 100 {
		t.Errorf("unexpected: %v", v)
	}
}

func TestAEDatastore_ErrConcurrentTransaction(t *testing.T) {
	ctx, close, err := newContext()
	if err != nil {
		t.Fatal(err)
	}
	defer close()

	type Data struct {
		Str string
	}

	key := datastore.NewKey(ctx, "Data", "a", 0, nil)
	_, err = datastore.Put(ctx, key, &Data{})
	if err != nil {
		t.Fatal(err)
	}

	// ErrConcurrent will be occur
	err = datastore.RunInTransaction(ctx, func(txCtx1 netcontext.Context) error {
		err := datastore.Get(txCtx1, key, &Data{})
		if err != nil {
			return err
		}

		err = datastore.RunInTransaction(ctx, func(txCtx2 netcontext.Context) error {
			err := datastore.Get(txCtx2, key, &Data{})
			if err != nil {
				return err
			}

			_, err = datastore.Put(txCtx2, key, &Data{Str: "#2"})
			return err
		}, &datastore.TransactionOptions{XG: true})
		if err != nil {
			return err
		}

		_, err = datastore.Put(txCtx1, key, &Data{Str: "#1"})
		return err
	}, &datastore.TransactionOptions{XG: true})
	if err != datastore.ErrConcurrentTransaction {
		t.Fatal(err)
	}
}

func TestAEDatastore_ObjectHasObjectSlice(t *testing.T) {
	type Inner struct {
		A string
		B string
	}

	type Data struct {
		Slice []Inner
	}

	ps, err := datastore.SaveStruct(&Data{
		Slice: []Inner{
			Inner{A: "A1", B: "B1"},
			Inner{A: "A2", B: "B2"},
			Inner{A: "A3", B: "B3"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if v := len(ps); v != 6 {
		t.Fatalf("unexpected: %v", v)
	}
	expects := []struct {
		Name     string
		Value    string
		Multiple bool
	}{
		{"Slice.A", "A1", true},
		{"Slice.B", "B1", true},
		{"Slice.A", "A2", true},
		{"Slice.B", "B2", true},
		{"Slice.A", "A3", true},
		{"Slice.B", "B3", true},
	}
	for idx, expect := range expects {
		t.Logf("idx: %d", idx)
		p := ps[idx]
		if v := p.Name; v != expect.Name {
			t.Fatalf("unexpected: %v", v)
		}
		if v := p.Value.(string); v != expect.Value {
			t.Fatalf("unexpected: %v", v)
		}
		if v := p.Multiple; v != expect.Multiple {
			t.Fatalf("unexpected: %v", v)
		}
	}
}

func TestAEDatastore_GeoPoint(t *testing.T) {
	ctx, close, err := newContext()
	if err != nil {
		t.Fatal(err)
	}
	defer close()

	type Data struct {
		A appengine.GeoPoint
		// B *appengine.GeoPoint
		C []appengine.GeoPoint
		// D []*appengine.GeoPoint
	}

	obj := &Data{
		A: appengine.GeoPoint{1.1, 2.2},
		// B: &appengine.GeoPoint{3.3, 4.4},
		C: []appengine.GeoPoint{
			{5.5, 6.6},
			{7.7, 8.8},
		},
		/*
			D: []*appengine.GeoPoint{
				{9.9, 10.10},
				{11.11, 12.12},
			},
		*/
	}

	key, err := datastore.Put(ctx, datastore.NewIncompleteKey(ctx, "Data", nil), obj)
	if err != nil {
		t.Fatal(err)
	}

	obj = &Data{}
	err = datastore.Get(ctx, key, obj)
	if err != nil {
		t.Fatal(err)
	}

	if v := obj.A.Lat; v != 1.1 {
		t.Errorf("unexpected: %v", v)
	}
	if v := obj.A.Lng; v != 2.2 {
		t.Errorf("unexpected: %v", v)
	}

	if v := len(obj.C); v != 2 {
		t.Fatalf("unexpected: %v", v)
	}
	if v := obj.C[0].Lat; v != 5.5 {
		t.Errorf("unexpected: %v", v)
	}
	if v := obj.C[0].Lng; v != 6.6 {
		t.Errorf("unexpected: %v", v)
	}
	if v := obj.C[1].Lat; v != 7.7 {
		t.Errorf("unexpected: %v", v)
	}
	if v := obj.C[1].Lng; v != 8.8 {
		t.Errorf("unexpected: %v", v)
	}
}
