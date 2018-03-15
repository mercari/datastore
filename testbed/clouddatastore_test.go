package testbed

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"cloud.google.com/go/datastore"
	"go.mercari.io/datastore/internal"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
)

type CloudDatastoreStruct struct {
	Test string
}

func cleanUp() error {
	ctx := context.Background()
	client, err := datastore.NewClient(ctx, internal.GetProjectID())
	if err != nil {
		return err
	}
	defer client.Close()

	q := datastore.NewQuery("__kind__").KeysOnly()
	iter := client.Run(ctx, q)
	var kinds []string
	for {
		key, err := iter.Next(nil)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		kinds = append(kinds, key.Name)
	}

	for _, kind := range kinds {
		q := datastore.NewQuery(kind).KeysOnly()
		keys, err := client.GetAll(ctx, q, nil)
		if err != nil {
			return err
		}
		err = client.DeleteMulti(ctx, keys)
		if err != nil {
			return err
		}
	}

	return nil
}

func TestCloudDatastore_Put(t *testing.T) {
	ctx := context.Background()
	client, err := datastore.NewClient(ctx, internal.GetProjectID())
	if err != nil {
		t.Fatal(err.Error())
	}
	defer client.Close()
	defer cleanUp()

	key := datastore.IncompleteKey("CloudDatastoreStruct", nil)
	key, err = client.Put(ctx, key, &CloudDatastoreStruct{"Hi!"})
	if err != nil {
		t.Fatal(err.Error())
	}

	t.Logf("key: %s", key.String())
}

func TestCloudDatastore_GetMulti(t *testing.T) {
	ctx := context.Background()
	client, err := datastore.NewClient(ctx, internal.GetProjectID())
	if err != nil {
		t.Fatal(err.Error())
	}
	defer client.Close()
	defer cleanUp()

	type Data struct {
		Str string
	}

	key1, err := client.Put(ctx, datastore.IDKey("Data", 1, nil), &Data{"Data1"})
	if err != nil {
		t.Fatal(err.Error())
	}
	key2, err := client.Put(ctx, datastore.IDKey("Data", 2, nil), &Data{"Data2"})
	if err != nil {
		t.Fatal(err.Error())
	}

	list := make([]*Data, 2)
	err = client.GetMulti(ctx, []*datastore.Key{key1, key2}, list)
	if err != nil {
		t.Fatal(err.Error())
	}

	if v := len(list); v != 2 {
		t.Fatalf("unexpected: %v", v)
	}
}

func TestCloudDatastore_Transaction(t *testing.T) {
	ctx := context.Background()
	client, err := datastore.NewClient(ctx, internal.GetProjectID())
	if err != nil {
		t.Fatal(err.Error())
	}
	defer client.Close()
	defer cleanUp()

	key := datastore.IncompleteKey("CloudDatastoreStruct", nil)
	key, err = client.Put(ctx, key, &CloudDatastoreStruct{"Hi!"})
	if err != nil {
		t.Fatal(err.Error())
	}

	{ // Commit
		tx, err := client.NewTransaction(ctx)
		if err != nil {
			t.Fatal(err.Error())
		}

		s := &CloudDatastoreStruct{}
		err = tx.Get(key, s)
		if err != nil {
			t.Fatal(err.Error())
		}

		s.Test = "Updated 1"
		_, err = tx.Put(key, s)
		if err != nil {
			t.Fatal(err.Error())
		}

		_, err = tx.Commit()
		if err != nil {
			t.Fatal(err.Error())
		}

		// should updated
		newS := &CloudDatastoreStruct{}
		err = client.Get(ctx, key, newS)
		if err != nil {
			t.Fatal(err.Error())
		}

		if v := newS.Test; v != "Updated 1" {
			t.Fatalf("unexpected: %+v", v)
		}
	}
	{ // Rollback
		tx, err := client.NewTransaction(ctx)
		if err != nil {
			t.Fatal(err.Error())
		}

		s := &CloudDatastoreStruct{}
		err = tx.Get(key, s)
		if err != nil {
			t.Fatal(err.Error())
		}

		s.Test = "Updated 2"
		_, err = tx.Put(key, s)
		if err != nil {
			t.Fatal(err.Error())
		}

		err = tx.Rollback()
		if err != nil {
			t.Fatal(err.Error())
		}

		// should not updated
		newS := &CloudDatastoreStruct{}
		err = client.Get(ctx, key, newS)
		if err != nil {
			t.Fatal(err.Error())
		}

		if v := newS.Test; v != "Updated 1" {
			t.Fatalf("unexpected: %+v", v)
		}
	}

	t.Logf("key: %s", key.String())
}

func TestCloudDatastore_TransactionDeleteAndGet(t *testing.T) {
	ctx := context.Background()
	client, err := datastore.NewClient(ctx, internal.GetProjectID())
	if err != nil {
		t.Fatal(err.Error())
	}
	defer client.Close()
	defer cleanUp()

	type Data struct {
		Str string
	}

	key, err := client.Put(ctx, datastore.IncompleteKey("Data", nil), &Data{"Data"})
	if err != nil {
		t.Fatal(err.Error())
	}

	tx, err := client.NewTransaction(ctx)
	if err != nil {
		t.Fatal(err.Error())
	}

	err = tx.Delete(key)
	if err != nil {
		t.Fatal(err.Error())
	}

	obj := &Data{}
	err = tx.Get(key, obj)
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}

	err = tx.Rollback()
	if err != nil {
		t.Fatal(err.Error())
	}
}

func TestCloudDatastore_SingleToBatch(t *testing.T) {
	ctx := context.Background()
	client, err := datastore.NewClient(ctx, internal.GetProjectID())
	if err != nil {
		t.Fatal(err.Error())
	}
	defer client.Close()
	defer cleanUp()

	type putResult struct {
		key   *datastore.Key
		error error
	}

	var m sync.Mutex
	var keys []*datastore.Key
	var srcList []interface{}
	var cList []chan *putResult

	put := func(key *datastore.Key, src interface{}) chan *putResult {
		m.Lock()
		defer m.Unlock()

		c := make(chan *putResult)

		keys = append(keys, key)
		srcList = append(srcList, src)
		cList = append(cList, c)

		return c
	}
	execBatchOps := func() {
		newKeys, err := client.PutMulti(ctx, keys, srcList)
		if merr, ok := err.(datastore.MultiError); ok {
			for idx, err := range merr {
				c := cList[idx]
				if err != nil {
					c <- &putResult{error: err}
				} else {
					c <- &putResult{key: newKeys[idx]}
				}
			}
			return
		} else if err != nil {
			for _, c := range cList {
				c <- &putResult{error: err}
			}
			return
		}

		for idx, newKey := range newKeys {
			c := cList[idx]
			c <- &putResult{key: newKey}
		}
	}
	unwrap := func(r *putResult) (key *datastore.Key, err error) {
		if r.error != nil {
			return nil, r.error
		}

		return r.key, nil
	}

	eg := &errgroup.Group{}
	{ // 1st entity

		key := datastore.IncompleteKey("CloudDatastoreStruct", nil)
		c := put(key, &CloudDatastoreStruct{"Hi!"})
		eg.Go(func() error {
			key, err := unwrap(<-c)
			if err != nil {
				return err
			}
			t.Logf("#1: %s", key.String())
			return nil
		})
	}
	{ // 2nd entity
		key := datastore.IncompleteKey("CloudDatastoreStruct", nil)
		c := put(key, &CloudDatastoreStruct{"Hi!"})
		eg.Go(func() error {
			key, err := unwrap(<-c)
			if err != nil {
				return err
			}
			t.Logf("#2: %s", key.String())
			return nil
		})
	}

	execBatchOps()
	err = eg.Wait()
	if err != nil {
		t.Fatal(err.Error())
	}
}

func TestCloudDatastore_Query(t *testing.T) {
	ctx := context.Background()
	client, err := datastore.NewClient(ctx, internal.GetProjectID())
	if err != nil {
		t.Fatal(err.Error())
	}
	defer client.Close()
	defer cleanUp()

	type Data struct {
		Str string
	}

	_, err = client.Put(ctx, datastore.IncompleteKey("Data", nil), &Data{"Data1"})
	if err != nil {
		t.Fatal(err.Error())
	}
	_, err = client.Put(ctx, datastore.IncompleteKey("Data", nil), &Data{"Data2"})
	if err != nil {
		t.Fatal(err.Error())
	}

	q := datastore.NewQuery("Data").Filter("Str =", "Data2")
	{
		var list []*Data
		_, err = client.GetAll(ctx, q, &list)
		if err != nil {
			t.Fatal(err.Error())
		}

		if v := len(list); v != 1 {
			t.Fatalf("unexpected: %v", v)
		}
	}
	{
		keys, err := client.GetAll(ctx, q.KeysOnly(), nil)
		if err != nil {
			t.Fatal(err.Error())
		}

		if v := len(keys); v != 1 {
			t.Fatalf("unexpected: %v", v)
		}
	}
}

func TestCloudDatastore_QueryCursor(t *testing.T) {
	ctx := context.Background()
	client, err := datastore.NewClient(ctx, internal.GetProjectID())
	if err != nil {
		t.Fatal(err.Error())
	}
	defer client.Close()
	defer cleanUp()

	type Data struct {
		Str string
	}

	{
		var keys []*datastore.Key
		var entities []*Data
		for i := 0; i < 100; i++ {
			keys = append(keys, datastore.IncompleteKey("Data", nil))
			entities = append(entities, &Data{Str: fmt.Sprintf("#%d", i+1)})
		}
		_, err = client.PutMulti(ctx, keys, entities)
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
		it := client.Run(ctx, q)

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

	if v := len(dataList); v != 100 {
		t.Errorf("unexpected: %v", v)
	}
}

func TestCloudDatastore_ErrConcurrentTransaction(t *testing.T) {
	ctx := context.Background()
	client, err := datastore.NewClient(ctx, internal.GetProjectID())
	if err != nil {
		t.Fatal(err.Error())
	}
	defer client.Close()
	defer cleanUp()

	type Data struct {
		Str string
	}

	key := datastore.NameKey("Data", "a", nil)
	_, err = client.Put(ctx, key, &Data{})
	if err != nil {
		t.Fatal(err)
	}

	// ErrConcurrent will be occur
	_, err = client.RunInTransaction(ctx, func(tx1 *datastore.Transaction) error {
		err := tx1.Get(key, &Data{})
		if err != nil {
			return err
		}

		_, err = client.RunInTransaction(ctx, func(tx2 *datastore.Transaction) error {
			err := tx2.Get(key, &Data{})
			if err != nil {
				return err
			}

			_, err = tx2.Put(key, &Data{Str: "#2"})
			return err
		})
		if err != nil {
			return err
		}

		_, err = tx1.Put(key, &Data{Str: "#1"})
		return err
	})
	if err != datastore.ErrConcurrentTransaction {
		t.Fatal(err)
	}
}

func TestCloudDatastore_ObjectHasObjectSlice(t *testing.T) {
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

	if v := len(ps); v != 1 {
		t.Fatalf("unexpected: %v", v)
	}
	p := ps[0]
	if v := p.Name; v != "Slice" {
		t.Fatalf("unexpected: %v", v)
	}
	es := p.Value.([]interface{})
	if v := len(es); v != 3 {
		t.Fatalf("unexpected: %v", v)
	}

	expects := []struct {
		Name  string
		Value string
	}{
		{"A", "A1"},
		{"B", "B1"},
		{"A", "A2"},
		{"B", "B2"},
		{"A", "A3"},
		{"B", "B3"},
	}

	for idx, entity := range es {
		e := entity.(*datastore.Entity)
		if v := len(e.Properties); v != 2 {
			t.Fatalf("unexpected: %v", v)
		}
		for pIdx, p := range e.Properties {
			expect := expects[idx*len(e.Properties)+pIdx]
			if v := p.Name; v != expect.Name {
				t.Errorf("unexpected: %v", v)
			}
			if v := p.Value.(string); v != expect.Value {
				t.Errorf("unexpected: %v", v)
			}
		}
	}
}

func TestCloudDatastore_ObjectHasObjectSliceFlatten(t *testing.T) {
	type Inner struct {
		A string
		B string
	}

	type Data struct {
		Slice []Inner `datastore:",flatten"`
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

	if v := len(ps); v != 2 {
		t.Fatalf("unexpected: %v", v)
	}

	expects := []struct {
		Name   string
		Values []interface{}
	}{
		{"Slice.A", []interface{}{"A1", "A2", "A3"}},
		{"Slice.B", []interface{}{"B1", "B2", "B3"}},
	}
	for idx, expect := range expects {
		p := ps[idx]
		if v := p.Name; v != expect.Name {
			t.Fatalf("unexpected: %v", v)
		}
		if v := p.Value.([]interface{}); !reflect.DeepEqual(v, expect.Values) {
			t.Fatalf("unexpected: %v", v)
		}
	}
}

func TestCloudDatastore_NestedEntityWithKey(t *testing.T) {
	ctx := context.Background()
	client, err := datastore.NewClient(ctx, internal.GetProjectID())
	if err != nil {
		t.Fatal(err.Error())
	}
	defer client.Close()
	defer cleanUp()

	type Inner struct {
		K *datastore.Key `datastore:"__key__"`
		A string
		B string
	}

	type Data struct {
		Slice []Inner
	}

	_, err = client.Put(ctx, datastore.IncompleteKey("Test", nil), &Data{
		Slice: []Inner{
			Inner{K: datastore.IDKey("TestInner", 1, nil), A: "A1", B: "B1"},
			Inner{K: datastore.IDKey("TestInner", 2, nil), A: "A2", B: "B2"},
			Inner{K: datastore.IDKey("TestInner", 3, nil), A: "A3", B: "B3"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestCloudDatastore_GeoPoint(t *testing.T) {
	ctx := context.Background()
	client, err := datastore.NewClient(ctx, internal.GetProjectID())
	if err != nil {
		t.Fatal(err.Error())
	}
	defer client.Close()
	defer cleanUp()

	type Data struct {
		A datastore.GeoPoint
		B *datastore.GeoPoint
		C []datastore.GeoPoint
		D []*datastore.GeoPoint
	}

	// NOTE Cloud Datastore can save *datastore.GeoPoint.
	// but it is not means that is can handling *datastore.GeoPoint.
	// *datastore.GeoPoint will convert to *datastore.Entity.
	obj := &Data{
		A: datastore.GeoPoint{1.1, 2.2},
		B: &datastore.GeoPoint{3.3, 4.4},
		C: []datastore.GeoPoint{
			{5.5, 6.6},
			{7.7, 8.8},
		},
		D: []*datastore.GeoPoint{
			{9.9, 10.10},
			{11.11, 12.12},
		},
	}

	key, err := client.Put(ctx, datastore.IncompleteKey("Data", nil), obj)
	if err != nil {
		t.Fatal(err)
	}

	obj = &Data{}
	err = client.Get(ctx, key, obj)
	if err != nil {
		t.Fatal(err)
	}

	if v := obj.A.Lat; v != 1.1 {
		t.Errorf("unexpected: %v", v)
	}
	if v := obj.A.Lng; v != 2.2 {
		t.Errorf("unexpected: %v", v)
	}

	if v := obj.B.Lat; v != 3.3 {
		t.Errorf("unexpected: %v", v)
	}
	if v := obj.B.Lng; v != 4.4 {
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

	if v := len(obj.D); v != 2 {
		t.Fatalf("unexpected: %v", v)
	}
	if v := obj.D[0].Lat; v != 9.9 {
		t.Errorf("unexpected: %v", v)
	}
	if v := obj.D[0].Lng; v != 10.10 {
		t.Errorf("unexpected: %v", v)
	}
	if v := obj.D[1].Lat; v != 11.11 {
		t.Errorf("unexpected: %v", v)
	}
	if v := obj.D[1].Lng; v != 12.12 {
		t.Errorf("unexpected: %v", v)
	}
}

func TestCloudDatastore_PutInterface(t *testing.T) {
	ctx := context.Background()
	client, err := datastore.NewClient(ctx, internal.GetProjectID())
	if err != nil {
		t.Fatal(err.Error())
	}
	defer client.Close()
	defer cleanUp()

	var e EntityInterface = &PutInterfaceTest{}

	key := datastore.IncompleteKey("Test", nil)
	_, err = client.Put(ctx, key, e)
	if err != nil {
		t.Fatal(err)
	}
}

func TestCloudDatastore_PutAndGetPropertyList(t *testing.T) {
	ctx := context.Background()
	client, err := datastore.NewClient(ctx, internal.GetProjectID())
	if err != nil {
		t.Fatal(err.Error())
	}
	defer client.Close()
	defer cleanUp()

	var ps datastore.PropertyList
	ps = append(ps, datastore.Property{
		Name:  "A",
		Value: "A-Value",
	})
	ps = append(ps, datastore.Property{
		Name:  "B",
		Value: true,
	})

	key := datastore.IncompleteKey("Test", nil)
	// passed datastore.PropertyList, would be error.
	_, err = client.Put(ctx, key, ps)
	if err != datastore.ErrInvalidEntityType {
		t.Fatal(err)
	}

	// ok!
	key, err = client.Put(ctx, key, &ps)
	if err != nil {
		t.Fatal(err)
	}

	// passed datastore.PropertyList, would be error.
	ps = datastore.PropertyList{}
	err = client.Get(ctx, key, ps)
	if err != datastore.ErrInvalidEntityType {
		t.Fatal(err)
	}

	// ok!
	ps = datastore.PropertyList{}
	err = client.Get(ctx, key, &ps)
	if err != nil {
		t.Fatal(err)
	}

	if v := len(ps); v != 2 {
		t.Fatalf("unexpected: %v", v)
	}
}

func TestCloudDatastore_PutAndGetMultiPropertyListSlice(t *testing.T) {
	ctx := context.Background()
	client, err := datastore.NewClient(ctx, internal.GetProjectID())
	if err != nil {
		t.Fatal(err.Error())
	}
	defer client.Close()
	defer cleanUp()

	var pss []datastore.PropertyList
	var keys []*datastore.Key
	{
		var ps datastore.PropertyList
		ps = append(ps, datastore.Property{
			Name:  "A",
			Value: "A-Value",
		})
		ps = append(ps, datastore.Property{
			Name:  "B",
			Value: true,
		})

		key := datastore.IncompleteKey("Test", nil)

		pss = append(pss, ps)
		keys = append(keys, key)
	}

	// passed *[]datastore.PropertyList, would be error.
	_, err = client.PutMulti(ctx, keys, &pss)
	if err == nil {
		t.Fatal(err)
	}

	// ok! []datastore.PropertyList
	keys, err = client.PutMulti(ctx, keys, pss)
	if err != nil {
		t.Fatal(err)
	}

	// passed *[]datastore.PropertyList, would be error.
	pss = make([]datastore.PropertyList, len(keys))
	err = client.GetMulti(ctx, keys, &pss)
	if err == nil {
		t.Fatal(err)
	}

	// passed []datastore.PropertyList with length 0, would be error.
	pss = make([]datastore.PropertyList, 0)
	err = client.GetMulti(ctx, keys, pss)
	if err == nil {
		t.Fatal(err)
	}

	// ok! []datastore.PropertyList with length == len(keys)
	pss = make([]datastore.PropertyList, len(keys))
	err = client.GetMulti(ctx, keys, pss)
	if err != nil {
		t.Fatal(err)
	}

	if v := len(pss); v != 1 {
		t.Fatalf("unexpected: %v", v)
	}
}

func TestCloudDatastore_PutAndGetBareStruct(t *testing.T) {
	ctx := context.Background()
	client, err := datastore.NewClient(ctx, internal.GetProjectID())
	if err != nil {
		t.Fatal(err.Error())
	}
	defer client.Close()
	defer cleanUp()

	type Data struct {
		Name string
	}

	key := datastore.IncompleteKey("Test", nil)
	// passed Data, would be error.
	_, err = client.Put(ctx, key, Data{Name: "A"})
	if err != datastore.ErrInvalidEntityType {
		t.Fatal(err)
	}

	// ok! *Data
	key, err = client.Put(ctx, key, &Data{Name: "A"})
	if err != nil {
		t.Fatal(err)
	}

	// ok! but struct are copied. can't watching Get result.
	obj := Data{}
	err = client.Get(ctx, key, obj)
	if err != datastore.ErrInvalidEntityType {
		t.Fatal(err)
	}

	if v := obj.Name; v != "" {
		t.Errorf("unexpected: '%v'", v)
	}
}

func TestCloudDatastore_PutAndGetMultiBareStruct(t *testing.T) {
	ctx := context.Background()
	client, err := datastore.NewClient(ctx, internal.GetProjectID())
	if err != nil {
		t.Fatal(err.Error())
	}
	defer client.Close()
	defer cleanUp()

	type Data struct {
		Name string
	}

	var list []Data
	var keys []*datastore.Key
	{
		obj := Data{Name: "A"}
		key := datastore.IncompleteKey("Test", nil)

		list = append(list, obj)
		keys = append(keys, key)
	}

	// ok!
	keys, err = client.PutMulti(ctx, keys, list)
	if err != nil {
		t.Fatal(err)
	}

	// passed []Data with length 0, would be error.
	list = make([]Data, 0)
	err = client.GetMulti(ctx, keys, list)
	if err == nil {
		t.Fatal(err)
	}

	// ok! []Data with length == len(keys)
	list = make([]Data, len(keys))
	err = client.GetMulti(ctx, keys, list)
	if err != nil {
		t.Fatal(err)
	}

	if v := len(list); v != 1 {
		t.Fatalf("unexpected: '%v'", v)
	}
	if v := list[0].Name; v != "A" {
		t.Errorf("unexpected: '%v'", v)
	}
}

func TestCloudDatastore_PutAndGetStringSynonym(t *testing.T) {
	ctx := context.Background()
	client, err := datastore.NewClient(ctx, internal.GetProjectID())
	if err != nil {
		t.Fatal(err.Error())
	}
	defer client.Close()
	defer cleanUp()

	type Email string

	type Data struct {
		Email Email
	}

	key, err := client.Put(ctx, datastore.IncompleteKey("Data", nil), &Data{Email: "test@example.com"})
	if err != nil {
		t.Fatal(err)
	}

	obj := &Data{}
	err = client.Get(ctx, key, obj)
	if err != nil {
		t.Fatal(err)
	}

	if v := obj.Email; v != "test@example.com" {
		t.Errorf("unexpected: '%v'", v)
	}
}

func TestCloudDatastore_QueryNextByPropertyList(t *testing.T) {
	ctx := context.Background()
	client, err := datastore.NewClient(ctx, internal.GetProjectID())
	if err != nil {
		t.Fatal(err.Error())
	}
	defer client.Close()
	defer cleanUp()

	type Data struct {
		Name string
	}

	_, err = client.Put(ctx, datastore.IncompleteKey("Data", nil), &Data{Name: "A"})
	if err != nil {
		t.Fatal(err)
	}

	q := datastore.NewQuery("Data")

	{ // passed datastore.PropertyList, would be error.
		iter := client.Run(ctx, q)

		var ps datastore.PropertyList
		_, err = iter.Next(ps)
		if err == nil {
			t.Fatal(err)
		}
	}
	{ // ok! *datastore.PropertyList
		iter := client.Run(ctx, q)

		var ps datastore.PropertyList
		_, err = iter.Next(&ps)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestCloudDatastore_GetAllByPropertyListSlice(t *testing.T) {
	ctx := context.Background()
	client, err := datastore.NewClient(ctx, internal.GetProjectID())
	if err != nil {
		t.Fatal(err.Error())
	}
	defer client.Close()
	defer cleanUp()

	type Data struct {
		Name string
	}

	_, err = client.Put(ctx, datastore.IncompleteKey("Data", nil), &Data{Name: "A"})
	if err != nil {
		t.Fatal(err)
	}

	q := datastore.NewQuery("Data")
	var psList []datastore.PropertyList

	// passed []datastore.PropertyList, would be error.
	_, err = client.GetAll(ctx, q, psList)
	if err == nil {
		t.Fatal(err)
	}

	// ok! *[]datastore.PropertyList
	psList = nil
	_, err = client.GetAll(ctx, q, &psList)
	if err != nil {
		t.Fatal(err)
	}
}

func TestCloudDatastore_PendingKeyWithCompleteKey(t *testing.T) {
	ctx := context.Background()
	client, err := datastore.NewClient(ctx, internal.GetProjectID())
	if err != nil {
		t.Fatal(err.Error())
	}
	defer client.Close()
	defer cleanUp()

	type Data struct {
		Name string
	}

	tx, err := client.NewTransaction(ctx)
	if err != nil {
		t.Fatal(err)
	}

	pKey, err := tx.Put(datastore.NameKey("Data", "a", nil), &Data{Name: "Test"})
	if err != nil {
		t.Fatal(err)
	}

	commit, err := tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := recover(); err == nil || err.(string) != "PendingKey was not created by corresponding transaction" {
			t.Errorf("unexpected: '%v'", err)
		}
	}()
	// panic occurred in this case.
	commit.Key(pKey)
}

func TestCloudDatastore_Namespace(t *testing.T) {
	ctx := context.Background()
	client, err := datastore.NewClient(ctx, internal.GetProjectID())
	if err != nil {
		t.Fatal(err.Error())
	}
	defer client.Close()
	defer cleanUp()

	type Data struct {
		Name string
	}

	key := datastore.IDKey("Test", 1, nil)
	key.Namespace = "no1"
	if v := key.String(); v != "/Test,1" {
		t.Fatalf("unexpected: %v", v)
	}

	_, err = client.Put(ctx, key, &Data{"Name #1"})
	if err != nil {
		t.Fatal(err)
	}

	key.Namespace = ""
	err = client.Get(ctx, key, &Data{})
	if err != datastore.ErrNoSuchEntity {
		t.Fatal(err)
	}

	key.Namespace = "no1"
	err = client.Get(ctx, key, &Data{})
	if err != nil {
		t.Fatal(err)
	}

	q := datastore.NewQuery("Test")
	q = q.KeysOnly()

	var keys []*datastore.Key

	keys, err = client.GetAll(ctx, q, nil)
	if err != nil {
		t.Fatal(err)
	}
	if v := len(keys); v != 0 {
		t.Fatalf("unexpected: %v", v)
	}

	q = q.Namespace("no1")
	keys, err = client.GetAll(ctx, q, nil)
	if err != nil {
		t.Fatal(err)
	}
	if v := len(keys); v != 1 {
		t.Fatalf("unexpected: %v", v)
	}
}
