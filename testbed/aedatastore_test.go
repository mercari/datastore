package testbed

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/favclip/testerator"
	_ "github.com/favclip/testerator/datastore"
	_ "github.com/favclip/testerator/memcache"

	netcontext "golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
)

type AEDatastoreStruct struct {
	Test string
}

func TestMain(m *testing.M) {
	_, _, err := testerator.SpinUp()
	if err != nil {
		fmt.Fprint(os.Stderr, err.Error())
		os.Exit(1)
	}

	status := m.Run()

	err = testerator.SpinDown()
	if err != nil {
		fmt.Fprint(os.Stderr, err.Error())
		os.Exit(1)
	}

	os.Exit(status)
}

func newContext() (context.Context, func(), error) {
	_, ctx, err := testerator.SpinUp()
	if err != nil {
		return nil, nil, err
	}

	return ctx, func() { testerator.SpinDown() }, nil
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
		newS := &AEDatastoreStruct{}
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
	{
		var list []*Data
		_, err = q.GetAll(ctx, &list)
		if err != nil {
			t.Fatal(err.Error())
		}

		if v := len(list); v != 1 {
			t.Fatalf("unexpected: %v", v)
		}
	}
	{
		keys, err := q.KeysOnly().GetAll(ctx, nil)
		if err != nil {
			t.Fatal(err.Error())
		}

		if v := len(keys); v != 1 {
			t.Fatalf("unexpected: %v", v)
		}
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

	sort.SliceStable(ps, func(i, j int) bool {
		a := ps[i]
		b := ps[j]
		if v := strings.Compare(a.Name, b.Name); v < 0 {
			return true
		}
		if v := strings.Compare(a.Value.(string), b.Value.(string)); v < 0 {
			return true
		}

		return false
	})

	expects := []struct {
		Name     string
		Value    string
		Multiple bool
	}{
		{"Slice.A", "A1", true},
		{"Slice.A", "A2", true},
		{"Slice.A", "A3", true},
		{"Slice.B", "B1", true},
		{"Slice.B", "B2", true},
		{"Slice.B", "B3", true},
	}
	for idx, expect := range expects {
		t.Logf("idx: %d", idx)
		p := ps[idx]
		if v := p.Name; v != expect.Name {
			t.Errorf("unexpected: %v", v)
		}
		if v := p.Value.(string); v != expect.Value {
			t.Errorf("unexpected: %v", v)
		}
		if v := p.Multiple; v != expect.Multiple {
			t.Errorf("unexpected: %v", v)
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

func TestAEDatastore_PutInterface(t *testing.T) {
	ctx, close, err := newContext()
	if err != nil {
		t.Fatal(err)
	}
	defer close()

	var e EntityInterface = &PutInterfaceTest{}

	key := datastore.NewIncompleteKey(ctx, "Test", nil)
	_, err = datastore.Put(ctx, key, e)
	if err != nil {
		t.Fatal(err)
	}
}

func TestAEDatastore_PutAndGetPropertyList(t *testing.T) {
	ctx, close, err := newContext()
	if err != nil {
		t.Fatal(err)
	}
	defer close()

	var ps datastore.PropertyList
	ps = append(ps, datastore.Property{
		Name:  "A",
		Value: "A-Value",
	})
	ps = append(ps, datastore.Property{
		Name:  "B",
		Value: true,
	})

	key := datastore.NewIncompleteKey(ctx, "Test", nil)
	// passed datastore.PropertyList, would be error.
	_, err = datastore.Put(ctx, key, ps)
	if err != datastore.ErrInvalidEntityType {
		t.Fatal(err)
	}

	// ok!
	key, err = datastore.Put(ctx, key, &ps)
	if err != nil {
		t.Fatal(err)
	}

	// passed datastore.PropertyList, would be error.
	ps = datastore.PropertyList{}
	err = datastore.Get(ctx, key, ps)
	if err != datastore.ErrInvalidEntityType {
		t.Fatal(err)
	}

	// ok!
	ps = datastore.PropertyList{}
	err = datastore.Get(ctx, key, &ps)
	if err != nil {
		t.Fatal(err)
	}

	if v := len(ps); v != 2 {
		t.Fatalf("unexpected: %v", v)
	}
}

func TestAEDatastore_PutAndGetMultiPropertyListSlice(t *testing.T) {
	ctx, close, err := newContext()
	if err != nil {
		t.Fatal(err)
	}
	defer close()

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

		key := datastore.NewIncompleteKey(ctx, "Test", nil)

		pss = append(pss, ps)
		keys = append(keys, key)
	}

	// passed *[]datastore.PropertyList, would be error.
	_, err = datastore.PutMulti(ctx, keys, &pss)
	if err == nil {
		t.Fatal(err)
	}

	// ok! []datastore.PropertyList
	keys, err = datastore.PutMulti(ctx, keys, pss)
	if err != nil {
		t.Fatal(err)
	}

	// passed *[]datastore.PropertyList, would be error.
	pss = []datastore.PropertyList{}
	err = datastore.GetMulti(ctx, keys, &pss)
	if err == nil {
		t.Fatal(err)
	}

	// passed []datastore.PropertyList with length 0, would be error.
	pss = make([]datastore.PropertyList, 0)
	err = datastore.GetMulti(ctx, keys, pss)
	if err == nil {
		t.Fatal(err)
	}

	// ok! []datastore.PropertyList with length == len(keys)
	pss = make([]datastore.PropertyList, len(keys))
	err = datastore.GetMulti(ctx, keys, pss)
	if err != nil {
		t.Fatal(err)
	}

	if v := len(pss); v != 1 {
		t.Fatalf("unexpected: %v", v)
	}
}

func TestAEDatastore_PutAndGetBareStruct(t *testing.T) {
	ctx, close, err := newContext()
	if err != nil {
		t.Fatal(err)
	}
	defer close()

	type Data struct {
		Name string
	}

	key := datastore.NewIncompleteKey(ctx, "Test", nil)
	// passed Data, would be error.
	_, err = datastore.Put(ctx, key, Data{Name: "A"})
	if err != datastore.ErrInvalidEntityType {
		t.Fatal(err)
	}

	// ok! *Data
	key, err = datastore.Put(ctx, key, &Data{Name: "A"})
	if err != nil {
		t.Fatal(err)
	}

	// ok! but struct are copied. can't watching Get result.
	obj := Data{}
	err = datastore.Get(ctx, key, obj)
	if err != datastore.ErrInvalidEntityType {
		t.Fatal(err)
	}

	if v := obj.Name; v != "" {
		t.Errorf("unexpected: '%v'", v)
	}
}

func TestAEDatastore_PutAndGetMultiBareStruct(t *testing.T) {
	ctx, close, err := newContext()
	if err != nil {
		t.Fatal(err)
	}
	defer close()

	type Data struct {
		Name string
	}

	var list []Data
	var keys []*datastore.Key
	{
		obj := Data{Name: "A"}
		key := datastore.NewIncompleteKey(ctx, "Test", nil)

		list = append(list, obj)
		keys = append(keys, key)
	}

	// ok!
	keys, err = datastore.PutMulti(ctx, keys, list)
	if err != nil {
		t.Fatal(err)
	}

	// passed []Data with length 0, would be error.
	list = make([]Data, 0)
	err = datastore.GetMulti(ctx, keys, list)
	if err == nil {
		t.Fatal(err)
	}

	// ok! []Data with length == len(keys)
	list = make([]Data, len(keys))
	err = datastore.GetMulti(ctx, keys, list)
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

func TestAEDatastore_PutAndGetStringSynonym(t *testing.T) {
	ctx, close, err := newContext()
	if err != nil {
		t.Fatal(err)
	}
	defer close()

	type Email string

	type Data struct {
		Email Email
	}

	key, err := datastore.Put(ctx, datastore.NewIncompleteKey(ctx, "Data", nil), &Data{Email: "test@example.com"})
	if err != nil {
		t.Fatal(err)
	}

	obj := &Data{}
	err = datastore.Get(ctx, key, obj)
	if err != nil {
		t.Fatal(err)
	}

	if v := obj.Email; v != "test@example.com" {
		t.Errorf("unexpected: '%v'", v)
	}
}

func TestAEDatastore_QueryNextByPropertyList(t *testing.T) {
	ctx, close, err := newContext()
	if err != nil {
		t.Fatal(err)
	}
	defer close()

	type Data struct {
		Name string
	}

	_, err = datastore.Put(ctx, datastore.NewIncompleteKey(ctx, "Data", nil), &Data{Name: "A"})
	if err != nil {
		t.Fatal(err)
	}

	q := datastore.NewQuery("Data")

	{ // passed datastore.PropertyList, would be error.
		iter := q.Run(ctx)

		var ps datastore.PropertyList
		_, err = iter.Next(ps)
		if err == nil {
			t.Fatal(err)
		}
	}
	{ // ok! *datastore.PropertyList
		iter := q.Run(ctx)

		var ps datastore.PropertyList
		_, err = iter.Next(&ps)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestAEDatastore_GetAllByPropertyListSlice(t *testing.T) {
	ctx, close, err := newContext()
	if err != nil {
		t.Fatal(err)
	}
	defer close()

	type Data struct {
		Name string
	}

	_, err = datastore.Put(ctx, datastore.NewIncompleteKey(ctx, "Data", nil), &Data{Name: "A"})
	if err != nil {
		t.Fatal(err)
	}

	q := datastore.NewQuery("Data")
	var psList []datastore.PropertyList

	// passed []datastore.PropertyList, would be error.
	_, err = q.GetAll(ctx, psList)
	if err == nil {
		t.Fatal(err)
	}

	// ok! *[]datastore.PropertyList
	psList = nil
	_, err = q.GetAll(ctx, &psList)
	if err != nil {
		t.Fatal(err)
	}
}

func TestAEDatastore_Namespace(t *testing.T) {
	ctx, close, err := newContext()
	if err != nil {
		t.Fatal(err)
	}
	defer close()

	type Data struct {
		Name string
	}

	nsCtx, err := appengine.Namespace(ctx, "no1")
	if err != nil {
		t.Fatal(err)
	}

	key := datastore.NewKey(nsCtx, "Test", "", 1, nil)
	if v := key.String(); v != "/Test,1" {
		t.Fatalf("unexpected: %v", v)
	}
	vanillaKey := datastore.NewKey(ctx, "Test", "", 1, nil)
	if v := vanillaKey.String(); v != "/Test,1" {
		t.Fatalf("unexpected: %v", v)
	}

	_, err = datastore.Put(ctx, key, &Data{"Name #1"})
	if err != nil {
		t.Fatal(err)
	}

	err = datastore.Get(ctx, vanillaKey, &Data{})
	if err != datastore.ErrNoSuchEntity {
		t.Fatal(err)
	}

	err = datastore.Get(ctx, key, &Data{})
	if err != nil {
		t.Fatal(err)
	}

	q := datastore.NewQuery("Test")
	q = q.KeysOnly()

	var keys []*datastore.Key

	keys, err = q.GetAll(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if v := len(keys); v != 0 {
		t.Fatalf("unexpected: %v", v)
	}

	keys, err = q.GetAll(nsCtx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if v := len(keys); v != 1 {
		t.Fatalf("unexpected: %v", v)
	}
}
