package testsuite

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.mercari.io/datastore"
)

func PutAndGet(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type TestEntity struct {
		String string
	}

	key := client.IncompleteKey("Test", nil)
	t.Log(key)
	newKey, err := client.Put(ctx, key, &TestEntity{String: "Test"})
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("new key: %s", newKey.String())

	entity := &TestEntity{}
	err = client.Get(ctx, newKey, entity)
	if err != nil {
		t.Fatal(err)
	}

	if v := entity.String; v != "Test" {
		t.Errorf("unexpected: %v", v)
	}
}

func PutAndGet_TimeTime(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type Data struct {
		At time.Time
	}

	key := client.IncompleteKey("Data", nil)

	l, err := time.LoadLocation("Europe/Berlin") // not UTC, not PST, not Asia/Tokyo(developer's local timezone)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2017, 12, 5, 10, 11, 22, 33, l)

	newKey, err := client.Put(ctx, key, &Data{At: now})
	if err != nil {
		t.Fatal(err)
	}

	obj := &Data{}
	err = client.Get(ctx, newKey, obj)
	if err != nil {
		t.Fatal(err)
	}

	// load by time.Local
	if v := obj.At.Location(); v != time.Local {
		t.Errorf("unexpected: %v", v)
	}
	if v := obj.At.UnixNano(); v != now.Truncate(time.Microsecond).UnixNano() {
		t.Errorf("unexpected: %v", v)
	}
}

func PutAndDelete(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type TestEntity struct {
		String string
	}

	key := client.IncompleteKey("Test", nil)
	t.Log(key)
	newKey, err := client.Put(ctx, key, &TestEntity{String: "Test"})
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("new key: %s", newKey.String())

	err = client.Delete(ctx, newKey)
	if err != nil {
		t.Fatal(err)
	}

	entity := &TestEntity{}
	err = client.Get(ctx, newKey, entity)
	if err != datastore.ErrNoSuchEntity {
		t.Fatal(err)
	}
}

func PutAndGet_ObjectHasObjectSlice(t *testing.T, ctx context.Context, client datastore.Client) {
	if IsAEDatastoreClient(ctx) {
		// flatten options must required in ae.
		t.SkipNow()
	}

	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type Inner struct {
		A string
		B string
	}

	type Data struct {
		Slice []Inner // `datastore:",flatten"` // If flatten removed, aedatastore env will fail.
	}

	key := client.NameKey("Test", "a", nil)
	_, err := client.Put(ctx, key, &Data{
		Slice: []Inner{
			Inner{A: "A1", B: "B1"},
			Inner{A: "A2", B: "B2"},
			Inner{A: "A3", B: "B3"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	obj := &Data{}
	err = client.Get(ctx, key, obj)
	if err != nil {
		t.Fatal(err)
	}

	if v := len(obj.Slice); v != 3 {
		t.Errorf("unexpected: %v", v)
	}

	for idx, s := range obj.Slice {
		if v := s.A; v != fmt.Sprintf("A%d", idx+1) {
			t.Errorf("unexpected: %v", v)
		}
		if v := s.B; v != fmt.Sprintf("B%d", idx+1) {
			t.Errorf("unexpected: %v", v)
		}
	}
}

func PutAndGet_ObjectHasObjectSliceWithFlatten(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type Inner struct {
		A string
		B string
	}

	type Data struct {
		Slice []Inner `datastore:",flatten"`
	}

	key := client.NameKey("Test", "a", nil)
	_, err := client.Put(ctx, key, &Data{
		Slice: []Inner{
			Inner{A: "A1", B: "B1"},
			Inner{A: "A2", B: "B2"},
			Inner{A: "A3", B: "B3"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	obj := &Data{}
	err = client.Get(ctx, key, obj)
	if err != nil {
		t.Fatal(err)
	}

	if v := len(obj.Slice); v != 3 {
		t.Errorf("unexpected: %v", v)
	}

	for idx, s := range obj.Slice {
		if v := s.A; v != fmt.Sprintf("A%d", idx+1) {
			t.Errorf("unexpected: %v", v)
		}
		if v := s.B; v != fmt.Sprintf("B%d", idx+1) {
			t.Errorf("unexpected: %v", v)
		}
	}
}

func PutEntityType(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type Inner struct {
		A string
		B string
	}

	type DataA struct {
		C *Inner
	}

	type DataB struct {
		C *Inner `datastore:",flatten"`
	}

	key := client.IncompleteKey("Test", nil)
	_, err := client.Put(ctx, key, &DataA{
		C: &Inner{
			A: "a",
			B: "b",
		},
	})
	if IsAEDatastoreClient(ctx) {
		if err != datastore.ErrInvalidEntityType {
			t.Fatal(err)
		}
	} else {
		if err != nil {
			t.Fatal(err)
		}
	}

	_, err = client.Put(ctx, key, &DataB{
		C: &Inner{
			A: "a",
			B: "b",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func PutAndGetNilKey(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type Data struct {
		KeyA datastore.Key
		KeyB datastore.Key
	}

	key := client.IncompleteKey("Test", nil)
	key, err := client.Put(ctx, key, &Data{
		KeyA: client.NameKey("Test", "a", nil),
		KeyB: nil,
	})
	if err != nil {
		t.Fatal(err)
	}

	obj := &Data{}
	err = client.Get(ctx, key, obj)
	if err != nil {
		t.Fatal(err)
	}

	if v := obj.KeyA; v == nil {
		t.Errorf("unexpected: %v", v)
	}
	if v := obj.KeyB; v != nil {
		t.Errorf("unexpected: %v", v)
	}
}

func PutAndGetNilKeySlice(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type Data struct {
		Keys []datastore.Key
	}

	key := client.IncompleteKey("Test", nil)
	key, err := client.Put(ctx, key, &Data{
		Keys: []datastore.Key{
			client.NameKey("Test", "a", nil),
			nil,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	obj := &Data{}
	err = client.Get(ctx, key, obj)
	if err != nil {
		t.Fatal(err)
	}

	if v := len(obj.Keys); v != 2 {
		t.Fatalf("unexpected: %v", v)
	}
	if v := obj.Keys[0]; v == nil {
		t.Errorf("unexpected: %v", v)
	}
	if v := obj.Keys[1]; v != nil {
		t.Errorf("unexpected: %v", v)
	}
}

type EntityInterface interface {
	Kind() string
	ID() string
}

type PutInterfaceTest struct {
	kind string
	id   string
}

func (e *PutInterfaceTest) Kind() string {
	return e.kind
}
func (e *PutInterfaceTest) ID() string {
	return e.id
}

func PutInterface(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	var e EntityInterface = &PutInterfaceTest{}

	key := client.IncompleteKey("Test", nil)
	_, err := client.Put(ctx, key, e)
	if err != nil {
		t.Fatal(err)
	}
}

func PutAndGetPropertyList(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	var ps datastore.PropertyList
	ps = append(ps, datastore.Property{
		Name:  "A",
		Value: "A-Value",
	})
	ps = append(ps, datastore.Property{
		Name:  "B",
		Value: true,
	})

	key := client.IncompleteKey("Test", nil)
	var err error

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

func PutAndGetMultiPropertyListSlice(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	var pss []datastore.PropertyList
	var keys []datastore.Key
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

		key := client.IncompleteKey("Test", nil)

		pss = append(pss, ps)
		keys = append(keys, key)
	}

	var err error

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

func PutAndGetBareStruct(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type Data struct {
		Name string
	}

	var err error

	key := client.IncompleteKey("Test", nil)
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

func PutAndGetMultiBareStruct(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type Data struct {
		Name string
	}

	var list []Data
	var keys []datastore.Key
	{
		obj := Data{Name: "A"}
		key := client.IncompleteKey("Test", nil)

		list = append(list, obj)
		keys = append(keys, key)
	}

	var err error

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
