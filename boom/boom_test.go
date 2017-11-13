package boom

import (
	"context"
	"testing"

	"go.mercari.io/datastore"
	"go.mercari.io/datastore/internal/testutils"
)

var _ datastore.PropertyTranslator = UserID(0)

type contextClient struct{}

type UserID int64
type DataID int64

func (id UserID) ToPropertyValue(ctx context.Context) (interface{}, error) {
	client := ctx.Value(contextClient{}).(datastore.Client)
	key := client.IDKey("User", int64(id), nil)
	return key, nil
}

func (id UserID) FromPropertyValue(ctx context.Context, p datastore.Property) (dst interface{}, err error) {
	key, ok := p.Value.(datastore.Key)
	if !ok {
		return nil, datastore.ErrInvalidEntityType
	}
	return UserID(key.ID()), nil
}

func (id DataID) ToPropertyValue(ctx context.Context) (interface{}, error) {
	client := ctx.Value(contextClient{}).(datastore.Client)
	key := client.IDKey("Data", int64(id), nil)
	return key, nil
}

func (id DataID) FromPropertyValue(ctx context.Context, p datastore.Property) (dst interface{}, err error) {
	key, ok := p.Value.(datastore.Key)
	if !ok {
		return nil, datastore.ErrInvalidEntityType
	}
	return DataID(key.ID()), nil
}

func TestBoom_Key(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupCloudDatastore(t)
	defer cleanUp()

	type Data struct {
		ID int64 `datastore:"-" boom:"id"`
	}

	bm := FromClient(ctx, client)

	key := bm.Key(&Data{111})
	if v := key.Kind(); v != "Data" {
		t.Errorf("unexpected: %v", v)
	}
	if v := key.ID(); v != 111 {
		t.Errorf("unexpected: %v", v)
	}
}

func TestBoom_KeyWithParent(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupCloudDatastore(t)
	defer cleanUp()

	type Data struct {
		ParentKey datastore.Key `datastore:"-" boom:"parent"`
		ID        int64         `datastore:"-" boom:"id"`
	}

	bm := FromClient(ctx, client)

	userKey := client.NameKey("User", "test", nil)
	key := bm.Key(&Data{userKey, 111})
	if v := key.ParentKey().Kind(); v != "User" {
		t.Errorf("unexpected: %v", v)
	}
	if v := key.ParentKey().Name(); v != "test" {
		t.Errorf("unexpected: %v", v)
	}
	if v := key.Kind(); v != "Data" {
		t.Errorf("unexpected: %v", v)
	}
	if v := key.ID(); v != 111 {
		t.Errorf("unexpected: %v", v)
	}
}

func TestBoom_Put(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupCloudDatastore(t)
	defer cleanUp()

	type Data struct {
		ID  int64  `datastore:"-" boom:"id"`
		Str string ``
	}

	bm := FromClient(ctx, client)

	key, err := bm.Put(ctx, &Data{111, "Str"})
	if err != nil {
		t.Fatal(err)
	}

	if v := key.Kind(); v != "Data" {
		t.Errorf("unexpected: %v", v)
	}
	if v := key.ID(); v != 111 {
		t.Errorf("unexpected: %v", v)
	}

	obj := &Data{}
	err = client.Get(ctx, key, obj)
	if err != nil {
		t.Fatal(err)
	}

	if v := obj.Str; v != "Str" {
		t.Errorf("unexpected: %v", v)
	}
}

func TestBoom_PutWithIncomplete(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupCloudDatastore(t)
	defer cleanUp()

	type Data struct {
		ID  int64  `datastore:"-" boom:"id"`
		Str string ``
	}

	bm := FromClient(ctx, client)

	obj := &Data{Str: "Str"}
	key, err := bm.Put(ctx, obj)
	if err != nil {
		t.Fatal(err)
	}

	if v := key.Kind(); v != "Data" {
		t.Errorf("unexpected: %v", v)
	}
	if v := key.ID(); v == 0 {
		t.Errorf("unexpected: %v", v)
	}
	if v := obj.ID; v != key.ID() {
		t.Errorf("unexpected: %v", v)
	}

	obj = &Data{}
	err = client.Get(ctx, key, obj)
	if err != nil {
		t.Fatal(err)
	}

	if v := obj.Str; v != "Str" {
		t.Errorf("unexpected: %v", v)
	}
}

func TestBoom_Get(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupCloudDatastore(t)
	defer cleanUp()

	type Data struct {
		ID  int64  `datastore:"-" boom:"id"`
		Str string ``
	}

	bm := FromClient(ctx, client)

	key := client.IDKey("Data", 111, nil)
	_, err := client.Put(ctx, key, &Data{Str: "Str"})
	if err != nil {
		t.Fatal(err)
	}

	obj := &Data{ID: 111}
	err = bm.Get(ctx, obj)
	if err != nil {
		t.Fatal(err)
	}

	if v := obj.Str; v != "Str" {
		t.Errorf("unexpected: %v", v)
	}
}

func TestBoom_DeleteByStruct(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupCloudDatastore(t)
	defer cleanUp()

	type Data struct {
		ID  int64  `datastore:"-" boom:"id"`
		Str string ``
	}

	bm := FromClient(ctx, client)

	key := client.IDKey("Data", 111, nil)
	_, err := client.Put(ctx, key, &Data{Str: "Str"})
	if err != nil {
		t.Fatal(err)
	}

	obj := &Data{ID: 111}
	err = bm.Delete(ctx, obj)
	if err != nil {
		t.Fatal(err)
	}

	err = client.Get(ctx, key, &Data{})
	if err != datastore.ErrNoSuchEntity {
		t.Fatal(err)
	}
}

func TestBoom_DeleteByKey(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupCloudDatastore(t)
	defer cleanUp()

	type Data struct {
		ID  int64  `datastore:"-" boom:"id"`
		Str string ``
	}

	bm := FromClient(ctx, client)

	key := client.IDKey("Data", 111, nil)
	_, err := client.Put(ctx, key, &Data{Str: "Str"})
	if err != nil {
		t.Fatal(err)
	}

	err = bm.Delete(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	err = client.Get(ctx, key, &Data{})
	if err != datastore.ErrNoSuchEntity {
		t.Fatal(err)
	}
}

func TestBoom_GetAll(t *testing.T) {
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

	_, err := bm.PutMulti(ctx, list)
	if err != nil {
		t.Fatal(err)
	}

	q := client.NewQuery(bm.Kind(&Data{}))
	list = make([]*Data, 0)
	_, err = bm.GetAll(ctx, q, &list)
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

func TestBoom_TagWithPropertyTranslator(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupCloudDatastore(t)
	defer cleanUp()

	ctx = context.WithValue(ctx, contextClient{}, client)

	bm := FromClient(ctx, client)

	{ // Put & Get with boom:"id"
		type Data struct {
			ID DataID `datastore:"-" boom:"id"`
		}

		key, err := bm.Put(ctx, &Data{ID: DataID(100)})
		if err != nil {
			t.Fatal(err)
		}

		if v := key.Kind(); v != "Data" {
			t.Errorf("unexpected: %v", v)
		}
		if v := key.ID(); v != 100 {
			t.Errorf("unexpected: %v", v)
		}

		err = bm.Get(ctx, &Data{ID: DataID(100)})
		if err != nil {
			t.Fatal(err)
		}
	}
	{ // Put & Get	 with boom:"parent"
		type Data struct {
			ParentUserID UserID `datastore:"-" boom:"parent"`
			ID           DataID `datastore:"-" boom:"id"`
		}

		key, err := bm.Put(ctx, &Data{ParentUserID: UserID(20), ID: DataID(100)})
		if err != nil {
			t.Fatal(err)
		}

		if v := key.Kind(); v != "Data" {
			t.Errorf("unexpected: %v", v)
		}
		if v := key.ID(); v != 100 {
			t.Errorf("unexpected: %v", v)
		}
		if v := key.ParentKey().Kind(); v != "User" {
			t.Errorf("unexpected: %v", v)
		}
		if v := key.ParentKey().ID(); v != 20 {
			t.Errorf("unexpected: %v", v)
		}

		err = bm.Get(ctx, &Data{ParentUserID: UserID(20), ID: DataID(100)})
		if err != nil {
			t.Fatal(err)
		}
	}
}
