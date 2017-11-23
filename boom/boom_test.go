package boom

import (
	"context"
	"testing"

	"go.mercari.io/datastore"
	"go.mercari.io/datastore/internal/testutils"
)

var _ datastore.PropertyTranslator = UserID(0)
var _ datastore.PropertyTranslator = DataID(0)
var _ datastore.PropertyTranslator = IntID(0)
var _ datastore.PropertyTranslator = StringID("")

type contextClient struct{}

type UserID int64
type DataID int64

type IntID int64
type StringID string

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

func (id IntID) ToPropertyValue(ctx context.Context) (interface{}, error) {
	// for boom.KeyError
	return int64(id), nil
}

func (id IntID) FromPropertyValue(ctx context.Context, p datastore.Property) (dst interface{}, err error) {
	key, ok := p.Value.(datastore.Key)
	if !ok {
		return nil, datastore.ErrInvalidEntityType
	}
	return IntID(key.ID()), nil
}

func (id StringID) ToPropertyValue(ctx context.Context) (interface{}, error) {
	// for boom.KeyError
	return string(id), nil
}

func (id StringID) FromPropertyValue(ctx context.Context, p datastore.Property) (dst interface{}, err error) {
	key, ok := p.Value.(datastore.Key)
	if !ok {
		return nil, datastore.ErrInvalidEntityType
	}
	return StringID(key.Name()), nil
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

func TestBoom_KeyWithPropertyTranslator(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupCloudDatastore(t)
	defer cleanUp()

	{ // IntID with PT
		type Data struct {
			ID IntID `datastore:"-" boom:"id"`
		}

		bm := FromClient(ctx, client)

		_, err := bm.Put(&Data{111})
		if err != nil {
			t.Fatal(err)
		}

		err = bm.Get(&Data{111})
		if err != nil {
			t.Fatal(err)
		}
	}
	{ // StringID with PT
		type Data struct {
			ID StringID `datastore:"-" boom:"id"`
		}

		bm := FromClient(ctx, client)

		_, err := bm.Put(&Data{"a"})
		if err != nil {
			t.Fatal(err)
		}

		err = bm.Get(&Data{"a"})
		if err != nil {
			t.Fatal(err)
		}
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

func TestBoom_AllocateID(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupCloudDatastore(t)
	defer cleanUp()

	type Data struct {
		ID IntID `datastore:"-" boom:"id"`
	}

	bm := FromClient(ctx, client)

	obj := &Data{}
	key, err := bm.AllocateID(obj)
	if err != nil {
		t.Fatal(err)
	}

	if v := key.Kind(); v != "Data" {
		t.Errorf("unexpected: %v", v)
	}
	if v := key.ID(); v == 0 {
		t.Errorf("unexpected: %v", v)
	}
	if v := int64(obj.ID); v != key.ID() {
		t.Errorf("unexpected: %v", v)
	}
}

func TestBoom_AllocateIDs(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupCloudDatastore(t)
	defer cleanUp()

	type Data struct {
		ID IntID `datastore:"-" boom:"id"`
	}

	bm := FromClient(ctx, client)

	type Spec struct {
		From   interface{}
		Kind   string
		Assert func(key datastore.Key, spec Spec)
	}

	specs := []Spec{
		// struct
		{&Data{}, "Data", func(key datastore.Key, spec Spec) {
			obj := spec.From.(*Data)
			if v := int64(obj.ID); v != key.ID() {
				t.Errorf("unexpected: %v", v)
			}
		}},
		// key without parent
		{client.IncompleteKey("User", nil), "User", nil},
		// key with parent
		{client.IncompleteKey("Todo", client.NameKey("User", "foo", nil)), "Todo", func(key datastore.Key, spec Spec) {
			if v := key.ParentKey(); v == nil {
				t.Fatalf("unexpected: %v", v)
			}
			if v := key.ParentKey().Kind(); v != "User" {
				t.Errorf("unexpected: %v", v)
			}
			if v := key.ParentKey().Name(); v != "foo" {
				t.Errorf("unexpected: %v", v)
			}
		}},
		// string
		{"Book", "Book", nil},
	}

	srcs := make([]interface{}, 0, len(specs))
	for _, spec := range specs {
		srcs = append(srcs, spec.From)
	}

	keys, err := bm.AllocateIDs(srcs)
	if err != nil {
		t.Fatal(err)
	}

	if v := len(keys); v != len(specs) {
		t.Errorf("unexpected: %v", v)
	}

	for idx, spec := range specs {
		key := keys[idx]
		if v := key.Kind(); v != spec.Kind {
			t.Errorf("unexpected: %v", v)
		}
		if v := key.ID(); v == 0 {
			t.Errorf("unexpected: %v", v)
		}
		if spec.Assert != nil {
			spec.Assert(key, spec)
		}
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

	key, err := bm.Put(&Data{111, "Str"})
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
	err = bm.Get(obj)
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
	err = bm.Delete(obj)
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

	err = bm.Delete(key)
	if err != nil {
		t.Fatal(err)
	}

	err = client.Get(ctx, key, &Data{})
	if err != datastore.ErrNoSuchEntity {
		t.Fatal(err)
	}
}

func TestBoom_Count(t *testing.T) {
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

	q := bm.NewQuery(bm.Kind(&Data{}))
	cnt, err := bm.Count(q)
	if err != nil {
		t.Fatal(err)
	}

	if v := cnt; v != 1 {
		t.Errorf("unexpected: %v", v)
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

	_, err := bm.PutMulti(list)
	if err != nil {
		t.Fatal(err)
	}

	q := bm.NewQuery(bm.Kind(&Data{}))
	{
		list = make([]*Data, 0)
		_, err = bm.GetAll(q, &list)
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
	{
		keys, err := bm.GetAll(q.KeysOnly(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if v := len(keys); v != size {
			t.Errorf("unexpected: %v", v)
		}
	}
}

func TestBoom_TagID(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupCloudDatastore(t)
	defer cleanUp()

	ctx = context.WithValue(ctx, contextClient{}, client)

	bm := FromClient(ctx, client)

	{ // ID(IntID)
		type Data struct {
			ID int64 `datastore:"-" boom:"id"`
		}

		key, err := bm.Put(&Data{ID: 1})
		if err != nil {
			t.Fatal(err)
		}

		if v := key.Kind(); v != "Data" {
			t.Errorf("unexpected: %v", v)
		}
		if v := key.ID(); v != 1 {
			t.Errorf("unexpected: %v", v)
		}

		err = bm.Get(&Data{1})
		if err != nil {
			t.Fatal(err)
		}
	}
	{ // Name(StringID)
		type Data struct {
			ID string `datastore:"-" boom:"id"`
		}

		key, err := bm.Put(&Data{ID: "a"})
		if err != nil {
			t.Fatal(err)
		}
		if v := key.Kind(); v != "Data" {
			t.Errorf("unexpected: %v", v)
		}
		if v := key.Name(); v != "a" {
			t.Errorf("unexpected: %v", v)
		}

		err = bm.Get(&Data{"a"})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestBoom_TagIDWithPropertyTranslator(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupCloudDatastore(t)
	defer cleanUp()

	ctx = context.WithValue(ctx, contextClient{}, client)

	bm := FromClient(ctx, client)

	{ // Put & Get with boom:"id"
		type Data struct {
			ID DataID `datastore:"-" boom:"id"`
		}

		key, err := bm.Put(&Data{ID: DataID(100)})
		if err != nil {
			t.Fatal(err)
		}

		if v := key.Kind(); v != "Data" {
			t.Errorf("unexpected: %v", v)
		}
		if v := key.ID(); v != 100 {
			t.Errorf("unexpected: %v", v)
		}

		err = bm.Get(&Data{ID: DataID(100)})
		if err != nil {
			t.Fatal(err)
		}
	}
	{ // Put & Get	 with boom:"parent"
		type Data struct {
			ParentUserID UserID `datastore:"-" boom:"parent"`
			ID           DataID `datastore:"-" boom:"id"`
		}

		key, err := bm.Put(&Data{ParentUserID: UserID(20), ID: DataID(100)})
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

		err = bm.Get(&Data{ParentUserID: UserID(20), ID: DataID(100)})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestBoom_TagParent(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupCloudDatastore(t)
	defer cleanUp()

	ctx = context.WithValue(ctx, contextClient{}, client)

	bm := FromClient(ctx, client)

	type Data struct {
		ParentKey datastore.Key `datastore:"-" boom:"parent"`
		ID        int64         `datastore:"-" boom:"id"`
	}

	parentKey := client.NameKey("Parent", "a", nil)
	key, err := bm.Put(&Data{ParentKey: parentKey, ID: 1})
	if err != nil {
		t.Fatal(err)
	}

	if v := key.Kind(); v != "Data" {
		t.Errorf("unexpected: %v", v)
	}
	if v := key.ID(); v != 1 {
		t.Errorf("unexpected: %v", v)
	}
	if v := key.ParentKey().Kind(); v != "Parent" {
		t.Errorf("unexpected: %v", v)
	}
	if v := key.ParentKey().Name(); v != "a" {
		t.Errorf("unexpected: %v", v)
	}

	err = bm.Get(&Data{ParentKey: parentKey, ID: 1})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBoom_TagParentWithNilParent(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupCloudDatastore(t)
	defer cleanUp()

	ctx = context.WithValue(ctx, contextClient{}, client)

	bm := FromClient(ctx, client)

	type Data struct {
		ParentKey datastore.Key `datastore:"-" boom:"parent"`
		ID        int64         `datastore:"-" boom:"id"`
	}

	key, err := bm.Put(&Data{ParentKey: nil, ID: 1})
	if err != nil {
		t.Fatal(err)
	}

	if v := key.ParentKey(); v != nil {
		t.Errorf("unexpected: %v", v)
	}

	err = bm.Get(&Data{ID: 1})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBoom_TagKind(t *testing.T) {
	ctx, client, cleanUp := testutils.SetupCloudDatastore(t)
	defer cleanUp()

	bm := FromClient(ctx, client)

	{
		type Data struct {
			Kind string `datastore:"-" boom:"kind,foo"`
			ID   int64  `datastore:"-" boom:"id"`
		}

		{
			obj := &Data{}

			if v := bm.Kind(obj); v != "foo" {
				t.Errorf("unexpected: %v", v)
			}

			key, err := bm.Put(obj)
			if err != nil {
				t.Fatal(err)
			}
			if v := key.Kind(); v != "foo" {
				t.Errorf("unexpected: %v", v)
			}

			err = bm.Get(obj)
			if err != nil {
				t.Fatal(err)
			}
			if v := bm.Kind(obj); v != "foo" {
				t.Errorf("unexpected: %v", v)
			}
		}
		{
			obj := &Data{Kind: "BAR"}

			if v := bm.Kind(obj); v != "BAR" {
				t.Errorf("unexpected: %v", v)
			}

			key, err := bm.Put(obj)
			if err != nil {
				t.Fatal(err)
			}
			if v := key.Kind(); v != "BAR" {
				t.Errorf("unexpected: %v", v)
			}

			err = bm.Get(obj)
			if err != nil {
				t.Fatal(err)
			}
			if v := bm.Kind(obj); v != "BAR" {
				t.Errorf("unexpected: %v", v)
			}
		}
	}
	{
		type Data struct {
			Kind string `datastore:"-" boom:"kind"`
			ID   int64  `datastore:"-" boom:"id"`
		}

		{
			obj := &Data{}

			if v := bm.Kind(obj); v != "Data" {
				t.Errorf("unexpected: %v", v)
			}

			key, err := bm.Put(obj)
			if err != nil {
				t.Fatal(err)
			}
			if v := key.Kind(); v != "Data" {
				t.Errorf("unexpected: %v", v)
			}

			err = bm.Get(obj)
			if err != nil {
				t.Fatal(err)
			}
			if v := bm.Kind(obj); v != "Data" {
				t.Errorf("unexpected: %v", v)
			}
		}
		{
			obj := &Data{Kind: "BAR"}

			if v := bm.Kind(obj); v != "BAR" {
				t.Errorf("unexpected: %v", v)
			}

			key, err := bm.Put(obj)
			if err != nil {
				t.Fatal(err)
			}
			if v := key.Kind(); v != "BAR" {
				t.Errorf("unexpected: %v", v)
			}

			err = bm.Get(obj)
			if err != nil {
				t.Fatal(err)
			}
			if v := bm.Kind(obj); v != "BAR" {
				t.Errorf("unexpected: %v", v)
			}
		}
	}
}
