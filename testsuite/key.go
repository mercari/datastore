package testsuite

import (
	"context"
	"testing"

	"go.mercari.io/datastore"
)

func Key_Equal(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	specs := []struct {
		A      datastore.Key
		B      datastore.Key
		Result bool
	}{
		{
			client.IncompleteKey("A", nil),
			client.IncompleteKey("A", nil),
			true,
		},
		{
			client.IncompleteKey("A", nil),
			client.IncompleteKey("B", nil),
			false,
		},
		{
			client.IDKey("A", 1, nil),
			client.IDKey("A", 1, nil),
			true,
		},
		{
			client.IDKey("A", 1, nil),
			client.IDKey("A", 2, nil),
			false,
		},
		{
			client.NameKey("A", "a", nil),
			client.NameKey("A", "a", nil),
			true,
		},
		{
			client.NameKey("A", "a", nil),
			client.NameKey("A", "b", nil),
			false,
		},
		{
			client.NameKey("A", "a", client.IDKey("Parent", 1, nil)),
			client.NameKey("A", "a", client.IDKey("Parent", 1, nil)),
			true,
		},
		{
			client.NameKey("A", "a", client.IDKey("Parent", 1, nil)),
			client.NameKey("A", "a", client.IDKey("Parent", 2, nil)),
			false,
		},
		{
			client.NameKey("A", "a", nil),
			client.NameKey("A", "a", client.IDKey("Parent", 1, nil)),
			false,
		},
		{
			client.NameKey("A", "a", client.IDKey("Parent", 1, nil)),
			client.NameKey("A", "a", nil),
			false,
		},
	}

	for idx, spec := range specs {
		if v := spec.A.Equal(spec.B); v != spec.Result {
			t.Errorf("unexpected: #%d %v", idx, v)
		}
	}
}

func Key_Incomplete(t *testing.T, ctx context.Context, client datastore.Client) {
	if v := client.IncompleteKey("A", nil).Incomplete(); !v {
		t.Errorf("unexpected: %v", v)
	}
	if v := client.NameKey("A", "a", nil).Incomplete(); v {
		t.Errorf("unexpected: %v", v)
	}
	if v := client.IDKey("A", 1, nil).Incomplete(); v {
		t.Errorf("unexpected: %v", v)
	}
}

func Key_PutAndGet(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type Data struct {
		Key     datastore.Key
		Keys    []datastore.Key
		NilKey  datastore.Key
		NilKeys []datastore.Key
	}

	obj := &Data{
		Key:     client.IDKey("A", 1, nil),
		Keys:    []datastore.Key{client.IDKey("A", 2, nil), client.IDKey("A", 3, nil)},
		NilKey:  nil,
		NilKeys: []datastore.Key{nil, nil},
	}

	key, err := client.Put(ctx, client.IDKey("Data", 100, nil), obj)
	if err != nil {
		t.Fatal(err)
	}

	obj = &Data{}
	err = client.Get(ctx, key, obj)
	if err != nil {
		t.Fatal(err)
	}

	if v := obj.Key.Kind(); v != "A" {
		t.Errorf("unexpected: %v", v)
	}
	if v := obj.Key.ID(); v != 1 {
		t.Errorf("unexpected: %v", v)
	}

	if v := len(obj.Keys); v != 2 {
		t.Errorf("unexpected: %v", v)
	}
	if v := obj.Keys[0].Kind(); v != "A" {
		t.Errorf("unexpected: %v", v)
	}
	if v := obj.Keys[0].ID(); v != 2 {
		t.Errorf("unexpected: %v", v)
	}
	if v := obj.Keys[1].Kind(); v != "A" {
		t.Errorf("unexpected: %v", v)
	}
	if v := obj.Keys[1].ID(); v != 3 {
		t.Errorf("unexpected: %v", v)
	}

	if v := obj.NilKey; v != nil {
		t.Errorf("unexpected: %v", v)
	}

	if v := len(obj.NilKeys); v != 2 {
		t.Errorf("unexpected: %v", v)
	}
	if v := obj.NilKeys[0]; v != nil {
		t.Errorf("unexpected: %v", v)
	}
	if v := obj.NilKeys[1]; v != nil {
		t.Errorf("unexpected: %v", v)
	}
}
