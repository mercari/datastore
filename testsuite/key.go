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
	if v := client.IncompleteKey("A", nil).Incomplete(); v != true {
		t.Errorf("unexpected: %v", v)
	}
	if v := client.NameKey("A", "a", nil).Incomplete(); v != false {
		t.Errorf("unexpected: %v", v)
	}
	if v := client.IDKey("A", 1, nil).Incomplete(); v != false {
		t.Errorf("unexpected: %v", v)
	}
}
