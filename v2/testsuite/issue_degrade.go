package testsuite

import (
	"context"
	"testing"

	"go.mercari.io/datastore/v2"
)

func checkIssue59(ctx context.Context, t *testing.T, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	// Issue description
	//   when using Cloud Datastore + storagecache
	//   Get → make cache on storage
	//   Put with Tx → expected: delete cache, actual: do nothing
	//   Get → hit old (unexpected) cache from storage

	type TestEntity struct {
		String string
	}

	key, err := client.Put(ctx, client.IncompleteKey("Test", nil), &TestEntity{String: "Test"})
	if err != nil {
		t.Fatal(err)
	}

	entity := &TestEntity{}
	err = client.Get(ctx, key, entity)
	if err != nil {
		t.Fatal(err)
	}
	if v := entity.String; v != "Test" {
		t.Errorf("unexpected: %v", v)
	}

	_, err = client.RunInTransaction(ctx, func(tx datastore.Transaction) error {
		entity.String = "Updated"
		_, err = tx.Put(key, entity)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	entity = &TestEntity{}
	err = client.Get(ctx, key, entity)
	if err != nil {
		t.Fatal(err)
	}
	if v := entity.String; v != "Updated" {
		t.Errorf("unexpected: %v", v)
	}
}
