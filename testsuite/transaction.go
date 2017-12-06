package testsuite

import (
	"context"
	"errors"
	"testing"

	"go.mercari.io/datastore"
)

func Transaction_Commit(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type Data struct {
		Str string
	}

	var key datastore.Key
	{ // Put
		tx, err := client.NewTransaction(ctx)
		if err != nil {
			t.Fatal(err)
		}

		key = client.IncompleteKey("Data", nil)
		pK, err := tx.Put(key, &Data{"Hi!"})
		if err != nil {
			t.Fatal(err)
		}

		c, err := tx.Commit()
		if err != nil {
			t.Fatal(err)
		}

		key = c.Key(pK)
		if v := key.ID(); v == 0 {
			t.Errorf("unexpected: %v", v)
		}
	}
	{ // Get
		tx, err := client.NewTransaction(ctx)
		if err != nil {
			t.Fatal(err)
		}

		obj := &Data{}
		err = tx.Get(key, obj)
		if err != nil {
			t.Fatal(err)
		}

		_, err = tx.Commit()
		if err != nil {
			t.Fatal(err)
		}
	}
	{ // Delete
		tx, err := client.NewTransaction(ctx)
		if err != nil {
			t.Fatal(err)
		}

		err = tx.Delete(key)
		if err != nil {
			t.Fatal(err)
		}

		_, err = tx.Commit()
		if err != nil {
			t.Fatal(err)
		}

		err = client.Get(ctx, key, &Data{})
		if err != datastore.ErrNoSuchEntity {
			t.Errorf("unexpected: %v", err)
		}
	}
}

func Transaction_Rollback(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type Data struct {
		Str string
	}

	key := client.NameKey("Data", "test", nil)

	{ // Put
		tx, err := client.NewTransaction(ctx)
		if err != nil {
			t.Fatal(err)
		}

		_, err = tx.Put(key, &Data{"Hi!"})
		if err != nil {
			t.Fatal(err)
		}

		err = tx.Rollback()
		if err != nil {
			t.Fatal(err)
		}

		err = client.Get(ctx, key, &Data{})
		if err != datastore.ErrNoSuchEntity {
			t.Errorf("unexpected: %v", err)
		}
	}
	{ // Delete
		_, err := client.Put(ctx, key, &Data{"Hi!"})
		if err != nil {
			t.Fatal(err)
		}

		tx, err := client.NewTransaction(ctx)
		if err != nil {
			t.Fatal(err)
		}

		err = tx.Delete(key)
		if err != nil {
			t.Fatal(err)
		}

		err = tx.Rollback()
		if err != nil {
			t.Fatal(err)
		}

		err = client.Get(ctx, key, &Data{})
		if err != nil {
			t.Errorf("unexpected: %v", err)
		}
	}
}

func Transaction_JoinAncesterQuery(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type Data struct {
		Str string
	}

	parentKey := client.NameKey("Parent", "p", nil)
	key := client.NameKey("Data", "d", parentKey)

	_, err := client.Put(ctx, key, &Data{Str: "Test"})
	if err != nil {
		t.Fatal(err)
	}

	tx1, err := client.NewTransaction(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tx2, err := client.NewTransaction(ctx)
	if err != nil {
		t.Fatal(err)
	}

	q := client.NewQuery("Data").Transaction(tx1).Ancestor(parentKey)
	var list1 []*Data
	_, err = client.GetAll(ctx, q, &list1)
	if err != nil {
		t.Fatal(err)
	}
	if v := len(list1); v != 1 {
		t.Fatalf("unexpected: %v", err)
	}
	obj1 := list1[0]

	obj2 := &Data{}
	err = tx2.Get(key, obj2)
	if err != nil {
		t.Fatal(err)
	}

	obj1.Str = "Test1"
	obj2.Str = "Test2"

	_, err = tx1.Put(key, obj1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx2.Put(key, obj2)
	if err != nil {
		t.Fatal(err)
	}

	_, err = tx2.Commit()
	if err != nil {
		t.Fatal(err)
	}

	_, err = tx1.Commit()
	if err != datastore.ErrConcurrentTransaction {
		t.Fatal(err)
	}
}

func RunInTransaction_Commit(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type Data struct {
		Str string
	}

	var pK datastore.PendingKey
	c, err := client.RunInTransaction(ctx, func(tx datastore.Transaction) error {
		key := client.IncompleteKey("Data", nil)
		var err error
		pK, err = tx.Put(key, &Data{"Hi!"})
		if err != nil {
			t.Fatal(err)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	key := c.Key(pK)
	if v := key.ID(); v == 0 {
		t.Errorf("unexpected: %v", v)
	}
}

func RunInTransaction_Rollback(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type Data struct {
		Str string
	}

	_, err := client.RunInTransaction(ctx, func(tx datastore.Transaction) error {
		key := client.IncompleteKey("Data", nil)
		_, err := tx.Put(key, &Data{"Hi!"})
		if err != nil {
			t.Fatal(err)
		}

		return errors.New("This tx should failure")
	})
	if err == nil {
		t.Fatal(err)
	}
}
