package testsuite

import (
	"context"
	"errors"
	"sync"
	"testing"

	"go.mercari.io/datastore"
)

func TransactionBatch_Put(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type Data struct {
		Str string
	}

	tx, err := client.NewTransaction(ctx)
	if err != nil {
		t.Fatal(err.Error())
	}

	// ざっくりした流れ
	//   1. TxBatch で2件Putする
	//   2. PendingKeyをKey化する
	//   3. sync.WaitGroup で待ち合わせする

	b := tx.Batch()
	wg := sync.WaitGroup{}
	cs := make([]chan datastore.Commit, 0)
	{ // 1st entity
		key := client.IncompleteKey("Data", nil)
		wg.Add(1)
		b.Put(key, &Data{"Hi!"}, func(pKey datastore.PendingKey, err error) error {
			if err != nil {
				return err
			}
			c := make(chan datastore.Commit)
			cs = append(cs, c)
			go func() {
				commit := <-c
				t.Logf("#1: %s", commit.Key(pKey).String())
				wg.Done()
			}()
			return nil
		})
	}
	{ // 2nd entity
		key := client.IncompleteKey("Data", nil)
		wg.Add(1)
		b.Put(key, &Data{"Hi!"}, func(pKey datastore.PendingKey, err error) error {
			if err != nil {
				return err
			}
			c := make(chan datastore.Commit)
			cs = append(cs, c)
			go func() {
				commit := <-c
				t.Logf("#2: %s", commit.Key(pKey).String())
				wg.Done()
			}()
			return nil
		})
	}

	err = b.Exec()
	if err != nil {
		t.Fatal(err.Error())
	}

	commit, err := tx.Commit()
	if err != nil {
		t.Fatal(err.Error())
	}
	for _, c := range cs {
		c <- commit
	}

	wg.Wait()
}

func TransactionBatch_PutWithCustomErrHandler(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type Data struct {
		Str string
	}

	tx, err := client.NewTransaction(ctx)
	if err != nil {
		t.Fatal(err.Error())
	}

	// ざっくりした流れ
	//   1. TxBatch で2件Putする
	//   2. PendingKeyをKey化する

	b := tx.Batch()
	testErr := errors.New("test")
	{ // 1st entity
		key := client.IncompleteKey("Data", nil)
		b.Put(key, &Data{"Hi!"}, func(pKey datastore.PendingKey, err error) error {
			return testErr
		})
	}
	{ // 2nd entity
		key := client.IncompleteKey("Data", nil)
		b.Put(key, &Data{"Hi!"}, nil)
	}

	err = b.Exec()
	if err == nil {
		t.Fatal(err.Error())
	}

	merr, ok := err.(datastore.MultiError)
	if !ok {
		t.Fatalf("unexpected: %v", ok)
	}
	if v := len(merr); v != 1 {
		t.Fatalf("unexpected: %v", ok)
	}
	if v := merr[0]; v != testErr {
		t.Errorf("unexpected: %v", v)
	}

	_, err = tx.Commit()
	if err != nil {
		t.Fatal(err.Error())
	}
}

func TransactionBatch_PutAndAllocateIDs(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type Data struct {
		Str string
	}

	tx, err := client.NewTransaction(ctx)
	if err != nil {
		t.Fatal(err.Error())
	}

	b := tx.Batch()
	{ // 1st entity
		keys, err := client.AllocateIDs(ctx, []datastore.Key{client.IncompleteKey("Data", nil)})
		if err != nil {
			t.Fatal(err.Error())
		}
		key := keys[0]
		b.Put(key, &Data{"Hi!"}, func(pKey datastore.PendingKey, err error) error {
			if err != nil {
				return err
			}
			t.Logf("#1: %s", key.String())
			return nil
		})
	}
	{ // 2nd entity
		keys, err := client.AllocateIDs(ctx, []datastore.Key{client.IncompleteKey("Data", nil)})
		if err != nil {
			t.Fatal(err.Error())
		}
		key := keys[0]
		b.Put(key, &Data{"Hi!"}, func(pKey datastore.PendingKey, err error) error {
			if err != nil {
				return err
			}
			t.Logf("#2: %s", key.String())
			return nil
		})
	}

	err = b.Exec()
	if err != nil {
		t.Fatal(err.Error())
	}

	_, err = tx.Commit()
	if err != nil {
		t.Fatal(err.Error())
	}
}

func TransactionBatch_Get(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type Data struct {
		Str string
	}

	key1, err := client.Put(ctx, client.IncompleteKey("Data", nil), &Data{"Data 1"})
	if err != nil {
		t.Fatal(err.Error())
	}
	key2, err := client.Put(ctx, client.IncompleteKey("Data", nil), &Data{"Data 2"})
	if err != nil {
		t.Fatal(err.Error())
	}

	tx, err := client.NewTransaction(ctx)
	if err != nil {
		t.Fatal(err.Error())
	}

	b := tx.Batch()
	{ // 1st entity
		dst := &Data{}
		b.Get(key1, dst, func(err error) error {
			if err != nil {
				return err
			}
			t.Logf("#1: %s", dst.Str)
			if v := dst.Str; v != "Data 1" {
				t.Logf("unexpected: %v", v)
			}
			return nil
		})
	}
	{ // 2nd entity
		dst := &Data{}
		b.Get(key2, dst, func(err error) error {
			if err != nil {
				return err
			}
			t.Logf("#2: %s", dst.Str)
			if v := dst.Str; v != "Data 2" {
				t.Logf("unexpected: %v", v)
			}
			return nil
		})
	}

	err = b.Exec()
	if err != nil {
		t.Fatal(err.Error())
	}

	err = tx.Rollback()
	if err != nil {
		t.Fatal(err.Error())
	}
}

func TransactionBatch_GetWithCustomErrHandler(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type Data struct {
		Str string
	}

	key1, err := client.Put(ctx, client.IncompleteKey("Data", nil), &Data{"Data 1"})
	if err != nil {
		t.Fatal(err.Error())
	}
	key2, err := client.Put(ctx, client.IncompleteKey("Data", nil), &Data{"Data 2"})
	if err != nil {
		t.Fatal(err.Error())
	}

	tx, err := client.NewTransaction(ctx)
	if err != nil {
		t.Fatal(err.Error())
	}

	b := tx.Batch()
	testErr := errors.New("test")
	{ // 1st entity
		dst := &Data{}
		b.Get(key1, dst, func(err error) error {
			return testErr
		})
	}
	{ // 2nd entity
		dst := &Data{}
		b.Get(key2, dst, nil)
	}

	err = b.Exec()
	if err == nil {
		t.Fatal(err.Error())
	}

	merr, ok := err.(datastore.MultiError)
	if !ok {
		t.Fatalf("unexpected: %v", ok)
	}
	if v := len(merr); v != 1 {
		t.Fatalf("unexpected: %v", ok)
	}
	if v := merr[0]; v != testErr {
		t.Errorf("unexpected: %v", v)
	}

	err = tx.Rollback()
	if err != nil {
		t.Fatal(err.Error())
	}
}

func TransactionBatch_Delete(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type Data struct {
		Str string
	}

	key1, err := client.Put(ctx, client.IncompleteKey("Data", nil), &Data{"Data 1"})
	if err != nil {
		t.Fatal(err.Error())
	}
	key2, err := client.Put(ctx, client.IncompleteKey("Data", nil), &Data{"Data 2"})
	if err != nil {
		t.Fatal(err.Error())
	}

	tx, err := client.NewTransaction(ctx)
	if err != nil {
		t.Fatal(err.Error())
	}

	b := tx.Batch()
	{ // 1st entity
		b.Delete(key1, func(err error) error {
			if err != nil {
				return err
			}
			t.Logf("#1: %s", key1.String())
			obj := &Data{}
			// we can get entity!
			err = tx.Get(key1, obj)
			return err
		})
	}
	{ // 2nd entity
		b.Delete(key2, func(err error) error {
			if err != nil {
				return err
			}
			t.Logf("#2: %s", key2.String())
			obj := &Data{}
			// we can get entity!
			err = tx.Get(key2, obj)
			return err
		})
	}

	err = b.Exec()
	if err != nil {
		t.Fatal(err.Error())
	}

	err = tx.Rollback()
	if err != nil {
		t.Fatal(err.Error())
	}
}

func TransactionBatch_DeleteWithCustomErrHandler(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type Data struct {
		Str string
	}

	key1, err := client.Put(ctx, client.IncompleteKey("Data", nil), &Data{"Data 1"})
	if err != nil {
		t.Fatal(err.Error())
	}
	key2, err := client.Put(ctx, client.IncompleteKey("Data", nil), &Data{"Data 2"})
	if err != nil {
		t.Fatal(err.Error())
	}

	tx, err := client.NewTransaction(ctx)
	if err != nil {
		t.Fatal(err.Error())
	}

	b := tx.Batch()
	testErr := errors.New("test")
	{ // 1st entity
		b.Delete(key1, func(err error) error {
			return testErr
		})
	}
	{ // 2nd entity
		b.Delete(key2, nil)
	}

	err = b.Exec()
	if err == nil {
		t.Fatal(err.Error())
	}

	merr, ok := err.(datastore.MultiError)
	if !ok {
		t.Fatalf("unexpected: %v", ok)
	}
	if v := len(merr); v != 1 {
		t.Fatalf("unexpected: %v", ok)
	}
	if v := merr[0]; v != testErr {
		t.Errorf("unexpected: %v", v)
	}

	err = tx.Rollback()
	if err != nil {
		t.Fatal(err.Error())
	}
}
