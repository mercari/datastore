package testsuite

import (
	"context"
	"sync"
	"testing"

	"go.mercari.io/datastore"
	"golang.org/x/sync/errgroup"
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
	//   2. errgroup.Group で待ち合わせする
	//   3. PendingKeyをKey化する
	//   4. sync.WaitGroup で待ち合わせする

	b := tx.Batch()
	eg := &errgroup.Group{}
	wg := sync.WaitGroup{}
	cs := make([]chan datastore.Commit, 0)
	{ // 1st entity
		key := client.IncompleteKey("Data", nil)
		c := b.Put(key, &Data{"Hi!"})
		wg.Add(1)
		eg.Go(func() error {
			pKey, err := b.UnwrapPutResult(<-c)
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
		c := b.Put(key, &Data{"Hi!"})
		wg.Add(1)
		eg.Go(func() error {
			pKey, err := b.UnwrapPutResult(<-c)
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

	b.Exec()

	err = eg.Wait()
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
	eg := &errgroup.Group{}
	{ // 1st entity
		keys, err := client.AllocatedIDs(ctx, []datastore.Key{client.IncompleteKey("Data", nil)})
		if err != nil {
			t.Fatal(err.Error())
		}
		key := keys[0]
		c := b.Put(key, &Data{"Hi!"})
		eg.Go(func() error {
			_, err := b.UnwrapPutResult(<-c)
			if err != nil {
				return err
			}
			t.Logf("#1: %s", key.String())
			return nil
		})
	}
	{ // 2nd entity
		keys, err := client.AllocatedIDs(ctx, []datastore.Key{client.IncompleteKey("Data", nil)})
		if err != nil {
			t.Fatal(err.Error())
		}
		key := keys[0]
		c := b.Put(key, &Data{"Hi!"})
		eg.Go(func() error {
			_, err := b.UnwrapPutResult(<-c)
			if err != nil {
				return err
			}
			t.Logf("#2: %s", key.String())
			return nil
		})
	}

	b.Exec()

	err = eg.Wait()
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
	eg := &errgroup.Group{}
	{ // 1st entity
		dst := &Data{}
		c := b.Get(key1, dst)
		eg.Go(func() error {
			err := <-c
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
		c := b.Get(key2, dst)
		eg.Go(func() error {
			err := <-c
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

	b.Exec()

	err = eg.Wait()
	if err != nil {
		t.Fatal(err.Error())
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
	eg := &errgroup.Group{}
	{ // 1st entity
		c := b.Delete(key1)
		eg.Go(func() error {
			err := <-c
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
		c := b.Delete(key2)
		eg.Go(func() error {
			err := <-c
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

	b.Exec()

	err = eg.Wait()
	if err != nil {
		t.Fatal(err.Error())
	}

	err = tx.Rollback()
	if err != nil {
		t.Fatal(err.Error())
	}
}
