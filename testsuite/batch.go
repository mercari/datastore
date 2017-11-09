package testsuite

import (
	"context"
	"testing"

	"go.mercari.io/datastore"
	"golang.org/x/sync/errgroup"
)

func Batch_Put(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type Data struct {
		Str string
	}

	b := client.Batch()
	eg := &errgroup.Group{}
	{ // 1st entity
		key := client.IncompleteKey("Data", nil)
		c := b.Put(key, &Data{"Hi!"})
		eg.Go(func() error {
			key, err := b.UnwrapPutResult(<-c)
			if err != nil {
				return err
			}
			t.Logf("#1: %s", key.String())
			return nil
		})
	}
	{ // 2nd entity
		key := client.IncompleteKey("Data", nil)
		c := b.Put(key, &Data{"Hi!"})
		eg.Go(func() error {
			key, err := b.UnwrapPutResult(<-c)
			if err != nil {
				return err
			}
			t.Logf("#2: %s", key.String())
			return nil
		})
	}

	b.Exec(ctx)

	err := eg.Wait()
	if err != nil {
		t.Fatal(err.Error())
	}
}

func Batch_Get(t *testing.T, ctx context.Context, client datastore.Client) {
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

	b := client.Batch()
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
				t.Errorf("unexpected: %v", v)
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
				t.Errorf("unexpected: %v", v)
			}
			return nil
		})
	}

	b.Exec(ctx)

	err = eg.Wait()
	if err != nil {
		t.Fatal(err.Error())
	}
}

func Batch_Delete(t *testing.T, ctx context.Context, client datastore.Client) {
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

	b := client.Batch()
	eg := &errgroup.Group{}
	{ // 1st entity
		c := b.Delete(key1)
		eg.Go(func() error {
			err := <-c
			if err != nil {
				return err
			}
			t.Logf("#1: %s", key1.String())
			err = client.Get(ctx, key1, &Data{})
			if err != datastore.ErrNoSuchEntity {
				t.Fatal(err)
			}
			return nil
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
			err = client.Get(ctx, key2, &Data{})
			if err != datastore.ErrNoSuchEntity {
				t.Fatal(err)
			}
			return nil
		})
	}

	b.Exec(ctx)

	err = eg.Wait()
	if err != nil {
		t.Fatal(err.Error())
	}
}
