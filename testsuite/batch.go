package testsuite

import (
	"context"
	"errors"
	"testing"

	"go.mercari.io/datastore"
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

	cnt := 0
	b := client.Batch()
	{ // 1st entity
		key := client.IncompleteKey("Data", nil)
		b.Put(key, &Data{"Hi!"}, func(key datastore.Key, err error) error {
			if err != nil {
				return err
			}
			t.Logf("#1: %s", key.String())
			cnt++
			return nil
		})
	}
	{ // 2nd entity
		key := client.IncompleteKey("Data", nil)
		b.Put(key, &Data{"Hi!"}, func(key datastore.Key, err error) error {
			if err != nil {
				return err
			}
			t.Logf("#2: %s", key.String())
			cnt++
			return nil
		})
	}

	err := b.Exec(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if cnt != 2 {
		t.Errorf("unexpected: %v", cnt)
	}
}

func Batch_PutWithCustomErrHandler(t *testing.T, ctx context.Context, client datastore.Client) {
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
	testErr := errors.New("test")
	{ // 1st entity
		key := client.IncompleteKey("Data", nil)
		b.Put(key, &Data{"Hi!"}, func(key datastore.Key, err error) error {
			return testErr
		})
	}
	{ // 2nd entity
		key := client.IncompleteKey("Data", nil)
		b.Put(key, &Data{"Hi!"}, nil)
	}

	err := b.Exec(ctx)
	if err == nil {
		t.Fatal(err)
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

	cnt := 0
	b := client.Batch()
	{ // 1st entity
		dst := &Data{}
		b.Get(key1, dst, func(err error) error {
			if err != nil {
				return err
			}
			t.Logf("#1: %s", dst.Str)
			if v := dst.Str; v != "Data 1" {
				t.Errorf("unexpected: %v", v)
			}
			cnt++
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
				t.Errorf("unexpected: %v", v)
			}
			cnt++
			return nil
		})
	}

	err = b.Exec(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if cnt != 2 {
		t.Errorf("unexpected: %v", cnt)
	}
}

func Batch_GetWithCustomErrHandler(t *testing.T, ctx context.Context, client datastore.Client) {
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

	err = b.Exec(ctx)
	if err == nil {
		t.Fatal(err)
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

	cnt := 0
	b := client.Batch()
	{ // 1st entity
		b.Delete(key1, func(err error) error {
			if err != nil {
				return err
			}
			t.Logf("#1: %s", key1.String())
			err = client.Get(ctx, key1, &Data{})
			if err != datastore.ErrNoSuchEntity {
				t.Fatal(err)
			}
			cnt++
			return nil
		})
	}
	{ // 2nd entity
		b.Delete(key2, func(err error) error {
			if err != nil {
				return err
			}
			t.Logf("#2: %s", key2.String())
			err = client.Get(ctx, key2, &Data{})
			if err != datastore.ErrNoSuchEntity {
				t.Fatal(err)
			}
			cnt++
			return nil
		})
	}

	err = b.Exec(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if cnt != 2 {
		t.Errorf("unexpected: %v", cnt)
	}
}

func Batch_DeleteWithCustomErrHandler(t *testing.T, ctx context.Context, client datastore.Client) {
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
	testErr := errors.New("test")
	{ // 1st entity
		b.Delete(key1, func(err error) error {
			return testErr
		})
	}
	{ // 2nd entity
		b.Delete(key2, nil)
	}

	err = b.Exec(ctx)
	if err == nil {
		t.Fatal(err)
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
}
