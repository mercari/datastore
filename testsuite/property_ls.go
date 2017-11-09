package testsuite

import (
	"context"
	"testing"
	"time"

	"go.mercari.io/datastore"
)

var _ datastore.PropertyLoadSaver = &DataPLS{}
var _ datastore.KeyLoader = &DataKL{}

type DataPLS struct {
	Name      string
	LoadCount int
	CreatedAt time.Time
}

func (d *DataPLS) Load(ctx context.Context, ps []datastore.Property) error {
	err := datastore.LoadStruct(ctx, d, ps)
	if err != nil {
		return err
	}

	d.LoadCount++

	return nil
}

func (d *DataPLS) Save(ctx context.Context) ([]datastore.Property, error) {
	if d.CreatedAt.IsZero() {
		d.CreatedAt = time.Now()
	}

	return datastore.SaveStruct(ctx, d)
}

func PLS_Basic(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	key := client.IncompleteKey("DataPLS", nil)
	obj := &DataPLS{Name: "Test"}
	key, err := client.Put(ctx, key, obj)
	if err != nil {
		t.Fatal(err)
	}

	if v := obj.CreatedAt; v.IsZero() {
		t.Fatalf("unexpected: %v", v)
	}
	if v := obj.LoadCount; v != 0 {
		t.Fatalf("unexpected: %v", v)
	}

	obj = &DataPLS{}
	err = client.Get(ctx, key, obj)
	if err != nil {
		t.Fatal(err)
	}
	if v := obj.CreatedAt; v.IsZero() {
		t.Fatalf("unexpected: %v", v)
	}
	if v := obj.LoadCount; v != 1 {
		t.Fatalf("unexpected: %v", v)
	}
}

type DataKL struct {
	ID   int64 `datastore:"-"`
	Name string
}

func (d *DataKL) LoadKey(ctx context.Context, k datastore.Key) error {
	d.ID = k.ID()

	return nil
}

func (d *DataKL) Load(ctx context.Context, ps []datastore.Property) error {
	return datastore.LoadStruct(ctx, d, ps)
}

func (d *DataKL) Save(ctx context.Context) ([]datastore.Property, error) {
	return datastore.SaveStruct(ctx, d)
}

func KL_Basic(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	key := client.IncompleteKey("DataKL", nil)
	obj := &DataKL{Name: "Test"}
	key, err := client.Put(ctx, key, obj)
	if err != nil {
		t.Fatal(err)
	}

	obj = &DataKL{}
	err = client.Get(ctx, key, obj)
	if err != nil {
		t.Fatal(err)
	}

	if v := obj.ID; v == 0 {
		t.Fatalf("unexpected: %v", v)
	}
}
