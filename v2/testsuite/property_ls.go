package testsuite

import (
	"context"
	"testing"
	"time"

	"go.mercari.io/datastore/v2"
)

var _ datastore.PropertyLoadSaver = &dataPLS{}
var _ datastore.KeyLoader = &dataKL{}

type dataPLS struct {
	Name      string
	LoadCount int
	CreatedAt time.Time
}

func (d *dataPLS) Load(ctx context.Context, ps []datastore.Property) error {
	err := datastore.LoadStruct(ctx, d, ps)
	if err != nil {
		return err
	}

	d.LoadCount++

	return nil
}

func (d *dataPLS) Save(ctx context.Context) ([]datastore.Property, error) {
	if d.CreatedAt.IsZero() {
		d.CreatedAt = time.Now()
	}

	return datastore.SaveStruct(ctx, d)
}

func plsBasic(ctx context.Context, t *testing.T, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	key := client.IncompleteKey("DataPLS", nil)
	obj := &dataPLS{Name: "Test"}
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

	obj = &dataPLS{}
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

type dataKL struct {
	ID   int64 `datastore:"-"`
	Name string
}

func (d *dataKL) LoadKey(ctx context.Context, k datastore.Key) error {
	d.ID = k.ID()

	return nil
}

func (d *dataKL) Load(ctx context.Context, ps []datastore.Property) error {
	return datastore.LoadStruct(ctx, d, ps)
}

func (d *dataKL) Save(ctx context.Context) ([]datastore.Property, error) {
	return datastore.SaveStruct(ctx, d)
}

func klBasic(ctx context.Context, t *testing.T, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	key := client.IncompleteKey("DataKL", nil)
	obj := &dataKL{Name: "Test"}
	key, err := client.Put(ctx, key, obj)
	if err != nil {
		t.Fatal(err)
	}

	obj = &dataKL{}
	err = client.Get(ctx, key, obj)
	if err != nil {
		t.Fatal(err)
	}

	if v := obj.ID; v == 0 {
		t.Fatalf("unexpected: %v", v)
	}
}
