package testsuite

import (
	"context"
	"testing"

	"go.mercari.io/datastore"
)

func geoPointPutAndGet(ctx context.Context, t *testing.T, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	// NOTE *datastore.GeoPoint is not officially supported by Datastore.
	// it convert to *datastore.Entity, but AEDatastore is not supported it.
	type Data struct {
		A datastore.GeoPoint
		B []datastore.GeoPoint
	}

	obj := &Data{
		A: datastore.GeoPoint{Lat: 1.1, Lng: 2.2},
		B: []datastore.GeoPoint{
			{Lat: 5.5, Lng: 6.6},
			{Lat: 7.7, Lng: 8.8},
		},
	}

	key, err := client.Put(ctx, client.IncompleteKey("Data", nil), obj)
	if err != nil {
		t.Fatal(err)
	}

	obj = &Data{}
	err = client.Get(ctx, key, obj)
	if err != nil {
		t.Fatal(err)
	}

	if v := obj.A.Lat; v != 1.1 {
		t.Errorf("unexpected: %v", v)
	}
	if v := obj.A.Lng; v != 2.2 {
		t.Errorf("unexpected: %v", v)
	}

	if v := len(obj.B); v != 2 {
		t.Fatalf("unexpected: %v", v)
	}
	if v := obj.B[0].Lat; v != 5.5 {
		t.Errorf("unexpected: %v", v)
	}
	if v := obj.B[0].Lng; v != 6.6 {
		t.Errorf("unexpected: %v", v)
	}
	if v := obj.B[1].Lat; v != 7.7 {
		t.Errorf("unexpected: %v", v)
	}
	if v := obj.B[1].Lng; v != 8.8 {
		t.Errorf("unexpected: %v", v)
	}
}
