package testsuite

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/gob"
	"testing"

	"go.mercari.io/datastore"
)

func GobDecode(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type Sub struct {
		S string
	}

	type Data struct {
		A string
		B int
		C datastore.GeoPoint
		D *Sub
		E datastore.Key
	}

	var b64 string
	if false {
		var ps datastore.PropertyList
		var err error
		ps, err = datastore.SaveStruct(ctx, &Data{
			A: "A",
			B: 2,
			C: datastore.GeoPoint{Lat: 1.1, Lng: 2.2},
			D: &Sub{S: "S"},
			E: client.IDKey("Test", 111, nil),
		})
		if err != nil {
			t.Fatal(err)
		}

		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		err = enc.Encode(ps)
		if err != nil {
			t.Fatal(err)
		}

		b64 = base64.StdEncoding.EncodeToString(buf.Bytes())
		t.Log(b64)
	} else {
		if IsCloudDatastoreClient(ctx) {
			b64 = `G/+DAgEBDFByb3BlcnR5TGlzdAH/hAAB/4IAADX/gQMBAQhQcm9wZXJ0eQH/ggABAwEETmFtZQEMAAEFVmFsdWUBEAABB05vSW5kZXgBAgAAAG//hAAFAQFBAQZzdHJpbmcMAwABQQABAUIBBWludDY0BAIABAABAUMBIGdvLm1lcmNhcmkuaW8vZGF0YXN0b3JlLkdlb1BvaW50/4UDAQEIR2VvUG9pbnQB/4YAAQIBA0xhdAEIAAEDTG5nAQgAAABc/4YVAfiamZmZmZnxPwH4mpmZmZmZAUAAAAEBRAERKmRhdGFzdG9yZS5FbnRpdHn/hwMBAQZFbnRpdHkB/4gAAQIBA0tleQH/igABClByb3BlcnRpZXMB/4wAAAAP/4kFAQEDS2V5Af+KAAAAI/+LAgEBFFtdZGF0YXN0b3JlLlByb3BlcnR5Af+MAAH/ggAAPv+IFAIBAQFTAQZzdHJpbmcMAwABUwAAAAEBRQEXKmNsb3VkZGF0YXN0b3JlLmtleUltcGz/jQUBAv+QAAAAbP+OaABmWP+RAwEBBmdvYktleQH/kgABBgEES2luZAEMAAEIU3RyaW5nSUQBDAABBUludElEAQQAAQZQYXJlbnQB/5IAAQVBcHBJRAEMAAEJTmFtZXNwYWNlAQwAAAAM/5IBBFRlc3QC/94AAA==`
		} else if IsAEDatastoreClient(ctx) {
			b64 = `G/+DAgEBDFByb3BlcnR5TGlzdAH/hAAB/4IAADX/gQMBAQhQcm9wZXJ0eQH/ggABAwEETmFtZQEMAAEFVmFsdWUBEAABB05vSW5kZXgBAgAAAG//hAAFAQFBAQZzdHJpbmcMAwABQQABAUIBBWludDY0BAIABAABAUMBIGdvLm1lcmNhcmkuaW8vZGF0YXN0b3JlLkdlb1BvaW50/4UDAQEIR2VvUG9pbnQB/4YAAQIBA0xhdAEIAAEDTG5nAQgAAABc/4YVAfiamZmZmZnxPwH4mpmZmZmZAUAAAAEBRAERKmRhdGFzdG9yZS5FbnRpdHn/hwMBAQZFbnRpdHkB/4gAAQIBA0tleQH/igABClByb3BlcnRpZXMB/4wAAAAP/4kFAQEDS2V5Af+KAAAAI/+LAgEBFFtdZGF0YXN0b3JlLlByb3BlcnR5Af+MAAH/ggAAO/+IFAIBAQFTAQZzdHJpbmcMAwABUwAAAAEBRQEUKmFlZGF0YXN0b3JlLmtleUltcGz/jQUBAv+QAAAAef+OdQBzWP+RAwEBBmdvYktleQH/kgABBgEES2luZAEMAAEIU3RyaW5nSUQBDAABBUludElEAQQAAQZQYXJlbnQB/5IAAQVBcHBJRAEMAAEJTmFtZXNwYWNlAQwAAAAZ/5IBBFRlc3QC/94CC2Rldn50ZXN0YXBwAAA=`
		} else {
			t.Fatal("unexpected state")
		}
	}

	b, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		t.Fatal(err)
	}

	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	var ps datastore.PropertyList
	err = dec.Decode(&ps)
	if err != nil {
		t.Fatal(err)
	}

	obj := &Data{}
	err = datastore.LoadStruct(ctx, obj, ps)
	if err != nil {
		t.Fatal(err)
	}

	if v := obj.A; v != "A" {
		t.Errorf("unexpected: %v", v)
	}
	if v := obj.B; v != 2 {
		t.Errorf("unexpected: %v", v)
	}
	if v := obj.C.Lat; v != 1.1 {
		t.Errorf("unexpected: %v", v)
	}
	if v := obj.C.Lng; v != 2.2 {
		t.Errorf("unexpected: %v", v)
	}
	if v := obj.D.S; v != "S" {
		t.Errorf("unexpected: %v", v)
	}
	if v := obj.E.Kind(); v != "Test" {
		t.Errorf("unexpected: %v", v)
	}
	if v := obj.E.ID(); v != 111 {
		t.Errorf("unexpected: %v", v)
	}
}
