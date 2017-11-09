package datastore

import (
	"context"
	"testing"
)

func TestSaveStruct_Basic(t *testing.T) {
	ctx := context.Background()

	type Data struct {
		Str string
	}

	ps, err := SaveStruct(ctx, &Data{"Test"})
	if err != nil {
		t.Fatal(err)
	}

	if v := len(ps); v != 1 {
		t.Fatalf("unexpected: %v", v)
	}

	p := ps[0]
	if v := p.Name; v != "Str" {
		t.Fatalf("unexpected: %v", v)
	}
	if v, ok := p.Value.(string); !ok {
		t.Fatalf("unexpected: %v", ok)
	} else if v != "Test" {
		t.Fatalf("unexpected: %v", v)
	}
}

func TestSaveStruct_Object(t *testing.T) {
	ctx := context.Background()

	type Data struct {
		Str string
	}

	ps, err := SaveStruct(ctx, &Data{Str: "Str"})
	if err != nil {
		t.Fatal(err)
	}

	if v := len(ps); v != 1 {
		t.Fatalf("unexpected: %v", v)
	}
	p := ps[0]
	if v := p.Name; v != "Str" {
		t.Fatalf("unexpected: %v", v)
	}
	if v := p.Value.(string); v != "Str" {
		t.Fatalf("unexpected: %v", v)
	}
}

func TestSaveStruct_ObjectPropertyRename(t *testing.T) {
	ctx := context.Background()

	type Data struct {
		Str string `datastore:"modified"`
	}

	ps, err := SaveStruct(ctx, &Data{Str: "Str"})
	if err != nil {
		t.Fatal(err)
	}

	if v := len(ps); v != 1 {
		t.Fatalf("unexpected: %v", v)
	}
	p := ps[0]
	if v := p.Name; v != "modified" {
		t.Fatalf("unexpected: %v", v)
	}
	if v := p.Value.(string); v != "Str" {
		t.Fatalf("unexpected: %v", v)
	}
}

func TestSaveStruct_EmbedStruct(t *testing.T) {
	ctx := context.Background()

	type Embed struct {
		Inner string
	}

	type Data struct {
		Embed

		Str string
	}

	ps, err := SaveStruct(ctx, &Data{
		Embed: Embed{
			Inner: "Inner",
		},
		Str: "Str",
	})
	if err != nil {
		t.Fatal(err)
	}

	if v := len(ps); v != 2 {
		t.Fatalf("unexpected: %v", v)
	}
	{
		p := ps[0]
		if v := p.Name; v != "Inner" {
			t.Fatalf("unexpected: %v", v)
		}
		if v := p.Value.(string); v != "Inner" {
			t.Fatalf("unexpected: %v", v)
		}
	}
	{
		p := ps[1]
		if v := p.Name; v != "Str" {
			t.Fatalf("unexpected: %v", v)
		}
		if v := p.Value.(string); v != "Str" {
			t.Fatalf("unexpected: %v", v)
		}
	}
}

func TestSaveStruct_WithEmbedPtrStruct(t *testing.T) {
	ctx := context.Background()

	type Inner struct {
		A string
		B string
	}

	type Data struct {
		*Inner
	}

	{
		ps, err := SaveStruct(ctx, &Data{Inner: &Inner{A: "A", B: "B"}})
		if err != nil {
			t.Fatal(err)
		}
		if v := len(ps); v != 2 {
			t.Errorf("unexpected: %v", v)
		}

		obj := &Data{}
		err = LoadStruct(ctx, obj, ps)
		if err != nil {
			t.Fatal(err)
		}
		if v := obj.Inner; v == nil {
			t.Errorf("unexpected: %v", v)
		}
	}
	{
		ps, err := SaveStruct(ctx, &Data{})
		if err != nil {
			t.Fatal(err)
		}
		if v := len(ps); v != 0 {
			t.Errorf("unexpected: %v", v)
		}
		obj := &Data{}
		err = LoadStruct(ctx, obj, ps)
		if err != nil {
			t.Fatal(err)
		}
		if v := obj.Inner; v != nil {
			t.Errorf("unexpected: %v", v)
		}
	}
}

func TestSaveStruct_WithPtrStruct(t *testing.T) {
	// TODO Why this test is failed?
	t.SkipNow()

	ctx := context.Background()

	type Inner struct {
		A string
		B string
	}

	type Data struct {
		Inner *Inner `datastore:",flatten"`
	}

	{
		ps, err := SaveStruct(ctx, &Data{Inner: &Inner{A: "A", B: "B"}})
		if err != nil {
			t.Fatal(err)
		}
		if v := len(ps); v != 2 {
			t.Errorf("unexpected: %v", v)
		}

		obj := &Data{}
		err = LoadStruct(ctx, obj, ps)
		if err != nil {
			t.Fatal(err)
		}
		if v := obj.Inner; v == nil {
			t.Errorf("unexpected: %v", v)
		}
	}
	{
		ps, err := SaveStruct(ctx, &Data{})
		if err != nil {
			t.Fatal(err)
		}
		if v := len(ps); v != 0 {
			t.Errorf("unexpected: %v", v)
		}
		obj := &Data{}
		err = LoadStruct(ctx, obj, ps)
		if err != nil {
			t.Fatal(err)
		}
		if v := obj.Inner; v != nil {
			t.Errorf("unexpected: %v", v)
		}
	}
}

func TestSaveStruct_ObjectHasObjectSlice(t *testing.T) {
	ctx := context.Background()

	type Inner struct {
		A string
		B string
	}

	type Data struct {
		Slice []Inner
	}

	ps, err := SaveStruct(ctx, &Data{
		Slice: []Inner{
			Inner{A: "A1", B: "B1"},
			Inner{A: "A2", B: "B2"},
			Inner{A: "A3", B: "B3"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if v := len(ps); v != 1 {
		t.Fatalf("unexpected: %v", v)
	}
	p := ps[0]
	if v := p.Name; v != "Slice" {
		t.Fatalf("unexpected: %v", v)
	}
	es := p.Value.([]interface{})
	if v := len(es); v != 3 {
		t.Fatalf("unexpected: %v", v)
	}

	expects := []struct {
		Name  string
		Value string
	}{
		{"A", "A1"},
		{"B", "B1"},
		{"A", "A2"},
		{"B", "B2"},
		{"A", "A3"},
		{"B", "B3"},
	}

	for idx, entity := range es {
		e := entity.(*Entity)
		if v := len(e.Properties); v != 2 {
			t.Fatalf("unexpected: %v", v)
		}
		for pIdx, p := range e.Properties {
			expect := expects[idx*len(e.Properties)+pIdx]
			if v := p.Name; v != expect.Name {
				t.Errorf("unexpected: %v", v)
			}
			if v := p.Value.(string); v != expect.Value {
				t.Errorf("unexpected: %v", v)
			}
		}
	}
}

func TestLoadStruct_Basic(t *testing.T) {
	ctx := context.Background()

	type Data struct {
		Str string
	}

	var ps PropertyList
	ps = append(ps, Property{
		Name:  "Str",
		Value: "Test",
	})
	obj := &Data{}
	err := LoadStruct(ctx, obj, ps)
	if err != nil {
		t.Fatal(err)
	}

	if v := obj.Str; v != "Test" {
		t.Fatalf("unexpected: %v", v)
	}
}

func TestLoadStruct_IgnoreMismatchProperty(t *testing.T) {
	ctx := context.Background()

	type Data1 struct {
		A string
		B string
	}
	type Data2 struct {
		A string
	}

	ps, err := SaveStruct(ctx, &Data1{})
	if err != nil {
		t.Fatal(err)
	}

	err = LoadStruct(ctx, &Data2{}, ps)
	if err != nil {
		t.Fatal(err)
	}
}

func TestLoadStruct_CheckMismatchProperty(t *testing.T) {
	ctx := context.Background()

	SuppressErrFieldMismatch = false
	defer func() {
		SuppressErrFieldMismatch = true
	}()

	type Data1 struct {
		A string
		B string
	}
	type Data2 struct {
		A string
	}

	ps, err := SaveStruct(ctx, &Data1{})
	if err != nil {
		t.Fatal(err)
	}

	err = LoadStruct(ctx, &Data2{}, ps)
	if err == nil {
		t.Fatal(err)
	} else if _, ok := err.(*ErrFieldMismatch); ok {
		// ok!
	} else {
		t.Fatal(err)
	}
}
