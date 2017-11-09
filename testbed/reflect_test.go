package testbed

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"go.mercari.io/datastore"
	cdatastore "go.mercari.io/datastore/clouddatastore"
)

var _ datastore.PropertyTranslator = UserKey(1)

type UserKey int64

func (v UserKey) ToPropertyValue(ctx context.Context) (interface{}, error) {
	client, err := cdatastore.FromContext(ctx)
	if err != nil {
		return datastore.Property{}, err
	}

	key := client.IDKey("User", int64(v), nil)
	return key, nil
}

func (v UserKey) FromPropertyValue(ctx context.Context, p datastore.Property) (dst interface{}, err error) {
	userKey, ok := p.Value.(datastore.Key)
	if !ok {
		return nil, fmt.Errorf("unknown type: %t", p.Value)
	}

	return UserKey(userKey.ID()), nil
}

func TestPropertyTranslaterMockup(t *testing.T) {
	ctx := context.Background()

	userKey := UserKey(100)
	v, err := userKey.ToPropertyValue(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if v, ok := v.(datastore.Key); !ok || v.Kind() != "User" || v.ID() != 100 {
		t.Fatalf("unexpected: %v", v)
	}

	f, ok := reflect.TypeOf(struct{ F UserKey }{}).FieldByName("F")
	if !ok {
		t.Fatalf("unexpected: %v", ok)
	}
	typeOfPropertyTranslater := reflect.TypeOf((*datastore.PropertyTranslator)(nil)).Elem()
	if v := f.Type.AssignableTo(typeOfPropertyTranslater); !v {
		t.Fatalf("unexpected: %v", ok)
	}

	newV := reflect.New(f.Type)
	pt := newV.Elem().Interface().(datastore.PropertyTranslator)
	dst, err := pt.FromPropertyValue(ctx, datastore.Property{Value: v})
	if err != nil {
		t.Fatal(err)
	}

	if v := dst.(UserKey); v != 100 {
		t.Fatalf("unexpected: %v", ok)
	}
}
