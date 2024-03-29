package testsuite

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"go.mercari.io/datastore/v2"
)

var _ datastore.PropertyTranslator = (*userID)(nil)
var _ datastore.PropertyTranslator = unixTime(time.Time{})
var _ json.Marshaler = unixTime(time.Time{})
var _ json.Unmarshaler = (*unixTime)(&time.Time{})

type contextClient struct{}

type userID int64
type unixTime time.Time

func (id userID) ToPropertyValue(ctx context.Context) (interface{}, error) {
	client := ctx.Value(contextClient{}).(datastore.Client)
	key := client.IDKey("User", int64(id), nil)
	return key, nil
}

func (id userID) FromPropertyValue(ctx context.Context, p datastore.Property) (dst interface{}, err error) {
	key, ok := p.Value.(datastore.Key)
	if !ok {
		return nil, datastore.ErrInvalidEntityType
	}
	return userID(key.ID()), nil
}

func (t unixTime) ToPropertyValue(ctx context.Context) (interface{}, error) {
	return time.Time(t), nil
}

func (t unixTime) FromPropertyValue(ctx context.Context, p datastore.Property) (dst interface{}, err error) {
	origT, ok := p.Value.(time.Time)
	if !ok {
		return nil, datastore.ErrInvalidEntityType
	}
	return unixTime(origT), nil
}

func (t unixTime) MarshalJSON() ([]byte, error) {
	unix := time.Time(t).UnixNano()
	jsonNumber := json.Number(fmt.Sprintf("%d", unix))
	return json.Marshal(jsonNumber)
}

func (t *unixTime) UnmarshalJSON(b []byte) error {
	var jsonNumber json.Number
	err := json.Unmarshal(b, &jsonNumber)
	if err != nil {
		return err
	}
	unix, err := jsonNumber.Int64()
	if err != nil {
		return err
	}

	l, err := time.LoadLocation("Asia/Tokyo")
	if err != nil {
		return err
	}

	v := time.Unix(0, unix).In(l)
	*t = unixTime(v)
	return nil
}

func propertyTranslaterPutAndGet(ctx context.Context, t *testing.T, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()
	ctx = context.WithValue(ctx, contextClient{}, client)
	client.SetContext(ctx)

	type User struct {
		ID   int64 `datastore:"-"`
		Name string
	}

	type Data struct {
		UserID    userID
		CreatedAt unixTime
	}

	userKey, err := client.Put(ctx, client.IncompleteKey("User", nil), &User{Name: "vvakame"})
	if err != nil {
		t.Fatal(err)
	}

	l, err := time.LoadLocation("Europe/Berlin") // not UTC, not PST, not Asia/Tokyo(developer's local timezone)
	if err != nil {
		t.Fatal(err)
	}
	now := unixTime(time.Date(2017, 11, 2, 10, 11, 22, 33, l))

	key, err := client.Put(ctx, client.IncompleteKey("Data", nil), &Data{
		UserID:    userID(userKey.ID()),
		CreatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}

	obj := &Data{}
	err = client.Get(ctx, key, obj)
	if err != nil {
		t.Fatal(err)
	}

	if v := obj.UserID; int64(v) != userKey.ID() {
		t.Errorf("unexpected: %v", v)
	}
	expectedNow := time.Time(now).Truncate(time.Microsecond).In(time.Local)
	if v := obj.CreatedAt; !time.Time(v).Equal(expectedNow) {
		t.Errorf("unexpected: %v", v)
	}
}
