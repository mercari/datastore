package testsuite

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"go.mercari.io/datastore"
)

var _ datastore.PropertyTranslator = (*UserID)(nil)
var _ datastore.PropertyTranslator = UnixTime(time.Time{})
var _ json.Marshaler = UnixTime(time.Time{})
var _ json.Unmarshaler = (*UnixTime)(&time.Time{})

type UserID int64
type UnixTime time.Time

func (id UserID) ToPropertyValue(ctx context.Context) (interface{}, error) {
	client, err := datastore.FromContext(ctx)
	if err != nil {
		return nil, err
	}
	key := client.IDKey("User", int64(id), nil)
	return key, nil
}

func (id UserID) FromPropertyValue(ctx context.Context, p datastore.Property) (dst interface{}, err error) {
	key, ok := p.Value.(datastore.Key)
	if !ok {
		return nil, datastore.ErrInvalidEntityType
	}
	return UserID(key.ID()), nil
}

func (t UnixTime) ToPropertyValue(ctx context.Context) (interface{}, error) {
	return time.Time(t), nil
}

func (t UnixTime) FromPropertyValue(ctx context.Context, p datastore.Property) (dst interface{}, err error) {
	origT, ok := p.Value.(time.Time)
	if !ok {
		return nil, datastore.ErrInvalidEntityType
	}
	return UnixTime(origT), nil
}

func (t UnixTime) MarshalJSON() ([]byte, error) {
	unix := time.Time(t).UnixNano()
	jsonNumber := json.Number(fmt.Sprintf("%d", unix))
	return json.Marshal(jsonNumber)
}

func (t *UnixTime) UnmarshalJSON(b []byte) error {
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
	*t = UnixTime(v)
	return nil
}

func PropertyTranslater_PutAndGet(t *testing.T, ctx context.Context, client datastore.Client) {
	defer func() {
		err := client.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	type User struct {
		ID   int64 `datastore:"-"`
		Name string
	}

	type Data struct {
		UserID    UserID
		CreatedAt UnixTime
	}

	userKey, err := client.Put(ctx, client.IncompleteKey("User", nil), &User{Name: "vvakame"})
	if err != nil {
		t.Fatal(err)
	}

	l, err := time.LoadLocation("Europe/Berlin") // not UTC, not PST, not Asia/Tokyo(developer's local timezone)
	if err != nil {
		t.Fatal(err)
	}
	now := UnixTime(time.Date(2017, 11, 2, 10, 11, 22, 33, l))

	key, err := client.Put(ctx, client.IncompleteKey("Data", nil), &Data{
		UserID:    UserID(userKey.ID()),
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
