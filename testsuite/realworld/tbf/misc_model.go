package tbf

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.mercari.io/datastore"
)

var _ datastore.PropertyTranslator = UnixTime(time.Time{})
var _ json.Marshaler = UnixTime(time.Time{})
var _ json.Unmarshaler = (*UnixTime)(&time.Time{})

type UnixTime time.Time

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
	unix := time.Time(t).UnixNano() / 1000000 // to JavaScript unix epoch
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

	v := time.Unix(unix*1000000, 0).In(l)
	*t = UnixTime(v)
	return nil
}
