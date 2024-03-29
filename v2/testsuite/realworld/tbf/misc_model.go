package tbf

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.mercari.io/datastore/v2"
)

var _ datastore.PropertyTranslator = unixTime(time.Time{})
var _ json.Marshaler = unixTime(time.Time{})
var _ json.Unmarshaler = (*unixTime)(&time.Time{})

type unixTime time.Time

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
	unix := time.Time(t).UnixNano() / 1000000 // to JavaScript unix epoch
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

	v := time.Unix(unix*1000000, 0).In(l)
	*t = unixTime(v)
	return nil
}
