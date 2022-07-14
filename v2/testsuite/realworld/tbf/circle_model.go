package tbf

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mercari.io/datastore/v2"
)

var _ datastore.PropertyTranslator = CircleID(0)
var _ datastore.KeyLoader = &Circle{}

const kindCircle = "Circle"

// CircleID means ID of Circle kind.
type CircleID int64

// Circle represents information on participating organizations.
type Circle struct {
	ID        CircleID  `json:"id" datastore:"-"`
	Name      string    `json:"name"`
	ImageIDs  []imageID `json:"-"`
	Images    []*Image  `json:"images" datastore:"-"`
	CreatedAt unixTime  `json:"createdAt"`
	UpdatedAt unixTime  `json:"updatedAt"`
}

func keyToCircleID(key datastore.Key) (CircleID, error) {
	if key.Kind() != kindCircle {
		return 0, fmt.Errorf("unexpected kind: %s", key.Kind())
	}

	return CircleID(key.ID()), nil
}

// ToPropertyValue convert the value to the valid value as the property of datastore.
func (id CircleID) ToPropertyValue(ctx context.Context) (interface{}, error) {
	client := ctx.Value(contextClient{}).(datastore.Client)
	key := client.IDKey(kindCircle, int64(id), nil)
	return key, nil
}

// FromPropertyValue convert property value to the valid value as the application.
func (id CircleID) FromPropertyValue(ctx context.Context, p datastore.Property) (dst interface{}, err error) {
	key, ok := p.Value.(datastore.Key)
	if !ok {
		return nil, datastore.ErrInvalidEntityType
	}
	return keyToCircleID(key)
}

// ToKey convert the value to datastore.Key.
func (id CircleID) ToKey(client datastore.Client) datastore.Key {
	return client.IDKey(kindCircle, int64(id), nil)
}

// Load loads all of the provided properties into struct.
func (circle *Circle) Load(ctx context.Context, ps []datastore.Property) error {
	err := datastore.LoadStruct(ctx, circle, ps)
	if err != nil {
		return err
	}

	// 子画像をBatchGetしていく

	batch, ok := ctx.Value(contextBatch{}).(*datastore.Batch)
	if !ok || batch == nil {
		return errors.New("can't pickup batch client")
	}

	client := ctx.Value(contextClient{}).(datastore.Client)

	circle.Images = make([]*Image, len(circle.ImageIDs))
	for idx, imageID := range circle.ImageIDs {
		imageKey := imageID.ToKey(client)

		image := &Image{}
		circle.Images[idx] = image
		batch.Get(imageKey, image, nil)
	}

	return nil
}

// Save saves all of struct fields to properties.
func (circle *Circle) Save(ctx context.Context) ([]datastore.Property, error) {
	if time.Time(circle.CreatedAt).IsZero() {
		circle.CreatedAt = unixTime(timeNow())
	}
	circle.UpdatedAt = unixTime(timeNow())

	return datastore.SaveStruct(ctx, circle)
}

// LoadKey loads key data into struct.
func (circle *Circle) LoadKey(ctx context.Context, k datastore.Key) error {
	circle.ID = CircleID(k.ID())
	return nil
}
