package tbf

import (
	"context"
	"fmt"
	"time"

	"go.mercari.io/datastore/v2"
)

var _ datastore.PropertyTranslator = imageID(0)
var _ datastore.KeyLoader = &Image{}

const kindImage = "Image"

type imageID int64

// Image provides information about circle image on GCS.
type Image struct {
	ID            imageID  `json:"id" datastore:"-"`
	OwnerCircleID CircleID `json:"ownerCircleID"`
	GCSPath       string   `json:"gcsPath"`
	CreatedAt     unixTime `json:"createdAt"`
	UpdatedAt     unixTime `json:"updatedAt"`
}

func keyToImageID(key datastore.Key) (imageID, error) {
	if key.Kind() != kindImage {
		return 0, fmt.Errorf("unexpected kind: %s", key.Kind())
	}

	return imageID(key.ID()), nil
}

// ToPropertyValue convert the value to the valid value as the property of datastore.
func (id imageID) ToPropertyValue(ctx context.Context) (interface{}, error) {
	client := ctx.Value(contextClient{}).(datastore.Client)
	key := client.IDKey(kindImage, int64(id), nil)
	return key, nil
}

// FromPropertyValue convert property value to the valid value as the application.
func (id imageID) FromPropertyValue(ctx context.Context, p datastore.Property) (dst interface{}, err error) {
	key, ok := p.Value.(datastore.Key)
	if !ok {
		return nil, datastore.ErrInvalidEntityType
	}
	return keyToImageID(key)
}

// ToKey convert the value to datastore.Key.
func (id imageID) ToKey(client datastore.Client) datastore.Key {
	return client.IDKey(kindImage, int64(id), nil)
}

// Load loads all of the provided properties into struct.
func (image *Image) Load(ctx context.Context, ps []datastore.Property) error {
	return datastore.LoadStruct(ctx, image, ps)
}

// Save saves all of struct fields to properties.
func (image *Image) Save(ctx context.Context) ([]datastore.Property, error) {
	if time.Time(image.CreatedAt).IsZero() {
		image.CreatedAt = unixTime(timeNow())
	}
	image.UpdatedAt = unixTime(timeNow())

	return datastore.SaveStruct(ctx, image)
}

// LoadKey loads key data into struct.
func (image *Image) LoadKey(ctx context.Context, k datastore.Key) error {
	image.ID = imageID(k.ID())
	return nil
}
