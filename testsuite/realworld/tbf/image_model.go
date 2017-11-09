package tbf

import (
	"context"
	"fmt"
	"time"

	"go.mercari.io/datastore"
)

var _ datastore.PropertyTranslator = ImageID(0)
var _ datastore.KeyLoader = &Image{}

const kindImage = "Image"

type ImageID int64

type Image struct {
	ID            ImageID  `json:"id" datastore:"-"`
	OwnerCircleID CircleID `json:"ownerCircleID"`
	GCSPath       string   `json:"gcsPath"`
	CreatedAt     UnixTime `json:"createdAt"`
	UpdatedAt     UnixTime `json:"updatedAt"`
}

func KeyToImageID(key datastore.Key) (ImageID, error) {
	if key.Kind() != kindImage {
		return 0, fmt.Errorf("unexpected kind: %s", key.Kind())
	}

	return ImageID(key.ID()), nil
}

func (id ImageID) ToPropertyValue(ctx context.Context) (interface{}, error) {
	client := ctx.Value(contextClient{}).(datastore.Client)
	key := client.IDKey(kindImage, int64(id), nil)
	return key, nil
}

func (id ImageID) FromPropertyValue(ctx context.Context, p datastore.Property) (dst interface{}, err error) {
	key, ok := p.Value.(datastore.Key)
	if !ok {
		return nil, datastore.ErrInvalidEntityType
	}
	return ImageID(key.ID()), nil
}

func (id ImageID) ToKey(client datastore.Client) datastore.Key {
	return client.IDKey(kindImage, int64(id), nil)
}

func (image *Image) Load(ctx context.Context, ps []datastore.Property) error {
	return datastore.LoadStruct(ctx, image, ps)
}

func (image *Image) Save(ctx context.Context) ([]datastore.Property, error) {
	if time.Time(image.CreatedAt).IsZero() {
		image.CreatedAt = UnixTime(timeNow())
	}
	image.UpdatedAt = UnixTime(timeNow())

	return datastore.SaveStruct(ctx, image)
}

func (image *Image) LoadKey(ctx context.Context, k datastore.Key) error {
	image.ID = ImageID(k.ID())
	return nil
}
