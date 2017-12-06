package tbf

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mercari.io/datastore"
)

var _ datastore.PropertyTranslator = CircleID(0)
var _ datastore.KeyLoader = &Circle{}

const kindCircle = "Circle"

type CircleID int64

type Circle struct {
	ID        CircleID  `json:"id" datastore:"-"`
	Name      string    `json:"name"`
	ImageIDs  []ImageID `json:"-"`
	Images    []*Image  `json:"images" datastore:"-"`
	CreatedAt UnixTime  `json:"createdAt"`
	UpdatedAt UnixTime  `json:"updatedAt"`
}

func KeyToCircleID(key datastore.Key) (CircleID, error) {
	if key.Kind() != kindCircle {
		return 0, fmt.Errorf("unexpected kind: %s", key.Kind())
	}

	return CircleID(key.ID()), nil
}

func (id CircleID) ToPropertyValue(ctx context.Context) (interface{}, error) {
	client := ctx.Value(contextClient{}).(datastore.Client)
	key := client.IDKey(kindCircle, int64(id), nil)
	return key, nil
}

func (id CircleID) FromPropertyValue(ctx context.Context, p datastore.Property) (dst interface{}, err error) {
	key, ok := p.Value.(datastore.Key)
	if !ok {
		return nil, datastore.ErrInvalidEntityType
	}
	return CircleID(key.ID()), nil
}

func (id CircleID) ToKey(client datastore.Client) datastore.Key {
	return client.IDKey(kindCircle, int64(id), nil)
}

func (circle *Circle) Load(ctx context.Context, ps []datastore.Property) error {
	err := datastore.LoadStruct(ctx, circle, ps)
	if err != nil {
		return err
	}

	// 子画像をBatchGetしていく

	batch, ok := ctx.Value(contextBatch{}).(*datastore.Batch)
	if !ok || batch == nil {
		return errors.New("Can't pickup batch client")
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

func (circle *Circle) Save(ctx context.Context) ([]datastore.Property, error) {
	if time.Time(circle.CreatedAt).IsZero() {
		circle.CreatedAt = UnixTime(timeNow())
	}
	circle.UpdatedAt = UnixTime(timeNow())

	return datastore.SaveStruct(ctx, circle)
}

func (circle *Circle) LoadKey(ctx context.Context, k datastore.Key) error {
	circle.ID = CircleID(k.ID())
	return nil
}
