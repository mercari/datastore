package clouddatastore

import (
	"context"
	"encoding/gob"
	"errors"

	"cloud.google.com/go/compute/metadata"
	"cloud.google.com/go/datastore"
	w "go.mercari.io/datastore"
	"go.mercari.io/datastore/internal"
	"go.mercari.io/datastore/internal/shared"
	"google.golang.org/api/option"
)

func init() {
	w.FromContext = FromContext

	gob.Register(&keyImpl{})
}

var projectID *string

func newClientSettings(opts ...w.ClientOption) *internal.ClientSettings {
	if projectID == nil {
		pID, err := metadata.ProjectID()
		if err != nil {
			// don't check again even if it was failed...
			pID = internal.GetProjectID()
		}
		projectID = &pID
	}
	settings := &internal.ClientSettings{
		ProjectID: *projectID,
	}
	for _, opt := range opts {
		opt.Apply(settings)
	}
	return settings
}

func FromContext(ctx context.Context, opts ...w.ClientOption) (w.Client, error) {
	settings := newClientSettings(opts...)
	origOpts := make([]option.ClientOption, 0, len(opts))
	if len(settings.Scopes) != 0 {
		origOpts = append(origOpts, option.WithScopes(settings.Scopes...))
	}
	if settings.TokenSource != nil {
		origOpts = append(origOpts, option.WithTokenSource(settings.TokenSource))
	}
	if settings.CredentialsFile != "" {
		origOpts = append(origOpts, option.WithCredentialsFile(settings.CredentialsFile))
	}
	if settings.HTTPClient != nil {
		origOpts = append(origOpts, option.WithHTTPClient(settings.HTTPClient))
	}

	client, err := datastore.NewClient(ctx, settings.ProjectID, origOpts...)
	if err != nil {
		return nil, err
	}

	return &datastoreImpl{ctx: ctx, client: client}, nil
}

func IsCloudDatastoreClient(client w.Client) bool {
	_, ok := client.(*datastoreImpl)
	return ok
}

var _ shared.OriginalClientBridge = &originalClientBridgeImpl{}
var _ shared.OriginalTransactionBridge = &originalTransactionBridgeImpl{}
var _ shared.OriginalIteratorBridge = &originalIteratorBridgeImpl{}

type originalClientBridgeImpl struct {
	d *datastoreImpl
}

func (ocb *originalClientBridgeImpl) AllocateIDs(ctx context.Context, keys []w.Key) ([]w.Key, error) {
	origKeys := toOriginalKeys(keys)

	origKeys, err := ocb.d.client.AllocateIDs(ctx, origKeys)
	return toWrapperKeys(origKeys), toWrapperError(err)
}

func (ocb *originalClientBridgeImpl) PutMulti(ctx context.Context, keys []w.Key, psList []w.PropertyList) ([]w.Key, error) {
	origKeys := toOriginalKeys(keys)
	origPss := toOriginalPropertyListList(psList)

	origKeys, err := ocb.d.client.PutMulti(ctx, origKeys, origPss)
	return toWrapperKeys(origKeys), toWrapperError(err)
}

func (ocb *originalClientBridgeImpl) GetMulti(ctx context.Context, keys []w.Key, psList []w.PropertyList) error {
	origKeys := toOriginalKeys(keys)
	origPss := toOriginalPropertyListList(psList)

	err := ocb.d.client.GetMulti(ctx, origKeys, origPss)
	wPss := toWrapperPropertyListList(origPss)
	copy(psList, wPss)
	return toWrapperError(err)
}

func (ocb *originalClientBridgeImpl) DeleteMulti(ctx context.Context, keys []w.Key) error {
	origKeys := toOriginalKeys(keys)

	err := ocb.d.client.DeleteMulti(ctx, origKeys)
	return toWrapperError(err)
}

func (ocb *originalClientBridgeImpl) Run(ctx context.Context, q w.Query, qDump *w.QueryDump) w.Iterator {
	qImpl := q.(*queryImpl)
	iter := ocb.d.client.Run(ctx, qImpl.q)

	return &iteratorImpl{
		client: ocb.d,
		q:      qImpl,
		qDump:  qDump,
		t:      iter,
		cacheInfo: &w.MiddlewareInfo{
			Context:     ctx,
			Client:      ocb.d,
			Transaction: qDump.Transaction,
		},
		firstError: qImpl.firstError,
	}
}

func (ocb *originalClientBridgeImpl) GetAll(ctx context.Context, q w.Query, qDump *w.QueryDump, psList *[]w.PropertyList) ([]w.Key, error) {
	qImpl := q.(*queryImpl)

	var origPss []datastore.PropertyList
	if !qDump.KeysOnly {
		origPss = toOriginalPropertyListList(*psList)
	}
	origKeys, err := ocb.d.client.GetAll(ctx, qImpl.q, &origPss)
	if err != nil {
		return nil, toWrapperError(err)
	}

	wKeys := toWrapperKeys(origKeys)

	if !qDump.KeysOnly {
		// TODO should be copy? not replace?
		*psList = toWrapperPropertyListList(origPss)
	}

	return wKeys, nil
}

func (ocb *originalClientBridgeImpl) Count(ctx context.Context, q w.Query, qDump *w.QueryDump) (int, error) {
	qImpl, ok := q.(*queryImpl)
	if !ok {
		return 0, errors.New("invalid query type")
	}
	if qImpl.firstError != nil {
		return 0, qImpl.firstError
	}

	count, err := ocb.d.client.Count(ctx, qImpl.q)
	if err != nil {
		return 0, toWrapperError(err)
	}

	return count, nil
}

type originalTransactionBridgeImpl struct {
	tx *transactionImpl
}

func (otb *originalTransactionBridgeImpl) PutMulti(keys []w.Key, psList []w.PropertyList) ([]w.PendingKey, error) {
	baseTx := getTx(otb.tx.client.ctx)
	if baseTx == nil {
		return nil, errors.New("unexpected context")
	}

	origKeys := toOriginalKeys(keys)
	origPss := toOriginalPropertyListList(psList)

	origPKeys, err := baseTx.PutMulti(origKeys, origPss)
	if err != nil {
		return nil, toWrapperError(err)
	}

	wPKeys := toWrapperPendingKeys(origPKeys)

	return wPKeys, nil
}

func (otb *originalTransactionBridgeImpl) GetMulti(keys []w.Key, psList []w.PropertyList) error {
	baseTx := getTx(otb.tx.client.ctx)
	if baseTx == nil {
		return errors.New("unexpected context")
	}

	origKeys := toOriginalKeys(keys)
	origPss := toOriginalPropertyListList(psList)

	err := baseTx.GetMulti(origKeys, origPss)
	wPss := toWrapperPropertyListList(origPss)
	copy(psList, wPss)
	if err != nil {
		return toWrapperError(err)
	}

	return nil
}

func (otb *originalTransactionBridgeImpl) DeleteMulti(keys []w.Key) error {
	baseTx := getTx(otb.tx.client.ctx)
	if baseTx == nil {
		return errors.New("unexpected context")
	}

	origKeys := toOriginalKeys(keys)

	err := baseTx.DeleteMulti(origKeys)
	return toWrapperError(err)
}

type originalIteratorBridgeImpl struct {
	qDump *w.QueryDump
}

func (oib *originalIteratorBridgeImpl) Next(iter w.Iterator, ps *w.PropertyList) (w.Key, error) {
	iterImpl := iter.(*iteratorImpl)

	var origPsPtr *datastore.PropertyList
	if !oib.qDump.KeysOnly {
		origPs := toOriginalPropertyList(*ps)
		origPsPtr = &origPs
	}

	origKey, err := iterImpl.t.Next(origPsPtr)
	if err != nil {
		return nil, toWrapperError(err)
	}

	if !oib.qDump.KeysOnly {
		*ps = toWrapperPropertyList(*origPsPtr)
	}

	return toWrapperKey(origKey), nil
}
