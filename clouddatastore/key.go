package clouddatastore

import (
	"context"

	"cloud.google.com/go/datastore"
	w "go.mercari.io/datastore"
)

var _ w.Key = (*keyImpl)(nil)
var _ w.PendingKey = (*pendingKeyImpl)(nil)

type keyImpl struct {
	kind      string
	id        int64
	name      string
	parent    *keyImpl
	namespace string
}

type pendingKeyImpl struct {
	pendingKey *datastore.PendingKey
}

type contextPendingKey struct{}

func (k *keyImpl) Kind() string {
	if k == nil {
		panic("k is nil")
	}
	return k.kind
}

func (k *keyImpl) ID() int64 {
	return k.id
}

func (k *keyImpl) Name() string {
	return k.name
}

func (k *keyImpl) ParentKey() w.Key {
	if k.parent == nil {
		return nil
	}
	return k.parent
}

func (k *keyImpl) Namespace() string {
	return k.namespace
}

func (k *keyImpl) String() string {
	// TODO 手で実装しなおしたほうがいいかも 互換性のため
	return toOriginalKey(k).String()
}

func (k *keyImpl) GobEncode() ([]byte, error) {
	// TODO 手で実装しなおしたほうがいいかも 互換性のため
	return toOriginalKey(k).GobEncode()
}

func (k *keyImpl) GobDecode(buf []byte) error {
	// TODO 手で実装しなおしたほうがいいかも 互換性のため

	origKey := &datastore.Key{}
	err := origKey.GobDecode(buf)
	if err != nil {
		return err
	}

	k.kind = origKey.Kind
	k.id = origKey.ID
	k.name = origKey.Name
	k.parent = toWrapperKey(origKey.Parent)
	k.namespace = origKey.Namespace

	return nil
}

func (k *keyImpl) MarshalJSON() ([]byte, error) {
	// TODO 手で実装しなおしたほうがいいかも 互換性のため

	return toOriginalKey(k).MarshalJSON()
}

func (k *keyImpl) UnmarshalJSON(buf []byte) error {
	// TODO 手で実装しなおしたほうがいいかも 互換性のため

	origKey := &datastore.Key{}
	err := origKey.UnmarshalJSON(buf)
	if err != nil {
		return err
	}

	k.kind = origKey.Kind
	k.id = origKey.ID
	k.name = origKey.Name
	k.parent = toWrapperKey(origKey.Parent)
	k.namespace = origKey.Namespace

	return nil
}

func (k *keyImpl) Encode() string {
	return toOriginalKey(k).Encode()
}

func (p *pendingKeyImpl) StoredContext() context.Context {
	return context.WithValue(context.Background(), contextPendingKey{}, p)
}
