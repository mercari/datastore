package shared

import (
	"context"

	"go.mercari.io/datastore"
)

var _ datastore.CacheStrategy = &CacheBridge{}

type CacheBridge struct {
	ocb  OriginalClientBridge
	otb  OriginalTransactionBridge
	oib  OriginalIteratorBridge
	cs   []datastore.CacheStrategy
	Info *datastore.CacheInfo
}

type OriginalClientBridge interface {
	PutMulti(ctx context.Context, keys []datastore.Key, psList []datastore.PropertyList) ([]datastore.Key, error)
	GetMulti(ctx context.Context, keys []datastore.Key, psList []datastore.PropertyList) error
	DeleteMulti(ctx context.Context, keys []datastore.Key) error
	Run(ctx context.Context, q datastore.Query, qDump *datastore.QueryDump) datastore.Iterator
	GetAll(ctx context.Context, q datastore.Query, qDump *datastore.QueryDump, psList *[]datastore.PropertyList) ([]datastore.Key, error)
}

type OriginalTransactionBridge interface {
	PutMulti(keys []datastore.Key, psList []datastore.PropertyList) ([]datastore.PendingKey, error)
	GetMulti(keys []datastore.Key, psList []datastore.PropertyList) error
	DeleteMulti(keys []datastore.Key) error
}

type OriginalIteratorBridge interface {
	Next(iter datastore.Iterator, ps *datastore.PropertyList) (datastore.Key, error)
}

func NewCacheBridge(info *datastore.CacheInfo, ocb OriginalClientBridge, otb OriginalTransactionBridge, oib OriginalIteratorBridge, cs []datastore.CacheStrategy) *CacheBridge {
	cb := &CacheBridge{
		ocb:  ocb,
		otb:  otb,
		oib:  oib,
		cs:   cs,
		Info: info,
	}
	cb.Info.Next = cb
	return cb
}

// TODO 全体的にcbは使いまわしたい Query周りでは1つのcbが複数回再利用されるかもしれないので破壊的変更の競合が怖い

func (cb *CacheBridge) PutMultiWithoutTx(info *datastore.CacheInfo, keys []datastore.Key, psList []datastore.PropertyList) ([]datastore.Key, error) {
	l := len(cb.cs)
	if l == 0 {
		return cb.ocb.PutMulti(info.Context, keys, psList)
	}

	// call last strategy
	current := cb.cs[l-1]
	left := &CacheBridge{
		ocb:  cb.ocb,
		otb:  cb.otb,
		oib:  cb.oib,
		cs:   cb.cs[:l-1],
		Info: cb.Info,
	}
	left.Info.Next = left

	return current.PutMultiWithoutTx(left.Info, keys, psList)
}

func (cb *CacheBridge) PutMultiWithTx(info *datastore.CacheInfo, keys []datastore.Key, psList []datastore.PropertyList) ([]datastore.PendingKey, error) {
	l := len(cb.cs)
	if l == 0 {
		return cb.otb.PutMulti(keys, psList)
	}

	// call last strategy
	current := cb.cs[l-1]
	left := &CacheBridge{
		ocb:  cb.ocb,
		otb:  cb.otb,
		oib:  cb.oib,
		cs:   cb.cs[:l-1],
		Info: cb.Info,
	}
	left.Info.Next = left

	return current.PutMultiWithTx(left.Info, keys, psList)
}

func (cb *CacheBridge) GetMultiWithoutTx(info *datastore.CacheInfo, keys []datastore.Key, psList []datastore.PropertyList) error {
	l := len(cb.cs)
	if l == 0 {
		return cb.ocb.GetMulti(info.Context, keys, psList)
	}

	// call last strategy
	current := cb.cs[l-1]
	left := &CacheBridge{
		ocb:  cb.ocb,
		otb:  cb.otb,
		oib:  cb.oib,
		cs:   cb.cs[:l-1],
		Info: cb.Info,
	}
	left.Info.Next = left

	return current.GetMultiWithoutTx(left.Info, keys, psList)
}

func (cb *CacheBridge) GetMultiWithTx(info *datastore.CacheInfo, keys []datastore.Key, psList []datastore.PropertyList) error {
	l := len(cb.cs)
	if l == 0 {
		return cb.otb.GetMulti(keys, psList)
	}

	// call last strategy
	current := cb.cs[l-1]
	left := &CacheBridge{
		ocb:  cb.ocb,
		otb:  cb.otb,
		oib:  cb.oib,
		cs:   cb.cs[:l-1],
		Info: cb.Info,
	}
	left.Info.Next = left

	return current.GetMultiWithTx(left.Info, keys, psList)
}

func (cb *CacheBridge) DeleteMultiWithoutTx(info *datastore.CacheInfo, keys []datastore.Key) error {
	l := len(cb.cs)
	if l == 0 {
		return cb.ocb.DeleteMulti(info.Context, keys)
	}

	// call last strategy
	current := cb.cs[l-1]
	left := &CacheBridge{
		ocb:  cb.ocb,
		otb:  cb.otb,
		oib:  cb.oib,
		cs:   cb.cs[:l-1],
		Info: cb.Info,
	}
	left.Info.Next = left

	return current.DeleteMultiWithoutTx(left.Info, keys)
}

func (cb *CacheBridge) DeleteMultiWithTx(info *datastore.CacheInfo, keys []datastore.Key) error {
	l := len(cb.cs)
	if l == 0 {
		return cb.otb.DeleteMulti(keys)
	}

	// call last strategy
	current := cb.cs[l-1]
	left := &CacheBridge{
		ocb:  cb.ocb,
		otb:  cb.otb,
		oib:  cb.oib,
		cs:   cb.cs[:l-1],
		Info: cb.Info,
	}
	left.Info.Next = left

	return current.DeleteMultiWithTx(left.Info, keys)
}

func (cb *CacheBridge) PostCommit(info *datastore.CacheInfo, tx datastore.Transaction, commit datastore.Commit) error {
	l := len(cb.cs)
	if l == 0 {
		return nil
	}

	// call last strategy
	current := cb.cs[l-1]
	left := &CacheBridge{
		ocb:  cb.ocb,
		otb:  cb.otb,
		oib:  cb.oib,
		cs:   cb.cs[:l-1],
		Info: cb.Info,
	}
	left.Info.Next = left

	return current.PostCommit(left.Info, tx, commit)
}

func (cb *CacheBridge) PostRollback(info *datastore.CacheInfo, tx datastore.Transaction) error {
	l := len(cb.cs)
	if l == 0 {
		return nil
	}

	// call last strategy
	current := cb.cs[l-1]
	left := &CacheBridge{
		ocb:  cb.ocb,
		otb:  cb.otb,
		oib:  cb.oib,
		cs:   cb.cs[:l-1],
		Info: cb.Info,
	}
	left.Info.Next = left

	return current.PostRollback(left.Info, tx)
}

func (cb *CacheBridge) Run(info *datastore.CacheInfo, q datastore.Query, qDump *datastore.QueryDump) datastore.Iterator {
	l := len(cb.cs)
	if l == 0 {
		return cb.ocb.Run(info.Context, q, qDump)
	}

	// call last strategy
	current := cb.cs[l-1]
	left := &CacheBridge{
		ocb:  cb.ocb,
		otb:  cb.otb,
		oib:  cb.oib,
		cs:   cb.cs[:l-1],
		Info: cb.Info,
	}
	left.Info.Next = left

	return current.Run(left.Info, q, qDump)
}

func (cb *CacheBridge) GetAll(info *datastore.CacheInfo, q datastore.Query, qDump *datastore.QueryDump, psList *[]datastore.PropertyList) ([]datastore.Key, error) {
	l := len(cb.cs)
	if l == 0 {
		return cb.ocb.GetAll(info.Context, q, qDump, psList)
	}

	// call last strategy
	current := cb.cs[l-1]
	left := &CacheBridge{
		ocb:  cb.ocb,
		otb:  cb.otb,
		oib:  cb.oib,
		cs:   cb.cs[:l-1],
		Info: cb.Info,
	}
	left.Info.Next = left

	return current.GetAll(left.Info, q, qDump, psList)
}

func (cb *CacheBridge) Next(info *datastore.CacheInfo, q datastore.Query, qDump *datastore.QueryDump, iter datastore.Iterator, ps *datastore.PropertyList) (datastore.Key, error) {
	l := len(cb.cs)
	if l == 0 {
		return cb.oib.Next(iter, ps)
	}

	// call last strategy
	current := cb.cs[l-1]
	left := &CacheBridge{
		ocb:  cb.ocb,
		otb:  cb.otb,
		oib:  cb.oib,
		cs:   cb.cs[:l-1],
		Info: cb.Info,
	}
	left.Info.Next = left

	return current.Next(left.Info, q, qDump, iter, ps)
}
