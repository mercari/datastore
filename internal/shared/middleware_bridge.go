package shared

import (
	"context"

	"go.mercari.io/datastore"
)

var _ datastore.Middleware = &MiddlewareBridge{}

type MiddlewareBridge struct {
	ocb  OriginalClientBridge
	otb  OriginalTransactionBridge
	oib  OriginalIteratorBridge
	mws  []datastore.Middleware
	Info *datastore.MiddlewareInfo
}

type OriginalClientBridge interface {
	AllocateIDs(ctx context.Context, keys []datastore.Key) ([]datastore.Key, error)
	PutMulti(ctx context.Context, keys []datastore.Key, psList []datastore.PropertyList) ([]datastore.Key, error)
	GetMulti(ctx context.Context, keys []datastore.Key, psList []datastore.PropertyList) error
	DeleteMulti(ctx context.Context, keys []datastore.Key) error
	Run(ctx context.Context, q datastore.Query, qDump *datastore.QueryDump) datastore.Iterator
	GetAll(ctx context.Context, q datastore.Query, qDump *datastore.QueryDump, psList *[]datastore.PropertyList) ([]datastore.Key, error)
	Count(ctx context.Context, q datastore.Query, qDump *datastore.QueryDump) (int, error)
}

type OriginalTransactionBridge interface {
	PutMulti(keys []datastore.Key, psList []datastore.PropertyList) ([]datastore.PendingKey, error)
	GetMulti(keys []datastore.Key, psList []datastore.PropertyList) error
	DeleteMulti(keys []datastore.Key) error
}

type OriginalIteratorBridge interface {
	Next(iter datastore.Iterator, ps *datastore.PropertyList) (datastore.Key, error)
}

func NewCacheBridge(info *datastore.MiddlewareInfo, ocb OriginalClientBridge, otb OriginalTransactionBridge, oib OriginalIteratorBridge, mws []datastore.Middleware) *MiddlewareBridge {
	cb := &MiddlewareBridge{
		ocb:  ocb,
		otb:  otb,
		oib:  oib,
		mws:  mws,
		Info: info,
	}
	cb.Info.Next = cb
	return cb
}

func (cb *MiddlewareBridge) AllocateIDs(info *datastore.MiddlewareInfo, keys []datastore.Key) ([]datastore.Key, error) {
	if len(cb.mws) == 0 {
		return cb.ocb.AllocateIDs(info.Context, keys)
	}

	current := cb.mws[0]
	left := &MiddlewareBridge{
		ocb:  cb.ocb,
		otb:  cb.otb,
		oib:  cb.oib,
		mws:  cb.mws[1:],
		Info: cb.Info,
	}
	left.Info.Next = left

	return current.AllocateIDs(left.Info, keys)
}

func (cb *MiddlewareBridge) PutMultiWithoutTx(info *datastore.MiddlewareInfo, keys []datastore.Key, psList []datastore.PropertyList) ([]datastore.Key, error) {
	if len(cb.mws) == 0 {
		return cb.ocb.PutMulti(info.Context, keys, psList)
	}

	current := cb.mws[0]
	left := &MiddlewareBridge{
		ocb:  cb.ocb,
		otb:  cb.otb,
		oib:  cb.oib,
		mws:  cb.mws[1:],
		Info: cb.Info,
	}
	left.Info.Next = left

	return current.PutMultiWithoutTx(left.Info, keys, psList)
}

func (cb *MiddlewareBridge) PutMultiWithTx(info *datastore.MiddlewareInfo, keys []datastore.Key, psList []datastore.PropertyList) ([]datastore.PendingKey, error) {
	if len(cb.mws) == 0 {
		return cb.otb.PutMulti(keys, psList)
	}

	current := cb.mws[0]
	left := &MiddlewareBridge{
		ocb:  cb.ocb,
		otb:  cb.otb,
		oib:  cb.oib,
		mws:  cb.mws[1:],
		Info: cb.Info,
	}
	left.Info.Next = left

	return current.PutMultiWithTx(left.Info, keys, psList)
}

func (cb *MiddlewareBridge) GetMultiWithoutTx(info *datastore.MiddlewareInfo, keys []datastore.Key, psList []datastore.PropertyList) error {
	if len(cb.mws) == 0 {
		return cb.ocb.GetMulti(info.Context, keys, psList)
	}

	current := cb.mws[0]
	left := &MiddlewareBridge{
		ocb:  cb.ocb,
		otb:  cb.otb,
		oib:  cb.oib,
		mws:  cb.mws[1:],
		Info: cb.Info,
	}
	left.Info.Next = left

	return current.GetMultiWithoutTx(left.Info, keys, psList)
}

func (cb *MiddlewareBridge) GetMultiWithTx(info *datastore.MiddlewareInfo, keys []datastore.Key, psList []datastore.PropertyList) error {
	if len(cb.mws) == 0 {
		return cb.otb.GetMulti(keys, psList)
	}

	current := cb.mws[0]
	left := &MiddlewareBridge{
		ocb:  cb.ocb,
		otb:  cb.otb,
		oib:  cb.oib,
		mws:  cb.mws[1:],
		Info: cb.Info,
	}
	left.Info.Next = left

	return current.GetMultiWithTx(left.Info, keys, psList)
}

func (cb *MiddlewareBridge) DeleteMultiWithoutTx(info *datastore.MiddlewareInfo, keys []datastore.Key) error {
	if len(cb.mws) == 0 {
		return cb.ocb.DeleteMulti(info.Context, keys)
	}

	current := cb.mws[0]
	left := &MiddlewareBridge{
		ocb:  cb.ocb,
		otb:  cb.otb,
		oib:  cb.oib,
		mws:  cb.mws[1:],
		Info: cb.Info,
	}
	left.Info.Next = left

	return current.DeleteMultiWithoutTx(left.Info, keys)
}

func (cb *MiddlewareBridge) DeleteMultiWithTx(info *datastore.MiddlewareInfo, keys []datastore.Key) error {
	if len(cb.mws) == 0 {
		return cb.otb.DeleteMulti(keys)
	}

	current := cb.mws[0]
	left := &MiddlewareBridge{
		ocb:  cb.ocb,
		otb:  cb.otb,
		oib:  cb.oib,
		mws:  cb.mws[1:],
		Info: cb.Info,
	}
	left.Info.Next = left

	return current.DeleteMultiWithTx(left.Info, keys)
}

func (cb *MiddlewareBridge) PostCommit(info *datastore.MiddlewareInfo, tx datastore.Transaction, commit datastore.Commit) error {
	if len(cb.mws) == 0 {
		return nil
	}

	current := cb.mws[0]
	left := &MiddlewareBridge{
		ocb:  cb.ocb,
		otb:  cb.otb,
		oib:  cb.oib,
		mws:  cb.mws[1:],
		Info: cb.Info,
	}
	left.Info.Next = left

	return current.PostCommit(left.Info, tx, commit)
}

func (cb *MiddlewareBridge) PostRollback(info *datastore.MiddlewareInfo, tx datastore.Transaction) error {
	if len(cb.mws) == 0 {
		return nil
	}

	current := cb.mws[0]
	left := &MiddlewareBridge{
		ocb:  cb.ocb,
		otb:  cb.otb,
		oib:  cb.oib,
		mws:  cb.mws[1:],
		Info: cb.Info,
	}
	left.Info.Next = left

	return current.PostRollback(left.Info, tx)
}

func (cb *MiddlewareBridge) Run(info *datastore.MiddlewareInfo, q datastore.Query, qDump *datastore.QueryDump) datastore.Iterator {
	if len(cb.mws) == 0 {
		return cb.ocb.Run(info.Context, q, qDump)
	}

	current := cb.mws[0]
	left := &MiddlewareBridge{
		ocb:  cb.ocb,
		otb:  cb.otb,
		oib:  cb.oib,
		mws:  cb.mws[1:],
		Info: cb.Info,
	}
	left.Info.Next = left

	return current.Run(left.Info, q, qDump)
}

func (cb *MiddlewareBridge) GetAll(info *datastore.MiddlewareInfo, q datastore.Query, qDump *datastore.QueryDump, psList *[]datastore.PropertyList) ([]datastore.Key, error) {
	if len(cb.mws) == 0 {
		return cb.ocb.GetAll(info.Context, q, qDump, psList)
	}

	current := cb.mws[0]
	left := &MiddlewareBridge{
		ocb:  cb.ocb,
		otb:  cb.otb,
		oib:  cb.oib,
		mws:  cb.mws[1:],
		Info: cb.Info,
	}
	left.Info.Next = left

	return current.GetAll(left.Info, q, qDump, psList)
}

func (cb *MiddlewareBridge) Next(info *datastore.MiddlewareInfo, q datastore.Query, qDump *datastore.QueryDump, iter datastore.Iterator, ps *datastore.PropertyList) (datastore.Key, error) {
	if len(cb.mws) == 0 {
		return cb.oib.Next(iter, ps)
	}

	current := cb.mws[0]
	left := &MiddlewareBridge{
		ocb:  cb.ocb,
		otb:  cb.otb,
		oib:  cb.oib,
		mws:  cb.mws[1:],
		Info: cb.Info,
	}
	left.Info.Next = left

	return current.Next(left.Info, q, qDump, iter, ps)
}

func (cb *MiddlewareBridge) Count(info *datastore.MiddlewareInfo, q datastore.Query, qDump *datastore.QueryDump) (int, error) {
	if len(cb.mws) == 0 {
		return cb.ocb.Count(info.Context, q, qDump)
	}

	current := cb.mws[0]
	left := &MiddlewareBridge{
		ocb:  cb.ocb,
		otb:  cb.otb,
		oib:  cb.oib,
		mws:  cb.mws[1:],
		Info: cb.Info,
	}
	left.Info.Next = left

	return current.Count(left.Info, q, qDump)
}
