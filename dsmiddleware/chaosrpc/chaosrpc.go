package chaosrpc

import (
	"errors"
	"math/rand"

	"go.mercari.io/datastore"
)

// NOTE Please give me a pull request if You can make more chaos (within the specification).

var _ datastore.Middleware = &chaosHandler{}

func New(s rand.Source) datastore.Middleware {
	return &chaosHandler{
		r: rand.New(s),
	}
}

type chaosHandler struct {
	r *rand.Rand
}

func (ch *chaosHandler) raiseError() error {
	// Make an error with a 20% rate
	if ch.r.Intn(5) == 0 {
		return errors.New("error from chaosrpc!!")
	}

	return nil
}

func (ch *chaosHandler) AllocateIDs(info *datastore.MiddlewareInfo, keys []datastore.Key) ([]datastore.Key, error) {
	if err := ch.raiseError(); err != nil {
		return nil, err
	}

	return info.Next.AllocateIDs(info, keys)
}

func (ch *chaosHandler) PutMultiWithoutTx(info *datastore.MiddlewareInfo, keys []datastore.Key, psList []datastore.PropertyList) ([]datastore.Key, error) {
	if err := ch.raiseError(); err != nil {
		return nil, err
	}

	return info.Next.PutMultiWithoutTx(info, keys, psList)
}

func (ch *chaosHandler) PutMultiWithTx(info *datastore.MiddlewareInfo, keys []datastore.Key, psList []datastore.PropertyList) ([]datastore.PendingKey, error) {
	if err := ch.raiseError(); err != nil {
		return nil, err
	}

	return info.Next.PutMultiWithTx(info, keys, psList)
}

func (ch *chaosHandler) GetMultiWithoutTx(info *datastore.MiddlewareInfo, keys []datastore.Key, psList []datastore.PropertyList) error {
	if err := ch.raiseError(); err != nil {
		return err
	}

	return info.Next.GetMultiWithoutTx(info, keys, psList)
}

func (ch *chaosHandler) GetMultiWithTx(info *datastore.MiddlewareInfo, keys []datastore.Key, psList []datastore.PropertyList) error {
	if err := ch.raiseError(); err != nil {
		return err
	}

	return info.Next.GetMultiWithTx(info, keys, psList)
}

func (ch *chaosHandler) DeleteMultiWithoutTx(info *datastore.MiddlewareInfo, keys []datastore.Key) error {
	if err := ch.raiseError(); err != nil {
		return err
	}

	return info.Next.DeleteMultiWithoutTx(info, keys)
}

func (ch *chaosHandler) DeleteMultiWithTx(info *datastore.MiddlewareInfo, keys []datastore.Key) error {
	if err := ch.raiseError(); err != nil {
		return err
	}

	return info.Next.DeleteMultiWithTx(info, keys)
}

func (ch *chaosHandler) PostCommit(info *datastore.MiddlewareInfo, tx datastore.Transaction, commit datastore.Commit) error {
	// PostCommit don't do RPC
	return info.Next.PostCommit(info, tx, commit)
}

func (ch *chaosHandler) PostRollback(info *datastore.MiddlewareInfo, tx datastore.Transaction) error {
	// PostRollback don't do RPC
	return info.Next.PostRollback(info, tx)
}

func (ch *chaosHandler) Run(info *datastore.MiddlewareInfo, q datastore.Query, qDump *datastore.QueryDump) datastore.Iterator {
	// can't returns error
	return info.Next.Run(info, q, qDump)
}

func (ch *chaosHandler) GetAll(info *datastore.MiddlewareInfo, q datastore.Query, qDump *datastore.QueryDump, psList *[]datastore.PropertyList) ([]datastore.Key, error) {
	if err := ch.raiseError(); err != nil {
		return nil, err
	}

	return info.Next.GetAll(info, q, qDump, psList)
}

func (ch *chaosHandler) Next(info *datastore.MiddlewareInfo, q datastore.Query, qDump *datastore.QueryDump, iter datastore.Iterator, ps *datastore.PropertyList) (datastore.Key, error) {
	// Next is not idempotent, don't retry in dsmiddleware/rpcretry.
	return info.Next.Next(info, q, qDump, iter, ps)
}

func (ch *chaosHandler) Count(info *datastore.MiddlewareInfo, q datastore.Query, qDump *datastore.QueryDump) (int, error) {
	if err := ch.raiseError(); err != nil {
		return 0, err
	}

	return info.Next.Count(info, q, qDump)
}
