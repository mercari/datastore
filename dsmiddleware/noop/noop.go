package noop

import "go.mercari.io/datastore"

var _ datastore.Middleware = &noop{}

// New no-op middleware creates and returns.
func New() datastore.Middleware {
	return &noop{}
}

type noop struct {
}

func (*noop) AllocateIDs(info *datastore.MiddlewareInfo, keys []datastore.Key) ([]datastore.Key, error) {
	return info.Next.AllocateIDs(info, keys)
}

func (*noop) PutMultiWithoutTx(info *datastore.MiddlewareInfo, keys []datastore.Key, psList []datastore.PropertyList) ([]datastore.Key, error) {
	return info.Next.PutMultiWithoutTx(info, keys, psList)
}

func (*noop) PutMultiWithTx(info *datastore.MiddlewareInfo, keys []datastore.Key, psList []datastore.PropertyList) ([]datastore.PendingKey, error) {
	return info.Next.PutMultiWithTx(info, keys, psList)
}

func (*noop) GetMultiWithoutTx(info *datastore.MiddlewareInfo, keys []datastore.Key, psList []datastore.PropertyList) error {
	return info.Next.GetMultiWithoutTx(info, keys, psList)
}

func (*noop) GetMultiWithTx(info *datastore.MiddlewareInfo, keys []datastore.Key, psList []datastore.PropertyList) error {
	return info.Next.GetMultiWithTx(info, keys, psList)
}

func (*noop) DeleteMultiWithoutTx(info *datastore.MiddlewareInfo, keys []datastore.Key) error {
	return info.Next.DeleteMultiWithoutTx(info, keys)
}

func (*noop) DeleteMultiWithTx(info *datastore.MiddlewareInfo, keys []datastore.Key) error {
	return info.Next.DeleteMultiWithTx(info, keys)
}

func (*noop) PostCommit(info *datastore.MiddlewareInfo, tx datastore.Transaction, commit datastore.Commit) error {
	return info.Next.PostCommit(info, tx, commit)
}

func (*noop) PostRollback(info *datastore.MiddlewareInfo, tx datastore.Transaction) error {
	return info.Next.PostRollback(info, tx)
}

func (*noop) Run(info *datastore.MiddlewareInfo, q datastore.Query, qDump *datastore.QueryDump) datastore.Iterator {
	return info.Next.Run(info, q, qDump)
}

func (*noop) GetAll(info *datastore.MiddlewareInfo, q datastore.Query, qDump *datastore.QueryDump, psList *[]datastore.PropertyList) ([]datastore.Key, error) {
	return info.Next.GetAll(info, q, qDump, psList)
}

func (*noop) Next(info *datastore.MiddlewareInfo, q datastore.Query, qDump *datastore.QueryDump, iter datastore.Iterator, ps *datastore.PropertyList) (datastore.Key, error) {
	return info.Next.Next(info, q, qDump, iter, ps)
}

func (*noop) Count(info *datastore.MiddlewareInfo, q datastore.Query, qDump *datastore.QueryDump) (int, error) {
	return info.Next.Count(info, q, qDump)
}
