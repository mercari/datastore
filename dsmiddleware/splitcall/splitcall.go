package splitcall

import (
	"context"

	"go.mercari.io/datastore"
)

var _ datastore.Middleware = &splitHandler{}

// New split call middleware will be returns.
func New(opts ...Option) datastore.Middleware {
	sh := &splitHandler{splitThreshold: 1000}
	for _, opt := range opts {
		opt.Apply(sh)
	}
	if sh.logf == nil {
		sh.logf = func(ctx context.Context, format string, args ...interface{}) {}
	}

	return sh
}

// A Option is an option for splitcall.
type Option interface {
	Apply(*splitHandler)
}

type splitHandler struct {
	splitThreshold int

	logf func(ctx context.Context, format string, args ...interface{})
}

func (sh *splitHandler) AllocateIDs(info *datastore.MiddlewareInfo, keys []datastore.Key) ([]datastore.Key, error) {
	return info.Next.AllocateIDs(info, keys)
}

func (sh *splitHandler) PutMultiWithoutTx(info *datastore.MiddlewareInfo, keys []datastore.Key, psList []datastore.PropertyList) ([]datastore.Key, error) {
	return info.Next.PutMultiWithoutTx(info, keys, psList)
}

func (sh *splitHandler) PutMultiWithTx(info *datastore.MiddlewareInfo, keys []datastore.Key, psList []datastore.PropertyList) ([]datastore.PendingKey, error) {
	return info.Next.PutMultiWithTx(info, keys, psList)
}

func (sh *splitHandler) GetMultiWithoutTx(info *datastore.MiddlewareInfo, keys []datastore.Key, psList []datastore.PropertyList) error {
	sh.logf(info.Context, "process %d keys", len(keys))
	if sh.splitThreshold <= 0 || len(keys) <= sh.splitThreshold {
		return info.Next.GetMultiWithoutTx(info, keys, psList)
	}
	for len(psList) < len(keys) {
		psList = append(psList, nil)
	}

	var mErr datastore.MultiError = make([]error, len(keys))
	var foundErr bool
	next := info.Next
	for i := 0; i < len(keys); i += sh.splitThreshold {
		end := i + sh.splitThreshold
		if len(keys) < end {
			end = len(keys)
		}
		sh.logf(info.Context, "process [%d, %d) range keys", i, end)
		err := next.GetMultiWithoutTx(info, keys[i:end], psList[i:end])
		if mErr2, ok := err.(datastore.MultiError); ok {
			for idx, err := range mErr2 {
				if err != nil {
					foundErr = true
					mErr[i+idx] = err
				}
			}
		} else if err != nil {
			return err
		}
	}
	if foundErr {
		return mErr
	}

	return nil
}

func (sh *splitHandler) GetMultiWithTx(info *datastore.MiddlewareInfo, keys []datastore.Key, psList []datastore.PropertyList) error {
	return info.Next.GetMultiWithTx(info, keys, psList)
}

func (sh *splitHandler) DeleteMultiWithoutTx(info *datastore.MiddlewareInfo, keys []datastore.Key) error {
	return info.Next.DeleteMultiWithoutTx(info, keys)
}

func (sh *splitHandler) DeleteMultiWithTx(info *datastore.MiddlewareInfo, keys []datastore.Key) error {
	return info.Next.DeleteMultiWithTx(info, keys)
}

func (sh *splitHandler) PostCommit(info *datastore.MiddlewareInfo, tx datastore.Transaction, commit datastore.Commit) error {
	return info.Next.PostCommit(info, tx, commit)
}

func (sh *splitHandler) PostRollback(info *datastore.MiddlewareInfo, tx datastore.Transaction) error {
	return info.Next.PostRollback(info, tx)
}

func (sh *splitHandler) Run(info *datastore.MiddlewareInfo, q datastore.Query, qDump *datastore.QueryDump) datastore.Iterator {
	return info.Next.Run(info, q, qDump)
}

func (sh *splitHandler) GetAll(info *datastore.MiddlewareInfo, q datastore.Query, qDump *datastore.QueryDump, psList *[]datastore.PropertyList) ([]datastore.Key, error) {
	return info.Next.GetAll(info, q, qDump, psList)
}

func (sh *splitHandler) Next(info *datastore.MiddlewareInfo, q datastore.Query, qDump *datastore.QueryDump, iter datastore.Iterator, ps *datastore.PropertyList) (datastore.Key, error) {
	return info.Next.Next(info, q, qDump, iter, ps)
}

func (sh *splitHandler) Count(info *datastore.MiddlewareInfo, q datastore.Query, qDump *datastore.QueryDump) (int, error) {
	return info.Next.Count(info, q, qDump)
}
