package rpcretry

import (
	"context"
	"math"
	"time"

	"go.mercari.io/datastore"
)

var _ datastore.Middleware = &retryHandler{}

func New(opts ...RetryOption) datastore.Middleware {
	rh := &retryHandler{
		retryLimit:         3,
		minBackoffDuration: 100 * time.Millisecond,
		logf:               func(ctx context.Context, format string, args ...interface{}) {},
	}

	for _, opt := range opts {
		opt.Apply(rh)
	}

	return rh
}

type retryHandler struct {
	retryLimit         int
	minBackoffDuration time.Duration
	maxBackoffDuration time.Duration
	maxDoublings       int
	logf               func(ctx context.Context, format string, args ...interface{})
}

type RetryOption interface {
	Apply(*retryHandler)
}

func (rh *retryHandler) waitDuration(retry int) time.Duration {
	d := 10 * time.Millisecond
	if 0 <= rh.minBackoffDuration {
		d = rh.minBackoffDuration
	}

	m := retry
	if 0 < rh.maxDoublings && rh.maxDoublings < m {
		m = rh.maxDoublings
	}
	if m <= 0 {
		m = 1
	}

	wait := math.Pow(2, float64(m-1)) * float64(d)

	if 0 < rh.maxBackoffDuration {
		wait = math.Min(wait, float64(rh.maxBackoffDuration))
	}

	return time.Duration(wait)
}

func (rh *retryHandler) try(ctx context.Context, logPrefix string, f func() error) {
	try := 1
	for {
		err := f()
		if err == nil {
			return
		} else if _, ok := err.(datastore.MultiError); ok {
			return
		}
		d := rh.waitDuration(try)
		rh.logf(ctx, "%s: err=%s, will be retry #%d after %s", logPrefix, err.Error(), try, d.String())
		time.Sleep(d)
		if rh.retryLimit <= try {
			break
		}
		try++
	}
}

func (rh *retryHandler) AllocateIDs(info *datastore.MiddlewareInfo, keys []datastore.Key) (retKeys []datastore.Key, retErr error) {
	next := info.Next
	rh.try(info.Context, "middleware/rpcretry.AllocateIDs", func() error {
		retKeys, retErr = next.AllocateIDs(info, keys)
		return retErr
	})
	return
}

func (rh *retryHandler) PutMultiWithoutTx(info *datastore.MiddlewareInfo, keys []datastore.Key, psList []datastore.PropertyList) (retKeys []datastore.Key, retErr error) {
	next := info.Next
	rh.try(info.Context, "middleware/rpcretry.PutMultiWithoutTx", func() error {
		retKeys, retErr = next.PutMultiWithoutTx(info, keys, psList)
		return retErr
	})
	return
}

func (rh *retryHandler) PutMultiWithTx(info *datastore.MiddlewareInfo, keys []datastore.Key, psList []datastore.PropertyList) (retPKeys []datastore.PendingKey, retErr error) {
	next := info.Next
	rh.try(info.Context, "middleware/rpcretry.PutMultiWithTx", func() error {
		retPKeys, retErr = next.PutMultiWithTx(info, keys, psList)
		return retErr
	})
	return
}

func (rh *retryHandler) GetMultiWithoutTx(info *datastore.MiddlewareInfo, keys []datastore.Key, psList []datastore.PropertyList) (retErr error) {
	next := info.Next
	rh.try(info.Context, "middleware/rpcretry.GetMultiWithoutTx", func() error {
		retErr = next.GetMultiWithoutTx(info, keys, psList)
		return retErr
	})
	return
}

func (rh *retryHandler) GetMultiWithTx(info *datastore.MiddlewareInfo, keys []datastore.Key, psList []datastore.PropertyList) (retErr error) {
	next := info.Next
	rh.try(info.Context, "middleware/rpcretry.GetMultiWithTx", func() error {
		retErr = next.GetMultiWithTx(info, keys, psList)
		return retErr
	})
	return
}

func (rh *retryHandler) DeleteMultiWithoutTx(info *datastore.MiddlewareInfo, keys []datastore.Key) (retErr error) {
	next := info.Next
	rh.try(info.Context, "middleware/rpcretry.DeleteMultiWithoutTx", func() error {
		retErr = next.DeleteMultiWithoutTx(info, keys)
		return retErr
	})
	return
}

func (rh *retryHandler) DeleteMultiWithTx(info *datastore.MiddlewareInfo, keys []datastore.Key) (retErr error) {
	next := info.Next
	rh.try(info.Context, "middleware/rpcretry.DeleteMultiWithTx", func() error {
		retErr = next.DeleteMultiWithTx(info, keys)
		return retErr
	})
	return
}

func (rh *retryHandler) PostCommit(info *datastore.MiddlewareInfo, tx datastore.Transaction, commit datastore.Commit) (retErr error) {
	return info.Next.PostCommit(info, tx, commit)
}

func (rh *retryHandler) PostRollback(info *datastore.MiddlewareInfo, tx datastore.Transaction) (retErr error) {
	return info.Next.PostRollback(info, tx)
}

func (rh *retryHandler) Run(info *datastore.MiddlewareInfo, q datastore.Query, qDump *datastore.QueryDump) datastore.Iterator {
	return info.Next.Run(info, q, qDump)
}

func (rh *retryHandler) GetAll(info *datastore.MiddlewareInfo, q datastore.Query, qDump *datastore.QueryDump, psList *[]datastore.PropertyList) (retKeys []datastore.Key, retErr error) {
	next := info.Next
	rh.try(info.Context, "middleware/rpcretry.GetAll", func() error {
		retKeys, retErr = next.GetAll(info, q, qDump, psList)
		return retErr
	})
	return
}

func (rh *retryHandler) Next(info *datastore.MiddlewareInfo, q datastore.Query, qDump *datastore.QueryDump, iter datastore.Iterator, ps *datastore.PropertyList) (datastore.Key, error) {
	// Next is not idempotent
	return info.Next.Next(info, q, qDump, iter, ps)
}

func (rh *retryHandler) Count(info *datastore.MiddlewareInfo, q datastore.Query, qDump *datastore.QueryDump) (retCnt int, retErr error) {
	next := info.Next
	rh.try(info.Context, "middleware/rpcretry.Count", func() error {
		retCnt, retErr = next.Count(info, q, qDump)
		return retErr
	})
	return
}
