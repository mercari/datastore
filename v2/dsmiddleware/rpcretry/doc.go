/*
Package rpcretry automatically retries when some RPCs end in error.
RPC sometimes fails rarely and this may be able to recover simply by retrying.

Non idempotency operations (Commit, Rollback, and Next) are not automatically retried.

By default, it retries up to 3 times.
First wait 100 milliseconds, then wait exponentially back off.
This value can be changed by option.
*/
package rpcretry // import "go.mercari.io/datastore/v2/dsmiddleware/rpcretry"
