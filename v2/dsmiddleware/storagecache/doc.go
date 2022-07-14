/*
Package storagecache provides a mechanism for using various storage as datastore's cache.
This package will not be used directly, but it will be used via aememcache or redicache.

This package automatically processes so that the cache state matches the Entity on Datastore.

The main problem is transactions.
Do not read from cache under transaction.
Under a transaction it should not be added to the cache until it is committed or rolled back.

In order to avoid troublesome bugs, under the transaction, Get, Put and Delete only record the Key,
delete all caches related to the Key recorded when committed.

For now, no caching is made for the Entity that returned from the query.
If you want to cache it, there is a way to query with KeysOnly first, and exec GetMulti next.

In all operations, the key target is determined by KeyFilter.
In order to make consistency easy, we recommend using the same settings throughout the application.
*/
package storagecache // import "go.mercari.io/datastore/v2/dsmiddleware/storagecache"
