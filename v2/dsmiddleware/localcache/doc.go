/*
Package localcache handles Put, Get etc to Datastore and provides caching by machine local memory.
How the cache is used is explained in the storagecache package's document.

The local cache can not be deleted from other machines.
Therefore, if the cache holding period becomes long, there is a possibility that the data is old.
As a countermeasure, we recommend keeping the lifetime of the cache as long as processing one request.
*/
package localcache // import "go.mercari.io/datastore/v2/dsmiddleware/localcache"
