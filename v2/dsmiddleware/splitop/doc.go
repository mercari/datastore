/*
Package splitop provides a avoid Datastore's limitation.

https://cloud.google.com/datastore/docs/concepts/limits

DO

* split GetMulti operation to under 1000 entity per one action.
  * > Maximum number of keys allowed for a Lookup operation in the Cloud Datastore API : 1,000

*/
package splitop // import "go.mercari.io/datastore/v2/dsmiddleware/splitop"
