# Datastore Wrapper

:construction: This package is unstable :construction: [github repo](https://github.com/mercari/datastore)

(AppEngine | Cloud) Datastore wrapper for Go 👉

Simple.
Happy.
Respect standard library.

```
$ go get -u go.mercari.io/datastore
```

## Feature

### DO

* Wrap `google.golang.org/appengine/datastore` and `cloud.google.com/go/datastore`
    * keep key behavior
    * align to `cloud.google.com/go/datastore` first
* Re-implement datastore package
* Re-implement datastore.SaveStruct & LoadStruct
    * Ignore unmapped property
    * Add PropertyTranslator interface
        * Convert types like mytime.Unix to time.Time and reverse it
        * Rename property like CreatedAt to createdAt or created_at and reverse it
* Re-implement PropertyLoadSaver
    * Pass context.Context to Save & Load method
    * It can catch entity from the cache layers as well as from the datastore
* Add retry feature to each RPC
    * e.g. Retry AllocateID when it failed
* Add cache layer uses without Tx
    * About...
        * Local Cache
        * Memcache
        * etc...
    * Easy to ON/OFF switching
* Add RPC hook layer
    * It makes implements [memvache](https://github.com/vvakame/memvache) like cache strategy
* Add some useful methods
    * ~~GetAsIntIDMap, GetAsStringIDMap~~
    * `aedatastore/TransactionContext`

### DON'T

* have utility functions
* support firestore

## Restriction

* `aedatastore` package
    * When using slice of struct, MUST specified `datastore:",flatten"` option.
        * original (ae & cloud) datastore.SaveStruct have different behaviors.
        * see aeprodtest/main.go `/api/test3`

## TODO

* Write tests for namespace
* Implement cache layers
* Retry feature

## Committers

 * Masahiro Wakame ([@vvakame](https://github.com/vvakame))

## Contribution

Please read the CLA below carefully before submitting your contribution.

https://www.mercari.com/cla/

### Setup environment & Run tests

* requirements
    * [gcloud sdk](https://cloud.google.com/sdk/docs/quickstarts)
        * `gcloud components install app-engine-go`
        * `gcloud components install beta cloud-datastore-emulator`
    * [dep](github.com/golang/dep)
        * `go get -u github.com/golang/dep/cmd/dep`

```
$ ./setup.sh # exec once
$ ./serve.sh # exec in background
$ ./test.sh
```

```
$ ./build-in-docker.sh
```

## License

Copyright 2017 Mercari, Inc.

Licensed under the MIT License.
