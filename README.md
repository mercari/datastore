# Datastore Wrapper [![Go Documentation](http://img.shields.io/badge/go-documentation-blue.svg?style=flat-square)][godoc] [![CI](https://github.com/mercari/datastore/actions/workflows/cicd.yml/badge.svg)](https://github.com/mercari/datastore/actions/workflows/cicd.yml)

[godoc]: https://pkg.go.dev/go.mercari.io/datastore/v2

(AppEngine | Cloud) Datastore wrapper for Go 👉

Simple.
Happy.
Respect standard library.

```
$ go get -u go.mercari.io/datastore/v2
```

see [v2 doc](https://pkg.go.dev/go.mercari.io/datastore/v2) at first.

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
* Add retry feature to each RPC
    * e.g. Retry AllocateID when it failed
* Add middleware layer
    * About...
        * Local Cache
        * AE Memcache
        * Logging
        * Retry
        * etc...
    * Easy to ON/OFF switching
* Add some useful methods
    * `aedatastore/TransactionContext`

### DON'T

* have utility functions
* support firestore

## Restriction

* `aedatastore` package
    * When using slice of struct, MUST specified `datastore:",flatten"` option.
        * original (ae & cloud) datastore.SaveStruct have different behaviors.
        * see aeprodtest/main.go `/api/test3`

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

1. Testing in local

```
$ ./setup.sh # exec once
$ ./serve.sh # exec in background
$ ./test.sh
```

## License

Copyright 2017 Mercari, Inc.

Licensed under the MIT License.
