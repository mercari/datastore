<a name="0.17.0"></a>
# [0.17.0](https://github.com/mercari/datastore/compare/v0.16.0...v0.17.0) (2018-03-27)


### Bug Fixes

* **all:** fix method name AllocatedIDs to AllocateID 🙇 ([68408f8](https://github.com/mercari/datastore/commit/68408f8))


### Features

* **ci:** update CI and local testing environment ([35c8f7a](https://github.com/mercari/datastore/commit/35c8f7a))


### BREAKING CHANGES

Replace AllocatedIDs to AllocateID. align to original libraries 🙇


<a name="0.16.0"></a>
# [0.16.0](https://github.com/mercari/datastore/compare/v0.15.0...v0.16.0) (2018-01-24)


### Features

* **core:** add Key#SetNamespace method ([56f6294](https://github.com/mercari/datastore/commit/56f6294))
* **dsmiddleware/aememcache,dsmiddleware/localcache,dsmiddleware/rediscache,dsmiddleware/storagecache:** add context.Context parameter to Key filter function ([7f8d7f7](https://github.com/mercari/datastore/commit/7f8d7f7))

### BREAKING CHANGES

Change KeyFilter function signature `func(key datastore.Key) bool` to `func(ctx context.Context, key datastore.Key) bool` .



<a name="0.15.0"></a>
# [0.15.0](https://github.com/mercari/datastore/compare/v0.14.0...v0.15.0) (2018-01-09)


### Features

* **dsmiddleware/aememcache,dsmiddleware/localcache,dsmiddleware/rediscache,dsmiddleware/storagecache:** change options format ([5af7561](https://github.com/mercari/datastore/commit/5af7561))

### BREAKING CHANGES

Change cache middleware signatures.

<a name="0.14.0"></a>
# [0.14.0](https://github.com/mercari/datastore/compare/v0.13.0...v0.14.0) (2018-01-09)


### Features

* **boom:** add Boom() and Transaction() method to each boom objects ([3c680d1](https://github.com/mercari/datastore/commit/3c680d1))
* **core:** add AllocateIDs & Count method to Middleware interface ([f548cca](https://github.com/mercari/datastore/commit/f548cca))
* **core:** replace SwapContext to Context & SetContext ([4b9ccaa](https://github.com/mercari/datastore/commit/4b9ccaa))

### BREAKING CHANGES

replace datastore.Client#SwapContext to datastore.Client#Context & datastore.Client#SetContext.


<a name="0.13.0"></a>
# [0.13.0](https://github.com/mercari/datastore/compare/v0.12.0...v0.13.0) (2017-12-19)


### Features

* **ci:** add redis sidecar container ([bc9908a](https://github.com/mercari/datastore/commit/bc9908a))
* **dsmiddleware/aememcache:** change to display both hit and miss to logging ([257064b](https://github.com/mercari/datastore/commit/257064b))
* **dsmiddleware/rediscache:** add dsmiddleware/rediscache package ([04cf0cb](https://github.com/mercari/datastore/commit/04cf0cb))



<a name="0.12.0"></a>
# [0.12.0](https://github.com/mercari/datastore/compare/v0.11.0...v0.12.0) (2017-12-13)


### Features

* **dsmiddleware/chaosrpc:** add dsmiddleware/chaosrpc middleware for testing ([7da792f](https://github.com/mercari/datastore/commit/7da792f))
* **dsmiddleware/noop:** add dsmiddleware/noop middleware ([5c5af95](https://github.com/mercari/datastore/commit/5c5af95))
* **dsmiddleware/rpcretry:** add dsmiddleware/rpcretry middleware ([17c5b17](https://github.com/mercari/datastore/commit/17c5b17))



<a name="0.11.0"></a>
# [0.11.0](https://github.com/mercari/datastore/compare/v0.10.1...v0.11.0) (2017-12-13)


### Features

* **middleware:** rename CacheStrategy to Middleware & move cache dir to dsmiddleware dir ([ae339b9](https://github.com/mercari/datastore/commit/ae339b9))

### BREAKING CHANGES

refactoring cache layer to middleware layer.


<a name="0.10.1"></a>
## [0.10.1](https://github.com/mercari/datastore/compare/v0.10.0...v0.10.1) (2017-12-12)


### Bug Fixes

* **core:** fix deadlock when recursive batch calling ([5162647](https://github.com/mercari/datastore/commit/5162647))



<a name="0.10.0"></a>
# [0.10.0](https://github.com/mercari/datastore/compare/v0.9.0...v0.10.0) (2017-12-07)


### Bug Fixes

* **cache/aememcache:** skip entity when gob encode & decode error occured ([2c3f8da](https://github.com/mercari/datastore/commit/2c3f8da))
* **core:** change order of application about CacheStrategy to first in - first apply ([231f40b](https://github.com/mercari/datastore/commit/231f40b))

### BREAKING CHANGES

Change the order of application of CacheStrategy first in - last apply to first in - first apply.


<a name="0.9.0"></a>
# [0.9.0](https://github.com/mercari/datastore/compare/v0.8.2...v0.9.0) (2017-12-06)

### Features

* **core,boom:** change batch operation signatures ([51da3ba](https://github.com/mercari/datastore/commit/51da3ba))

### BREAKING CHANGES

For batch processing, we stopped asynchronous processing using chan and switched to synchronous processing using callback function.


<a name="0.8.2"></a>
## [0.8.2](https://github.com/mercari/datastore/compare/v0.8.1...v0.8.2) (2017-12-06)


### Bug Fixes

* **boom:** fix PendingKey handling fixes [#30](https://github.com/mercari/datastore/issues/30) thanks [@sinmetal](https://github.com/sinmetal) ([eaa5729](https://github.com/mercari/datastore/commit/eaa5729))
* **cache/storagecache:** fix MultiError handling that ErrNoSuchEntity contaminated ([d42850b](https://github.com/mercari/datastore/commit/d42850b))



<a name="0.8.1"></a>
## [0.8.1](https://github.com/mercari/datastore/compare/v0.8.0...v0.8.1) (2017-12-05)


### Bug Fixes

* **core:** fix time.Time's default location. fit to Cloud Datastore behaviour ([4226d8f](https://github.com/mercari/datastore/commit/4226d8f))



<a name="0.8.0"></a>
# [0.8.0](https://github.com/mercari/datastore/compare/v0.7.0...v0.8.0) (2017-12-04)


### Features

* **cache/storagecache:** implement WithIncludeKinds, WithExcludeKinds, WithKeyFilter options ([a8b5857](https://github.com/mercari/datastore/commit/a8b5857))



<a name="0.7.0"></a>
# [0.7.0](https://github.com/mercari/datastore/compare/v0.6.0...v0.7.0) (2017-12-04)


### Features

* **cache** implement cache layer & cache strategies ([203ab21](https://github.com/mercari/datastore/commit/203ab21))
* **core,ae,cloud:** add datastore#Client.DecodeKey method ([42fa040](https://github.com/mercari/datastore/commit/42fa040))



<a name="0.6.0"></a>
# [0.6.0](https://github.com/mercari/datastore/compare/v0.5.3...v0.6.0) (2017-11-24)


### Features

* **boom:** add NewQuery method ([a31adb0](https://github.com/mercari/datastore/commit/a31adb0)) thanks @timakin !



<a name="0.5.3"></a>
## [0.5.3](https://github.com/mercari/datastore/compare/v0.5.2...v0.5.3) (2017-11-24)


### Bug Fixes

* **ae,cloud:** fix datastore.PropertyList handling when Put & Get ([0355f35](https://github.com/mercari/datastore/commit/0355f35))
* **ae,cloud:** fix struct (without pointer) handling when Put & Get ([de3eb4c](https://github.com/mercari/datastore/commit/de3eb4c))
* **boom:** fix nil parent key handling ([7dc317b](https://github.com/mercari/datastore/commit/7dc317b))



<a name="0.5.2"></a>
## [0.5.2](https://github.com/mercari/datastore/compare/v0.5.1...v0.5.2) (2017-11-22)


### Bug Fixes

* **core:** fix datastore.Key or []datastore.Key Save & Load handling ([29f465d](https://github.com/mercari/datastore/commit/29f465d))



<a name="0.5.1"></a>
## [0.5.1](https://github.com/mercari/datastore/compare/v0.5.0...v0.5.1) (2017-11-21)


### Bug Fixes

* **boom:** fix *boom.Boom#GetAll with KeysOnly query ([420bb37](https://github.com/mercari/datastore/commit/420bb37))
* **boom:** fix *boom.Iterator#Next with KeysOnly query ([e8bbeed](https://github.com/mercari/datastore/commit/e8bbeed))



<a name="0.5.0"></a>
# [0.5.0](https://github.com/mercari/datastore/compare/v0.4.0...v0.5.0) (2017-11-21)


### Features

* **boom:** add boom.ToAECompatibleTransaction and *boom.AECompatibleTransaction ([dedb72a](https://github.com/mercari/datastore/commit/dedb72a))
* **boom:** add Kind, Key, KeyError method to *boom.Transaction ([5d5da7d](https://github.com/mercari/datastore/commit/5d5da7d))
* **core:** add Equal and Incomplete methods to datastore.Key ([5668f1b](https://github.com/mercari/datastore/commit/5668f1b))



<a name="0.4.0"></a>
# [0.4.0](https://github.com/mercari/datastore/compare/v0.3.0...v0.4.0) (2017-11-20)


### Features

* **boom:** implements AllocateID and AllocateIDs ([014e321](https://github.com/mercari/datastore/commit/014e321))
* **core:** add datastore.Client#SwapContext ([eb26e60](https://github.com/mercari/datastore/commit/eb26e60))



<a name="0.3.0"></a>
# [0.3.0](https://github.com/mercari/datastore/compare/v0.2.0...v0.3.0) (2017-11-14)


### Bug Fixes

* **boom:** improve goon compatibility ([03beb64](https://github.com/mercari/datastore/commit/03beb64))



<a name="0.2.0"></a>
# [0.2.0](https://github.com/mercari/datastore/compare/v0.1.0...v0.2.0) (2017-11-14)


### Features

* **aedatastore:** add custom import path checking ([801299f](https://github.com/mercari/datastore/commit/801299f))
* **ci:** add .circleci/config.yml ([cfc3877](https://github.com/mercari/datastore/commit/cfc3877))
* **clouddatastore:** add custom import path checking ([5585c22](https://github.com/mercari/datastore/commit/5585c22))
* **core:** align TransactionBatch api to Batch api ([3f49066](https://github.com/mercari/datastore/commit/3f49066))
* **boom:** implement boom package ([8c2ed5e](https://github.com/mercari/datastore/commit/8c2ed5e))
