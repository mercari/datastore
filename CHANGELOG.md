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
