#!/bin/sh -eux

cd `dirname $0`

targets=`find . -type f \( -name '*.go' -and -not -iwholename '*vendor*' -and -not -iwholename '*testdata*' \)`
packages=`go list ./... | grep -v internal/c | grep -v internal/pb`
packages_wo_internal=`go list ./... | grep -v internal`

# Apply tools
export PATH=$(pwd)/build-cmd:$PATH
which goimports golint staticcheck jwg qbg
goimports -w $targets
for package in $packages
do
    go vet $package
done
golint -set_exit_status -min_confidence 0.6 $packages_wo_internal
staticcheck $packages
go generate $packages

export DATASTORE_EMULATOR_HOST=localhost:8081
export DATASTORE_PROJECT_ID=datastore-wrapper
export REDIS_HOST=
export REDIS_PORT=6379
export MEMCACHE_ADDR=localhost:11211

# use -p 1. Cloud Datastore Emulator can't dedicated by connections. go will running package concurrently.
# goapp test $packages -p 1 $@

# Connect Cloud Datastore (production env)
# (if you need login) → gcloud auth application-default login
go test $packages -count 1 -p 1 -coverpkg=./... -covermode=atomic -coverprofile=coverage.txt $@
