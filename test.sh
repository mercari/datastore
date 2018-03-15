#!/bin/sh -eux

targets=`find . -type f \( -name '*.go' -and -not -iwholename '*vendor*' -and -not -iwholename '*testdata*' \)`
packages=`go list ./... | grep -v internal/c | grep -v internal/pb`

# Apply tools
export PATH=$(pwd)/build-cmd:$PATH
which goimports golint staticcheck gosimple unused jwg qbg
goimports -w $targets
go tool vet $targets
# golint $packages
staticcheck $packages
gosimple $packages
unused $packages
go generate $packages

export DATASTORE_EMULATOR_HOST=localhost:8081
export DATASTORE_PROJECT_ID=datastore-wrapper
export REDIS_HOST=
export REDIS_PORT=6379

# use -p 1. Cloud Datastore Emulator can't dedicated by connections. go will running package concurrently.
goapp test $packages -p 1 $@

# Connect Cloud Datastore (production env)
# (if you need login) → gcloud auth application-default login
# go test $packages -p 1 $@
