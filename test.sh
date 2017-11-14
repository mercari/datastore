#!/bin/sh -eux

targets=`find . -type f \( -name '*.go' -and -not -iwholename '*vendor*' -and -not -iwholename '*testdata*' \)`
packages=`go list ./...`

# Apply tools
./build-cmd/goimports -w $targets
go tool vet $targets
# ./build-cmd/golint $packages
./build-cmd/staticcheck $packages
# ./build-cmd/gosimple $packages
./build-cmd/unused $packages

# Testing in local env
if [ "${CI:=''}" != "true" ]; then
  $(gcloud beta emulators datastore env-init)
else
  export DATASTORE_EMULATOR_HOST=localhost:8081
fi
# use -p 1. Cloud Datastore Emulator can't dedicated by connections. go will running package concurrently.
goapp test $packages -p 1 $@
if [ "${CI:=''}" != "true" ]; then
  $(gcloud beta emulators datastore env-unset)
fi

# Connect Cloud Datastore (production env)
# (if you need login) → gcloud auth application-default login
# go test $packages -p 1 $@
