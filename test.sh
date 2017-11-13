#!/bin/sh -eux

targets=`find . -type f \( -name '*.go' -and -not -iwholename '*vendor*' -and -not -iwholename '*testdata*' \)`

# Apply tools
./build-cmd/goimports -w $targets
go tool vet $targets
# ./build-cmd/golint $(go list ./...)
./build-cmd/staticcheck $(go list ./...)
# ./build-cmd/gosimple $(go list ./...)
./build-cmd/unused $(go list ./...)

# Testing in local env
if [ "${CI:=''}" != "true" ]; then
  $(gcloud beta emulators datastore env-init)
else
  export DATASTORE_EMULATOR_HOST=localhost:8081
fi
goapp test $(go list ./...) $@
if [ "${CI:=''}" != "true" ]; then
  $(gcloud beta emulators datastore env-unset)
fi

# Connect Cloud Datastore (production env)
# (if you need login) → gcloud auth application-default login
# go test $(go list ./...) $@
