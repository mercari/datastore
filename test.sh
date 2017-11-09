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
$(gcloud beta emulators datastore env-init)
goapp test $(go list ./...) $@
$(gcloud beta emulators datastore env-unset)

# Connect Cloud Datastore (production env)
# (if you need login) → gcloud auth application-default login
# go test $(go list ./...) $@
