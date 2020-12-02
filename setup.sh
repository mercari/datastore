#!/bin/bash -eux

cd `dirname $0`

# install or fetch dependencies
# gcloud components install --quiet app-engine-go
# gcloud components install --quiet beta cloud-datastore-emulator
go mod download

# build tools
rm -rf build-cmd/
mkdir build-cmd

export GOBIN=`pwd -P`/build-cmd
go install golang.org/x/tools/cmd/goimports
go install golang.org/x/lint/golint
go install github.com/favclip/jwg/cmd/jwg
go install github.com/favclip/qbg/cmd/qbg

# copy utils from other repo
rm -rf   internal/c internal/pb
mkdir -p internal/c internal/pb

cp -r $(go list -f '{{ .Dir }}' -m cloud.google.com/go)/internal/fields internal/c/fields
cp -r $(go list -f '{{ .Dir }}' -m google.golang.org/appengine)/internal/memcache internal/pb/memcache

# go mod files have 0111
chmod -R a+w internal/c
chmod -R a+w internal/pb
rm -rf internal/c/**/*_test.go
rm -rf internal/c/**/*.sh
