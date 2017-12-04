#!/bin/sh -eux

# install or fetch dependencies
# gcloud components install --quiet app-engine-go
# gcloud components install --quiet beta cloud-datastore-emulator
go get -u github.com/golang/dep/cmd/dep
dep ensure

# build tools
rm -rf build-cmd/
mkdir build-cmd
go build -o build-cmd/goimports   ./vendor/golang.org/x/tools/cmd/goimports
go build -o build-cmd/golint      ./vendor/github.com/golang/lint/golint
go build -o build-cmd/gosimple    ./vendor/honnef.co/go/tools/cmd/gosimple
go build -o build-cmd/staticcheck ./vendor/honnef.co/go/tools/cmd/staticcheck
go build -o build-cmd/unused      ./vendor/honnef.co/go/tools/cmd/unused
go build -o build-cmd/jwg         ./vendor/github.com/favclip/jwg/cmd/jwg
go build -o build-cmd/qbg         ./vendor/github.com/favclip/qbg/cmd/qbg

# copy utils from other repo
rm -rf   internal/c internal/pb
mkdir -p internal/c internal/pb

cp -r vendor/cloud.google.com/go/internal/atomiccache internal/c/atomiccache
cp -r vendor/cloud.google.com/go/internal/fields internal/c/fields
cp -r vendor/google.golang.org/appengine/internal/memcache internal/pb/memcache

if [ `uname` = "Darwin" ]; then
  sed -i '' -e 's/"cloud.google.com\/go\/internal\/atomiccache"/"go.mercari.io\/datastore\/internal\/c\/atomiccache"/g' internal/c/fields/fields.go
elif [ `uname` = "Linux" ]; then
  sed -i -e 's/"cloud.google.com\/go\/internal\/atomiccache"/"go.mercari.io\/datastore\/internal\/c\/atomiccache"/g' internal/c/fields/fields.go
fi

rm -rf internal/c/**/*_test.go
rm -rf internal/c/**/*.sh
