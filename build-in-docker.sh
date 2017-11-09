#!/bin/bash -eux

docker build -t mercari/datastore .
docker run -t --rm \
  -v $(pwd):/go/src/go.mercari.io/datastore \
  mercari/datastore \
  /bin/bash -ci "cd /go/src/go.mercari.io/datastore && ./setup.sh && (./serve.sh > /dev/null 2>&1 &) && ./test.sh"
