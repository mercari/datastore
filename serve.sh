#!/bin/sh -eux

export CLOUDSDK_CORE_PROJECT=datastore-wrapper
gcloud beta emulators datastore start --no-store-on-disk --consistency 1.0
