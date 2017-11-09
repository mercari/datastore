#!/bin/sh -eux

projectID=${PROJECT_ID:=$(gcloud config get-value core/project)}
echo $projectID
appcfg.py update --application=${projectID} --version=datastore-test ./app.yaml
