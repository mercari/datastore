FROM google/cloud-sdk:178.0.0
LABEL maintainer="vvakame@mercari.com"

# AppEngine Datastore & Cloud Datastore testing environment

ENV GOLANG_VERSION 1.9.2

# setup go environment
ENV PATH=$PATH:/go/bin:/usr/local/go/bin
ENV GOPATH=/go
RUN curl -o go.tar.gz https://storage.googleapis.com/golang/go${GOLANG_VERSION}.linux-amd64.tar.gz && \
    tar -zxvf go.tar.gz && \
    mv go /usr/local && \
    rm go.tar.gz

# setup ae environment
ENV PATH=$PATH:/usr/lib/google-cloud-sdk/platform/google_appengine
RUN chmod +x /usr/lib/google-cloud-sdk/platform/google_appengine/goapp
