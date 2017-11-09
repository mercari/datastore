// Copyright 2017 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package option contains options for Google API clients.

package datastore

import (
	"net/http"

	"go.mercari.io/datastore/internal"
	"golang.org/x/oauth2"
)

type ClientOption interface {
	Apply(*internal.ClientSettings)
}

func WithProjectID(projectID string) ClientOption {
	return withProjectID{projectID}
}

type withProjectID struct{ s string }

func (w withProjectID) Apply(o *internal.ClientSettings) {
	o.ProjectID = w.s
}

// WithTokenSource returns a ClientOption that specifies an OAuth2 token
// source to be used as the basis for authentication.
func WithTokenSource(s oauth2.TokenSource) ClientOption {
	return withTokenSource{s}
}

type withTokenSource struct{ ts oauth2.TokenSource }

func (w withTokenSource) Apply(o *internal.ClientSettings) {
	o.TokenSource = w.ts
}

type withCredFile string

func (w withCredFile) Apply(o *internal.ClientSettings) {
	o.CredentialsFile = string(w)
}

// WithCredentialsFile returns a ClientOption that authenticates
// API calls with the given service account or refresh token JSON
// credentials file.
func WithCredentialsFile(filename string) ClientOption {
	return withCredFile(filename)
}

// WithScopes returns a ClientOption that overrides the default OAuth2 scopes
// to be used for a service.
func WithScopes(scope ...string) ClientOption {
	return withScopes(scope)
}

type withScopes []string

func (w withScopes) Apply(o *internal.ClientSettings) {
	s := make([]string, len(w))
	copy(s, w)
	o.Scopes = s
}

// WithHTTPClient returns a ClientOption that specifies the HTTP client to use
// as the basis of communications. This option may only be used with services
// that support HTTP as their communication transport. When used, the
// WithHTTPClient option takes precedent over all other supplied options.
func WithHTTPClient(client *http.Client) ClientOption {
	return withHTTPClient{client}
}

type withHTTPClient struct{ client *http.Client }

func (w withHTTPClient) Apply(o *internal.ClientSettings) {
	o.HTTPClient = w.client
}
