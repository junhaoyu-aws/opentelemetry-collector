// Copyright The OpenTelemetry Authors
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

package httpsprovider

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap"
)

// A HTTP client mocking httpmapprovider works in normal cases
type testClient struct{}

// Implement Get() for testClient in normal cases
func (client *testClient) Get(url string) (resp *http.Response, err error) {
	f, err := ioutil.ReadFile("./testdata/otel-config.yaml")
	if err != nil {
		return &http.Response{StatusCode: 404, Body: io.NopCloser(strings.NewReader("Cannot find the config file"))}, nil
	}
	return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(f))}, nil
}

// Create a provider mocking httpmapprovider works in normal cases
func NewTestProvider() confmap.Provider {
	return &provider{client: &testClient{}}
}

// A HTTP client mocking httpmapprovider works when the returned config file is invalid
type testInvalidClient struct{}

// Implement Get() for testInvalidClient when the returned config file is invalid
func (client *testInvalidClient) Get(url string) (resp *http.Response, err error) {
	return &http.Response{}, fmt.Errorf("the downloaded config file is invalid")
}

// Create a provider mocking httpmapprovider works when the returned config file is invalid
func NewTestInvalidProvider() confmap.Provider {
	return &provider{client: &testInvalidClient{}}
}

// A HTTP client mocking httpmapprovider works when there is no corresponding config file according to the given https-uri
type testNonExistClient struct{}

// Implement Get() for testNonExistClient when there is no corresponding config file according to the given https-uri
func (client *testNonExistClient) Get(url string) (resp *http.Response, err error) {
	f, err := ioutil.ReadFile("./testdata/nonexist-otel-config.yaml")
	if err != nil {
		return &http.Response{StatusCode: 404, Body: io.NopCloser(strings.NewReader("Cannot find the config file"))}, nil
	}
	return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(f))}, nil
}

// Create a provider mocking httpmapprovider works when there is no corresponding config file according to the given https-uri
func NewTestNonExistProvider() confmap.Provider {
	return &provider{client: &testNonExistClient{}}
}

// A HTTP client mocking httpmapprovider works when the RootCA is not from a trusted CA
type testNoRootCAClient struct{}

// Implement Get() for testNonExistClient when the RootCA is not from a trusted CA
func (client *testNoRootCAClient) Get(url string) (resp *http.Response, err error) {
	return &http.Response{}, fmt.Errorf("invalid RootCA in local")
}

// Create a provider mocking httpmapprovider works when the RootCA is not from a trusted CA
func NewTestNoRootCAProvider() confmap.Provider {
	return &provider{client: &testNoRootCAClient{}}
}

func TestFunctionalityDownloadFileHTTPS(t *testing.T) {
	fp := NewTestProvider()
	_, err := fp.Retrieve(context.Background(), "https://...", nil)
	assert.NoError(t, err)
	assert.NoError(t, fp.Shutdown(context.Background()))
}

func TestUnsupportedScheme(t *testing.T) {
	fp := NewTestProvider()
	_, err := fp.Retrieve(context.Background(), "s3://...", nil)
	assert.Error(t, err)
	assert.NoError(t, fp.Shutdown(context.Background()))
}

func TestEmptyURI(t *testing.T) {
	fp := NewTestProvider()
	_, err := fp.Retrieve(context.Background(), "", nil)
	require.Error(t, err)
	require.NoError(t, fp.Shutdown(context.Background()))
}

func TestNonExistent(t *testing.T) {
	fp := NewTestNonExistProvider()
	_, err := fp.Retrieve(context.Background(), "https://non-exist-domain/default-config.yaml", nil)
	assert.Error(t, err)
	require.NoError(t, fp.Shutdown(context.Background()))
}

func TestInvalidYAML(t *testing.T) {
	fp := NewTestInvalidProvider()
	_, err := fp.Retrieve(context.Background(), "https://.../invalidConfig", nil)
	require.Error(t, err)
	require.NoError(t, fp.Shutdown(context.Background()))
}

func TestNoRootCA(t *testing.T) {
	fp := NewTestNoRootCAProvider()
	_, err := fp.Retrieve(context.Background(), "https://...", nil)
	require.Error(t, err)
	require.NoError(t, fp.Shutdown(context.Background()))
}

func TestScheme(t *testing.T) {
	fp := NewTestProvider()
	assert.Equal(t, "https", fp.Scheme())
	require.NoError(t, fp.Shutdown(context.Background()))
}
