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

package httpprovider

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
	"go.opentelemetry.io/collector/confmap/provider/internal"
)

// A HTTP client mocking httpmapprovider works in normal cases
type testClient struct{}

// A provider mocking httpmapprovider works in normal cases
type testProvider struct {
	client testClient
}

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
	return &testProvider{client: testClient{}}
}

func (fp *testProvider) Retrieve(ctx context.Context, uri string, watcher confmap.WatcherFunc) (confmap.Retrieved, error) {
	if !strings.HasPrefix(uri, schemeName+"://") {
		return confmap.Retrieved{}, fmt.Errorf("%q uri is not supported by %q provider", uri, schemeName)
	}

	// get request
	resp, err := fp.client.Get(uri)
	if err != nil {
		return confmap.Retrieved{}, fmt.Errorf("unable to download the file via HTTP GET for uri %q", uri)
	}
	defer resp.Body.Close()
	// if the http status code is 404, non-exist
	if resp.StatusCode == 404 {
		return confmap.Retrieved{}, fmt.Errorf("fail to download the response body from uri %q", uri)
	}
	// read the response body
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return confmap.Retrieved{}, fmt.Errorf("fail to read the response body from uri %q", uri)
	}

	return internal.NewRetrievedFromYAML(body)
}

func (fp *testProvider) Scheme() string {
	return schemeName
}

func (fp *testProvider) Shutdown(context.Context) error {
	return nil
}

// A HTTP client mocking httpmapprovider works when the returned config file is invalid
type testInvalidClient struct{}

// A provider mocking httpmapprovider works when the returned config file is invalid
type testInvalidProvider struct {
	client testInvalidClient
}

// Implement Get() for testInvalidClient when the returned config file is invalid
func (client *testInvalidClient) Get(url string) (resp *http.Response, err error) {
	f := []byte("wrong yaml:[")
	return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(f))}, nil
}

// Create a provider mocking httpmapprovider works when the returned config file is invalid
func NewTestInvalidProvider() confmap.Provider {
	return &testInvalidProvider{client: testInvalidClient{}}
}

func (fp *testInvalidProvider) Retrieve(ctx context.Context, uri string, watcher confmap.WatcherFunc) (confmap.Retrieved, error) {
	if !strings.HasPrefix(uri, schemeName+"://") {
		return confmap.Retrieved{}, fmt.Errorf("%q uri is not supported by %q provider", uri, schemeName)
	}

	// get request
	resp, err := fp.client.Get(uri)
	if err != nil {
		return confmap.Retrieved{}, fmt.Errorf("unable to download the file via HTTP GET for uri %q", uri)
	}
	defer resp.Body.Close()
	// if the http status code is 404, non-exist
	if resp.StatusCode == 404 {
		return confmap.Retrieved{}, fmt.Errorf("fail to download the response body from uri %q", uri)
	}
	// read the response body
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return confmap.Retrieved{}, fmt.Errorf("fail to read the response body from uri %q", uri)
	}

	return internal.NewRetrievedFromYAML(body)
}

func (fp *testInvalidProvider) Scheme() string {
	return schemeName
}

func (fp *testInvalidProvider) Shutdown(context.Context) error {
	return nil
}

// A HTTP client mocking httpmapprovider works when there is no corresponding config file according to the given http-uri
type testNonExistClient struct{}

// A provider mocking httpmapprovider works when there is no corresponding config file according to the given http-uri
type testNonExistProvider struct {
	client testNonExistClient
}

// Implement Get() for testNonExistClient when there is no corresponding config file according to the given http-uri
func (client *testNonExistClient) Get(url string) (resp *http.Response, err error) {
	f, err := ioutil.ReadFile("./testdata/nonexist-otel-config.yaml")
	if err != nil {
		return &http.Response{StatusCode: 404, Body: io.NopCloser(strings.NewReader("Cannot find the config file"))}, nil
	}
	return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(f))}, nil
}

// Create a provider mocking httpmapprovider works when there is no corresponding config file according to the given http-uri
func NewTestNonExistProvider() confmap.Provider {
	return &testNonExistProvider{client: testNonExistClient{}}
}

func (fp *testNonExistProvider) Retrieve(ctx context.Context, uri string, watcher confmap.WatcherFunc) (confmap.Retrieved, error) {
	if !strings.HasPrefix(uri, schemeName+"://") {
		return confmap.Retrieved{}, fmt.Errorf("%q uri is not supported by %q provider", uri, schemeName)
	}

	// get request
	resp, err := fp.client.Get(uri)
	if err != nil {
		return confmap.Retrieved{}, fmt.Errorf("unable to download the file via HTTP GET for uri %q", uri)
	}
	defer resp.Body.Close()
	// if the http status code is 404, non-exist
	if resp.StatusCode == 404 {
		return confmap.Retrieved{}, fmt.Errorf("fail to download the response body from uri %q", uri)
	}
	// read the response body
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return confmap.Retrieved{}, fmt.Errorf("fail to read the response body from uri %q", uri)
	}

	return internal.NewRetrievedFromYAML(body)
}

func (fp *testNonExistProvider) Scheme() string {
	return schemeName
}

func (fp *testNonExistProvider) Shutdown(context.Context) error {
	return nil
}

func TestFunctionalityDownloadFileHTTP(t *testing.T) {
	fp := NewTestProvider()
	_, err := fp.Retrieve(context.Background(), "http://...", nil)
	assert.NoError(t, err)
	assert.NoError(t, fp.Shutdown(context.Background()))
}

func TestUnsupportedScheme(t *testing.T) {
	fp := NewTestProvider()
	_, err := fp.Retrieve(context.Background(), "https://google.com", nil)
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
	_, err := fp.Retrieve(context.Background(), "http://non-exist-domain/...", nil)
	assert.Error(t, err)
	require.NoError(t, fp.Shutdown(context.Background()))
}

func TestInvalidYAML(t *testing.T) {
	fp := NewTestInvalidProvider()
	_, err := fp.Retrieve(context.Background(), "http://.../invalidConfig", nil)
	assert.Error(t, err)
	require.NoError(t, fp.Shutdown(context.Background()))
}

func TestScheme(t *testing.T) {
	fp := NewTestProvider()
	assert.Equal(t, "http", fp.Scheme())
	require.NoError(t, fp.Shutdown(context.Background()))
}
