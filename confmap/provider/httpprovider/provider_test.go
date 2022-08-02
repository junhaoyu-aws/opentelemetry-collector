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
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/provider/internal"
)

// testRetrieve: Mock how Retrieve() works in normal cases
type testRetrieve struct{}

func NewTestRetrieve() confmap.Provider {
	return &testRetrieve{}
}

func (fp *testRetrieve) Retrieve(ctx context.Context, uri string, watcher confmap.WatcherFunc) (confmap.Retrieved, error) {
	if !strings.HasPrefix(uri, schemeName+"://") {
		return confmap.Retrieved{}, fmt.Errorf("%q uri is not supported by %q provider", uri, schemeName)
	}

	// mock a HTTP server via httptest
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f, err := ioutil.ReadFile("./testdata/otel-config.yaml")
		if err != nil {
			log.Fatal("HTTP server fails to read config file and return")
		}
		w.WriteHeader(200)
		w.Write(f)
	})

	// get request
	req := httptest.NewRequest("GET", uri, nil)
	w := httptest.NewRecorder()
	handler(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	// read the response body
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return confmap.Retrieved{}, fmt.Errorf("fail to read the response body from uri %q", uri)
	}

	return internal.NewRetrievedFromYAML(body)
}

func (fp *testRetrieve) Scheme() string {
	return schemeName
}

func (fp *testRetrieve) Shutdown(context.Context) error {
	return nil
}

// testInvalidRetrieve: Mock how Retrieve() works when the returned config file is invalid
type testInvalidRetrieve struct{}

func NewTestInvalidRetrieve() confmap.Provider {
	return &testInvalidRetrieve{}
}

func (fp *testInvalidRetrieve) Retrieve(ctx context.Context, uri string, watcher confmap.WatcherFunc) (confmap.Retrieved, error) {
	if !strings.HasPrefix(uri, schemeName+"://") {
		return confmap.Retrieved{}, fmt.Errorf("%q uri is not supported by %q provider", uri, schemeName)
	}

	// mock a HTTP server via httptest
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f := []byte("wrong yaml:[")
		w.WriteHeader(200)
		w.Write(f)
	})

	// get request
	req := httptest.NewRequest("GET", uri, nil)
	w := httptest.NewRecorder()
	handler(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	// read the response body
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return confmap.Retrieved{}, fmt.Errorf("fail to read the response body from uri %q", uri)
	}

	return internal.NewRetrievedFromYAML(body)
}

func (fp *testInvalidRetrieve) Scheme() string {
	return schemeName
}

func (fp *testInvalidRetrieve) Shutdown(context.Context) error {
	return nil
}

// testNonExistRetrieve: Mock how Retrieve() works when there is no corresponding config file according to the given http-uri
type testNonExistRetrieve struct{}

func NewTestNonExistRetrieve() confmap.Provider {
	return &testNonExistRetrieve{}
}

func (fp *testNonExistRetrieve) Retrieve(ctx context.Context, uri string, watcher confmap.WatcherFunc) (confmap.Retrieved, error) {
	if !strings.HasPrefix(uri, schemeName+"://") {
		return confmap.Retrieved{}, fmt.Errorf("%q uri is not supported by %q provider", uri, schemeName)
	}

	// mock a HTTP server via httptest
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f, err := ioutil.ReadFile("./testdata/nonexist-otel-config.yaml")
		if err != nil {
			w.WriteHeader(404)
			return
		}
		w.WriteHeader(200)
		w.Write(f)
	})

	// get request
	req := httptest.NewRequest("GET", uri, nil)
	w := httptest.NewRecorder()
	handler(w, req)

	resp := w.Result()
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

func (fp *testNonExistRetrieve) Scheme() string {
	return schemeName
}

func (fp *testNonExistRetrieve) Shutdown(context.Context) error {
	return nil
}

func TestFunctionalityDownloadFileHTTP(t *testing.T) {
	fp := NewTestRetrieve()
	_, err := fp.Retrieve(context.Background(), "http://...", nil)
	assert.NoError(t, err)
	assert.NoError(t, fp.Shutdown(context.Background()))
}

func TestUnsupportedScheme(t *testing.T) {
	fp := NewTestRetrieve()
	_, err := fp.Retrieve(context.Background(), "https://google.com", nil)
	assert.Error(t, err)
	assert.NoError(t, fp.Shutdown(context.Background()))
}

func TestEmptyURI(t *testing.T) {
	fp := NewTestRetrieve()
	_, err := fp.Retrieve(context.Background(), "", nil)
	require.Error(t, err)
	require.NoError(t, fp.Shutdown(context.Background()))
}

func TestNonExistent(t *testing.T) {
	fp := NewTestNonExistRetrieve()
	_, err := fp.Retrieve(context.Background(), "http://non-exist-domain/...", nil)
	assert.Error(t, err)
	require.NoError(t, fp.Shutdown(context.Background()))
}

func TestInvalidYAML(t *testing.T) {
	fp := NewTestInvalidRetrieve()
	_, err := fp.Retrieve(context.Background(), "http://.../invalidConfig", nil)
	assert.Error(t, err)
	require.NoError(t, fp.Shutdown(context.Background()))
}

func TestScheme(t *testing.T) {
	fp := NewTestRetrieve()
	assert.Equal(t, "http", fp.Scheme())
	require.NoError(t, fp.Shutdown(context.Background()))
}
