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
	"context"
	"crypto/x509"
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

	// create a certificate pool, then add the root CA into it
	myCAPath := "./testdata/RootCA.crt"
	if myCAPath == "" {
		return confmap.Retrieved{}, fmt.Errorf("unable to fetch the Root CA for uri %q", uri)
	}
	pool, err := x509.SystemCertPool()
	if err != nil {
		return confmap.Retrieved{}, fmt.Errorf("unable to create a cert pool")
	}
	crt, err := ioutil.ReadFile(myCAPath)
	if err != nil {
		return confmap.Retrieved{}, fmt.Errorf("unable to read CA from uri %q", myCAPath)
	}
	if ok := pool.AppendCertsFromPEM(crt); !ok {
		return confmap.Retrieved{}, fmt.Errorf("unable to add CA from uri %q into the cert pool", myCAPath)
	}

	// mock a HTTPS server via httptest
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

// testRetrieve: Mock how Retrieve() works when the returned config file is invalid
type testInvalidRetrieve struct{}

func NewTestInvalidRetrieve() confmap.Provider {
	return &testInvalidRetrieve{}
}

func (fp *testInvalidRetrieve) Retrieve(ctx context.Context, uri string, watcher confmap.WatcherFunc) (confmap.Retrieved, error) {
	if !strings.HasPrefix(uri, schemeName+"://") {
		return confmap.Retrieved{}, fmt.Errorf("%q uri is not supported by %q provider", uri, schemeName)
	}

	// create a certificate pool, then add the root CA into it
	myCAPath := "./testdata/RootCA.crt"
	if myCAPath == "" {
		return confmap.Retrieved{}, fmt.Errorf("unable to fetch the Root CA for uri %q", uri)
	}
	pool, err := x509.SystemCertPool()
	if err != nil {
		return confmap.Retrieved{}, fmt.Errorf("unable to create a cert pool")
	}
	crt, err := ioutil.ReadFile(myCAPath)
	if err != nil {
		return confmap.Retrieved{}, fmt.Errorf("unable to read CA from uri %q", myCAPath)
	}
	if ok := pool.AppendCertsFromPEM(crt); !ok {
		return confmap.Retrieved{}, fmt.Errorf("unable to add CA from uri %q into the cert pool", myCAPath)
	}

	// mock a HTTPS server via httptest
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

// testRetrieve: Mock how Retrieve() works when there is no corresponding config file according to the given http-uri
type testNonExistRetrieve struct{}

func NewTestNonExistRetrieve() confmap.Provider {
	return &testNonExistRetrieve{}
}

func (fp *testNonExistRetrieve) Retrieve(ctx context.Context, uri string, watcher confmap.WatcherFunc) (confmap.Retrieved, error) {
	if !strings.HasPrefix(uri, schemeName+"://") {
		return confmap.Retrieved{}, fmt.Errorf("%q uri is not supported by %q provider", uri, schemeName)
	}

	// create a certificate pool, then add the root CA into it
	myCAPath := "./testdata/RootCA.crt"
	if myCAPath == "" {
		return confmap.Retrieved{}, fmt.Errorf("unable to fetch the Root CA for uri %q", uri)
	}
	pool, err := x509.SystemCertPool()
	if err != nil {
		return confmap.Retrieved{}, fmt.Errorf("unable to create a cert pool")
	}
	crt, err := ioutil.ReadFile(myCAPath)
	if err != nil {
		return confmap.Retrieved{}, fmt.Errorf("unable to read CA from uri %q", myCAPath)
	}
	if ok := pool.AppendCertsFromPEM(crt); !ok {
		return confmap.Retrieved{}, fmt.Errorf("unable to add CA from uri %q into the cert pool", myCAPath)
	}

	// mock a HTTPS server via httptest
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

// testRetrieve: Mock how Retrieve() works when the RootCA is not from a trusted CA
type testNoRootCARetrieve struct{}

func NewTestNoRootCARetrieve() confmap.Provider {
	return &testNoRootCARetrieve{}
}

func (fp *testNoRootCARetrieve) Retrieve(ctx context.Context, uri string, watcher confmap.WatcherFunc) (confmap.Retrieved, error) {
	if !strings.HasPrefix(uri, schemeName+"://") {
		return confmap.Retrieved{}, fmt.Errorf("%q uri is not supported by %q provider", uri, schemeName)
	}

	// create a certificate pool, then add the root CA into it
	myCAPath := "./testdata/nontrusted-RootCA.crt"
	if myCAPath == "" {
		return confmap.Retrieved{}, fmt.Errorf("unable to fetch the Root CA for uri %q", uri)
	}
	pool, err := x509.SystemCertPool()
	if err != nil {
		return confmap.Retrieved{}, fmt.Errorf("unable to create a cert pool")
	}
	crt, err := ioutil.ReadFile(myCAPath)
	if err != nil {
		return confmap.Retrieved{}, fmt.Errorf("unable to read CA from uri %q", myCAPath)
	}
	if ok := pool.AppendCertsFromPEM(crt); !ok {
		return confmap.Retrieved{}, fmt.Errorf("unable to add CA from uri %q into the cert pool", myCAPath)
	}

	// mock a HTTPS server via httptest
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

func (fp *testNoRootCARetrieve) Scheme() string {
	return schemeName
}

func (fp *testNoRootCARetrieve) Shutdown(context.Context) error {
	return nil
}

func TestFunctionalityDownloadFileHTTPS(t *testing.T) {
	fp := NewTestRetrieve()
	_, err := fp.Retrieve(context.Background(), "https://...", nil)
	assert.NoError(t, err)
	assert.NoError(t, fp.Shutdown(context.Background()))
}

func TestUnsupportedScheme(t *testing.T) {
	fp := NewTestRetrieve()
	_, err := fp.Retrieve(context.Background(), "s3://...", nil)
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
	_, err := fp.Retrieve(context.Background(), "https://non-exist-domain/default-config.yaml", nil)
	assert.Error(t, err)
	require.NoError(t, fp.Shutdown(context.Background()))
}

func TestInvalidYAML(t *testing.T) {
	fp := NewTestInvalidRetrieve()
	_, err := fp.Retrieve(context.Background(), "https://.../invalidConfig", nil)
	require.Error(t, err)
	require.NoError(t, fp.Shutdown(context.Background()))
}

func TestNoRootCA(t *testing.T) {
	fp := NewTestNoRootCARetrieve()
	_, err := fp.Retrieve(context.Background(), "https://...", nil)
	require.Error(t, err)
	require.NoError(t, fp.Shutdown(context.Background()))
}

func TestScheme(t *testing.T) {
	fp := NewTestRetrieve()
	assert.Equal(t, "https", fp.Scheme())
	require.NoError(t, fp.Shutdown(context.Background()))
}
