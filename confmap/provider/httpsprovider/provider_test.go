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
	"crypto/x509"
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

// testRetrieve and testClient: Mock how Retrieve() and HTTPS client works in normal cases
type testClient struct {
	Get func(uri string) (resp *http.Response, err error)
}

func NewTestClient() *testClient {
	return &testClient{
		Get: func(uri string) (resp *http.Response, err error) {
			// read local config file and return
			f, err := ioutil.ReadFile("../../testdata/config.yaml")
			if err != nil {
				return &http.Response{}, err
			}
			return &http.Response{Body: io.NopCloser(bytes.NewReader(f))}, nil
		}}
}

type testRetrieve struct {
	httpsClient *testClient
}

func NewTestRetrieve() confmap.Provider {
	return &testRetrieve{httpsClient: NewTestClient()}
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

	// https client
	httpsClient := fp.httpsClient

	//GET
	r, err := httpsClient.Get(uri)
	if err != nil {
		return confmap.Retrieved{}, fmt.Errorf("unable to download the file via HTTPS GET for uri %q", uri)
	}
	defer r.Body.Close()

	body, err := ioutil.ReadAll(r.Body)
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

// testInvalidClient and testInvalidRetrieve: Mock how Retrieve() and HTTPS client works when the returned config file is invalid
type testInvalidClient struct {
	Get func(uri string) (resp *http.Response, err error)
}

func NewTestInvalidClient() *testInvalidClient {
	return &testInvalidClient{
		Get: func(uri string) (resp *http.Response, err error) {
			// read local config file and return
			f := []byte("wrong yaml:[")
			return &http.Response{Body: io.NopCloser(bytes.NewReader(f))}, nil
		}}
}

type testInvalidRetrieve struct {
	httpsClient *testInvalidClient
}

func NewTestInvalidRetrieve() confmap.Provider {
	return &testInvalidRetrieve{httpsClient: NewTestInvalidClient()}
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

	// https client
	httpsClient := fp.httpsClient

	//GET
	r, err := httpsClient.Get(uri)
	if err != nil {
		return confmap.Retrieved{}, fmt.Errorf("unable to download the file via HTTPS GET for uri %q", uri)
	}
	defer r.Body.Close()

	body, err := ioutil.ReadAll(r.Body)
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

// testNonExistClient and testNonExistRetrieve: Mock how Retrieve() and HTTPS client works when there is no corresponding config file according to the given https-uri
type testNonExistClient struct {
	Get func(uri string) (resp *http.Response, err error)
}

func NewTestNonExistClient() *testNonExistClient {
	return &testNonExistClient{
		Get: func(uri string) (resp *http.Response, err error) {
			// read local config file and return
			f, err := ioutil.ReadFile("../../testdata/nonexist-config.yaml")
			if err != nil {
				return &http.Response{}, err
			}
			return &http.Response{Body: io.NopCloser(bytes.NewReader(f))}, nil
		}}
}

type testNonExistRetrieve struct {
	httpsClient *testNonExistClient
}

func NewTestNonExistRetrieve() confmap.Provider {
	return &testNonExistRetrieve{httpsClient: NewTestNonExistClient()}
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

	// https client
	httpsClient := fp.httpsClient

	//GET
	r, err := httpsClient.Get(uri)
	if err != nil {
		return confmap.Retrieved{}, fmt.Errorf("unable to download the file via HTTPS GET for uri %q", uri)
	}
	defer r.Body.Close()

	body, err := ioutil.ReadAll(r.Body)
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

// testNoRootCAClient and testNoRootCARetrieve: Mock how Retrieve() and HTTPS client works when there is no corresponding Root CA for env SSL_CERT_FILE
type testNoRootCAClient struct {
	Get func(uri string) (resp *http.Response, err error)
}

func NewTestNoRootCAClient() *testNoRootCAClient {
	return &testNoRootCAClient{
		Get: func(uri string) (resp *http.Response, err error) {
			// read local config file and return
			f, err := ioutil.ReadFile("../../testdata/nonexist-config.yaml")
			if err != nil {
				return &http.Response{}, err
			}
			return &http.Response{Body: io.NopCloser(bytes.NewReader(f))}, nil
		}}
}

type testNoRootCARetrieve struct {
	httpsClient *testNoRootCAClient
}

func NewTestNoRootCARetrieve() confmap.Provider {
	return &testNoRootCARetrieve{httpsClient: NewTestNoRootCAClient()}
}

func (fp *testNoRootCARetrieve) Retrieve(ctx context.Context, uri string, watcher confmap.WatcherFunc) (confmap.Retrieved, error) {
	if !strings.HasPrefix(uri, schemeName+"://") {
		return confmap.Retrieved{}, fmt.Errorf("%q uri is not supported by %q provider", uri, schemeName)
	}

	// create a certificate pool, then add the root CA into it
	myCAPath := "./testdata/nonexist-RootCA.crt"
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

	// https client
	httpsClient := fp.httpsClient

	//GET
	r, err := httpsClient.Get(uri)
	if err != nil {
		return confmap.Retrieved{}, fmt.Errorf("unable to download the file via HTTPS GET for uri %q", uri)
	}
	defer r.Body.Close()

	body, err := ioutil.ReadAll(r.Body)
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
