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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/provider/internal"
)

// testRetrieve: Mock how HTTP server works in normal cases
type testRetrieve struct{}

func NewTestRetrieve() confmap.Provider {
	return &testRetrieve{}
}

func (tr *testRetrieve) Retrieve(ctx context.Context, uri string, watcher confmap.WatcherFunc) (confmap.Retrieved, error) {
	// check URI's prefix
	if !strings.HasPrefix(uri, schemeName+"://") {
		return confmap.Retrieved{}, fmt.Errorf("%q uri is not supported by %q provider", uri, schemeName)
	}
	// read local config file and return
	f, err := ioutil.ReadFile("../../testdata/config.yaml")
	if err != nil {
		return confmap.Retrieved{}, err
	}
	return internal.NewRetrievedFromYAML(f)
}

func (tr *testRetrieve) Scheme() string {
	return schemeName
}

func (tr *testRetrieve) Shutdown(context.Context) error {
	return nil
}

// testInvalidRetrieve: Mock how HTTP server works when the returned config file is invalid
type testInvalidRetrieve struct{}

func NewTestInvalidRetrieve() confmap.Provider {
	return &testInvalidRetrieve{}
}

func (tir *testInvalidRetrieve) Retrieve(ctx context.Context, uri string, watcher confmap.WatcherFunc) (confmap.Retrieved, error) {
	// check URI's prefix
	if !strings.HasPrefix(uri, schemeName+"://") {
		return confmap.Retrieved{}, fmt.Errorf("%q uri is not supported by %q provider", uri, schemeName)
	}
	// invalid config file and return
	return internal.NewRetrievedFromYAML([]byte("wrong yaml:["))
}

func (tir *testInvalidRetrieve) Scheme() string {
	return schemeName
}

func (tir *testInvalidRetrieve) Shutdown(context.Context) error {
	return nil
}

// testNonExisitRetrieve: Mock how HTTP server works when there is no corresponding config file according to the given http-uri
type testNonExisitRetrieve struct{}

func NewTestNonExistRetrieve() confmap.Provider {
	return &testNonExisitRetrieve{}
}

func (tnr *testNonExisitRetrieve) Retrieve(ctx context.Context, uri string, watcher confmap.WatcherFunc) (confmap.Retrieved, error) {
	// check URI's prefix
	if !strings.HasPrefix(uri, schemeName+"://") {
		return confmap.Retrieved{}, fmt.Errorf("%q uri is not supported by %q provider", uri, schemeName)
	}
	// read local config file and return
	f, err := ioutil.ReadFile("../../testdata/non-exist-config.yaml")
	if err != nil {
		return confmap.Retrieved{}, err
	}
	return internal.NewRetrievedFromYAML(f)
}

func (tnr *testNonExisitRetrieve) Scheme() string {
	return schemeName
}

func (tnr *testNonExisitRetrieve) Shutdown(context.Context) error {
	return nil
}

func TestFunctionalityDownloadFileHTTP(t *testing.T) {
	fp := NewTestRetrieve()
	_, err := fp.Retrieve(context.Background(), "http://localhost:3333/validConfig", nil)
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
	_, err := fp.Retrieve(context.Background(), "http://non-exist-domain/default-config.yaml", nil)
	assert.Error(t, err)
	require.NoError(t, fp.Shutdown(context.Background()))
}

func TestInvalidYAML(t *testing.T) {
	fp := NewTestInvalidRetrieve()
	_, err := fp.Retrieve(context.Background(), "http://localhost:3333/invalidConfig", nil)
	assert.Error(t, err)
	require.NoError(t, fp.Shutdown(context.Background()))
}

func TestScheme(t *testing.T) {
	fp := NewTestRetrieve()
	assert.Equal(t, "http", fp.Scheme())
	require.NoError(t, fp.Shutdown(context.Background()))
}
