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

package s3provider

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/provider/internal"
)

// checkURI checks whether the s3-uri is valid
func checkURI(uri string) error {
	// check uri's prefix valid or not
	if !strings.HasPrefix(uri, schemeName+":") {
		return fmt.Errorf("%q uri is not supported by %q provider", uri, schemeName)
	}
	// Check if users set up their env for S3 Auth check yet
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" || os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		return fmt.Errorf("unable to fetch access keys for S3 Auth")
	}
	// check uri valid or not, should with 'Bucket, Region, Key'
	_, _, _, err := s3URISplit(uri)
	if err != nil {
		return err
	}
	return nil
}

// testRetrieve: Mock how Retrieve() works in normal cases
type testRetrieve struct{}

func NewTestRetrieve() confmap.Provider {
	return &testRetrieve{}
}

func (fp *testRetrieve) Retrieve(ctx context.Context, uri string, watcher confmap.WatcherFunc) (confmap.Retrieved, error) {
	// check URI
	err := checkURI(uri)
	if err != nil {
		return confmap.Retrieved{}, err
	}
	// read local config file and return
	f, err := ioutil.ReadFile("../../testdata/config.yaml")
	if err != nil {
		return confmap.Retrieved{}, err
	}
	return internal.NewRetrievedFromYAML(f)
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
	// check URI
	err := checkURI(uri)
	if err != nil {
		return confmap.Retrieved{}, err
	}
	// invalid config file and return
	return internal.NewRetrievedFromYAML([]byte("wrong yaml:["))
}

func (fp *testInvalidRetrieve) Scheme() string {
	return schemeName
}

func (fp *testInvalidRetrieve) Shutdown(context.Context) error {
	return nil
}

// testNonExisitRetrieve: Mock how Retrieve() works when there is no corresponding config file according to the given s3-uri
type testNonExisitRetrieve struct{}

func NewTestNonExistRetrieve() confmap.Provider {
	return &testNonExisitRetrieve{}
}

func (fp *testNonExisitRetrieve) Retrieve(ctx context.Context, uri string, watcher confmap.WatcherFunc) (confmap.Retrieved, error) {
	// check URI
	err := checkURI(uri)
	if err != nil {
		return confmap.Retrieved{}, err
	}
	// read local config file and return
	f, err := ioutil.ReadFile("../../testdata/non-exist-config.yaml")
	if err != nil {
		return confmap.Retrieved{}, err
	}
	return internal.NewRetrievedFromYAML(f)
}

func (fp *testNonExisitRetrieve) Scheme() string {
	return schemeName
}

func (fp *testNonExisitRetrieve) Shutdown(context.Context) error {
	return nil
}

func TestFunctionalityDownloadFileS3(t *testing.T) {
	fp := NewTestRetrieve()
	_, err := fp.Retrieve(context.Background(), "s3://bucket.s3.region.amazonaws.com/key", nil)
	assert.NoError(t, err)
	assert.NoError(t, fp.Shutdown(context.Background()))
}

func TestFunctionalityS3URISplit(t *testing.T) {
	fp := NewTestRetrieve()
	bucket, region, key, err := s3URISplit("s3://bucket.s3.region.amazonaws.com/key")
	assert.NoError(t, err)
	assert.Equal(t, "bucket", bucket)
	assert.Equal(t, "region", region)
	assert.Equal(t, "key", key)
	assert.NoError(t, fp.Shutdown(context.Background()))
}

func TestInvalidS3URISplit(t *testing.T) {
	fp := NewTestRetrieve()
	_, err := fp.Retrieve(context.Background(), "s3://bucket.s3.region.amazonaws", nil)
	assert.Error(t, err)
	_, err = fp.Retrieve(context.Background(), "s3://bucket.s3.region.aws.com/key", nil)
	assert.Error(t, err)
	require.NoError(t, fp.Shutdown(context.Background()))
}

func TestUnsupportedScheme(t *testing.T) {
	fp := NewTestRetrieve()
	_, err := fp.Retrieve(context.Background(), "https://google.com", nil)
	assert.Error(t, err)
	assert.NoError(t, fp.Shutdown(context.Background()))
}

func TestEmptyBucket(t *testing.T) {
	fp := NewTestRetrieve()
	_, err := fp.Retrieve(context.Background(), "s3://.s3.region.amazonaws.com/key", nil)
	require.Error(t, err)
	require.NoError(t, fp.Shutdown(context.Background()))
}

func TestEmptyKey(t *testing.T) {
	fp := NewTestRetrieve()
	_, err := fp.Retrieve(context.Background(), "s3://bucket.s3.region.amazonaws.com/", nil)
	require.Error(t, err)
	require.NoError(t, fp.Shutdown(context.Background()))
}

func TestNonExistent(t *testing.T) {
	fp := NewTestNonExistRetrieve()
	_, err := fp.Retrieve(context.Background(), "s3://non-exist-bucket.s3.region.amazonaws.com/key", nil)
	assert.Error(t, err)
	_, err = fp.Retrieve(context.Background(), "s3://bucket.s3.region.amazonaws.com/non-exist-key.yaml", nil)
	assert.Error(t, err)
	_, err = fp.Retrieve(context.Background(), "s3://bucket.s3.non-exist-region.amazonaws.com/key", nil)
	assert.Error(t, err)
	require.NoError(t, fp.Shutdown(context.Background()))
}

func TestInvalidYAML(t *testing.T) {
	fp := NewTestInvalidRetrieve()
	_, err := fp.Retrieve(context.Background(), "s3://bucket.s3.region.amazonaws.com/key", nil)
	assert.Error(t, err)
	require.NoError(t, fp.Shutdown(context.Background()))
}

func TestScheme(t *testing.T) {
	fp := NewTestRetrieve()
	assert.Equal(t, "s3", fp.Scheme())
	require.NoError(t, fp.Shutdown(context.Background()))
}
