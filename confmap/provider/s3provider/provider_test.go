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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFunctionalityDownloadFileS3(t *testing.T) {
	fp := New()
	_, err := fp.Retrieve(context.Background(), "s3://aws-otel-intern-test-s3.s3.us-west-2.amazonaws.com/s3config/otel-config.yaml", nil)
	assert.NoError(t, err)
	assert.NoError(t, fp.Shutdown(context.Background()))
}

func TestFunctionalityS3URISplit(t *testing.T) {
	fp := New()
	bucket, region, key, err := S3URISplit("s3://aws-otel-intern-test-s3.s3.us-west-2.amazonaws.com/s3config/default-config.yaml")
	assert.NoError(t, err)
	assert.Equal(t, "aws-otel-intern-test-s3", bucket)
	assert.Equal(t, "us-west-2", region)
	assert.Equal(t, "s3config/default-config.yaml", key)
	assert.NoError(t, fp.Shutdown(context.Background()))
}

func TestInvalidS3URISplit(t *testing.T) {
	fp := New()
	_, err := fp.Retrieve(context.Background(), "s3://aws-otel-intern-test-s3.s3.us-west-2.amazonaws", nil)
	assert.Error(t, err)
	_, err = fp.Retrieve(context.Background(), "s3://aws-otel-intern-test-s3.s3.us-west-2.amazonaws.com/s3config/invalid-config.yaml", nil)
	assert.Error(t, err)
	_, err = fp.Retrieve(context.Background(), "s3://aws-otel-intern-test-s3.s3.us-west-2.aws.com/s3config/invalid-config.yaml", nil)
	assert.Error(t, err)
	require.NoError(t, fp.Shutdown(context.Background()))
}

func TestUnsupportedScheme(t *testing.T) {
	fp := New()
	_, err := fp.Retrieve(context.Background(), "https://google.com", nil)
	assert.Error(t, err)
	assert.NoError(t, fp.Shutdown(context.Background()))
}

func TestEmptyBucket(t *testing.T) {
	fp := New()
	_, err := fp.Retrieve(context.Background(), "s3://.s3.us-west-2.amazonaws.com/s3config/default-config.yaml", nil)
	require.Error(t, err)
	require.NoError(t, fp.Shutdown(context.Background()))
}

func TestEmptyKey(t *testing.T) {
	fp := New()
	_, err := fp.Retrieve(context.Background(), "s3://aws-otel-intern-test-s3.s3.us-west-2.amazonaws.com/", nil)
	require.Error(t, err)
	require.NoError(t, fp.Shutdown(context.Background()))
}

func TestNonExistent(t *testing.T) {
	fp := New()
	_, err := fp.Retrieve(context.Background(), "s3://non-exist-bucket.s3.us-west-2.amazonaws.com/s3config/default-config.yaml", nil)
	assert.Error(t, err)
	_, err = fp.Retrieve(context.Background(), "s3://aws-otel-intern-test-s3.s3.us-west-2.amazonaws.com/non-exist-key.yaml", nil)
	assert.Error(t, err)
	_, err = fp.Retrieve(context.Background(), "s3://aws-otel-intern-test-s3.s3.non-exist-region.amazonaws.com/s3config/default-config.yaml", nil)
	assert.Error(t, err)
	require.NoError(t, fp.Shutdown(context.Background()))
}

func TestInvalidYAML(t *testing.T) {
	fp := New()
	_, err := fp.Retrieve(context.Background(), "s3://aws-otel-intern-test-s3.s3.us-west-2.amazonaws.com/s3config/invalid-config.yaml", nil)
	assert.Error(t, err)
	require.NoError(t, fp.Shutdown(context.Background()))
}

func TestScheme(t *testing.T) {
	fp := New()
	assert.Equal(t, "s3", fp.Scheme())
	require.NoError(t, fp.Shutdown(context.Background()))
}
