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
	"io"
	"log"
	"regexp"
	"strings"

	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/provider/internal"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	schemeName = "s3"
)

type provider struct{}

// New returns a new confmap.Provider that reads the configuration from a file.
//
// This Provider supports "s3" scheme, and can be called with a "uri" that follows:
//   s3-uri : s3://[BUCKET].s3.[REGION].amazonaws.com/[KEY]
//
// One example for s3-uri be like: s3://DOC-EXAMPLE-BUCKET.s3.us-west-2.amazonaws.com/photos/puppy.jpg
//
// Examples:
// `s3://DOC-EXAMPLE-BUCKET.s3.us-west-2.amazonaws.com/photos/puppy.jpg` - (unix, windows)
func New() confmap.Provider {
	return &provider{}
}

func (fmp *provider) Retrieve(ctx context.Context, uri string, _ confmap.WatcherFunc) (confmap.Retrieved, error) {
	if !strings.HasPrefix(uri, schemeName+":") {
		return confmap.Retrieved{}, fmt.Errorf("%q uri is not supported by %q provider", uri, schemeName)
	}

	// Split the uri and get [BUCKET], [REGION], [KEY]
	bucket, region, key, err := S3URISplit(uri)
	if err != nil {
		return confmap.Retrieved{}, fmt.Errorf("%q uri is not valid s3-url", uri)
	}

	// AWS SDK default config
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return confmap.Retrieved{}, fmt.Errorf("AWS SDK's default configuration fail to load")
	}

	// to create a s3 client and also a s3 downloader
	// s3 client provides interfaces for Bucket/File Management in Amazon S3
	// s3 downloader is especially for s3 downloading operation
	client := s3.NewFromConfig(cfg)
	resp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return confmap.Retrieved{}, fmt.Errorf("file in S3 failed to fetch : uri %q", uri)
	}

	// create a buffer and read content from the response body
	buffer := make([]byte, int(resp.ContentLength))
	defer resp.Body.Close()
	_, err = resp.Body.Read(buffer)
	if err != io.EOF && err != nil {
		log.Println(err)
		return confmap.Retrieved{}, fmt.Errorf("failed to read content from the downloaded config file via uri %q", uri)
	}

	return internal.NewRetrievedFromYAML(buffer)
}

func (*provider) Scheme() string {
	return schemeName
}

func (*provider) Shutdown(context.Context) error {
	return nil
}

// S3URISplit splits the s3 uri and get the [BUCKET], [REGION], [KEY] in it
// INPUT : s3 uri (like s3://[BUCKET].s3.[REGION].amazonaws.com/[KEY])
// OUTPUT :
//		-  [BUCKET] : The name of a bucket in Amazon S3.
//		-  [REGION] : Where are servers from, e.g. us-west-2.
//		-  [KEY]    : The key exists in a given bucket, can be used to retrieve a file.
func S3URISplit(uri string) (string, string, string, error) {
	matched, err := regexp.MatchString("s3://(.*)\\.s3\\.(.*).amazonaws\\.com/(.*)", uri)
	if err != nil || !matched {
		return "", "", "", fmt.Errorf("invalid s3-uri")
	}
	splitted := strings.Split(uri, ".")
	// [REGION] : easy to get
	region := splitted[2]
	// [BUCKET] : split s3:[BUCKET] using '://'
	bucketString := splitted[0]
	bucketSplitted := strings.Split(bucketString, "://")
	bucket := bucketSplitted[1]
	// [KEY] : split uri using '.amazonaws.com/'
	keyString := uri
	keySplitted := strings.Split(keyString, ".amazonaws.com/")
	key := keySplitted[1]
	// check if any of them is empty
	if bucket == "" || region == "" || key == "" {
		return "", "", "", fmt.Errorf("invalid s3-uri")
	}
	// return
	return bucket, region, key, nil
}
