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
	"log"
	"os"
	"strings"

	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/provider/internal"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	schemeName   = "s3"
	stop         = "."
	colonSlashes = "://"
	s3URITail    = ".amazonaws.com/"
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

func (fmp *provider) Retrieve(_ context.Context, uri string, _ confmap.WatcherFunc) (confmap.Retrieved, error) {
	if !strings.HasPrefix(uri, schemeName+":") {
		return confmap.Retrieved{}, fmt.Errorf("%q uri is not supported by %q provider", uri, schemeName)
	}

	// Check if users set up their env for S3 Auth check yet
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" || os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		return confmap.Retrieved{}, fmt.Errorf("unable to fetch access keys for S3 Auth")
	}

	// Split the uri and get [BUCKET], [REGION], [KEY]
	bucket, region, key, err := S3URISplit(uri)
	if err != nil {
		log.Println(err)
		return confmap.Retrieved{}, fmt.Errorf("%q uri is not valid s3-url", uri)
	}

	// AWS SDK default config
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	if err != nil {
		log.Println(err)
		return confmap.Retrieved{}, fmt.Errorf("AWS SDK's default configuration fail to load")
	}

	// s3 client / manager
	client := s3.NewFromConfig(cfg)
	downloader := manager.NewDownloader(client)

	// headObject : metadata, to check the length of the YAML bytes
	headObject, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		log.Println(err)
		return confmap.Retrieved{}, fmt.Errorf("HeadObject failed to fetch : Bucket %q, Key %q, Region %q", bucket, key, region)
	}
	buf := make([]byte, int(headObject.ContentLength))
	w := manager.NewWriteAtBuffer(buf)

	// download op
	_, err = downloader.Download(context.TODO(), w, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		log.Println(err)
		return confmap.Retrieved{}, fmt.Errorf("file in S3 failed to fetch : uri %q", uri)
	}

	return internal.NewRetrievedFromYAML(w.Bytes())
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
	// split with '.'
	// should be at least 5 splitted parts s3:[BUCKET], s3, [REGION], amazonaws, com/[KEY]
	// check the length
	splitted := strings.Split(uri, stop)
	if len(splitted) < 5 {
		return "", "", "", fmt.Errorf("invalid s3-uri")
	}
	// [REGION] : easy to get
	region := splitted[2]
	// [BUCKET] : split s3:[BUCKET] using '://'
	bucketString := splitted[0]
	bucketSplitted := strings.Split(bucketString, colonSlashes)
	if len(bucketSplitted) < 2 {
		return "", "", "", fmt.Errorf("invalid s3-uri")
	}
	bucket := bucketSplitted[1]
	// [KEY] : split uri using '.amazonaws.com/'
	keyString := uri
	keySplitted := strings.Split(keyString, s3URITail)
	if len(keySplitted) < 2 {
		return "", "", "", fmt.Errorf("invalid s3-uri")
	}
	key := keySplitted[1]
	// check if any of them is empty
	if bucket == "" || region == "" || key == "" {
		return "", "", "", fmt.Errorf("invalid s3-uri")
	}
	log.Println(bucket + " " + region + " " + key)
	// return
	return bucket, region, key, nil
}
