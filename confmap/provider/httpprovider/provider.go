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

package httpprovider // import "go.opentelemetry.io/collector/confmap/provider/httpprovider"

import (
	"context"
	"fmt"
	"io/ioutil"
	"strings"

	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/provider/internal"

	"net/http"
)

const (
	schemeName = "http"
)

type httpClient interface {
	Get(url string) (resp *http.Response, err error)
}

type provider struct {
	client httpClient
}

// New returns a new confmap.Provider that reads the configuration from a file.
//
// This Provider supports "http" scheme, and can be called with a "uri" that follows:
//
// One example for http-uri be like: http://localhost:3333/getConfig
//
// Examples:
// `http://localhost:3333/getConfig` - (unix, windows)
func New() confmap.Provider {
	return &provider{client: &http.Client{}}
}

func (fmp *provider) Retrieve(_ context.Context, uri string, _ confmap.WatcherFunc) (confmap.Retrieved, error) {
	if !strings.HasPrefix(uri, schemeName+"://") {
		return confmap.Retrieved{}, fmt.Errorf("%q uri is not supported by %q provider", uri, schemeName)
	}

	// send a HTTP GET request
	resp, err := fmp.client.Get(uri)
	if err != nil {
		return confmap.Retrieved{}, fmt.Errorf("unable to download the file via HTTP GET for uri %q, with err: %w ", uri, err)
	}
	defer resp.Body.Close()

	// read the response body
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return confmap.Retrieved{}, fmt.Errorf("fail to read the response body from uri %q, with err: %w ", uri, err)
	}

	return internal.NewRetrievedFromYAML(body)
}

func (*provider) Scheme() string {
	return schemeName
}

func (*provider) Shutdown(context.Context) error {
	return nil
}
