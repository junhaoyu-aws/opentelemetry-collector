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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"

	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/provider/internal"
)

const (
	schemeName = "https"
)

type provider struct{}

// New returns a new confmap.Provider that reads the configuration from a file.
//
// This Provider supports "https" scheme, and can be called with a "uri" that follows:
//   https-uri : https://host/xxx
//
// One example for https-uri be like: https://localhost:4444/getConfig
//
// Examples:
// `https://localhost:4444/getConfig` - (unix, windows)
func New() confmap.Provider {
	return &provider{}
}

func (fmp *provider) Retrieve(_ context.Context, uri string, _ confmap.WatcherFunc) (confmap.Retrieved, error) {
	if !strings.HasPrefix(uri, schemeName+"://") {
		return confmap.Retrieved{}, fmt.Errorf("%q uri is not supported by %q provider", uri, schemeName)
	}

	// create a certificate pool, then add the root CA into it
	myCAPath := os.Getenv("SSL_CERT_FILE")
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
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: false,
				RootCAs:            pool,
			},
		},
	}

	//GET
	r, err := client.Get(uri)
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

func (*provider) Scheme() string {
	return schemeName
}

func (*provider) Shutdown(context.Context) error {
	return nil
}
