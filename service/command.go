// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package service // import "go.opentelemetry.io/collector/service"

import (
	"context"
	"log"

	"github.com/spf13/cobra"

	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/converter/overwritepropertiesconverter"
	"go.opentelemetry.io/collector/service/featuregate"
)

// NewCommand constructs a new cobra.Command using the given CollectorSettings.
func NewCommand(set CollectorSettings) *cobra.Command {
	flagSet := flags()
	var newCtx context.Context
	rootCmd := &cobra.Command{
		Use:          set.BuildInfo.Command,
		Version:      set.BuildInfo.Version,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			featuregate.GetRegistry().Apply(gatesList)
			if set.ConfigProvider == nil {
				var err error
				configURIs := getConfigFlag(flagSet)
				log.Println(configURIs)
				cfgSet := newDefaultConfigProviderSettings(configURIs)
				//
				newCtx = context.WithValue(cmd.Context(), "configURIs", configURIs)
				// Append the "overwrite properties converter" as the first converter.
				cfgSet.MapConverters = append(
					[]confmap.Converter{overwritepropertiesconverter.New(getSetFlag(flagSet))},
					cfgSet.MapConverters...)
				set.ConfigProvider, err = NewConfigProvider(cfgSet)
				if err != nil {
					return err
				}
			}
			col, err := New(set)
			if err != nil {
				return err
			}
			return col.Run(newCtx)
		},
	}

	rootCmd.Flags().AddGoFlagSet(flagSet)
	return rootCmd
}
