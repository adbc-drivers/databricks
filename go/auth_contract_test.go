// Copyright (c) 2026 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package databricks

import (
	"context"
	"encoding/json"
	"os"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

type authContract struct {
	Options map[string]struct {
		Secret bool `json:"secret"`
		// Example is a valid value for options that do not accept any string;
		// the default is "v".
		Example string `json:"example"`
	} `json:"options"`
	Inferred  map[string][]string `json:"inferred_when_unset"`
	AuthTypes map[string]struct {
		Required []string `json:"required"`
		Optional []string `json:"optional"`
	} `json:"auth_types"`
}

// secretAuthOptions are the auth options that hold a secret. The contract file
// marks the same options.
var secretAuthOptions = map[string]bool{
	OptionAccessToken:       true,
	OptionOAuthClientSecret: true,
	OptionAzureClientSecret: true,
}

// The contract file lists the same auth types and required options as the
// login method table, and every option it names can be set and read.
func TestAuthContractMatchesDriver(t *testing.T) {
	raw, err := os.ReadFile("auth_contract.json")
	require.NoError(t, err)
	var contract authContract
	require.NoError(t, json.Unmarshal(raw, &contract))

	for name := range loginMethods {
		require.Contains(t, contract.AuthTypes, name, "auth type missing from the contract")
	}
	for name, entry := range contract.AuthTypes {
		method, ok := loginMethods[name]
		require.True(t, ok, "contract lists unknown auth type %q", name)

		required := []string{}
		for _, f := range method.required {
			required = append(required, f.option)
		}
		require.ElementsMatch(t, entry.Required, required, "required options of %q", name)
		require.ElementsMatch(t, entry.Optional, method.optional, "optional options of %q", name)

		for _, option := range append(slices.Clone(entry.Required), entry.Optional...) {
			require.Contains(t, contract.Options, option)
		}
	}

	// The secret flags are the driver's set of secret options.
	for option, info := range contract.Options {
		require.Equal(t, secretAuthOptions[option], info.Secret, "secret flag of %q", option)
	}
	for option := range secretAuthOptions {
		require.Contains(t, contract.Options, option)
	}

	// Setting the listed options, without databricks.auth_type, selects that auth
	// type. Any one of them is enough to select it: when the others are missing the
	// error names the selected type's missing option.
	ctx := context.Background()
	for name, options := range contract.Inferred {
		require.Contains(t, contract.AuthTypes, name)
		all := &databaseImpl{}
		for _, option := range options {
			require.NoError(t, all.SetOption(ctx, option, "v"))
		}
		got, _, err := all.resolveLoginMethod()
		require.NoError(t, err, name)
		require.Equal(t, name, got)

		for _, option := range options {
			db := &databaseImpl{}
			require.NoError(t, db.SetOption(ctx, option, "v"))
			if _, _, err := db.resolveLoginMethod(); err != nil {
				require.ErrorContains(t, err, "when using auth type '"+name+"'", "inferred from %q", option)
			}
		}
	}

	for option, info := range contract.Options {
		value := "v"
		if info.Example != "" {
			value = info.Example
		}
		db := &databaseImpl{}
		require.NoError(t, db.SetOption(ctx, option, value), option)
		got, err := db.GetOption(ctx, option)
		require.NoError(t, err, option)
		require.Equal(t, value, got, option)
	}
}
