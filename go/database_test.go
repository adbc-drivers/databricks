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
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseURIForStaging(t *testing.T) {
	tests := []struct {
		name      string
		uri       string
		wantHost  string
		wantPort  int
		wantToken string
		wantErr   bool
	}{
		{
			name:      "standard URI with token",
			uri:       "databricks://token:dapi1234abcd@workspace.cloud.databricks.com:443/sql/1.0/warehouses/abc",
			wantHost:  "workspace.cloud.databricks.com",
			wantPort:  443,
			wantToken: "dapi1234abcd",
		},
		{
			name:      "URI with custom port",
			uri:       "databricks://token:mytoken@host.example.com:8443/path",
			wantHost:  "host.example.com",
			wantPort:  8443,
			wantToken: "mytoken",
		},
		{
			name:      "URI without port",
			uri:       "databricks://token:mytoken@host.example.com/path",
			wantHost:  "host.example.com",
			wantPort:  0,
			wantToken: "mytoken",
		},
		{
			name:      "URI without password",
			uri:       "databricks://user@host.example.com:443/path",
			wantHost:  "host.example.com",
			wantPort:  443,
			wantToken: "",
		},
		{
			name:      "URI with special characters in token",
			uri:       "databricks://token:dapi%2Fabc123@host.com:443/path",
			wantHost:  "host.com",
			wantPort:  443,
			wantToken: "dapi/abc123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &databaseImpl{}
			err := d.parseURIForStaging(tt.uri)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantHost, d.serverHostname)
			assert.Equal(t, tt.wantPort, d.port)
			assert.Equal(t, tt.wantToken, d.accessToken)
		})
	}
}

func TestStagingOptionsWithURI(t *testing.T) {
	d := &databaseImpl{}

	// Staging options should be allowed alongside URI
	err := d.SetOptions(map[string]string{
		adbc.OptionKeyURI:       "databricks://token:mytoken@host.com:443/path",
		OptionStagingVolumePath: "cat.sch.vol",
		OptionStagingPrefix:     "my_prefix",
	})
	require.NoError(t, err)

	assert.Equal(t, "cat.sch.vol", d.stagingVolumePath)
	assert.Equal(t, "my_prefix", d.stagingPrefix)
	assert.Equal(t, "host.com", d.serverHostname)
	assert.Equal(t, 443, d.port)
	assert.Equal(t, "mytoken", d.accessToken)
}

func TestStagingOptionsGetSet(t *testing.T) {
	d := &databaseImpl{}

	require.NoError(t, d.SetOption(OptionStagingVolumePath, "cat.sch.vol"))
	require.NoError(t, d.SetOption(OptionStagingPrefix, "my_prefix"))

	val, err := d.GetOption(OptionStagingVolumePath)
	require.NoError(t, err)
	assert.Equal(t, "cat.sch.vol", val)

	val, err = d.GetOption(OptionStagingPrefix)
	require.NoError(t, err)
	assert.Equal(t, "my_prefix", val)
}

func TestNewStagingClient(t *testing.T) {
	t.Run("returns nil when no volume path", func(t *testing.T) {
		d := &databaseImpl{}
		assert.Nil(t, d.newStagingClient())
	})

	t.Run("creates client with volume path", func(t *testing.T) {
		d := &databaseImpl{
			serverHostname:    "host.com",
			port:              443,
			accessToken:       "token",
			stagingVolumePath: "cat.sch.vol",
			stagingPrefix:     "prefix",
		}
		client := d.newStagingClient()
		require.NotNil(t, client)
		assert.Equal(t, "host.com", client.serverHostname)
		assert.Equal(t, 443, client.port)
		assert.Equal(t, "token", client.accessToken)
		assert.Equal(t, "cat.sch.vol", client.volumePath)
		assert.Equal(t, "prefix", client.prefix)
		assert.NotNil(t, client.httpClient)
	})
}
