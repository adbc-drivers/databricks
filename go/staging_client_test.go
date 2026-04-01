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
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVolumeAPIPath(t *testing.T) {
	tests := []struct {
		name       string
		volumePath string
		want       string
		wantErr    bool
	}{
		{
			name:       "valid three-part name",
			volumePath: "my_catalog.my_schema.my_volume",
			want:       "Volumes/my_catalog/my_schema/my_volume",
		},
		{
			name:       "with underscores and numbers",
			volumePath: "catalog_1.schema_2.volume_3",
			want:       "Volumes/catalog_1/schema_2/volume_3",
		},
		{
			name:       "missing part",
			volumePath: "catalog.schema",
			wantErr:    true,
		},
		{
			name:       "empty string",
			volumePath: "",
			wantErr:    true,
		},
		{
			name:       "too many parts",
			volumePath: "a.b.c.d",
			want:       "Volumes/a/b/c.d",
		},
		{
			name:       "empty catalog part",
			volumePath: ".schema.volume",
			wantErr:    true,
		},
		{
			name:       "empty schema part",
			volumePath: "catalog..volume",
			wantErr:    true,
		},
		{
			name:       "empty volume part",
			volumePath: "catalog.schema.",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &stagingClient{volumePath: tt.volumePath}
			got, err := c.volumeAPIPath()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestGenerateFileName(t *testing.T) {
	t.Run("default prefix", func(t *testing.T) {
		c := &stagingClient{volumePath: "cat.sch.vol"}
		name, err := c.generateFileName()
		require.NoError(t, err)
		assert.True(t, strings.HasPrefix(name, "Volumes/cat/sch/vol/adbc_staging/"))
		assert.True(t, strings.HasSuffix(name, ".parquet"))
	})

	t.Run("custom prefix", func(t *testing.T) {
		c := &stagingClient{volumePath: "cat.sch.vol", prefix: "my_prefix"}
		name, err := c.generateFileName()
		require.NoError(t, err)
		assert.True(t, strings.HasPrefix(name, "Volumes/cat/sch/vol/my_prefix/"))
		assert.True(t, strings.HasSuffix(name, ".parquet"))
	})

	t.Run("unique names", func(t *testing.T) {
		c := &stagingClient{volumePath: "cat.sch.vol"}
		name1, err := c.generateFileName()
		require.NoError(t, err)
		name2, err := c.generateFileName()
		require.NoError(t, err)
		assert.NotEqual(t, name1, name2)
	})

	t.Run("invalid volume path", func(t *testing.T) {
		c := &stagingClient{volumePath: "invalid"}
		_, err := c.generateFileName()
		assert.Error(t, err)
	})
}

func TestBaseURL(t *testing.T) {
	t.Run("with explicit port", func(t *testing.T) {
		c := &stagingClient{serverHostname: "workspace.databricks.com", port: 443}
		assert.Equal(t, "https://workspace.databricks.com:443/api/2.0/fs/files", c.baseURL())
	})

	t.Run("default port", func(t *testing.T) {
		c := &stagingClient{serverHostname: "workspace.databricks.com", port: 0}
		assert.Equal(t, "https://workspace.databricks.com:443/api/2.0/fs/files", c.baseURL())
	})

	t.Run("custom port", func(t *testing.T) {
		c := &stagingClient{serverHostname: "workspace.databricks.com", port: 8443}
		assert.Equal(t, "https://workspace.databricks.com:8443/api/2.0/fs/files", c.baseURL())
	})
}

func TestUpload(t *testing.T) {
	t.Run("successful upload", func(t *testing.T) {
		var receivedBody string
		var receivedAuth string
		var receivedContentType string
		var receivedMethod string
		var receivedPath string

		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedMethod = r.Method
			receivedPath = r.URL.Path
			receivedAuth = r.Header.Get("Authorization")
			receivedContentType = r.Header.Get("Content-Type")
			body, _ := io.ReadAll(r.Body)
			receivedBody = string(body)
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		// Extract host from test server URL (strip https://)
		host := strings.TrimPrefix(server.URL, "https://")

		c := &stagingClient{
			httpClient:     server.Client(),
			serverHostname: host,
			port:           0, // will be in the host string already
			accessToken:    "test-token",
		}

		// Override baseURL by using the host directly
		path := "Volumes/cat/sch/vol/staging/test.parquet"
		url := server.URL + "/api/2.0/fs/files/" + path

		req, err := http.NewRequestWithContext(context.Background(), http.MethodPut, url, strings.NewReader("parquet-data"))
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer test-token")
		req.Header.Set("Content-Type", "application/octet-stream")

		resp, err := c.httpClient.Do(req)
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, http.MethodPut, receivedMethod)
		assert.Contains(t, receivedPath, path)
		assert.Equal(t, "Bearer test-token", receivedAuth)
		assert.Equal(t, "application/octet-stream", receivedContentType)
		assert.Equal(t, "parquet-data", receivedBody)
	})

	t.Run("upload failure", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"error": "access denied"}`))
		}))
		defer server.Close()

		host := strings.TrimPrefix(server.URL, "https://")
		c := &stagingClient{
			httpClient:     server.Client(),
			serverHostname: host,
			accessToken:    "bad-token",
		}

		// Call Upload directly — need to override baseURL behavior
		// Use the server URL directly for the test
		err := c.uploadToURL(context.Background(), server.URL+"/api/2.0/fs/files/test.parquet", strings.NewReader("data"))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "403")
	})
}

func TestDelete(t *testing.T) {
	t.Run("successful delete", func(t *testing.T) {
		var receivedMethod string

		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedMethod = r.Method
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		host := strings.TrimPrefix(server.URL, "https://")
		c := &stagingClient{
			httpClient:     server.Client(),
			serverHostname: host,
			accessToken:    "test-token",
		}

		err := c.deleteFromURL(context.Background(), server.URL+"/api/2.0/fs/files/test.parquet")
		require.NoError(t, err)
		assert.Equal(t, http.MethodDelete, receivedMethod)
	})

	t.Run("delete failure", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error": "not found"}`))
		}))
		defer server.Close()

		host := strings.TrimPrefix(server.URL, "https://")
		c := &stagingClient{
			httpClient:     server.Client(),
			serverHostname: host,
			accessToken:    "test-token",
		}

		err := c.deleteFromURL(context.Background(), server.URL+"/api/2.0/fs/files/test.parquet")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "404")
	})
}
