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
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// stagingClient handles HTTP operations against the Databricks Files API
// for uploading and deleting staging files in Unity Catalog Volumes.
type stagingClient struct {
	httpClient     *http.Client
	serverHostname string
	port           int
	accessToken    string
	volumePath     string // Three-part name: "catalog.schema.volume"
	prefix         string // Subfolder prefix within volume
}

// volumeAPIPath converts "catalog.schema.volume" to "Volumes/catalog/schema/volume".
func (c *stagingClient) volumeAPIPath() (string, error) {
	parts := strings.SplitN(c.volumePath, ".", 3)
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return "", fmt.Errorf("invalid volume path %q: expected format 'catalog.schema.volume'", c.volumePath)
	}
	return fmt.Sprintf("Volumes/%s/%s/%s", parts[0], parts[1], parts[2]), nil
}

// baseURL returns the base URL for the Databricks Files API.
func (c *stagingClient) baseURL() string {
	port := c.port
	if port == 0 {
		port = 443
	}
	return fmt.Sprintf("https://%s:%d/api/2.0/fs/files", c.serverHostname, port)
}

// generateFileName creates a unique staging file path within the volume.
func (c *stagingClient) generateFileName() (string, error) {
	volumeAPI, err := c.volumeAPIPath()
	if err != nil {
		return "", err
	}

	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("failed to generate random file name: %w", err)
	}

	prefix := c.prefix
	if prefix == "" {
		prefix = "adbc_staging"
	}

	return fmt.Sprintf("%s/%s/%s.parquet", volumeAPI, prefix, hex.EncodeToString(b)), nil
}

// Upload uploads data to the staging area via the Databricks Files API.
func (c *stagingClient) Upload(ctx context.Context, path string, data io.Reader) error {
	url := fmt.Sprintf("%s/%s", c.baseURL(), path)
	return c.uploadToURL(ctx, url, data)
}

// uploadToURL performs the actual PUT request to the given URL.
func (c *stagingClient) uploadToURL(ctx context.Context, url string, data io.Reader) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, data)
	if err != nil {
		return fmt.Errorf("failed to create upload request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.accessToken)
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("upload request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("upload failed with status %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

// Delete removes a file from the staging area via the Databricks Files API.
func (c *stagingClient) Delete(ctx context.Context, path string) error {
	url := fmt.Sprintf("%s/%s", c.baseURL(), path)
	return c.deleteFromURL(ctx, url)
}

// deleteFromURL performs the actual DELETE request to the given URL.
func (c *stagingClient) deleteFromURL(ctx context.Context, url string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return fmt.Errorf("failed to create delete request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.accessToken)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("delete request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("delete failed with status %d: %s", resp.StatusCode, string(body))
	}
	return nil
}
