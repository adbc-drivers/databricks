/*
* Copyright (c) 2025 ADBC Drivers Contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;

namespace AdbcDrivers.Databricks.StatementExecution
{
    /// <summary>
    /// Interface for Statement Execution API operations.
    /// </summary>
    internal interface IStatementExecutionClient
    {
        /// <summary>
        /// Creates a new SQL session.
        /// </summary>
        /// <param name="request">The session creation request.</param>
        /// <param name="cancellationToken">A cancellation token.</param>
        /// <returns>The session creation response.</returns>
        Task<CreateSessionResponse> CreateSessionAsync(CreateSessionRequest request, CancellationToken cancellationToken);

        /// <summary>
        /// Deletes an existing SQL session.
        /// </summary>
        /// <param name="sessionId">The session ID to delete.</param>
        /// <param name="warehouseId">The warehouse ID (required by Databricks API).</param>
        /// <param name="cancellationToken">A cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        Task DeleteSessionAsync(string sessionId, string warehouseId, CancellationToken cancellationToken);

        /// <summary>
        /// Executes a SQL statement.
        /// </summary>
        /// <param name="request">The statement execution request.</param>
        /// <param name="cancellationToken">A cancellation token.</param>
        /// <returns>The statement execution response.</returns>
        Task<ExecuteStatementResponse> ExecuteStatementAsync(ExecuteStatementRequest request, CancellationToken cancellationToken);

        /// <summary>
        /// Gets the status and results of a SQL statement.
        /// </summary>
        /// <param name="statementId">The statement ID.</param>
        /// <param name="cancellationToken">A cancellation token.</param>
        /// <returns>The statement status and results.</returns>
        Task<GetStatementResponse> GetStatementAsync(string statementId, CancellationToken cancellationToken);

        /// <summary>
        /// Gets a specific result chunk by index (for incremental fetching).
        /// </summary>
        /// <param name="statementId">The statement ID.</param>
        /// <param name="chunkIndex">The zero-based chunk index.</param>
        /// <param name="cancellationToken">A cancellation token.</param>
        /// <returns>The result chunk data.</returns>
        Task<ResultData> GetResultChunkAsync(string statementId, long chunkIndex, CancellationToken cancellationToken);

        /// <summary>
        /// Cancels a running SQL statement.
        /// </summary>
        /// <param name="statementId">The statement ID to cancel.</param>
        /// <param name="cancellationToken">A cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        Task CancelStatementAsync(string statementId, CancellationToken cancellationToken);

        /// <summary>
        /// Closes a SQL statement and releases its resources.
        /// </summary>
        /// <param name="statementId">The statement ID to close.</param>
        /// <param name="cancellationToken">A cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        Task CloseStatementAsync(string statementId, CancellationToken cancellationToken);
    }

    /// <summary>
    /// Client for communicating with the Databricks Statement Execution API.
    /// </summary>
    internal class StatementExecutionClient : IStatementExecutionClient
    {
        private readonly HttpClient _httpClient;
        private readonly string _baseUrl;

        private const string SessionsEndpoint = "/api/2.0/sql/sessions";
        private const string StatementsEndpoint = "/api/2.0/sql/statements";

        // JSON serialization options - ignore null values when writing
        private static readonly JsonSerializerOptions s_jsonOptions = new JsonSerializerOptions
        {
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
        };

        /// <summary>
        /// Initializes a new instance of the <see cref="StatementExecutionClient"/> class.
        /// </summary>
        /// <param name="httpClient">The HTTP client to use for requests.</param>
        /// <param name="host">The Databricks workspace host.</param>
        public StatementExecutionClient(HttpClient httpClient, string host)
        {
            _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));

            if (string.IsNullOrWhiteSpace(host))
            {
                throw new ArgumentException("Host cannot be null or whitespace.", nameof(host));
            }

            // Ensure the host doesn't have a trailing slash and has https://
            host = host.TrimEnd('/');
            if (!host.StartsWith("http://") && !host.StartsWith("https://"))
            {
                host = "https://" + host;
            }

            _baseUrl = host;
        }

        /// <summary>
        /// Creates a new SQL session.
        /// </summary>
        /// <param name="request">The session creation request.</param>
        /// <param name="cancellationToken">A cancellation token.</param>
        /// <returns>The session creation response.</returns>
        public async Task<CreateSessionResponse> CreateSessionAsync(
            CreateSessionRequest request,
            CancellationToken cancellationToken)
        {
            if (request == null)
            {
                throw new ArgumentNullException(nameof(request));
            }

            var url = $"{_baseUrl}{SessionsEndpoint}";
            var jsonContent = JsonSerializer.Serialize(request, s_jsonOptions);
            var content = new StringContent(jsonContent, Encoding.UTF8, "application/json");

            var httpRequest = new HttpRequestMessage(HttpMethod.Post, url)
            {
                Content = content
            };

            var response = await _httpClient.SendAsync(httpRequest, cancellationToken).ConfigureAwait(false);

            await EnsureSuccessStatusCodeAsync(response).ConfigureAwait(false);

            var responseContent = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
            var sessionResponse = JsonSerializer.Deserialize<CreateSessionResponse>(responseContent, s_jsonOptions);

            if (sessionResponse == null)
            {
                throw new DatabricksException("Failed to deserialize CreateSessionResponse");
            }

            return sessionResponse;
        }

        /// <summary>
        /// Deletes an existing SQL session.
        /// </summary>
        /// <param name="sessionId">The session ID to delete.</param>
        /// <param name="warehouseId">The warehouse ID (required by Databricks API).</param>
        /// <param name="cancellationToken">A cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public async Task DeleteSessionAsync(string sessionId, string warehouseId, CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(sessionId))
            {
                throw new ArgumentException("Session ID cannot be null or whitespace.", nameof(sessionId));
            }

            if (string.IsNullOrWhiteSpace(warehouseId))
            {
                throw new ArgumentException("Warehouse ID cannot be null or whitespace.", nameof(warehouseId));
            }

            // Databricks requires warehouse_id as query parameter even for DELETE
            var url = $"{_baseUrl}{SessionsEndpoint}/{sessionId}?warehouse_id={Uri.EscapeDataString(warehouseId)}";
            var httpRequest = new HttpRequestMessage(HttpMethod.Delete, url);

            var response = await _httpClient.SendAsync(httpRequest, cancellationToken).ConfigureAwait(false);

            await EnsureSuccessStatusCodeAsync(response).ConfigureAwait(false);
        }

        /// <summary>
        /// Executes a SQL statement.
        /// </summary>
        /// <param name="request">The statement execution request.</param>
        /// <param name="cancellationToken">A cancellation token.</param>
        /// <returns>The statement execution response.</returns>
        public async Task<ExecuteStatementResponse> ExecuteStatementAsync(
            ExecuteStatementRequest request,
            CancellationToken cancellationToken)
        {
            if (request == null)
            {
                throw new ArgumentNullException(nameof(request));
            }

            var url = $"{_baseUrl}{StatementsEndpoint}";
            var jsonContent = JsonSerializer.Serialize(request, s_jsonOptions);
            var content = new StringContent(jsonContent, Encoding.UTF8, "application/json");

            var httpRequest = new HttpRequestMessage(HttpMethod.Post, url)
            {
                Content = content
            };

            var response = await _httpClient.SendAsync(httpRequest, cancellationToken).ConfigureAwait(false);

            await EnsureSuccessStatusCodeAsync(response).ConfigureAwait(false);

            var responseContent = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
            var executeResponse = JsonSerializer.Deserialize<ExecuteStatementResponse>(responseContent, s_jsonOptions);

            if (executeResponse == null)
            {
                throw new DatabricksException("Failed to deserialize ExecuteStatementResponse");
            }

            // Check for FAILED state and throw exception (like JDBC driver does)
            if (executeResponse.Status?.State == "FAILED")
            {
                var errorMessage = $"Statement execution failed. State: {executeResponse.Status.State}";
                if (executeResponse.Status.Error != null)
                {
                    errorMessage += $". Error Code: {executeResponse.Status.Error.ErrorCode}, Message: {executeResponse.Status.Error.Message}";
                }
                throw new DatabricksException(errorMessage);
            }

            return executeResponse;
        }

        /// <summary>
        /// Gets the status and results of a SQL statement.
        /// </summary>
        /// <param name="statementId">The statement ID.</param>
        /// <param name="cancellationToken">A cancellation token.</param>
        /// <returns>The statement status and results.</returns>
        public async Task<GetStatementResponse> GetStatementAsync(
            string statementId,
            CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(statementId))
            {
                throw new ArgumentException("Statement ID cannot be null or whitespace.", nameof(statementId));
            }

            var url = $"{_baseUrl}{StatementsEndpoint}/{statementId}";
            var httpRequest = new HttpRequestMessage(HttpMethod.Get, url);

            var response = await _httpClient.SendAsync(httpRequest, cancellationToken).ConfigureAwait(false);

            await EnsureSuccessStatusCodeAsync(response).ConfigureAwait(false);

            var responseContent = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
            var getResponse = JsonSerializer.Deserialize<GetStatementResponse>(responseContent, s_jsonOptions);

            if (getResponse == null)
            {
                throw new DatabricksException("Failed to deserialize GetStatementResponse");
            }

            return getResponse;
        }

        /// <summary>
        /// Gets a specific result chunk by index (for incremental fetching).
        /// </summary>
        /// <param name="statementId">The statement ID.</param>
        /// <param name="chunkIndex">The zero-based chunk index.</param>
        /// <param name="cancellationToken">A cancellation token.</param>
        /// <returns>The result chunk data.</returns>
        public async Task<ResultData> GetResultChunkAsync(
            string statementId,
            long chunkIndex,
            CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(statementId))
            {
                throw new ArgumentException("Statement ID cannot be null or whitespace.", nameof(statementId));
            }

            if (chunkIndex < 0)
            {
                throw new ArgumentException("Chunk index must be non-negative.", nameof(chunkIndex));
            }

            var url = $"{_baseUrl}{StatementsEndpoint}/{statementId}/result/chunks/{chunkIndex}";
            var httpRequest = new HttpRequestMessage(HttpMethod.Get, url);

            var response = await _httpClient.SendAsync(httpRequest, cancellationToken).ConfigureAwait(false);

            await EnsureSuccessStatusCodeAsync(response).ConfigureAwait(false);

            var responseContent = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
            var resultData = JsonSerializer.Deserialize<ResultData>(responseContent, s_jsonOptions);

            if (resultData == null)
            {
                throw new DatabricksException("Failed to deserialize ResultData");
            }

            return resultData;
        }

        /// <summary>
        /// Cancels a running SQL statement.
        /// </summary>
        /// <param name="statementId">The statement ID to cancel.</param>
        /// <param name="cancellationToken">A cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public async Task CancelStatementAsync(string statementId, CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(statementId))
            {
                throw new ArgumentException("Statement ID cannot be null or whitespace.", nameof(statementId));
            }

            var url = $"{_baseUrl}{StatementsEndpoint}/{statementId}/cancel";
            var httpRequest = new HttpRequestMessage(HttpMethod.Post, url);

            var response = await _httpClient.SendAsync(httpRequest, cancellationToken).ConfigureAwait(false);

            await EnsureSuccessStatusCodeAsync(response).ConfigureAwait(false);
        }

        /// <summary>
        /// Closes a SQL statement and releases its resources.
        /// Note: Uses DELETE method on /statements/{statement_id}, not POST to /close endpoint.
        /// </summary>
        /// <param name="statementId">The statement ID to close.</param>
        /// <param name="cancellationToken">A cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public async Task CloseStatementAsync(string statementId, CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(statementId))
            {
                throw new ArgumentException("Statement ID cannot be null or whitespace.", nameof(statementId));
            }

            // Databricks uses DELETE on /statements/{statement_id}, not POST to /close
            var url = $"{_baseUrl}{StatementsEndpoint}/{statementId}";
            var httpRequest = new HttpRequestMessage(HttpMethod.Delete, url);

            var response = await _httpClient.SendAsync(httpRequest, cancellationToken).ConfigureAwait(false);

            await EnsureSuccessStatusCodeAsync(response).ConfigureAwait(false);
        }

        /// <summary>
        /// Ensures that the HTTP response indicates success, otherwise throws an exception.
        /// </summary>
        /// <param name="response">The HTTP response message.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        private async Task EnsureSuccessStatusCodeAsync(HttpResponseMessage response)
        {
            if (response.IsSuccessStatusCode)
            {
                return;
            }

            var errorContent = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
            var errorMessage = $"Statement Execution API request failed with status code {(int)response.StatusCode} ({response.StatusCode})";

            // Try to parse error details from JSON response
            try
            {
                var errorResponse = JsonSerializer.Deserialize<ServiceError>(errorContent, s_jsonOptions);
                if (errorResponse?.ErrorCode != null || errorResponse?.Message != null)
                {
                    errorMessage = $"{errorMessage}. Error Code: {errorResponse.ErrorCode ?? "Unknown"}, Message: {errorResponse.Message ?? "Unknown"}";
                }
                else
                {
                    errorMessage = $"{errorMessage}. Response: {errorContent}";
                }
            }
            catch
            {
                // If JSON parsing fails, include raw error content
                errorMessage = $"{errorMessage}. Response: {errorContent}";
            }

            throw new DatabricksException(errorMessage);
        }
    }
}
