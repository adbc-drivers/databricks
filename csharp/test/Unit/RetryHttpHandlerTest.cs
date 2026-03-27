/*
* Copyright (c) 2025 ADBC Drivers Contributors
*
* This file has been modified from its original version, which is
* under the Apache License:
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tracing;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit
{
    /// <summary>
    /// Tests for the RetryHttpHandler class.
    ///
    /// IMPORTANT: These tests verify retry behavior in isolation. In production, RetryHttpHandler
    /// must be positioned INSIDE (closer to network) ThriftErrorMessageHandler in the handler chain
    /// so that retries happen before exceptions are thrown. See DatabricksConnection.CreateHttpHandler()
    /// for the correct handler chain ordering and detailed explanation.
    /// </summary>
    public class RetryHttpHandlerTest
    {
        /// <summary>
        /// Mock activity tracer for testing.
        /// </summary>
        private class MockActivityTracer : IActivityTracer
        {
            public string? TraceParent { get; set; }
            public ActivityTrace Trace => new ActivityTrace("TestSource", "1.0.0", TraceParent);
            public string AssemblyVersion => "1.0.0";
            public string AssemblyName => "TestAssembly";
        }

        /// <summary>
        /// Tests that RetryHttpHandler implements IActivityTracer and uses TraceActivityAsync for logging.
        /// </summary>
        [Fact]
        public async Task RetryHandlerUsesTraceActivityAsyncForLogging()
        {
            var mockHandler = new MockHttpMessageHandler(
                new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
                {
                    Headers = { { "Retry-After", "1" } },
                    Content = new StringContent("Service Unavailable")
                });

            mockHandler.SetResponseAfterRetryCount(2, new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("Success")
            });

            var mockTracer = new MockActivityTracer();
            var retryHandler = new RetryHttpHandler(mockHandler, mockTracer, 10, 10, true, true);

            // Verify IActivityTracer implementation
            IActivityTracer tracerInterface = retryHandler;
            Assert.NotNull(tracerInterface);
            Assert.Equal("1.0.0", tracerInterface.AssemblyVersion);
            Assert.Equal("TestAssembly", tracerInterface.AssemblyName);

            // Verify retry behavior works
            var httpClient = new HttpClient(retryHandler);
            var response = await httpClient.GetAsync("http://test.com");

            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal(3, mockHandler.RequestCount);
        }

        /// <summary>
        /// Tests that the RetryHttpHandler properly processes 503 responses with Retry-After headers.
        /// </summary>
        [Fact]
        public async Task RetryAfterHandlerProcesses503Response()
        {
            // Create a mock handler that returns a 503 response with a Retry-After header
            var mockHandler = new MockHttpMessageHandler(
                new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
                {
                    Headers = { { "Retry-After", "1" } },
                    Content = new StringContent("Service Unavailable")
                });

            // Create the RetryHttpHandler with retry enabled and a 5-second timeout
            var mockTracer = new MockActivityTracer();
            var retryHandler = new RetryHttpHandler(mockHandler, mockTracer, 5, 5, true, true);

            // Create an HttpClient with our handler
            var httpClient = new HttpClient(retryHandler);

            // Set the mock handler to return a success response after the first retry
            mockHandler.SetResponseAfterRetryCount(1, new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("Success")
            });

            // Send a request
            var response = await httpClient.GetAsync("http://test.com");

            // Verify the response is OK
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal("Success", await response.Content.ReadAsStringAsync());
            Assert.Equal(2, mockHandler.RequestCount); // Initial request + 1 retry
        }

        /// <summary>
        /// Tests that the RetryHttpHandler throws an exception when the retry timeout is exceeded.
        /// </summary>
        [Fact]
        public async Task RetryAfterHandlerThrowsWhenTimeoutExceeded()
        {
            // Create a mock handler that always returns a 503 response with a Retry-After header
            var mockHandler = new MockHttpMessageHandler(
                new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
                {
                    Headers = { { "Retry-After", "2" } },
                    Content = new StringContent("Service Unavailable")
                });

            // Create the RetryHttpHandler with retry enabled and a 1-second timeout
            var mockTracer = new MockActivityTracer();
            var retryHandler = new RetryHttpHandler(mockHandler, mockTracer, 1, 1, true, true);

            // Create an HttpClient with our handler
            var httpClient = new HttpClient(retryHandler);

            // Send a request and expect an AdbcException
            var exception = await Assert.ThrowsAsync<DatabricksException>(async () =>
                await httpClient.GetAsync("http://test.com"));

            // Verify the exception has the correct SQL state
            Assert.Contains("08001", exception.SqlState);
            Assert.Equal(AdbcStatusCode.IOError, exception.Status);

            // Verify we only tried once (since the Retry-After value of 2 exceeds our timeout of 1)
            Assert.Equal(1, mockHandler.RequestCount);
        }

        /// <summary>
        /// Tests that the RetryHttpHandler handles non-retryable responses correctly.
        /// </summary>
        [Fact]
        public async Task RetryAfterHandlerHandlesNonRetryableResponse()
        {
            // Create a mock handler that returns a 404 response
            var mockHandler = new MockHttpMessageHandler(
                new HttpResponseMessage(HttpStatusCode.NotFound)
                {
                    Content = new StringContent("Not Found")
                });

            // Create the RetryHttpHandler with retry enabled
            var mockTracer = new MockActivityTracer();
            var retryHandler = new RetryHttpHandler(mockHandler, mockTracer, 5, 5, true, true);

            // Create an HttpClient with our handler
            var httpClient = new HttpClient(retryHandler);

            // Send a request
            var response = await httpClient.GetAsync("http://test.com");

            // Verify the response is 404
            Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
            Assert.Equal("Not Found", await response.Content.ReadAsStringAsync());
            Assert.Equal(1, mockHandler.RequestCount); // Only the initial request, no retries
        }

        /// <summary>
        /// Tests that the RetryHttpHandler handles 503 responses without Retry-After headers using exponential backoff.
        /// </summary>
        [Fact]
        public async Task RetryHandlerUsesExponentialBackoffFor503WithoutRetryAfterHeader()
        {
            // Create a mock handler that returns a 503 response without a Retry-After header
            var mockHandler = new MockHttpMessageHandler(
                new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
                {
                    Content = new StringContent("Service Unavailable")
                });

            // Create the RetryHttpHandler with retry enabled
            var mockTracer = new MockActivityTracer();
            var retryHandler = new RetryHttpHandler(mockHandler, mockTracer, 5, 5, true, true);

            // Create an HttpClient with our handler
            var httpClient = new HttpClient(retryHandler);

            // Set the mock handler to return a success response after the second retry
            mockHandler.SetResponseAfterRetryCount(2, new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("Success")
            });

            // Send a request
            var response = await httpClient.GetAsync("http://test.com");

            // Verify the response is OK
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal("Success", await response.Content.ReadAsStringAsync());
            Assert.Equal(3, mockHandler.RequestCount); // Initial request + 2 retries
        }

        /// <summary>
        /// Tests that the RetryHttpHandler handles invalid Retry-After headers by using exponential backoff.
        /// </summary>
        [Fact]
        public async Task RetryHandlerUsesExponentialBackoffForInvalidRetryAfterHeader()
        {
            // Create a mock handler that returns a 503 response with an invalid Retry-After header
            var mockHandler = new MockHttpMessageHandler(
                new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
                {
                    Content = new StringContent("Service Unavailable")
                });

            // Add the invalid Retry-After header directly in the test
            var response = new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
            {
                Content = new StringContent("Service Unavailable")
            };
            response.Headers.TryAddWithoutValidation("Retry-After", "invalid");
            mockHandler.SetResponseAfterRetryCount(0, response);

            // Set the mock handler to return a success response after the first retry
            mockHandler.SetResponseAfterRetryCount(1, new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("Success")
            });

            // Create the RetryHttpHandler with retry enabled
            var mockTracer = new MockActivityTracer();
            var retryHandler = new RetryHttpHandler(mockHandler, mockTracer, 5, 5, true, true);

            // Create an HttpClient with our handler
            var httpClient = new HttpClient(retryHandler);

            // Send a request
            response = await httpClient.GetAsync("http://test.com");

            // Verify the response is OK
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal("Success", await response.Content.ReadAsStringAsync());
            Assert.Equal(2, mockHandler.RequestCount); // Initial request + 1 retry
        }

        /// <summary>
        /// Tests that the RetryHttpHandler properly processes retryable status codes.
        /// </summary>
        [Theory]
        [InlineData(HttpStatusCode.RequestTimeout, "Request Timeout")]      // 408
        [InlineData(HttpStatusCode.BadGateway, "Bad Gateway")]              // 502
        [InlineData(HttpStatusCode.ServiceUnavailable, "Service Unavailable")] // 503
        [InlineData(HttpStatusCode.GatewayTimeout, "Gateway Timeout")]      // 504
        public async Task RetryHandlerProcessesRetryableStatusCodes(HttpStatusCode statusCode, string errorMessage)
        {
            // Create a mock handler that returns the specified status code
            var mockHandler = new MockHttpMessageHandler(
                new HttpResponseMessage(statusCode)
                {
                    Content = new StringContent(errorMessage)
                });

            // Create the RetryHttpHandler with retry enabled
            var mockTracer = new MockActivityTracer();
            var retryHandler = new RetryHttpHandler(mockHandler, mockTracer, 5, 5, true, true);

            // Create an HttpClient with our handler
            var httpClient = new HttpClient(retryHandler);

            // Set the mock handler to return a success response after the first retry
            mockHandler.SetResponseAfterRetryCount(1, new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("Success")
            });

            // Send a request
            var response = await httpClient.GetAsync("http://test.com");

            // Verify the response is OK
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal("Success", await response.Content.ReadAsStringAsync());
            Assert.Equal(2, mockHandler.RequestCount); // Initial request + 1 retry
        }

        /// <summary>
        /// Tests that the RetryHttpHandler properly handles multiple retries with exponential backoff.
        /// </summary>
        [Fact]
        public async Task RetryHandlerHandlesMultipleRetriesWithExponentialBackoff()
        {
            // Create a mock handler that returns a 503 response without a Retry-After header
            var mockHandler = new MockHttpMessageHandler(
                new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
                {
                    Content = new StringContent("Service Unavailable")
                });

            // Create the RetryHttpHandler with retry enabled and a generous timeout
            var mockTracer = new MockActivityTracer();
            var retryHandler = new RetryHttpHandler(mockHandler, mockTracer, 10, 10, true, true);

            // Create an HttpClient with our handler
            var httpClient = new HttpClient(retryHandler);

            // Set the mock handler to return a success response after the third retry
            mockHandler.SetResponseAfterRetryCount(3, new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("Success")
            });

            // Send a request
            var response = await httpClient.GetAsync("http://test.com");

            // Verify the response is OK
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal("Success", await response.Content.ReadAsStringAsync());
            Assert.Equal(4, mockHandler.RequestCount); // Initial request + 3 retries
        }

        /// <summary>
        /// Tests that the RetryHttpHandler throws an exception when the server keeps returning errors
        /// and we reach the timeout with exponential backoff.
        /// </summary>
        [Theory]
        [InlineData(HttpStatusCode.RequestTimeout)]      // 408
        [InlineData(HttpStatusCode.BadGateway)]          // 502
        [InlineData(HttpStatusCode.ServiceUnavailable)]  // 503
        [InlineData(HttpStatusCode.GatewayTimeout)]      // 504
        public async Task RetryHandlerThrowsWhenServerNeverRecovers(HttpStatusCode statusCode)
        {
            // Create a mock handler that always returns the error status code
            var mockHandler = new MockHttpMessageHandler(
                new HttpResponseMessage(statusCode)
                {
                    Content = new StringContent($"Error: {statusCode}")
                });

            // Create the RetryHttpHandler with a short timeout to make the test run faster
            var mockTracer = new MockActivityTracer();
            var retryHandler = new RetryHttpHandler(mockHandler, mockTracer, 3, 3, true, true);

            // Create an HttpClient with our handler
            var httpClient = new HttpClient(retryHandler);

            // Send a request and expect a DatabricksException
            var exception = await Assert.ThrowsAsync<DatabricksException>(async () =>
                await httpClient.GetAsync("http://test.com"));

            // Verify the exception has the correct SQL state
            Assert.Contains("08001", exception.SqlState);
            Assert.Equal(AdbcStatusCode.IOError, exception.Status);

            // Verify we tried multiple times before giving up
            Assert.True(mockHandler.RequestCount > 1, $"Expected multiple requests, but got {mockHandler.RequestCount}");
        }

        /// <summary>
        /// Tests that the RetryHttpHandler properly handles HTTP TooManyRequests (429) responses with separate timeout.
        /// </summary>
        [Fact]
        public async Task RetryHandlerHandlesRateLimitWithSeparateTimeout()
        {
            // Create a mock handler that returns a TooManyRequests (429) response with a Retry-After header
            var mockHandler = new MockHttpMessageHandler(
                new HttpResponseMessage((HttpStatusCode)429)
                {
                    Headers = { { "Retry-After", "1" } },
                    Content = new StringContent("Too Many Requests")
                });

            // Create the RetryHttpHandler with different timeouts: 900s for ServiceUnavailable, 2s for TooManyRequests
            var mockTracer = new MockActivityTracer();
            var retryHandler = new RetryHttpHandler(mockHandler, mockTracer, 900, 2, true, true);

            // Create an HttpClient with our handler
            var httpClient = new HttpClient(retryHandler);

            // Set the mock handler to return a success response after the first retry
            mockHandler.SetResponseAfterRetryCount(1, new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("Success")
            });

            // Send a request
            var response = await httpClient.GetAsync("http://test.com");

            // Verify the response is OK
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal("Success", await response.Content.ReadAsStringAsync());
            Assert.Equal(2, mockHandler.RequestCount); // Initial request + 1 retry
        }

        /// <summary>
        /// Tests that the RetryHttpHandler respects the rate limit timeout for TooManyRequests (429) responses.
        /// </summary>
        [Fact]
        public async Task RetryHandlerRespectsRateLimitTimeout()
        {
            // Create a mock handler that always returns a TooManyRequests (429) response with a Retry-After header
            var mockHandler = new MockHttpMessageHandler(
                new HttpResponseMessage((HttpStatusCode)429)
                {
                    Headers = { { "Retry-After", "2" } },
                    Content = new StringContent("Too Many Requests")
                });

            // Create the RetryHttpHandler with different timeouts: 900s for ServiceUnavailable, 1s for TooManyRequests
            var mockTracer = new MockActivityTracer();
            var retryHandler = new RetryHttpHandler(mockHandler, mockTracer, 900, 1, true, true);

            // Create an HttpClient with our handler
            var httpClient = new HttpClient(retryHandler);

            // Send a request and expect a DatabricksException
            var exception = await Assert.ThrowsAsync<DatabricksException>(async () =>
                await httpClient.GetAsync("http://test.com"));

            // Verify the exception has the correct SQL state
            Assert.Contains("08001", exception.SqlState);
            Assert.Equal(AdbcStatusCode.IOError, exception.Status);

            // Verify we only tried once (since the Retry-After value of 2 exceeds our TooManyRequests timeout of 1)
            Assert.Equal(1, mockHandler.RequestCount);
        }

        /// <summary>
        /// Tests that transport errors (HttpRequestException) are retried and succeed after recovery.
        /// </summary>
        [Fact]
        public async Task TransportError_HttpRequestException_RetriesAndSucceeds()
        {
            var mockHandler = new MockHttpMessageHandler(
                new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent("Success")
                });

            // Throw HttpRequestException for the first 2 attempts, then succeed
            mockHandler.SetExceptionForRequestCount(2, new HttpRequestException("Connection refused"));

            var mockTracer = new MockActivityTracer();
            var retryHandler = new RetryHttpHandler(mockHandler, mockTracer, 10, 10, true, true,
                transportErrorRetryEnabled: true, httpRequestTimeoutSeconds: 0);

            var httpClient = new HttpClient(retryHandler);
            var response = await httpClient.GetAsync("http://test.com");

            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal(3, mockHandler.RequestCount); // 2 failures + 1 success
        }

        /// <summary>
        /// Tests that transport errors (SocketException) are retried.
        /// </summary>
        [Fact]
        public async Task TransportError_SocketException_RetriesAndSucceeds()
        {
            var mockHandler = new MockHttpMessageHandler(
                new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent("Success")
                });

            mockHandler.SetExceptionForRequestCount(1, new SocketException((int)SocketError.ConnectionRefused));

            var mockTracer = new MockActivityTracer();
            var retryHandler = new RetryHttpHandler(mockHandler, mockTracer, 10, 10, true, true,
                transportErrorRetryEnabled: true, httpRequestTimeoutSeconds: 0);

            var httpClient = new HttpClient(retryHandler);
            var response = await httpClient.GetAsync("http://test.com");

            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal(2, mockHandler.RequestCount);
        }

        /// <summary>
        /// Tests that transport errors (IOException) are retried.
        /// </summary>
        [Fact]
        public async Task TransportError_IOException_RetriesAndSucceeds()
        {
            var mockHandler = new MockHttpMessageHandler(
                new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent("Success")
                });

            mockHandler.SetExceptionForRequestCount(1, new IOException("Connection reset by peer"));

            var mockTracer = new MockActivityTracer();
            var retryHandler = new RetryHttpHandler(mockHandler, mockTracer, 10, 10, true, true,
                transportErrorRetryEnabled: true, httpRequestTimeoutSeconds: 0);

            var httpClient = new HttpClient(retryHandler);
            var response = await httpClient.GetAsync("http://test.com");

            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal(2, mockHandler.RequestCount);
        }

        /// <summary>
        /// Tests that wrapped transport errors (HttpRequestException wrapping SocketException) are retried.
        /// </summary>
        [Fact]
        public async Task TransportError_WrappedException_RetriesAndSucceeds()
        {
            var mockHandler = new MockHttpMessageHandler(
                new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent("Success")
                });

            // Simulate the real exception chain: HttpRequestException -> WebException -> SocketException
            var innerException = new SocketException((int)SocketError.ConnectionRefused);
            var outerException = new HttpRequestException("An error occurred while sending the request.", innerException);
            mockHandler.SetExceptionForRequestCount(1, outerException);

            var mockTracer = new MockActivityTracer();
            var retryHandler = new RetryHttpHandler(mockHandler, mockTracer, 10, 10, true, true,
                transportErrorRetryEnabled: true, httpRequestTimeoutSeconds: 0);

            var httpClient = new HttpClient(retryHandler);
            var response = await httpClient.GetAsync("http://test.com");

            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal(2, mockHandler.RequestCount);
        }

        /// <summary>
        /// Tests that transport error retry throws DatabricksException when timeout is exceeded.
        /// </summary>
        [Fact]
        public async Task TransportError_ThrowsWhenTimeoutExceeded()
        {
            var mockHandler = new MockHttpMessageHandler(
                new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent("Success")
                });

            // Always throw — never recover
            mockHandler.SetExceptionForRequestCount(int.MaxValue, new HttpRequestException("Connection refused"));

            var mockTracer = new MockActivityTracer();
            // Use a very short transport error timeout so it exceeds quickly
            var retryHandler = new RetryHttpHandler(mockHandler, mockTracer, 1, 1, true, true,
                transportErrorRetryEnabled: true, httpRequestTimeoutSeconds: 0);

            var httpClient = new HttpClient(retryHandler);

            var exception = await Assert.ThrowsAsync<DatabricksException>(async () =>
                await httpClient.GetAsync("http://test.com"));

            Assert.Contains("08001", exception.SqlState);
            Assert.Equal(AdbcStatusCode.IOError, exception.Status);
            Assert.NotNull(exception.InnerException);
            Assert.IsType<HttpRequestException>(exception.InnerException);
            Assert.True(mockHandler.RequestCount >= 1);
        }

        /// <summary>
        /// Tests that transport error retry is disabled when transportErrorRetryEnabled is false.
        /// </summary>
        [Fact]
        public async Task TransportError_NotRetriedWhenDisabled()
        {
            var mockHandler = new MockHttpMessageHandler(
                new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent("Success")
                });

            mockHandler.SetExceptionForRequestCount(1, new HttpRequestException("Connection refused"));

            var mockTracer = new MockActivityTracer();
            var retryHandler = new RetryHttpHandler(mockHandler, mockTracer, 10, 10, true, true,
                transportErrorRetryEnabled: false, httpRequestTimeoutSeconds: 0);

            var httpClient = new HttpClient(retryHandler);

            // Should throw immediately without retry
            await Assert.ThrowsAsync<HttpRequestException>(async () =>
                await httpClient.GetAsync("http://test.com"));

            Assert.Equal(1, mockHandler.RequestCount);
        }

        /// <summary>
        /// Tests that user cancellation is NOT retried even when it looks like a transport error.
        /// </summary>
        [Fact]
        public async Task TransportError_UserCancellationNotRetried()
        {
            var cts = new CancellationTokenSource();
            var mockHandler = new MockHttpMessageHandler(
                new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent("Success")
                });

            // Simulate: the handler throws TaskCanceledException because the user's token was cancelled
            mockHandler.SetCancelOnRequest(1, cts);

            var mockTracer = new MockActivityTracer();
            var retryHandler = new RetryHttpHandler(mockHandler, mockTracer, 10, 10, true, true,
                transportErrorRetryEnabled: true, httpRequestTimeoutSeconds: 0);

            var httpClient = new HttpClient(retryHandler);

            var ex = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
                await httpClient.GetAsync("http://test.com", cts.Token));

            Assert.True(cts.IsCancellationRequested);
            Assert.Equal(1, mockHandler.RequestCount);
        }

        /// <summary>
        /// Tests that per-request timeout (TaskCanceledException from internal CTS) is retried.
        /// </summary>
        [Fact]
        public async Task TransportError_PerRequestTimeoutIsRetried()
        {
            var mockHandler = new MockHttpMessageHandler(
                new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent("Success")
                });

            // Simulate a dead connection by delaying longer than the per-request timeout
            mockHandler.SetDelayForRequestCount(1, TimeSpan.FromSeconds(5));

            var mockTracer = new MockActivityTracer();
            // Set a very short per-request timeout (1 second) to trigger quickly
            var retryHandler = new RetryHttpHandler(mockHandler, mockTracer, 10, 10, true, true,
                transportErrorRetryEnabled: true, httpRequestTimeoutSeconds: 1);

            var httpClient = new HttpClient(retryHandler);
            var response = await httpClient.GetAsync("http://test.com");

            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal(2, mockHandler.RequestCount); // 1 timeout + 1 success
        }

        /// <summary>
        /// Mock HttpMessageHandler for testing the RetryHttpHandler.
        /// </summary>
        private class MockHttpMessageHandler : HttpMessageHandler
        {
            private readonly HttpResponseMessage _defaultResponse;
            private HttpResponseMessage? _responseAfterRetryCount;
            private int _retryCountForResponse;
            private Exception? _exception;
            private int _exceptionRequestCount;
            private int _delayRequestCount;
            private TimeSpan _delay;
            private int _cancelOnRequest;
            private CancellationTokenSource? _cancelCts;

            public int RequestCount { get; private set; }

            public MockHttpMessageHandler(HttpResponseMessage defaultResponse)
            {
                _defaultResponse = defaultResponse;
            }

            public void SetResponseAfterRetryCount(int retryCount, HttpResponseMessage response)
            {
                _retryCountForResponse = retryCount;
                _responseAfterRetryCount = response;
            }

            /// <summary>
            /// Throw the specified exception for the first N requests, then return the default response.
            /// </summary>
            public void SetExceptionForRequestCount(int count, Exception exception)
            {
                _exceptionRequestCount = count;
                _exception = exception;
            }

            /// <summary>
            /// Delay the response for the first N requests (to trigger per-request timeout).
            /// </summary>
            public void SetDelayForRequestCount(int count, TimeSpan delay)
            {
                _delayRequestCount = count;
                _delay = delay;
            }

            /// <summary>
            /// Cancel the given CTS on request N to simulate user cancellation.
            /// </summary>
            public void SetCancelOnRequest(int requestNumber, CancellationTokenSource cts)
            {
                _cancelOnRequest = requestNumber;
                _cancelCts = cts;
            }

            protected override async Task<HttpResponseMessage> SendAsync(
                HttpRequestMessage request,
                CancellationToken cancellationToken)
            {
                RequestCount++;

                // Simulate user cancellation
                if (_cancelCts != null && RequestCount == _cancelOnRequest)
                {
                    _cancelCts.Cancel();
                    cancellationToken.ThrowIfCancellationRequested();
                }

                // Simulate transport error
                if (_exception != null && RequestCount <= _exceptionRequestCount)
                {
                    throw _exception;
                }

                // Simulate dead connection (delay to trigger per-request timeout)
                if (_delayRequestCount > 0 && RequestCount <= _delayRequestCount)
                {
                    await Task.Delay(_delay, cancellationToken);
                }

                if (_responseAfterRetryCount != null && RequestCount > _retryCountForResponse)
                {
                    return _responseAfterRetryCount;
                }

                // Create a new response instance to avoid modifying the original
                var response = new HttpResponseMessage
                {
                    StatusCode = _defaultResponse.StatusCode,
                    Content = _defaultResponse.Content
                };

                // Copy headers only if they exist
                if (_defaultResponse.Headers.Contains("Retry-After"))
                {
                    foreach (var value in _defaultResponse.Headers.GetValues("Retry-After"))
                    {
                        response.Headers.Add("Retry-After", value);
                    }
                }

                return response;
            }
        }
    }
}
