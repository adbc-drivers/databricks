# ProxyControlApi.Api.DefaultApi

All URIs are relative to *http://localhost:8081*

| Method | HTTP request | Description |
|--------|--------------|-------------|
| [**DisableScenario**](DefaultApi.md#disablescenario) | **POST** /scenarios/{name}/disable | Disable a failure scenario |
| [**EnableScenario**](DefaultApi.md#enablescenario) | **POST** /scenarios/{name}/enable | Enable a failure scenario |
| [**GetThriftCalls**](DefaultApi.md#getthriftcalls) | **GET** /thrift/calls | Get Thrift method call history |
| [**ListScenarios**](DefaultApi.md#listscenarios) | **GET** /scenarios | List all available failure scenarios |
| [**ResetThriftCalls**](DefaultApi.md#resetthriftcalls) | **POST** /thrift/calls/reset | Reset Thrift call history |
| [**VerifyThriftCalls**](DefaultApi.md#verifythriftcalls) | **POST** /thrift/calls/verify | Verify Thrift call sequence |

<a id="disablescenario"></a>
# **DisableScenario**
> ScenarioStatus DisableScenario (string name)

Disable a failure scenario

Disables a failure scenario by name. Disabled scenarios will not trigger even if matching requests are made through the proxy.  **Note:** Scenarios auto-disable after injection, so manual disable is typically only needed to cancel a scenario before it triggers.


### Parameters

| Name | Type | Description | Notes |
|------|------|-------------|-------|
| **name** | **string** | The unique name of the failure scenario (from proxy-config.yaml) |  |

### Return type

[**ScenarioStatus**](ScenarioStatus.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json, text/plain


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Scenario disabled successfully |  -  |
| **404** | Scenario not found |  -  |

[[Back to top]](#) [[Back to API list]](../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../README.md#documentation-for-models) [[Back to README]](../../README.md)

<a id="enablescenario"></a>
# **EnableScenario**
> ScenarioStatus EnableScenario (string name)

Enable a failure scenario

Enables a failure scenario by name. Once enabled, the scenario will trigger on the next matching request (CloudFetch download or Thrift operation).  **Note:** Scenarios auto-disable after first injection (one-shot behavior).


### Parameters

| Name | Type | Description | Notes |
|------|------|-------------|-------|
| **name** | **string** | The unique name of the failure scenario (from proxy-config.yaml) |  |

### Return type

[**ScenarioStatus**](ScenarioStatus.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json, text/plain


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Scenario enabled successfully |  -  |
| **404** | Scenario not found |  -  |

[[Back to top]](#) [[Back to API list]](../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../README.md#documentation-for-models) [[Back to README]](../../README.md)

<a id="getthriftcalls"></a>
# **GetThriftCalls**
> ThriftCallHistory GetThriftCalls ()

Get Thrift method call history

Returns the history of Thrift method calls tracked by the proxy. Includes method name, timestamp, message type, and sequence ID.  **Note:** Call history is automatically reset when a scenario is enabled.


### Parameters
This endpoint does not need any parameter.
### Return type

[**ThriftCallHistory**](ThriftCallHistory.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Call history retrieved successfully |  -  |

[[Back to top]](#) [[Back to API list]](../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../README.md#documentation-for-models) [[Back to README]](../../README.md)

<a id="listscenarios"></a>
# **ListScenarios**
> ScenarioList ListScenarios ()

List all available failure scenarios

Returns a list of all configured failure scenarios with their current status. Each scenario includes its name, description, and enabled state.


### Parameters
This endpoint does not need any parameter.
### Return type

[**ScenarioList**](ScenarioList.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | List of scenarios retrieved successfully |  -  |

[[Back to top]](#) [[Back to API list]](../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../README.md#documentation-for-models) [[Back to README]](../../README.md)

<a id="resetthriftcalls"></a>
# **ResetThriftCalls**
> ResetThriftCalls200Response ResetThriftCalls ()

Reset Thrift call history

Manually resets the Thrift call history to empty.  **Note:** Call history is automatically reset when a scenario is enabled, so manual reset is typically not needed.


### Parameters
This endpoint does not need any parameter.
### Return type

[**ResetThriftCalls200Response**](ResetThriftCalls200Response.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Call history reset successfully |  -  |

[[Back to top]](#) [[Back to API list]](../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../README.md#documentation-for-models) [[Back to README]](../../README.md)

<a id="verifythriftcalls"></a>
# **VerifyThriftCalls**
> ThriftVerificationResult VerifyThriftCalls (ThriftVerificationRequest thriftVerificationRequest)

Verify Thrift call sequence

Verifies that Thrift method calls match expected patterns.  Supports four verification types: - **exact_sequence**: Exact match of method sequence - **contains_sequence**: Methods appear in order (not necessarily consecutive) - **method_count**: Verify specific method called N times - **method_exists**: Verify method was called at least once


### Parameters

| Name | Type | Description | Notes |
|------|------|-------------|-------|
| **thriftVerificationRequest** | [**ThriftVerificationRequest**](ThriftVerificationRequest.md) |  |  |

### Return type

[**ThriftVerificationResult**](ThriftVerificationResult.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Verification result |  -  |
| **400** | Invalid request |  -  |

[[Back to top]](#) [[Back to API list]](../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../README.md#documentation-for-models) [[Back to README]](../../README.md)
