# ProxyControlApi.Api.DefaultApi

All URIs are relative to *http://localhost:8081*

| Method | HTTP request | Description |
|--------|--------------|-------------|
| [**DisableScenario**](DefaultApi.md#disablescenario) | **POST** /scenarios/{name}/disable | Disable a failure scenario |
| [**EnableScenario**](DefaultApi.md#enablescenario) | **POST** /scenarios/{name}/enable | Enable a failure scenario |
| [**ListScenarios**](DefaultApi.md#listscenarios) | **GET** /scenarios | List all available failure scenarios |

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
