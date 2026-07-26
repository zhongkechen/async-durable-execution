# Testing API

## Local and Cloud Runners

The runner executes durable handlers locally in memory or invokes deployed,
qualified Lambda functions in the cloud.

Use stable operation names and inspect results by name, such as
`result.get_step("fetch-user")`, instead of depending on operation order. Pass
the handler event through `input=`, and keep inputs and step or callback values
compatible with their configured serializer.

The local runner requires no AWS credentials. The cloud runner requires a
deployed function qualified by version, alias, or `$LATEST`; see
[Deploy and Invoke](../deployment.md).

::: async_durable_execution
    options:
      members:
        - create_local_runner
        - create_cloud_runner
        - DurableFunctionLocalTestRunner
        - DurableFunctionCloudTestRunner
        - DurableFunctionTestResult
