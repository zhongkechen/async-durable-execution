# Runner API

Import these public symbols from `async_durable_execution`.

## Local Runner

The local runner executes durable handlers in memory without AWS credentials.
Use stable operation names and inspect results by name instead of depending on
operation order.

::: async_durable_execution
    options:
      members:
        - create_local_runner
        - DurableFunctionLocalTestRunner

## Cloud Runner

The cloud runner invokes a deployed durable Lambda function qualified by
version, alias, or `$LATEST`. See [Deploy and Invoke](../deployment.md).

::: async_durable_execution
    options:
      members:
        - create_cloud_runner
        - DurableFunctionCloudTestRunner

## Test Results

Use stable operation names and inspect results by name, such as
`result.get_step("fetch-user")`, instead of depending on operation order.

::: async_durable_execution
    options:
      members:
        - DurableFunctionTestResult
