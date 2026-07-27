# Cloud Runner

Internal implementation module: `async_durable_execution._runner.cloud`.

The cloud runner invokes a deployed durable Lambda function qualified by
version, alias, or `$LATEST`. See [Deploy and Invoke](../../deployment.md).

::: async_durable_execution._runner.cloud
    options:
      members:
        - create_cloud_runner
        - DurableFunctionCloudTestRunner
