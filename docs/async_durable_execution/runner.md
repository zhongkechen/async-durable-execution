# Testing API

## Local and Cloud Runners

The runner executes durable handlers locally in memory or invokes deployed,
qualified Lambda functions in the cloud.

::: async_durable_execution
    options:
      members:
        - create_local_runner
        - create_cloud_runner
        - DurableFunctionLocalTestRunner
        - DurableFunctionCloudTestRunner
        - DurableFunctionTestResult
