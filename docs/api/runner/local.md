# Local Runner

Implementation module: `async_durable_execution.runner.local`.

The local runner executes durable handlers in memory without AWS credentials.
Use stable operation names and inspect results by name instead of depending on
operation order.

::: async_durable_execution.runner.local
    options:
      members:
        - create_local_runner
        - DurableFunctionLocalTestRunner
