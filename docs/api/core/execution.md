# Execution

Implementation module: `async_durable_execution.core.execution`.

Public exports are also available from `async_durable_execution.core`.

Handler input is deserialized from the durable execution payload before user
code runs. Empty or whitespace payloads are normalized to `{}`, and malformed
JSON fails the invocation before the handler executes.

::: async_durable_execution.core.execution
    options:
      members:
        - durable_callable
        - durable_execution
