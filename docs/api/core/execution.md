# Execution

Internal implementation module: `async_durable_execution._core.execution`.

Import these public symbols from `async_durable_execution`.

Handler input is deserialized from the durable execution payload before user
code runs. Empty or whitespace payloads are normalized to `{}`, and malformed
JSON fails the invocation before the handler executes.

::: async_durable_execution._core.execution
    options:
      members:
        - durable_callable
        - durable_execution
