# Retry Durable Work

Internal implementation module: `async_durable_execution._extension.with_retry`.

Use `with_retry()` to retry a block that can contain multiple durable
operations.

::: async_durable_execution._extension.with_retry
    options:
      members:
        - WithRetryContext
        - get_with_retry_context
        - with_retry
