# Retry Durable Work


Use `with_retry()` to retry a block that can contain multiple durable
operations.

::: async_durable_execution
    options:
      members:
        - WithRetryContext
        - get_with_retry_context
        - with_retry
