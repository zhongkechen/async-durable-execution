# Child Context

Internal implementation module: `async_durable_execution._primitive.child`.

Use a child context to group durable operations in an isolated operation scope.
The child function may be sync when it only returns an ordinary value. It must
use `async def` when it creates steps, waits, or other durable operations.

::: async_durable_execution._primitive.child
    options:
      members:
        - SummaryGenerator
        - run_in_child_context
