# Recursive Invocation

Implementation module: `async_durable_execution.extension.recurse`.

Use `recurse()` to invoke the current Lambda function as a new durable
execution. Unlike a Python recursive call, it does not grow the Python call
stack. See [recursive self-invocation](../../advanced-usage.md#recursive-self-invocation)
for payload validation, recursion levels, Lambda permissions, and recursion
protection.

::: async_durable_execution.extension.recurse
    options:
      members:
        - recurse
