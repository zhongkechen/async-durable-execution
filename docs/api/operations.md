# Durable Operations

Durable operations checkpoint nondeterministic work or suspend execution. Give
operations stable names so execution history and tests remain readable. For
declarative composition of these operations, see [DAG workflows](dag.md).

## SDK Extensions

Operations that are not directly supported by the durable execution backend live
in `async_durable_execution.extension`. They include `flow()`, `map()`,
`parallel()`, `wait_for_callback()`, `wait_for_condition()`, `with_retry()`,
`recurse()`, and the replay-safe helpers. The package-root imports remain the
public API.

The official Python SDK does not provide the following helper operations.

### Replay-Safe Helper Values

Use `random()`, `now()`, `timestamp()`, and `uuid()` when workflow code needs
common nondeterministic values. Each helper creates a named durable step and
reuses the checkpointed value during replay.

```python
from async_durable_execution import now, random as durable_random, timestamp, uuid


request_id = await uuid(name="request_id")
created_at = await now(name="created_at")
created_at_seconds = await timestamp(name="created_at_seconds")
sample = await durable_random(name="sample")
```

### Recursive Self-Invocation

Use `recurse()` to invoke the current Lambda function as a new durable
execution. Unlike a Python recursive call, it does not grow the Python call
stack. See [recursive self-invocation](../advanced-usage.md#recursive-self-invocation)
for payload validation, recursion levels, Lambda permissions, and recursion
protection.

::: async_durable_execution
    options:
      members:
        - step
        - wait
        - StepSemantics
        - invoke
        - recurse
        - run_in_child_context
        - create_callback
        - Callback
        - CallbackError
        - now
        - timestamp
        - uuid
        - random
