# Durable Operations

Durable operations checkpoint nondeterministic work or suspend execution. Give
operations stable names so execution history and tests remain readable. For
declarative composition of these operations, see [DAG workflows](dag.md).

## SDK Extensions

The official Python SDK does not provide the following operations.

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

### Declarative DAG Workflows

Use `flow()` to execute a validated graph declared with `@durable_dag`,
`@durable_node`, and `node()`. DAG workflows support typed projections,
inferred and conditional dependencies, failure routes, and execution pruning.
See the [DAG workflow API](dag.md) for the complete programming model.

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
