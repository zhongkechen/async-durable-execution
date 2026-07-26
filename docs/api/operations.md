# Durable Operations

Durable operations checkpoint nondeterministic work or suspend execution. Give
operations stable names so execution history and tests remain readable. For
declarative composition of these operations, see [DAG workflows](dag.md).

## Replay-Safe Helper Values

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
