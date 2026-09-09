# Replay-Safe Values


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
        - random
        - now
        - timestamp
        - uuid
