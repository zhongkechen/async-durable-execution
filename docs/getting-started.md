# Getting Started

This guide creates and locally tests a durable order workflow. You need Python
3.10 or newer.

## Install

=== "Standard"

    ```console
    pip install async-durable-execution
    ```

=== "Async Lambda client"

    ```console
    pip install "async-durable-execution[aioboto]"
    ```

    The `aioboto` extra uses `aiobotocore` for Lambda checkpoint and state API
    calls. Without it, the SDK runs its bundled synchronous client through an
    async adapter.

## Create a Workflow

Save this as `order_workflow.py`:

```python
from datetime import timedelta

from async_durable_execution import (
    durable_callable,
    durable_execution,
    step,
    wait,
)


@durable_callable
async def validate_order(order_id: str) -> dict:
    return {"order_id": order_id, "valid": True}


@durable_execution
async def handler(event: dict) -> dict:
    order = await step(
        validate_order(event["order_id"]),
        name="validate-order",
    )
    await wait(timedelta(seconds=5), name="processing-delay")
    return {"status": "complete", "order": order}
```

`validate_order()` is a durable callable. `step()` checkpoints its result, and
`wait()` suspends execution until the delay ends.

## Test Locally

```python
import asyncio

from async_durable_execution import InvocationStatus, create_local_runner
from order_workflow import handler


async def main() -> None:
    with create_local_runner(
        handler=handler,
        input={"order_id": "order-123"},
        timeout=10,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_step("validate-order").step_details is not None
    print(result.get_deserialized_result())


asyncio.run(main())
```

The local runner intercepts checkpoint operations in memory and requires no AWS
credentials.

## Understand Replay

On resume, durable workflows run again from the beginning. Completed operations
return saved results, but ordinary Python code executes again.

!!! warning "Keep replayed code deterministic"

    Put API calls, database queries, filesystem access, UUID generation, random
    values, and clock reads inside `step()`. Do not mutate outer variables from
    a step or perform side effects directly in the handler.

Standard `logging` calls are replay-aware when they run within a durable
execution.

## Continue

- [Durable operations](api/operations.md)
- [Background tasks and advanced patterns](advanced-usage.md)
- [Using synchronous libraries](using-synchronous-code.md)
- [Deploying the included example](https://github.com/zhongkechen/async-durable-execution#deploy-now)
