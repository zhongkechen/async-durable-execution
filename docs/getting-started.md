# Getting Started

This guide creates and locally tests a durable order workflow. You need Python
3.10 or newer.

Import user-facing APIs from `async_durable_execution`, including extension
contracts, filesystem SerDes stages, and preview helpers. The former
`async_durable_execution.extension`, `async_durable_execution.filesystem_serdes`,
and `async_durable_execution.preview` modules are now private implementation
modules; their old public import paths are no longer supported. For example:

```python
from async_durable_execution import (
    ExtensionStepResult,
    FileSystemSerDesStage,
    PreviewConfig,
    PreviewMode,
    get_extension_context,
)
```

## Install

=== "Standard"

    ```console
    pip install async-durable-execution
    ```

=== "Async Lambda client"

    ```console
    pip install "async-durable-execution[httpx]"
    ```

    The `httpx` extra installs HTTPX for asynchronous model-free Lambda REST
    calls. Without it, the SDK sends the same signed requests with botocore's
    synchronous HTTP transport through an async adapter.

    The legacy `aioboto` extra remains a backward-compatible alias for `httpx`.

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

### Compose Durable Operations Outside Steps

A callable passed to `step()` is one atomic operation and cannot start another
durable operation. Use `run_in_child_context()` when a reusable sub-workflow
needs its own steps or waits:

```python
from datetime import timedelta

from async_durable_execution import (
    durable_callable,
    run_in_child_context,
    step,
    wait,
)


@durable_callable
async def load_order(order_id: str) -> dict:
    return {"order_id": order_id}


@durable_callable
async def process_order(order_id: str) -> dict:
    order = await step(load_order(order_id), name="load-order")
    await wait(timedelta(seconds=1), name="processing-delay")
    return order


result = await run_in_child_context(
    process_order("order-123"),
    name="process-order",
)
```

Handlers and child contexts compose durable operations. Step bodies perform the
nondeterministic work that should be checkpointed atomically.

### Clean Up at Logical Completion

Do not put durable cleanup in `finally`, `with`, or `async with` around an
operation that can suspend. Python executes lexical cleanup while the SDK
unwinds the current invocation for a wait, callback, retry, or replay boundary.

Use [`terminal_scope()`](terminal-scopes.md) to register durable cleanup and
failure-only compensation that runs at the logical end of the scope.

## Continue

- [API reference](async_durable_execution.md)
- [Durable terminal scopes](terminal-scopes.md)
- [Declarative DAG workflows](api/extension/flow.md)
- [Workflow patterns](workflow-patterns.md)
- [Deploy and invoke](deployment.md)
- [Background tasks and advanced patterns](advanced-usage.md)
- [Using synchronous libraries](using-synchronous-code.md)
- [Deploying and testing the included examples](https://github.com/zhongkechen/async-durable-execution/blob/main/CONTRIBUTING.md#example-integration-tests-and-deployment)
