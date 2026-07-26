# Migrating from the Official AWS Python SDK

This guide helps you move code from the official AWS Durable Execution Python SDK
(`aws-durable-execution-sdk-python`) to this async-first SDK
(`async-durable-execution`).

The durable execution model is the same: code outside durable operations replays, step
results are checkpointed, waits suspend without compute charges, callbacks resume from
external signals, and invoked durable functions must use qualified function names.

The main migration is mechanical: replace the official SDK's synchronous
`DurableContext` method calls with async top-level operations and make user-provided
durable code `async def`.

## Package Changes

| Official SDK | This SDK |
| --- | --- |
| `aws-durable-execution-sdk-python` | `async-durable-execution` |
| `aws-durable-execution-sdk-python-testing` | `async_durable_execution` runner exports in `async-durable-execution` |
| `aws_durable_execution_sdk_python` imports | `async_durable_execution` imports |
| Python 3.13+ in the official quickstart | Python 3.10+ |

Install the runtime package:

```console
pip install async-durable-execution
```

The local/cloud test runner is included in `async-durable-execution`; import it
from `async_durable_execution`.

## API Mapping

| Official SDK | This SDK |
| --- | --- |
| `@durable_execution def handler(event, context)` | `@durable_execution async def handler(event)` |
| `@durable_step def step_fn(step_context, ...)` | `@durable_callable async def step_fn(...)` |
| `context.step(my_step(args))` | `await step(my_step(args), name="my-step")` |
| `context.wait(Duration.from_seconds(10))` | `await wait(timedelta(seconds=10), name="delay")` |
| `context.create_callback(...)` | `await create_callback(...)` |
| `callback.result()` | `await callback.result()` |
| `context.wait_for_callback(...)` | `await wait_for_callback(submitter(), ...)` |
| `context.invoke(function_name=..., payload=...)` | `await invoke(function_name=..., payload=..., name="...")` |
| `context.run_in_child_context(...)` | `await run_in_child_context(child(), name="...")` |
| `context.map(...)` | `await map(func=..., items=..., ...)` |
| `context.parallel(...)` | `await parallel(branches=[...], ...)` |
| No direct declarative DAG equivalent | `await flow(my_dag(...), name="...")` |
| `context.logger` or `step_context.logger` | standard `logging.getLogger(__name__)` |

This SDK binds the active durable context internally while your async callable runs. If
you need execution metadata, call `get_current_context()` and read fields such as
`durable_execution_arn`, `operation_id`, `operation_name`, `lambda_context`, or
`is_replaying()`.

Durable operation helpers return `asyncio.Task` objects. You can keep the simple
`await step(...)` style during migration, or start multiple independent operations
first and await them later with `asyncio.gather`. This gives you background execution
and direct concurrency without switching to `parallel()` or `map()`. On Python 3.12
and newer, operation task creation uses `asyncio.eager_task_factory` so a task starts
immediately and runs to its first suspension point. Python 3.10 and 3.11 do not
support eager task start, so the SDK falls back to normal lazy `asyncio` task
scheduling.

## Quickstart Migration

Official SDK:

```python
from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import DurableContext, StepContext, durable_step
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_step
def my_step(step_context: StepContext) -> str:
    step_context.logger.info("Hello from my_step")
    return "Hello from Durable Lambda!"


@durable_execution
def lambda_handler(event, context: DurableContext) -> dict:
    message = context.step(my_step())
    context.wait(Duration.from_seconds(10))
    context.logger.info("Resumed after wait")
    return {"statusCode": 200, "body": message}
```

This SDK:

```python
import logging
from datetime import timedelta

from async_durable_execution import (
    durable_callable,
    durable_execution,
    step,
    wait,
)

logger = logging.getLogger(__name__)


@durable_callable
async def my_step() -> str:
    logger.info("Hello from my_step")
    return "Hello from Durable Lambda!"


@durable_execution
async def lambda_handler(event: dict) -> dict:
    message = await step(my_step(), name="my-step")
    await wait(duration=timedelta(seconds=10), name="delay")
    logger.info("Resumed after wait")
    return {"statusCode": 200, "body": message}
```

## Step Migration

Official steps receive a `StepContext` argument and run synchronously. In this SDK, a
step function is an async callable created with `@durable_callable`. Pass the resulting
zero-argument callable to `step()`.

```python
from async_durable_execution import durable_callable, step


@durable_callable
async def add_numbers(a: int, b: int) -> int:
    return a + b


result = await step(add_numbers(5, 3), name="add-numbers")
```

To run independent steps concurrently, create their tasks first and await them
together:

```python
import asyncio

from async_durable_execution import durable_callable, step


@durable_callable
async def price_item(item_id: str) -> int:
    return 100


tasks = [
    step(price_item(item_id), name=f"price-{item_id}")
    for item_id in item_ids
]
prices = await asyncio.gather(*tasks)
```

Put nondeterministic work and side effects inside steps just as you did with the
official SDK. Reads of time, UUID generation, random values, API calls, database
queries, and writes should stay inside `@durable_callable` functions that run through
`step()`.

Step retry configuration is passed directly to `step()`:

```python
from datetime import timedelta

from async_durable_execution import RetryStrategy, step


retry_strategy = RetryStrategy(
    max_attempts=3,
    initial_delay=timedelta(seconds=1),
    backoff_rate=2.0,
)

result = await step(
    add_numbers(5, 3),
    name="add-numbers",
    retry_strategy=retry_strategy,
)
```

For at-most-once step semantics, pass `step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY`.

## Waits and Durations

The official SDK uses `Duration` helpers. This SDK uses Python `datetime.timedelta`:

```python
from datetime import timedelta

from async_durable_execution import wait


await wait(duration=timedelta(seconds=30), name="cooldown")
await wait(duration=timedelta(hours=1), name="hourly-window")
```

Do not replace durable waits with `time.sleep()` or `asyncio.sleep()` for workflow
delays. Native sleeps consume Lambda execution time and do not checkpoint the workflow.

## Callbacks

Callback creation and result waiting are both awaitable.

```python
from datetime import timedelta

from async_durable_execution import create_callback, durable_callable, step


@durable_callable
async def send_approval_request_step(callback_id: str) -> None:
    send_approval_request(callback_id)


callback = await create_callback(
    name="approval",
    timeout=timedelta(hours=24),
)
await step(
    send_approval_request_step(callback.callback_id),
    name="submit-approval",
)
approval = await callback.result()
```

For the combined submit-and-wait pattern, make the submitter a durable callable:

```python
from datetime import timedelta

from async_durable_execution import (
    WaitForCallbackContext,
    durable_callable,
    get_current_context,
    wait_for_callback,
)


@durable_callable
async def submit_approval() -> None:
    callback_context = get_current_context()
    assert isinstance(callback_context, WaitForCallbackContext)
    send_approval_request(callback_context.callback_id)


approval = await wait_for_callback(
    submit_approval(),
    name="approval",
    timeout=timedelta(hours=24),
)
```

The external system still completes callbacks through the Lambda
`SendDurableExecutionCallbackSuccess` and `SendDurableExecutionCallbackFailure` APIs.

## Child Contexts, Parallel, and Map

Durable operations cannot be nested inside a step. Use `run_in_child_context()` to group
durable operations into a reusable sub-workflow:

```python
from datetime import timedelta

from async_durable_execution import durable_callable, run_in_child_context, step, wait


@durable_callable
async def process_order(order: dict) -> dict:
    validated = await step(validate(order), name="validate")
    await wait(timedelta(seconds=1), name="settle")
    return await step(process(validated), name="process")


result = await run_in_child_context(process_order(order), name="process-order")
```

For fan-out work, migrate official context methods to top-level async helpers:

```python
from async_durable_execution import CompletionConfig, map, parallel


batch_results = await map(
    func=process_item,
    items=items,
    max_concurrency=5,
    completion_config=CompletionConfig.thresholds(tolerated_failure_count=2),
    name="process-items",
)
values = batch_results.get_results()

parallel_results = await parallel(
    branches=[branch_a(), branch_b()],
    max_concurrency=2,
    name="parallel-work",
)
```

## Declarative DAG Flows

`flow()` is an additional composition API in this SDK rather than a mechanical
replacement for an official context method. Use it when the workflow is a static
acyclic graph and benefits from inferred data dependencies, conditional success or
failure routes, and concurrent independent nodes.

```python
from async_durable_execution import durable_dag, durable_node, flow, node


@durable_node
async def load_order(order_id: str) -> dict:
    return {"id": order_id}


@durable_node
async def process_order(order: dict) -> dict:
    return {"id": order["id"], "status": "processed"}


@durable_dag
def order_flow(order_id: str):
    loaded = node(load_order(order_id), name="load-order")
    processed = node(process_order(loaded.outcome), name="process-order")
    return processed.outcome


result = await flow(order_flow(order_id), name="order-flow")
```

Unlike other user-provided callables, a `@durable_dag` definition is synchronous
and must be deterministic. `@durable_node` bodies are async and can contain normal
durable operations. See [flow and DAG workflows](api/extension/flow.md) for the complete dependency,
failure, and output model.

## Logging

Use standard Python logging:

```python
import logging

logger = logging.getLogger(__name__)
logger.info("Starting workflow")
```

The SDK configures replay-aware logging for standard loggers. Avoid `print()` for
workflow progress because ordinary side effects outside durable operations repeat on
replay.

## Testing

Replace the official testing package with `async_durable_execution`.

```python
from async_durable_execution import InvocationStatus, create_local_runner

from my_workflow import lambda_handler


async def test_workflow() -> None:
    with create_local_runner(
        handler=lambda_handler,
        input={"order_id": "order-123"},
        timeout=10,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_step("my-step").status is InvocationStatus.SUCCEEDED
```

Name durable operations during migration. Named operations make tests resilient because
assertions can use `result.get_step("my-step")` instead of depending on operation order.

## Migration Checklist

1. Replace package dependencies and imports.
2. Change every durable handler, step, child context, flow node, callback submitter,
   map function, parallel branch, and wait-for-condition check to `async def`; keep
   `@durable_dag` definitions synchronous.
3. Replace `DurableContext` method calls with awaited top-level operations.
4. Replace `@durable_step` with `@durable_callable`.
5. Remove explicit `DurableContext` and `StepContext` parameters unless you are reading
   metadata through `get_current_context()`.
6. Replace `Duration` with `datetime.timedelta`.
7. Move all nondeterministic work and side effects into steps.
8. Replace context loggers with standard `logging` loggers.
9. Name operations and update tests to use `async_durable_execution`.
10. Keep Lambda deployment settings, IAM durable execution permissions, and qualified
    function invocation practices from the official SDK.
