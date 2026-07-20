# Advanced Usage

## Background Operation Tasks

Durable operation helpers such as `step()`, `wait()`, `invoke()`, `recurse()`,
`run_in_child_context()`, `wait_for_callback()`, `wait_for_condition()`, and
`with_retry()` return `asyncio.Task` objects. Awaiting an operation directly still
works:

```python
result = await step(fetch_order(order_id), name="fetch-order")
```

Because the operation is a task, you can also start several independent operations
and let them run in the background before awaiting them together. This uses normal
`asyncio` task patterns and does not require `parallel()` or `map()`:

```python
import asyncio

pricing_tasks = [
    step(fetch_item(item_id), name=f"fetch-item-{item_id}")
    for item_id in item_ids
]
items = await asyncio.gather(*pricing_tasks)
```

The same pattern works with different operation types when they are independent:

```python
delay_task = wait(duration=timedelta(seconds=30), name="cooldown")
invoke_task = invoke(
    function_name=processor_function,
    payload={"order_id": order_id},
    name="process-order",
)

_, invoke_result = await asyncio.gather(delay_task, invoke_task)
```

On Python 3.12 and newer, the SDK uses `asyncio.eager_task_factory` when creating
operation tasks. This starts the operation task immediately and runs it until its
first suspension point, making task creation order deterministic.

On Python 3.10 and 3.11, `asyncio` does not provide eager task start. The SDK falls
back to normal lazy task creation with `loop.create_task()`. Eager start is treated as
a performance and ordering optimization only; the durable operation contract is still
that the helper returns an `asyncio.Task` that can run in the background and be awaited
later.

## Static DAG Workflows

Use `@durable_dag`, `@durable_node`, `node()`, and `flow()` to define a static
acyclic workflow with declarative dependencies. Both decorators bind arguments
without running user code. `flow()` synchronously evaluates and validates the
complete graph before creating its durable child context.

```python
from typing import cast

from async_durable_execution import (
    FlowNode,
    FlowNodeContext,
    durable_callable,
    durable_dag,
    durable_node,
    flow,
    get_current_context,
    node,
    step,
)


@durable_callable
async def fetch_order(order_id: str) -> dict:
    return {"id": order_id, "status": "ready"}


@durable_callable
async def charge_order(order: dict) -> dict:
    return {"order": order, "charged": True}


@durable_node
async def fetch(order_id: str) -> dict:
    return await step(fetch_order(order_id), name="fetch-order")


@durable_node
async def charge(fetch_node: FlowNode[dict]) -> dict:
    context = cast(FlowNodeContext, get_current_context())
    order = context.result(fetch_node).outcome
    return await step(charge_order(order), name="charge-order")


@durable_dag
def order_flow(order_id: str):
    fetch_node = node(fetch(order_id), name="fetch")
    charge_node = node(charge(fetch_node), name="charge")
    fetch_node >> charge_node
    return charge_node


result = await flow(order_flow("order-123"), name="process-order")
charge_result = result.output
```

Definition code must be deterministic and cannot start `step()`, `wait()`,
`invoke()`, another `flow()`, or any other durable operation. Node bodies run only
after validation inside their own durable child contexts, where they can use all
normal durable operations. Call `get_current_context()` inside a node body to access
its `FlowNodeContext` and direct dependency results.

Dependency operators build the graph:

- `a >> b` and `a.succeeded >> b` run `b` after `a` succeeds.
- `a.failed >> b` runs `b` after `a` fails.
- `a.completed >> b` runs `b` after any logical terminal status.
- `(a & b) >> c` requires every dependency to match.
- `(a | b) >> c` starts after the first matching dependency.
- `a >> (b, c)` fans out to both targets.

Use parentheses around `&` and `|` expressions. Each target accepts one dependency
expression, so combine multiple dependencies explicitly instead of assigning them in
separate statements.

`FlowResult.results` contains every node result keyed by node name. `outputs` contains
the nodes selected by the definition return value, while `output` is a convenience
property that preserves zero, one, or multiple output arity. A `FlowNodeResult`
preserves its `SUCCEEDED`, `FAILED`, or `SKIPPED` status together with its outcome and
captured error.

A matching `.failed` route handles its source failure. After all runnable nodes settle,
`flow()` raises `FlowExecutionError` if failures remain unhandled; the exception's
`result` field contains the complete checkpointed `FlowResult`. `.completed` observes
a failure but does not handle it.

Flows must remain structurally acyclic. Loops and repeated node activation are not
supported because every iteration would extend durable checkpoint history and replay
cost.

## Batch Completion Conditions

`map()` and `parallel()` both accept `completion_config` to decide when a batch-style
operation has collected enough item or branch results. Use the `CompletionConfig`
factory methods for common strategies:

```python
from async_durable_execution import CompletionConfig, map, parallel


results = await map(
    func=process_item,
    items=items,
    max_concurrency=5,
    completion_config=CompletionConfig.thresholds(
        min_successful=8,
        tolerated_failure_count=2,
    ),
    name="process-items",
)
```

`CompletionConfig.thresholds()` accepts either or both threshold fields:

- `min_successful`: complete successfully once this many items or branches succeed.
- `tolerated_failure_count`: complete as failed once failures exceed this count.

Other built-in factories cover the most common policies:

```python
# Complete after the first success, even if other work is still running.
first = CompletionConfig.first_successful()

# Use the default no-threshold policy.
all_done = CompletionConfig.all_completed()

# Require all work to succeed. The first failure exceeds the zero-failure tolerance.
all_ok = CompletionConfig.all_successful()
```

Both `CompletionConfig.all_completed()` and the default `CompletionConfig()` have no
explicit thresholds. They complete successfully when all work completes without
failures, but any observed failure completes the operation as failed because no
failure tolerance is configured.

By default, `parallel()` uses `CompletionConfig.all_successful()`, while `map()` uses
`CompletionConfig()`. Pass an explicit `completion_config` when you want a different
policy.

For custom policies, use `CompletionConfig.custom()` with a deterministic callback
that returns a `CompletionDecision`:

```python
from async_durable_execution import (
    CompletionConfig,
    CompletionDecision,
    CompletionReason,
    CompletionStatus,
)


def complete_after_half(status: CompletionStatus) -> CompletionDecision:
    if status.success_count >= (status.total_count + 1) // 2:
        return CompletionDecision.complete(
            CompletionReason.CUSTOM_COMPLETION_SUCCEEDED
        )
    return CompletionDecision.continue_execution()


completion_config = CompletionConfig.custom(complete_after_half)
```

Custom completion callbacks run as workflow code, so they must be deterministic and
must not perform external side effects.

## Recursive Self Invocation

Use `recurse()` when a durable function needs to invoke itself as a new durable
execution. This is useful for algorithms that naturally split into smaller units of
work, or for workflows that need to continue with a changed input without growing the
Python call stack. `recurse()` is implemented with the same backend chained invoke
operation as `invoke()`.

```python
from typing import Any

from async_durable_execution import durable_execution, get_current_context, recurse


@durable_execution
async def handler(event: dict[str, Any]) -> dict[str, Any]:
    values = [int(value) for value in event["values"]]
    recursive_level = get_current_context().recursive_level

    if len(values) <= 1:
        return {
            "sorted": values,
            "count": len(values),
            "recursive_level": recursive_level,
        }

    pivot_index = len(values) // 2
    pivot = values[pivot_index]
    rest = [*values[:pivot_index], *values[pivot_index + 1 :]]
    left = [value for value in rest if value < pivot]
    right = [value for value in rest if value >= pivot]

    sorted_left: list[int] = left
    sorted_right: list[int] = right
    max_recursive_level = recursive_level

    if len(left) > 1:
        left_result = await recurse(
            {"values": left},
            name=f"sort-left-{recursive_level + 1}",
            with_recursive_level=True,
        )
        sorted_left = list(left_result["sorted"])
        max_recursive_level = max(
            max_recursive_level,
            int(left_result["recursive_level"]),
        )

    if len(right) > 1:
        right_result = await recurse(
            {"values": right},
            name=f"sort-right-{recursive_level + 1}",
            with_recursive_level=True,
        )
        sorted_right = list(right_result["sorted"])
        max_recursive_level = max(
            max_recursive_level,
            int(right_result["recursive_level"]),
        )

    return {
        "sorted": [*sorted_left, pivot, *sorted_right],
        "count": len(values),
        "recursive_level": max_recursive_level,
    }
```

When `function_name` is omitted, `recurse()` resolves the current Lambda function from
the active Lambda context. You can pass `function_name=` explicitly in tests or unusual
runtimes. Recursive self-invokes still require `lambda:InvokeFunction` permission for
the current function, and the function identifier must be qualified with a version,
alias, or `$LATEST`.

`recurse()` validates that the payload differs from the current execution input. This
keeps accidental self-invocation loops from repeatedly starting the same execution
input. When `with_recursive_level=True`, the payload must be a `dict`; the SDK copies
it and writes an internal `__recursive_level` field. User code should read the public
`get_current_context().recursive_level` property rather than reading that field
directly. The first recursive call has `recursive_level == 1`.

AWS Lambda recursion protection counts the original invocation as part of the invoke
lineage. Because the SDK's `recursive_level` starts at the first recursive call, the
example test uses deterministic input where the middle pivot peels off one singleton
side at each level. A 31-item list reaches `recursive_level == 14` and should not
trigger protection. A similar 33-item list attempts `recursive_level == 15`, which is
16 total Lambda invocations and is expected to fail with Lambda's maximum recursion
depth protection. See
`async-durable-execution-examples/src/async_durable_execution_examples/invoke/recurse.py`
for an executable example that verifies the computed result and covers both recursion
protection cases.

## Wait For Condition Results

`wait_for_condition()` passes each `check` result to the configured
`PollingStrategy`. The strategy returns the next polling delay, or `None` to complete
with the current result. The default `PollingStrategy` completes when the result
evaluates to `True` or when max attempts are exhausted, so custom result types can
still decide completion with `__bool__()`.

When the result is a custom type, provide a `SerDes` implementation so the SDK can
checkpoint and replay it durably:

```python
import json
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from async_durable_execution import SerDes
from async_durable_execution import PollingStrategy
from async_durable_execution import wait_for_condition


@dataclass(frozen=True)
class JobStatus:
    job_id: str
    attempts: int
    status: str

    def __bool__(self) -> bool:
        return self.status == "completed"

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "attempts": self.attempts,
            "status": self.status,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "JobStatus":
        return cls(
            job_id=str(data["job_id"]),
            attempts=int(data["attempts"]),
            status=str(data["status"]),
        )


class JobStatusSerDes(SerDes[JobStatus]):
    async def serialize(self, value: JobStatus) -> str:
        return json.dumps(value.to_dict())

    async def deserialize(self, payload: str) -> JobStatus:
        return JobStatus.from_dict(json.loads(payload))


async def check_job(state: JobStatus | None) -> JobStatus:
    attempts = 1 if state is None else state.attempts + 1
    status = get_job_status("job-123")
    return JobStatus(job_id="job-123", attempts=attempts, status=status)


result = await wait_for_condition(
    check=check_job,
    initial_state=None,
    polling_strategy=PollingStrategy[JobStatus](
        initial_delay=timedelta(seconds=2),
    ),
    serdes=JobStatusSerDes(),
    name="wait-for-job",
)
```

## Lambda Client Selection

The SDK chooses a Lambda API client for durable checkpoint and state APIs based on the installed dependencies.

With the optional `aioboto` extra, the SDK creates an async Lambda client by default:

```console
pip install "async-durable-execution[aioboto]"
```

Without the extra, the SDK uses the bundled `botocore` dependency through a threaded async adapter.

Explicitly provided Lambda API clients are detected as sync or async and wrapped accordingly. Code that must force the sync `botocore` client can create one explicitly and pass it to the durable handler:

```python
from async_durable_execution import create_default_sync_client, durable_execution

lambda_client = create_default_sync_client()


@durable_execution(boto3_client=lambda_client)
async def handler(event: dict) -> dict:
    return {"ok": True}
```

## Lambda Layer Packaging

Use a Lambda layer when you want multiple durable functions to share the SDK instead of vendoring it in each function zip.

Build a local layer archive from this checkout:

```console
hatch run python scripts/build_layer.py \
  --sdk-source async-durable-execution \
  --output dist/async-durable-execution-layer.zip
```

You can also use a prebuilt layer published by GitHub Actions. The layer ARN is shown in the summary of the [Lambda layer publish workflow](https://github.com/zhongkechen/async-durable-execution/actions/workflows/lambda-layer-publish.yml).

Publish the zip as an `AWS::Serverless::LayerVersion` or `AWS::Lambda::LayerVersion`, then add the layer ARN to Python durable functions.
