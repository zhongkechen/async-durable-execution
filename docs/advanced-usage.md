# Advanced Usage

## Background Operation Tasks

Durable operation helpers such as `step()`, `wait()`, `invoke()`, `recurse()`,
`run_in_child_context()`, `wait_for_callback()`, `wait_for_condition()`,
`with_retry()`, `terminal_scope()`, and `flow()` return `asyncio.Task` objects.
Awaiting an operation directly still works:

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

See the [flow API reference](api/extension/flow.md) for dependency syntax, input
projections, failure handling, result access, and execution pruning.

```python
from async_durable_execution import (
    durable_callable,
    durable_dag,
    durable_node,
    flow,
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
async def charge(order: dict) -> dict:
    return await step(charge_order(order), name="charge-order")


@durable_dag
def order_flow(order_id: str):
    fetch_node = node(fetch(order_id), name="fetch")
    charge_node = node(charge(fetch_node.outcome), name="charge")
    return charge_node.outcome


result = await flow(order_flow("order-123"), name="process-order")
charge_result = result.output
```

Definition code must be deterministic and cannot start `step()`, `wait()`,
`invoke()`, another `flow()`, or any other durable operation. Node bodies run only
after validation inside their own durable child contexts, where they can use all
normal durable operations. Passing `dependency_node.outcome`,
`dependency_node.error`, or `dependency_node.result` as a node argument infers the
required dependency and injects the projected value. For explicit complex
conditions, use `dependency=` and read available direct dependency results by stable
name through `FlowNodeContext`.

Projected inputs may be nested in `list`, `tuple`, and `dict` values. Other iterable
containers are rejected, while object fields containing a projection are rejected
during definition because the SDK cannot resolve them without changing the argument's
type or semantics. Materialize iterators before passing them to a node.

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

The definition returns `node.outcome`, `node.error`, `node.result`, a tuple of these
projections, or `None`; returning a `FlowNode` directly is invalid. `FlowResult.results`
contains every node result keyed by node name. `outputs` contains the projected values,
while `output` preserves zero, one, or multiple output arity. Returning `.error` or
`.result` explicitly observes and handles a failure selected as an output.
An `.outcome` output requires that node to succeed. If the node fails or is skipped,
`flow()` raises `FlowExecutionError` after checkpointing the result and lists the node
in `FlowResult.unavailable_outputs`; return `node.result` when a conditional output
may legitimately be failed or skipped.

Execution starts from the selected output nodes and follows their dependencies in
reverse. Declared nodes outside that reverse-reachable subgraph do not create child
operations and appear as `SKIPPED` in `FlowResult.results`. A definition that returns
`None` therefore executes no nodes.

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

When a completion policy finishes a batch early, started items or branches that did
not settle are cancelled and recorded in the parent `BatchResult` with
`BatchItemStatus.CANCELLED`. They are available through `result.cancelled()` and
`result.cancelled_count`, are not counted as successes or failures, and remain
cancelled when the parent result is replayed. Work that never started is omitted.

With `NestingType.FLAT`, an aggregate larger than the context checkpoint limit
uses a compact replay summary containing each entered branch's terminal status,
failure details, and the completion reason. A custom `summary_generator` result
is retained alongside this SDK metadata. Successful branch bodies are replayed
using their completed durable operations; failed, cancelled, and unstarted
branches are not run again. As with ordinary replay, side effects belong inside
`step()` rather than directly in a branch body.

Older oversized flat checkpoints with an empty summary can reconstruct
all-success groups that have no early-success/custom completion policy. If the
history is ambiguous, or rebuilding a result would require a new or unfinished
durable operation, replay raises `ExecutionError` without issuing checkpoints or
running that effect. Oversized replay metadata itself is rejected before the
aggregate is marked complete. Nested aggregates and normal result serialization
retain their existing formats.

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

from async_durable_execution import durable_execution, get_durable_context, recurse


@durable_execution
async def handler(event: dict[str, Any]) -> dict[str, Any]:
    values = [int(value) for value in event["values"]]
    recursive_level = get_durable_context().recursive_level

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
`get_durable_context().recursive_level` property rather than reading that field
directly. The first recursive call has `recursive_level == 1`.

AWS Lambda recursion protection counts the original invocation as part of the invoke
lineage. Because the SDK's `recursive_level` starts at the first recursive call, the
example test uses deterministic input where the middle pivot peels off one singleton
side at each level. A 31-item list reaches `recursive_level == 14` and should not
trigger protection. See `examples/invoke/recurse.py` for the executable workflow and
`test_examples/invoke/test_recurse.py` for coverage that verifies the computed result
below the recursion protection threshold.

## Wait For Condition Results

`wait_for_condition()` passes each `check` result to the configured
`PollingStrategy`. The strategy returns the next polling delay, or `None` to complete
with the current result. The default `PollingStrategy` completes when the result
evaluates to `True`, so custom result types can decide completion with `__bool__()`.
If the result remains false after the configured maximum attempts, the strategy raises
`WaitForConditionError`. A true result on the final attempt still completes
successfully. A custom polling strategy can return `None` to complete with any result.

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

## AWS Wire Mappings

Core durable dataclass models expose `to_dict()` and `from_dict()`. The SDK
owns the Lambda REST routes and JSON mappings used by those models, so new
fields can be supported without waiting for a botocore Lambda service-model
release. AWS API mappings preserve native Python values, while Lambda
invocation input models encode timestamps as Unix milliseconds. Invocation
state serializers recursively convert their nested `Operation` models using
the JSON representation.

## Lambda Client Selection

The SDK chooses a transport for its model-free Lambda REST client based on the
installed dependencies. Botocore supplies AWS credential resolution, endpoint
metadata, and SigV4 signing in both modes; the generated botocore Lambda
service model is not loaded.

With the optional `httpx` extra, the SDK installs HTTPX and creates an async
Lambda client by default:

```console
pip install "async-durable-execution[httpx]"
```

The previous `aioboto` extra remains available as a backward-compatible alias.

Without the extra, the SDK uses botocore's synchronous HTTP session through a
threaded async adapter.

Both transports honor botocore's `legacy` and `standard` retry counts while
reusing one checkpoint idempotency token across attempts. The model-free
transport rejects `adaptive` retry mode because it cannot preserve botocore's
client-side adaptive rate limiter without using the generated client stack.

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

You can also use a prebuilt layer published by GitHub Actions. The layer
includes the `httpx` extra and supports Python 3.10 through 3.15. The layer
ARNs are shown in the summary of the [Lambda layer publish workflow](https://github.com/zhongkechen/async-durable-execution/actions/workflows/lambda-layer-publish.yml).

Publish the zip as an `AWS::Serverless::LayerVersion` or `AWS::Lambda::LayerVersion`, then add the layer ARN to Python durable functions.
