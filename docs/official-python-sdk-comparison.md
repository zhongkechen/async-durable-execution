# Official Python SDK Comparison

This page compares the official AWS Durable Execution Python SDK
(`aws-durable-execution-sdk-python`) with this community-maintained async-first SDK
(`async-durable-execution`).

Both SDKs target AWS Lambda Durable Functions and share the same durable execution
model: workflow code can replay, completed step results are checkpointed, waits
suspend without Lambda compute charges, callbacks resume from external signals, and
durable invokes must use qualified function names. This SDK is a fork of the upstream
Apache-2.0 codebase and preserves the same durable execution concepts; the main
difference is the Python programming model.

## Summary

| Area | Official Python SDK | `async-durable-execution` |
| --- | --- | --- |
| Package | `aws-durable-execution-sdk-python` | `async-durable-execution` |
| Ownership | AWS official SDK | Community-maintained fork under Apache-2.0 |
| Python support | Python 3.11+ | Python 3.10+ |
| Handler shape | Synchronous `def handler(event, context)` | `async def handler(event)` with `@durable_execution` |
| User durable functions | Synchronous `@durable_step` functions with `StepContext` | Sync or async functions decorated with `@durable_callable` |
| Durable operations | `context.step(...)`, `context.wait(...)`, `context.invoke(...)` | Top-level `await step(...)`, `await wait(...)`, `await invoke(...)` |
| Async library integration | Requires bridging async code from sync call sites | Native `await` for async clients and services |
| Fan-out concurrency | SDK fan-out helpers such as `context.parallel()` and `context.map()` | `parallel()`, `map()`, and normal `asyncio.gather()` over operation tasks |
| Declarative DAG workflows | No direct equivalent | `flow()` with typed node inputs, conditional dependencies, and failure routes |
| Logging | Context logger APIs | Standard `logging.getLogger(...)` with SDK replay filtering |
| Test runner | Separate testing package | Local and cloud runners included in the main package |
| Documentation | AWS official documentation | Generated API reference, migration guide, and async-focused guides |
| Lambda layer | Supported through normal Lambda packaging flows | Repository tooling and workflows for building an SDK Lambda layer |
| Maturity | Official AWS package and support channel | Community-maintained, async-focused project |
| Best fit | Synchronous Python codebases and teams requiring official AWS support | Async services, mixed sync/async workflows, AI workflows, and codebases using `asyncio` |

## Programming Model

The official SDK uses an explicit durable context object. Durable operations are
methods on that context and user steps are synchronous functions.

```python
from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
)
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_step
def fetch_user(step_context: StepContext, user_id: str) -> dict:
    step_context.logger.info("Fetching user")
    return {"id": user_id}


@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    user = context.step(fetch_user(event["user_id"]))
    context.wait(Duration.from_seconds(30))
    return {"user": user}
```

This SDK binds the active durable context internally and exposes durable
operations as top-level awaitable helpers. User handlers use `async def`.
Steps, child contexts, `flow` nodes, callback submitters, map item functions,
parallel branches, and condition checks may use `def` or `async def`.
Serializers use the async `SerDes` or synchronous `SyncSerDes` interface.
Synchronous leaf callables run in a worker thread. The `@durable_dag` function
that declares a flow graph is synchronous, deterministic, and evaluated
directly.

```python
import logging
from datetime import timedelta

from async_durable_execution import durable_callable, durable_execution, step, wait


logger = logging.getLogger(__name__)


@durable_callable
async def fetch_user(user_id: str) -> dict:
    logger.info("Fetching user")
    return {"id": user_id}


@durable_execution
async def handler(event: dict) -> dict:
    user = await step(fetch_user(event["user_id"]), name="fetch-user")
    await wait(duration=timedelta(seconds=30), name="delay")
    return {"user": user}
```

For async-first codebases, this removes the need to wrap async clients with
`asyncio.run()` from synchronous steps. That matters when code already depends on
event-loop resources such as async HTTP clients, async database pools, async AWS
clients, or agent frameworks.

## Durable Semantics

The durable rules are the same in both SDKs:

- Code outside durable operations can run again during replay.
- Nondeterministic work belongs inside steps.
- Side effects outside steps can repeat.
- Durable operations must not be nested inside a step.
- Mutating closure state inside a step is not a durable state mechanism.
- Standard replay-aware logging is safe; ordinary side effects such as `print()` are
  not a substitute for durable operations.

Choosing this SDK does not change the replay contract. It changes how Python code is
written around that contract.

## API Style

| Concern | Official Python SDK | `async-durable-execution` |
| --- | --- | --- |
| Operation access | Methods on `DurableContext` | Top-level helpers imported from `async_durable_execution` |
| Context metadata | `DurableContext` and `StepContext` parameters | Typed context getters such as `get_durable_context()` and `get_step_context()` |
| Durations | SDK duration wrapper objects | Standard `datetime.timedelta` |
| Operation names | Usually passed through operation config or context APIs | Keyword-only `name=...` arguments |
| Retry configuration | SDK retry config objects | `RetryStrategy(...)` passed to operations |
| Imports | Longer official package path | Short top-level imports |
| API shape | More context/config-object oriented | More direct keyword arguments and fewer wrapper objects |

The migration is mostly mechanical: replace context methods with awaited
top-level helpers, convert only callables that need to await work to
`async def`, use `datetime.timedelta`, and replace context loggers with standard
Python loggers. See the [migration guide](migrating-from-official-python-sdk.md)
for the full mapping.

## Concurrency

The official SDK provides durable fan-out APIs such as `parallel()` and `map()` through
the durable context. Those APIs are still useful for structured fan-out patterns.

This SDK also supports `parallel()` and `map()`, but durable operation helpers return
`asyncio.Task` objects. That means independent operations can be started first and
awaited later with normal Python concurrency patterns:

```python
import asyncio

from async_durable_execution import durable_callable, step


@durable_callable
async def price_item(item_id: str) -> int:
    return 100


price_tasks = [
    step(price_item(item_id), name=f"price-{item_id}")
    for item_id in item_ids
]
prices = await asyncio.gather(*price_tasks)
```

Use `map()` when you want item-level result aggregation, completion thresholds, or
bounded fan-out semantics. Use `parallel()` when you want explicit durable branches.
Use `flow()` when a static DAG benefits from inferred data dependencies, conditional
routes, and graph validation before execution.
Use `asyncio.gather()` when independent durable operations fit normal async task
composition.

## Async Integrations

The largest practical difference appears when workflow code already uses async
libraries.

| Integration scenario | Official Python SDK | `async-durable-execution` |
| --- | --- | --- |
| Async HTTP clients such as `httpx.AsyncClient` | Requires sync-to-async bridging | Direct `await` inside durable callables |
| Async AWS clients | Requires bridging or sync clients | Optional `aioboto` extra for async Lambda client support |
| Lambda service client | Synchronous botocore-style client usage | Async client when `aioboto` is installed; otherwise a threaded async adapter over the bundled sync client |
| Async database pools | Harder to share cleanly from sync steps | Natural event-loop usage |
| AI or agent loops | Often needs wrapper code around model/tool calls | Durable workflow can be written as an async loop |
| Existing synchronous business logic | Natural fit | Accepted directly and dispatched to worker threads |

If most workflow orchestration is synchronous, the official SDK may still be
the simpler default because its durable operations are synchronous methods. If
the workflow uses async clients, mixed sync/async callables, or agent
orchestration, this SDK usually keeps the code smaller and easier to compose.

## Runtime And Performance

The core durable behavior is intentionally aligned:

- Replay rules, checkpoint persistence, waits, callbacks, invokes, child contexts,
  retries, `map()`, and `parallel()` follow the same durable execution model.
- Runtime overhead is generally dominated by Lambda execution, checkpoint operations,
  and the business logic inside steps rather than by the Python wrapper style.
- This SDK can reduce application-level overhead for async-heavy workflows by avoiding
  sync-to-async adapter code in user steps.
- On Python 3.12 and newer, this SDK uses `asyncio.eager_task_factory` when creating
  durable operation tasks. That is an ordering and task-start optimization; it does
  not change durable semantics. Python 3.10 and 3.11 use normal `asyncio` task
  scheduling.

## Documentation, Examples, And Packaging

The official SDK benefits from AWS-owned documentation and release channels. This SDK
adds async-focused project documentation:

- Generated API reference published through GitHub Pages.
- A migration guide for moving from the official synchronous Python SDK.
- Async-focused examples, including `asyncio.gather()` over durable operation tasks.
- Declarative DAG examples covering fan-out/fan-in and failure recovery.
- Local/cloud runner examples and typed result inspection patterns.
- Expanded test coverage and published coverage reporting for this repository.
- Repository scripts and CI workflows for packaging the SDK as an AWS Lambda layer.

## Testing

Both SDKs support local testing and testing deployed durable Lambda functions. The main
packaging difference is where the runner lives.

| Concern | Official Python SDK | `async-durable-execution` |
| --- | --- | --- |
| Runtime package | `aws-durable-execution-sdk-python` | `async-durable-execution` |
| Testing package | Separate testing package, commonly used with pytest | Runner helpers included in `async_durable_execution` |
| Local runner | In-memory durable service simulation | In-memory durable service simulation |
| Cloud runner | Exercises a deployed durable Lambda function | Exercises a deployed durable Lambda function |
| Result lookup | Test helper APIs | Typed result helpers such as `result.get_step(name)` |

Name durable operations in either SDK. Named operations make tests resilient because
assertions can target behavior rather than operation order.

## Deployment And Runtime

Deployment concepts do not materially change between the SDKs:

- Enable durable execution on the Lambda function.
- Grant the durable execution IAM permissions required by Lambda.
- Publish a version or create an alias.
- Invoke durable functions with a qualified function name or ARN.
- Add `lambda:InvokeFunction` permissions for durable invokes.
- Add callback permissions for external systems that complete callbacks.

This SDK adds repository tooling for building an AWS Lambda layer and supports an
optional async Lambda client through the `aioboto` extra. Those are packaging and
runtime integration conveniences; they do not remove the underlying Lambda Durable
Functions deployment requirements.

## Migration Direction

Moving from the official SDK to this SDK is usually straightforward:

1. Replace package dependencies and imports.
2. Convert handlers to `async def`. Convert other user callables when they need
   to await durable operations or async libraries; synchronous leaf callables
   may remain `def`.
3. Replace `context.step(...)`, `context.wait(...)`, and related methods with awaited
   top-level helpers.
4. Replace `@durable_step` with `@durable_callable`.
5. Replace SDK duration wrappers with `datetime.timedelta`.
6. Replace context loggers with standard `logging` loggers.
7. Update tests to use the runner helpers from `async_durable_execution`.

Moving from this SDK back to the official SDK is possible, but async workflows usually
need to be reshaped around synchronous entry points. That is more disruptive when the
workflow depends heavily on async clients or `asyncio` task composition.

## Maturity And Support

The official SDK is the conservative choice when official AWS ownership, support, and
release stability are the primary constraints. This SDK is a community-maintained fork
with a narrower goal: making the Python durable function programming model natural for
`asyncio` codebases.

Treat repository popularity metrics such as GitHub stars as useful signals, not API or
support guarantees. For production use of either SDK, validate workflows with the local
runner, deployed cloud runner tests, and replay/failure scenarios that match your
application.

## Which SDK Should You Choose?

Choose the official Python SDK when:

- Your Lambda workflow code is mostly synchronous.
- Official AWS SDK ownership and support are a hard requirement.
- The team prefers the explicit `DurableContext` programming model.
- Async calls are rare enough that bridging them from synchronous code is acceptable.
- You want the most conservative maturity and support posture.

Choose `async-durable-execution` when:

- The workflow already uses `asyncio`, async clients, or async application code.
- You want durable handlers and steps to use normal `async`/`await`.
- You want to compose independent durable operations with `asyncio.gather()`.
- You are building AI workflows, agent loops, or service orchestration that is already
  async-first.
- You are comfortable using a community-maintained fork that preserves the durable
  execution semantics while changing the Python API surface.
- You want integrated runner helpers, async-focused examples, and a Lambda layer
  packaging workflow.

The short version: use the official SDK for conservative synchronous codebases; use
this SDK when async Python is central to the workflow design.
