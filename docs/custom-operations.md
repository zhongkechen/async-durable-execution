# Custom Durable Operations

Third-party packages can build reusable durable operations with the public
extension-author interface in `async_durable_execution.extension`. The interface
allocates stable operation identities and delegates checkpointing, suspension,
replay, serialization, and failures to SDK-owned primitive state machines.

Extension packages do not register with the SDK and do not send raw checkpoint
updates. An extension is ordinary Python code that composes reserved STEP, WAIT,
CHAINED_INVOKE, CALLBACK, and CONTEXT primitives.

All SDK-provided operation helpers, including `step()`, `wait()`, `invoke()`,
`create_callback()`, and `run_in_child_context()`, use this same reservation
interface internally. Their canonical private implementation package is
`async_durable_execution._operation`; backend checkpoint state machines remain
isolated under `async_durable_execution._primitive`. The SPI delegates to those
internal executors rather than duplicating primitive lifecycle behavior. The former
`async_durable_execution._extension` modules remain import aliases for
compatibility, but both underscore-prefixed packages are private; third-party
operations should depend only on `async_durable_execution.extension` and the
top-level public exports.

## Basic Extension

This extension starts two reserved steps and combines their results:

```python
import asyncio
from collections.abc import Awaitable, Callable

from async_durable_execution import (
    ExtensionStepResult,
    get_extension_context,
)


async def pair(
    left: Callable[[], Awaitable[str]],
    right: Callable[[], Awaitable[str]],
    *,
    name: str,
) -> str:
    extension = get_extension_context()
    left_operation = extension.reserve(f"{name}-left")
    right_operation = extension.reserve(f"{name}-right")

    async def run_left(_state: None):
        return ExtensionStepResult.succeed(await left())

    async def run_right(_state: None):
        return ExtensionStepResult.succeed(await right())

    left_task = left_operation.step(run_left, sub_type="AcmePairStep")
    right_task = right_operation.step(run_right, sub_type="AcmePairStep")
    left_value, right_value = await asyncio.gather(left_task, right_task)
    return left_value + right_value
```

Application code imports and awaits the extension like any other async helper:

```python
result = await pair(load_left, load_right, name="load-pair")
```

No automatic child context surrounds an extension. Reserve primitives in the
current scope unless the extension specifically requires an isolated child
context.

## Reservations

`ExtensionContext.reserve()` immediately consumes an operation identity and
returns an opaque, one-shot `ExtensionOperation`. Reserve operations before
launching them when launch order may vary:

```python
extension = get_extension_context()
first = extension.reserve("first")
second = extension.reserve("second")

# Launch order may differ from reservation order.
second_task = second.step(run_second, sub_type="AcmeStep")
first_task = first.step(run_first, sub_type="AcmeStep")
```

Sequential reservations must occur in the same order on every replay. Reordering,
inserting, or removing them is a workflow compatibility change.

Schedulers whose registration order may change can use stable local IDs:

```python
operation = get_extension_context().reserve(
    "process-node",
    local_operation_id="node-a",
)
result = await operation.step(
    process_node,
    sub_type="AcmeNode",
    initial_state={"node": "a"},
)
```

A local ID must be a nonblank string and unique within the current durable
context. Reserve every local-ID operation before selecting any reserved
primitive; late local-ID reservations are rejected because future hashed IDs
cannot be discovered from replay history. Local IDs are namespaced by the SDK;
extension code never receives the backend operation ID. Changing or reusing a
local ID is a workflow compatibility change.

Reservation names must be nonblank strings when provided. An
`ExtensionContext` may only reserve operations, and a reservation may only be
claimed, while the durable handler or child context that created it is active.
They cannot create operations from inside a step function or another durable
scope.

Each reservation can create exactly one primitive. Reusing it raises
`RuntimeError`.

## Primitive Methods

An `ExtensionOperation` exposes one explicit method per backend primitive:

| Method | Backend type | Result |
| --- | --- | --- |
| `step()` | `STEP` | Stateful checkpointed result |
| `wait()` | `WAIT` | Completion after a durable delay |
| `invoke()` | `CHAINED_INVOKE` | Invoked durable function result |
| `create_callback()` | `CALLBACK` | `Callback` handle |
| `run_in_child_context()` | `CONTEXT` | Child-context result |

Every method requires a subtype. A subtype may be an existing
`OperationSubType` or a nonblank extension-owned string such as `"AcmePoll"`.
An extension-owned string must not match a reserved `OperationSubType` value.
Pass the enum member explicitly when selecting an SDK subtype.
The method selects the backend operation type; the subtype only identifies the
logical operation in history, plugins, logs, and replay validation. Changing a
subtype is a workflow compatibility change.

Extensions cannot define new backend state machines or emit raw lifecycle
actions.

## Stateful Steps

An extension step receives checkpointed state and returns either
`ExtensionStepResult.succeed(value)` or
`ExtensionStepResult.retry(state, delay)`:

```python
from async_durable_execution import (
    ExtensionStepResult,
    get_extension_context,
)


async def poll(state: dict | None):
    current = state or {"attempts": 0, "ready": False}
    refreshed = await refresh_status(current)
    if refreshed["ready"]:
        return ExtensionStepResult.succeed(refreshed)
    return ExtensionStepResult.retry(refreshed, delay=5)


result = await get_extension_context().reserve("poll").step(
    poll,
    sub_type="AcmePoll",
    initial_state={"attempts": 0, "ready": False},
)
```

Retry state is serialized into the STEP checkpoint and restored on the next
attempt. `get_step_context().attempt` remains available inside the function.
The optional `serdes` applies to both retry state and the final result.

Thrown exceptions are terminal unless `retry_strategy` returns
`ExtensionStepResult.retry(...)`:

```python
def retry_error(error, state, attempt):
    if attempt >= 3:
        return None
    return ExtensionStepResult.retry(state, delay=attempt)
```

Use `step_semantics` to select the same at-least-once or at-most-once attempt
behavior as a normal durable step.

## Child Contexts

Create a custom child context only when the extension needs an isolated
operation namespace:

```python
async def run_batch():
    child_extension = get_extension_context()
    item = child_extension.reserve(
        "item",
        local_operation_id="item-1",
    )
    return await item.step(process_item, sub_type="AcmeBatchItem")


result = await get_extension_context().reserve("batch").run_in_child_context(
    run_batch,
    sub_type="AcmeBatch",
)
```

Inside `run_batch`, `get_extension_context()` returns the child scope. Nested
local IDs are automatically namespaced beneath the child context.

## Replay Contract

Extension code is replayed under the same rules as handler code:

- Reserve the same logical operations on every replay.
- Keep sequential reservation order deterministic.
- Local IDs use a separate identity namespace and do not consume sequential
  reservation positions.
- Keep local IDs, primitive choices, subtypes, names, and parent scope stable.
- Put nondeterministic work and side effects inside reserved steps.
- Keep retry strategies deterministic and side-effect free.
- Treat changing an extension's checkpoint topology as a workflow migration.

The SDK validates reserved operation type, subtype, name, and parent metadata
against existing checkpoints before replaying them.

## Testing Extensions

Use `create_local_runner()` to exercise first execution, suspension, and replay.
Tests should cover changed launch order after reservation, custom local IDs,
stateful retries, nested contexts, bounded concurrent children that suspend and
resume, and compatibility with checkpoints produced by the previous extension
version.
