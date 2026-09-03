# Durable Terminal Scopes

Use `terminal_scope()` when cleanup or compensation belongs to the logical end
of durable work rather than the end of the current Python invocation.

Durable workflows replay from the beginning. A wait, callback, retry delay,
invoke, map branch, parallel branch, or flow node can suspend by unwinding the
Python stack. Normal `finally`, `with`, `async with`, `ExitStack`, and
`AsyncExitStack` cleanup therefore runs at the wrong lifecycle boundary: it can
release a resource while the durable workflow is still waiting to resume.

`terminal_scope()` provides an isolated durable child context and a registry for
actions that run only after a logical terminal outcome.

## Lifecycle Contract

| Scope outcome | Compensation | Cleanup |
| --- | --- | --- |
| Success | Does not run | Runs |
| Application or operation failure | Runs | Runs |
| `asyncio` cancellation | Controlled by `TerminalScopeConfig`; skipped by default | Controlled by `TerminalScopeConfig`; skipped by default |
| Durable suspension or replay boundary | Does not run | Does not run |
| Retryable invocation interruption | Does not run | Does not run |
| Fatal SDK or process signal | Does not run | Does not run |

Compensations run sequentially in reverse registration order. Cleanups then run
sequentially in reverse registration order. Every ordinary action failure is
recorded and later actions are still attempted.

## MicroVM Review Example

The following workflow acquires a MicroVM, registers unconditional cleanup and
failure-only compensation, dispatches external review work, and waits for a
callback:

```python
from functools import partial

from async_durable_execution import (
    DurableTerminalActions,
    create_callback,
    durable_callable,
    durable_execution,
    step,
    terminal_scope,
)


@durable_callable
async def launch_microvm(review_id: str) -> dict[str, str]:
    return await microvm_service.launch(review_id)


@durable_callable
async def terminate_microvm(microvm_id: str) -> None:
    await microvm_service.terminate(microvm_id)


@durable_callable
async def cancel_review(microvm_id: str) -> None:
    await review_service.cancel(microvm_id)


@durable_callable
async def dispatch_review(
    microvm: dict[str, str],
    callback_id: str,
) -> None:
    await review_service.dispatch(microvm, callback_id)


async def review_with_microvm(
    event: dict,
    terminal: DurableTerminalActions,
) -> dict:
    microvm = await step(
        launch_microvm(event["review_id"]),
        name="launch-microvm",
    )

    terminal.cleanup(
        terminate_microvm(microvm["microvm_id"]),
        name="terminate-microvm",
    )
    terminal.compensate(
        cancel_review(microvm["microvm_id"]),
        name="cancel-review",
    )

    callback = await create_callback(name="review-complete")
    await step(
        dispatch_review(microvm, callback.callback_id),
        name="dispatch-review",
    )
    return await callback.result()


@durable_execution
async def handler(event: dict) -> dict:
    return await terminal_scope(
        partial(review_with_microvm, event),
        name="review-with-microvm",
    )
```

When `callback.result()` suspends, neither `cancel-review` nor
`terminate-microvm` runs. After callback success, only `terminate-microvm`
runs. If the scope body fails, `cancel-review` runs before
`terminate-microvm`.

The repository contains an executable local and cloud version at
`examples/terminal_scope/terminal_scope_microvm.py`.

## Why `finally` Is Unsafe

Do not use lexical cleanup around durable operations:

```python
microvm = await step(launch_microvm(review_id), name="launch-microvm")

try:
    callback = await create_callback(name="review-complete")
    return await callback.result()
finally:
    # Unsafe: Python executes this when callback.result() suspends.
    await step(
        terminate_microvm(microvm["microvm_id"]),
        name="terminate-microvm",
    )
```

The same problem applies to:

- synchronous and asynchronous context managers;
- generator and asynchronous-generator context managers;
- callbacks registered with `ExitStack` or `AsyncExitStack`;
- cleanup around `wait()`, `wait_for_condition()`, invokes, and retry delays;
- cleanup inside map, parallel, or flow work that can suspend.

An SDK terminal scope recognizes durable suspension as a non-terminal outcome
and lets the internal signal propagate without starting terminal actions.

## Registration Rules

`terminal_scope()` passes a `DurableTerminalActions` interface to its body.
Register bound async callables, normally produced by `@durable_callable`:

```python
terminal.cleanup(
    close_session(session_id),
    name="close-session",
)
terminal.compensate(
    reverse_charge(payment_id),
    name="reverse-charge",
)
```

Registrations:

- must occur directly in the terminal-scope body, not inside a nested step or
  child context;
- must be reached deterministically for the same input and checkpoint history;
- should use static, descriptive names;
- may capture only deterministic inputs or checkpointed results;
- do not checkpoint arbitrary captured Python state;
- close when the body returns or raises.

Register an action after the resource or business effect it corresponds to has
completed durably. On replay, that completed operation returns its checkpointed
result and the body reconstructs the same registration before reaching the same
terminal or suspension path.

## Retry and Step Semantics

Each terminal action is a normal durable step with its own retry policy:

```python
from datetime import timedelta

from async_durable_execution import RetryStrategy, StepSemantics


terminal.cleanup(
    delete_resource(resource_id),
    name="delete-resource",
    retry_strategy=RetryStrategy(
        max_attempts=5,
        initial_delay=timedelta(seconds=1),
    ),
    step_semantics=StepSemantics.AT_LEAST_ONCE_PER_RETRY,
)
```

If an action schedules a retry, the terminal scope suspends. On replay:

1. the body reruns and reconstructs its registrations;
2. completed terminal actions consume their checkpoints without rerunning;
3. the pending action resumes its durable retry;
4. later actions continue after it reaches a terminal result.

Terminal side effects are not exactly once. Use idempotent APIs or stable
idempotency keys, and choose `AT_MOST_ONCE_PER_RETRY` only when its interruption
tradeoff is appropriate.

## Failure Semantics

If the body fails and every terminal action succeeds, the original body
exception is re-raised unchanged.

Before a successful body runs cleanup, the SDK serializes the scope result and
prepares any oversized-result summary. A non-retryable result serialization or
summary failure is therefore a logical scope failure: compensation runs,
followed by cleanup. A retryable SerDes failure remains an invocation
interruption and runs no terminal action until Lambda retries.

If one or more terminal actions fail:

- all remaining ordinary compensation and cleanup actions are attempted;
- `TerminalScopeError.body_failure` preserves structured body failure details;
- `TerminalScopeError.action_failures` preserves failures in execution order;
- `TerminalScopeError.body_error` and `__cause__` retain the original exception
  during the invocation where it occurred;
- checkpoint replay restores the structured failure summaries.

On a successful body, any exhausted cleanup failure fails the scope with
`TerminalScopeError`.

Retryable `InvocationError` instances are invocation interruptions, not logical
body failures. They bypass terminal actions so Lambda can retry with a fresh
checkpoint token.

## Cancellation Policy

`asyncio.CancelledError` is distinct from durable suspension. Configure it
explicitly:

```python
from async_durable_execution import TerminalScopeConfig


config = TerminalScopeConfig(
    compensate_on_cancellation=True,
    cleanup_on_cancellation=True,
)
result = await terminal_scope(body, name="work", config=config)
```

Both options default to `False`. `map()`, `parallel()`, and `flow()` use task
cancellation to abandon unfinished work after an aggregate operation reaches an
early result. The default therefore prevents an abandoned branch from
accidentally running terminal actions.

Enable cancellation actions only when cancellation is a deterministic,
application-owned terminal transition. A lost Lambda invocation, externally
terminated compute, or service transition that does not schedule user code
cannot run terminal actions. Resources that must eventually be reclaimed still
need leases, TTLs, idempotent deletion, or an external reaper.

## Nested and Concurrent Scopes

Every terminal scope is a durable child context with its own operation
namespace. It can be:

- nested inside another terminal scope;
- used in a map item;
- used in a parallel branch;
- called from a flow node;
- called directly from a top-level durable handler or ordinary child context.

An inner scope completes its own terminal phase before its result returns to the
outer body. If the outer body later fails, the outer scope then runs its own
compensation and cleanup.

Concurrent branches must not share a `DurableTerminalActions` object. Each
branch creates and owns its own scope.

## History and Observability

Execution history uses dedicated subtypes:

- `TerminalScope` for the child context;
- `TerminalCompensation` for compensation steps;
- `TerminalCleanup` for cleanup steps.

Names identify the business action, while subtype and parent context identify
its terminal phase. A suspended scope remains started and has no terminal
action operations until a logical terminal path is selected.

## API Reference

See the [terminal-scope API reference](api/extension/terminal_scope.md) and the
[accepted design RFC](rfcs/0001-durable-terminal-scopes.md).
