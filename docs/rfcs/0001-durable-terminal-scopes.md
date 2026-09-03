# RFC 0001: Durable Terminal Scopes

- Status: Accepted
- Date: 2026-09-03
- Issue: `zhongkechen/async-durable-execution#318`

## Summary

Add an async-native `terminal_scope()` operation that registers durable cleanup
and compensation actions and runs them only after a logical terminal outcome.
Durable suspension, replay boundaries, and retryable invocation interruptions
do not start terminal work.

The operation is composed from an isolated durable child context and existing
durable step semantics. It adds no new backend operation type.

## Context

Python lexical cleanup is tied to stack unwinding. The SDK suspends an
invocation by raising an internal `SuspendExecution` signal derived from
`BaseException`. Although `except Exception` does not catch that signal,
Python still executes active:

- `finally` blocks;
- synchronous and asynchronous context-manager exits;
- `ExitStack` and `AsyncExitStack` callbacks;
- generator and asynchronous-generator cleanup.

That lifecycle differs from durable execution. A coroutine or invocation can
exit while its logical workflow remains active and will resume from durable
history later.

The unsafe pattern is:

```text
acquire resource
try:
    perform durable work that can suspend
finally:
    release resource
```

The release can run on an ordinary wait or callback suspension.

Applications can duplicate cleanup after success and in `except Exception`,
but that becomes error-prone with multiple resources, reverse-order rollback,
nested scopes, retries, concurrent branches, and cancellation.

## Goals

- Distinguish logical terminal outcomes from durable suspension.
- Provide unconditional cleanup and failure-only compensation.
- Preserve deterministic replay and stable operation identity.
- Run terminal actions as retryable, checkpointed durable steps.
- Resume partially completed terminal processing without repeating completed
  actions.
- Support handlers, child contexts, map items, parallel branches, flow nodes,
  and nested terminal scopes.
- Expose precise async typing and durable history subtypes.
- Specify body, action, cancellation, and fatal-signal error behavior.

## Non-goals

- Guarantee cleanup after hard infrastructure termination when no invocation
  can execute user code.
- Replace leases, TTLs, resource reapers, or reconciliation.
- Provide exactly-once external side effects.
- Make arbitrary context managers suspension aware.
- Encourage applications to catch SDK suspension signals.
- Add a backend-visible operation state machine in the first version.

## Decision

### Public API

```python
class DurableTerminalActions(Protocol):
    def cleanup(
        self,
        func: Callable[[], Awaitable[None]],
        *,
        name: str | None = None,
        retry_strategy: Callable[[Exception, int], Duration | None] | None = None,
        step_semantics: StepSemantics = StepSemantics.AT_LEAST_ONCE_PER_RETRY,
    ) -> None: ...

    def compensate(
        self,
        func: Callable[[], Awaitable[None]],
        *,
        name: str | None = None,
        retry_strategy: Callable[[Exception, int], Duration | None] | None = None,
        step_semantics: StepSemantics = StepSemantics.AT_LEAST_ONCE_PER_RETRY,
    ) -> None: ...


@dataclass(frozen=True)
class TerminalScopeConfig:
    compensate_on_cancellation: bool = False
    cleanup_on_cancellation: bool = False


def terminal_scope(
    body: Callable[[DurableTerminalActions], Awaitable[T]],
    *,
    name: str | None = None,
    config: TerminalScopeConfig | None = None,
    serdes: SerDes[T] | None = None,
    summary_generator: SummaryGenerator[T] | None = None,
) -> asyncio.Task[T]: ...
```

The callback form is selected instead of an async context manager because it:

- owns an isolated operation namespace;
- controls the complete body and terminal lifecycle;
- classifies suspension before lexical cleanup runs;
- types the body result directly;
- does not imply that arbitrary `async with` blocks are durable.

### Outcome Classification

The SDK boundary classifies outcomes in this order:

1. `asyncio.CancelledError`
2. ordinary `Exception`
3. all remaining `BaseException` signals
4. success

Within ordinary exceptions, a retryable `InvocationError` is a non-terminal
invocation interruption.

| Outcome | Compensation | Cleanup | Propagation |
| --- | --- | --- | --- |
| Success after result serialization/summary preparation | No | Yes | Return body result unless cleanup fails |
| Ordinary body failure | Yes | Yes | Re-raise body error, or raise `TerminalScopeError` when actions also fail |
| Non-retryable result serialization or summary failure | Yes | Yes | Preserve preparation failure semantics |
| Non-retryable invocation/control failure | Yes | Yes | Preserve failure semantics |
| Retryable `InvocationError` | No | No | Propagate for Lambda retry |
| `SuspendExecution` or `TimedSuspendExecution` | No | No | Propagate suspension |
| `asyncio.CancelledError` | Configured | Configured | Re-raise cancellation |
| Other `BaseException` | No | No | Propagate unchanged |

`OrphanedChildException`, fatal checkpoint-thread errors, `KeyboardInterrupt`,
`SystemExit`, and `GeneratorExit` therefore bypass terminal work.

### Registration and Replay

The scope body reruns on replay and reconstructs registrations from
deterministic workflow state. Registration itself is not checkpointed.

A registration is valid only:

- while the body is open;
- in the exact durable context where the registry was supplied;
- with an async zero-argument callable;
- with a nonblank optional name and valid `StepSemantics`.

The body may acquire a resource through a completed durable operation and then
register its action. During replay, the acquisition returns its checkpointed
result before registration is reconstructed.

Captured values must come from deterministic input or checkpointed operation
results. Registration does not persist arbitrary Python objects.

### Durable Identity

`terminal_scope()` reserves one normal child-context operation with subtype
`TerminalScope`.

Body operations allocate identities in that child context. After the body
returns, the child context serializes its result and prepares any oversized
summary. Only then do registered actions allocate subsequent step identities in
deterministic execution order:

1. compensations in reverse registration order;
2. cleanups in reverse registration order.

Action steps use `TerminalCompensation` and `TerminalCleanup` subtypes. Names
remain application supplied.

Because each action is a normal step:

- a completed action is skipped on replay;
- a retrying action suspends and resumes through existing step history;
- an at-least-once interruption can repeat the side effect;
- at-most-once-per-retry retains its existing interruption tradeoff.

No separate terminal-phase checkpoint is required. The deterministic replay of
the body reconstructs all registrations before terminal actions resume.

### Ordering

The first version is intentionally sequential:

- compensation is last-in, first-out;
- cleanup is last-in, first-out;
- cleanup starts after compensation;
- an ordinary action failure is recorded and later actions are attempted;
- suspension or retryable invocation interruption stops the current pass and
  resumes through replay.

Parallel terminal actions are not included because they complicate stable
ordering, partial failure, and deterministic diagnostics.

### Error Model

If a body fails and every terminal action succeeds, the original exception is
re-raised unchanged.

If result serialization or oversized-summary generation fails before the
success checkpoint, the failure is classified like a body failure:
compensation and cleanup run before the child context records failure.
Retryable SerDes errors remain invocation interruptions and bypass terminal
actions.

If terminal actions fail, the scope raises `TerminalScopeError`, an
`ExecutionError` with:

- optional `body_failure`;
- ordered `action_failures`;
- in-invocation `body_error`;
- the original body error as `__cause__`.

Each `TerminalFailure` records phase, name, error type, message, and any
checkpointed stack trace. The SDK registers a control-error codec so structured
details survive the child-context failure checkpoint and replay.

On body success, exhausted cleanup failures also raise `TerminalScopeError`.

The body failure remains the primary structured failure. Action failures remain
ordered by actual terminal execution.

### Cancellation

Cancellation defaults to no terminal actions.

This SDK uses `asyncio` task cancellation internally when `map()`,
`parallel()`, or `flow()` reaches an early result and abandons unfinished work.
Treating every `CancelledError` as a terminal business failure would release or
compensate resources in branches the aggregate explicitly abandoned.

Applications can opt into compensation and cleanup independently through
`TerminalScopeConfig`. Opt-in is intended for deterministic,
application-owned cancellation. Cancellation is always re-raised after the
configured terminal pass.

This policy cannot make hard runtime termination executable. External leases
and reapers remain required for unconditional eventual reclamation.

### Serialization

The scope result supports the same `serdes` and oversized-result
`summary_generator` behavior as `run_in_child_context()`. Serialization and
summary preparation complete before success cleanup. The prepared payload is
then reused by the child success checkpoint without a second serialization.

Terminal action results are typed as `None` and use normal step serialization.
`TerminalScopeError` stores compact JSON control metadata in the failed context
checkpoint.

### Observability

The history representation is:

```text
CONTEXT TerminalScope "review-with-microvm"
  STEP Step "launch-microvm"
  CALLBACK Callback "review-complete"
  STEP Step "dispatch-review"
  STEP TerminalCompensation "cancel-review"  # failure/cancellation policy only
  STEP TerminalCleanup "terminate-microvm"
```

A suspension leaves the scope context started and creates no terminal action
operations.

## Alternatives Considered

### Document Manual `try`/`except`

Rejected as the only solution. It duplicates control flow, scales poorly to
multiple resources, and does not provide a reusable cancellation or error
model.

### `finally` or Context Managers

Rejected because lexical stack exit occurs during durable suspension.

### Async Context Manager With Special `__aexit__`

Possible, but rejected for the first version. It can recognize SDK signals, yet
the callback form owns identity and lifecycle more clearly and avoids implying
that normal context managers are durable.

### Persist Registration Descriptors

Deferred. Deterministic replay already reconstructs registrations and captured
checkpointed values without a new backend or metadata state machine.
Persisted descriptors would require a serialization format, compatibility
rules, and a callable lookup mechanism.

### New Backend Operation Type

Deferred. Composition over child contexts and steps delivers the lifecycle
contract without requiring service rollout or older-runtime negotiation.

### Cleanup-only Initial Version

Rejected. Reverse-order compensation is the primary failure-path use case and
uses the same registry, identity, retry, and error machinery.

## Compatibility

The API is additive.

Persisted compatibility depends on keeping:

- the terminal scope name and position;
- body operation order;
- registration order;
- action names;
- action subtype and step semantics;
- cancellation configuration;
- result and failure serialization

stable for executions that can replay old history.

Changing these values follows the same compatibility rules as changing other
durable operation identities.

## Testing Requirements

The implementation is accepted only with coverage for:

- success cleanup and failure compensation/cleanup order;
- suspension and timed suspension bypass;
- callback suspension before cleanup;
- non-retryable result serialization and summary failures before compensation;
- retryable invocation interruption bypass;
- fatal `BaseException` propagation;
- cancellation default and opt-in policies;
- action retry suspension and resume;
- completed action replay skipping;
- structured multiple-failure round trips;
- nested scopes;
- aggregate early cancellation;
- public exports and type checking;
- local runner replay;
- deployable cloud example success and failure;
- strict documentation and generated API reference.

## Cross-language Contract

Other Durable Execution SDKs can expose different idiomatic APIs while sharing
these requirements:

1. invocation suspension is not logical terminal completion;
2. cleanup runs on logical success and failure;
3. compensation runs on failure and an explicitly defined cancellation policy;
4. actions are durable, replay-safe operations with stable identity;
5. completed terminal actions do not repeat on replay;
6. ordering and multiple-failure semantics are documented;
7. hard termination limitations are explicit.
