"""Public durable effects interpreted against journal tickets."""

from __future__ import annotations

import asyncio
import functools
import inspect
import json
import random as _random
import time
import sys
import uuid as _uuid
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Generic, ParamSpec, TypeVar, overload

from ._journal import Entry, Journal, Pause, cursor
from ._scope import (
    DurableContext,
    Identity,
    StepContext,
    WaitForCallbackContext,
    WaitForConditionCheckContext,
    WithRetryContext,
    binding,
    get_durable_context,
    get_step_context,
    install_logging,
)
from ._serde import JsonSerDes, SerDes
from ._types import (
    Duration,
    DurableServiceClient,
    LambdaContext,
    CallbackError,
    CallableRuntimeError,
    ErrorObject,
    ExtensionStepResult,
    ExecutionError,
    InvocationError,
    OperationSubType,
    OperationType,
    PollingStrategy,
    RetryStrategy,
    RetryableSerDesError,
    SerDesError,
    StepInterruptedError,
    StepSemantics,
    WaitForConditionError,
    seconds,
)

P = ParamSpec("P")
T = TypeVar("T")
SummaryGenerator = Callable[[T], str]
"""Callable receiving a result during oversized-result preparation and returning a summary
string.
"""
_failure_readers: dict[str, Any] = {}


class _PermanentInvocation(InvocationError):
    def is_retryable(self):
        return False


def error_record(error):
    record = ErrorObject.from_exception(error).to_dict()
    if isinstance(error, (ExecutionError, InvocationError, SerDesError)):
        extra = {"adeError": 3, "kind": type(error).__name__}
        if isinstance(error, InvocationError) and not error.is_retryable():
            extra["permanentInvocation"] = True
        if isinstance(error, CallbackError):
            extra["callback_id"] = error.callback_id
        if hasattr(error, "_journal_error"):
            extra["detail"] = error._journal_error()
        record["ErrorData"] = json.dumps(extra)
    return record


def raise_record(record):
    error = ErrorObject.from_dict(
        record or {"ErrorMessage": "Durable operation failed"}
    )
    try:
        meta = json.loads(error.data or "null")
    except ValueError:
        meta = None
    if isinstance(meta, dict) and meta.get("adeError") == 3:
        if meta.get("permanentInvocation") is True:
            raise _PermanentInvocation(error.message)
        kind = meta.get("kind")
        if kind in _failure_readers:
            raise _failure_readers[kind](error.message, meta.get("detail"))
        if kind == "CallbackError":
            raise CallbackError(error.message, meta.get("callback_id"))
        if kind == "WaitForConditionError":
            raise WaitForConditionError(error.message)
        if kind in ("ExecutionError", "SerDesError", "SerDesPipelineError"):
            raise ExecutionError(error.message)
    raise CallableRuntimeError.from_error_object(error)


def durable_callable(
    func: Callable[P, Awaitable[T]],
) -> Callable[P, Callable[[], Awaitable[T]]]:
    """Bind arguments to an async callable without executing its body.

    Pass the returned zero-argument callable to step() or run_in_child_context().
    Class and static methods support either decorator order.

    Args:
        func: Async function whose arguments should be bound at the call site.

    Returns:
        (Callable): A wrapper that produces a deferred, zero-argument async callable.
    """
    if isinstance(func, (classmethod, staticmethod)):
        return type(func)(durable_callable(func.__func__))

    @functools.wraps(func)
    def bind(*args: P.args, **kwargs: P.kwargs):
        call = functools.partial(func, *args, **kwargs)
        setattr(call, "__name__", func.__name__)
        return call

    return bind


def reserve(kind, subtype, name=None, *, local_id=None):
    return cursor().reserve(name or None, local_id).select(kind, subtype)


async def step_effect(
    ticket,
    func,
    *,
    retry_strategy=None,
    step_semantics=StepSemantics.AT_LEAST_ONCE_PER_RETRY,
    serdes=None,
    stateful=False,
    initial_state=None,
    original_error=False,
    context_type=StepContext,
):
    saved = ticket.entry
    if saved and saved.status == "SUCCEEDED":
        result = (
            await ticket.decode(saved.payload, serdes, saved.attempt)
            if saved.payload is not None
            else None
        )
        if not ticket.owner.remaining:
            ticket.owner.replaying = False
        return result
    if saved and saved.status in ("FAILED", "CANCELLED", "TIMED_OUT", "STOPPED"):
        raise_record(saved.error)
    if saved and saved.status == "PENDING":
        raise Pause(saved.due)
    error: Exception
    attempt = (saved.attempt if saved else 0) + 1
    state = initial_state
    if stateful and saved and saved.payload is not None:
        state = await ticket.decode(saved.payload, serdes, saved.attempt)
    interrupted = (
        saved
        and saved.status == "STARTED"
        and step_semantics is StepSemantics.AT_MOST_ONCE_PER_RETRY
    )
    if not saved or saved.status == "READY":
        await ticket.write("START")
    try:
        if interrupted:
            raise StepInterruptedError(
                "Step interrupted before durable completion", ticket.key
            )
        with binding(context_type(ticket.journal, ticket.identity, attempt)):
            value = await func(state) if stateful else await func()
        if stateful:
            if not isinstance(value, ExtensionStepResult):
                raise TypeError("Stateful steps must return ExtensionStepResult")
            if value.is_retry:
                assert value.retry_delay is not None
                payload = await ticket.encode(value.value, serdes, attempt)
                await ticket.write(
                    "RETRY",
                    Payload=payload,
                    StepOptions={
                        "NextAttemptDelaySeconds": max(1, seconds(value.retry_delay))
                    },
                )
                raise Pause(ticket.entry.due if ticket.entry else None)
            value = value.value
        payload = await ticket.encode(value, serdes, attempt)
    except (WaitForConditionError, RetryableSerDesError) as caught:
        error = caught
    except (InvocationError, SerDesError, ExecutionError):
        if not interrupted:
            raise
        error = StepInterruptedError(
            "Step interrupted before durable completion", ticket.key
        )
    except Exception as caught:
        error = caught
    else:
        await ticket.write("SUCCEED", Payload=payload)
        return await ticket.decode(payload, serdes, attempt)
    retry = None
    try:
        if stateful:
            retry = (
                retry_strategy(error, state, attempt)
                if retry_strategy is not None
                else None
            )
            if retry is not None:
                if not isinstance(retry, ExtensionStepResult) or not retry.is_retry:
                    raise TypeError(
                        "Stateful retry policy must return a retry result or None"
                    )
                assert retry.retry_delay is not None
                delay = max(1, seconds(retry.retry_delay))
                extra = {"Payload": await ticket.encode(retry.value, serdes, attempt)}
        else:
            retry = (
                retry_strategy
                if retry_strategy is not None
                else RetryStrategy.default()
            )(error, attempt)
            if retry is not None:
                delay = max(1, seconds(retry))
                extra = {"Error": error_record(error)}
    except Exception as policy_error:
        retry, error = None, policy_error
    if retry is not None:
        await ticket.write(
            "RETRY", StepOptions={"NextAttemptDelaySeconds": delay}, **extra
        )
        raise Pause(ticket.entry.due if ticket.entry else None)
    await ticket.write("FAIL", Error=error_record(error))
    if original_error or isinstance(error, StepInterruptedError):
        raise error
    raise_record(error_record(error))


def step(
    func: Callable[[], Awaitable[T]],
    *,
    name: str | None = None,
    retry_strategy: Callable[[Exception, int], Duration | None] | None = None,
    step_semantics: StepSemantics = StepSemantics.AT_LEAST_ONCE_PER_RETRY,
    serdes: SerDes[T] | None = None,
) -> asyncio.Task[T]:
    """Start a durable step that checkpoints a zero-argument async callable.

    Put nondeterministic work and external side effects in the callable. It runs
    in StepContext and cannot compose nested durable operations. A completed step
    returns its saved value on replay; interrupted attempts follow step_semantics.

    Args:
        func: Bound async callable, typically created with durable_callable().
        name: Stable operation name; defaults to the callable's name when available.
        retry_strategy: Callback receiving the exception and one-based attempt count.
            Return a delay to retry or None to stop. None selects
            RetryStrategy.default().
        step_semantics: Whether an interrupted attempt may execute its callable again.
        serdes: Value codec for the step result; None selects the default typed codec.

    Returns:
        (asyncio.Task[T]): The step's deserialized result. Scheduling a durable retry
            suspends the current invocation until the backend makes it runnable.

    Raises:
        RuntimeError: Called outside a durable handler or child scope.
        CallableRuntimeError: A callable failure is checkpointed after retries stop.
    """
    ticket = reserve(
        "STEP", OperationSubType.STEP, name or getattr(func, "__name__", None)
    )
    return ticket.spawn(
        step_effect(
            ticket,
            func,
            retry_strategy=retry_strategy,
            step_semantics=step_semantics,
            serdes=serdes,
        )
    )


async def wait_effect(ticket, duration):
    if ticket.entry is None:
        await ticket.write(
            "START", WaitOptions={"WaitSeconds": seconds(duration, positive=True)}
        )
    saved = ticket.entry
    if saved and saved.status == "SUCCEEDED":
        return None
    if saved and saved.status not in ("STARTED", "PENDING"):
        raise ExecutionError("Durable wait was cancelled or stopped")
    raise Pause(saved.due if saved else None)


def wait(duration: Duration, *, name: str | None = None) -> asyncio.Task[None]:
    """Start a durable delay without keeping a Lambda invocation computing.

    A pending wait suspends the invocation. Once its recorded deadline has passed,
    replay consumes the completed wait and continues.

    Args:
        duration: Integer seconds or timedelta; at least one whole second is required.
        name: Optional stable operation name.

    Returns:
        (asyncio.Task[None]): Completion of the recorded wait.

    Raises:
        ValidationError: The duration has an unsupported type or is below one second.
        RuntimeError: No durable composition context is active.
    """
    duration = seconds(duration, positive=True)
    ticket = reserve("WAIT", OperationSubType.WAIT, name)
    return ticket.spawn(wait_effect(ticket, duration))


async def invoke_effect(
    ticket,
    function_name,
    payload,
    *,
    serdes_payload=None,
    serdes_result=None,
    tenant_id=None,
):
    if ticket.entry is None:
        options = {"FunctionName": function_name}
        if tenant_id is not None:
            options["TenantId"] = tenant_id
        await ticket.write(
            "START",
            Payload=await ticket.encode(payload, serdes_payload or JsonSerDes()),
            ChainedInvokeOptions=options,
        )
    saved = ticket.entry
    if saved and saved.status == "SUCCEEDED":
        return (
            await ticket.decode(saved.payload, serdes_result or JsonSerDes())
            if saved.payload is not None
            else None
        )
    if saved and saved.status in ("FAILED", "CANCELLED", "TIMED_OUT", "STOPPED"):
        raise_record(saved.error)
    raise Pause()


def invoke(
    function_name: str,
    payload: Any,
    *,
    name: str | None = None,
    serdes_payload: SerDes | None = None,
    serdes_result: SerDes[T] | None = None,
    tenant_id: str | None = None,
) -> asyncio.Task[T]:
    """Invoke a durable Lambda function and await its checkpointed result.

    The backend starts the target function. While it is pending, this workflow
    suspends and later replays the recorded invocation result.

    Args:
        function_name: Target function name or ARN, qualified for durable invocation.
        payload: Application input sent to the target.
        name: Optional stable name for this invocation operation.
        serdes_payload: Input codec; defaults to JsonSerDes.
        serdes_result: Result codec; defaults to JsonSerDes.
        tenant_id: Optional tenant identifier forwarded to the target invocation.

    Returns:
        (asyncio.Task[T]): Deserialized target result, or None when no payload exists.

    Raises:
        CallableRuntimeError: The target fails or otherwise ends unsuccessfully.
    """
    ticket = reserve("CHAINED_INVOKE", OperationSubType.CHAINED_INVOKE, name)
    return ticket.spawn(
        invoke_effect(
            ticket,
            function_name,
            payload,
            serdes_payload=serdes_payload,
            serdes_result=serdes_result,
            tenant_id=tenant_id,
        )
    )


class Callback(Generic[T]):
    """Handle for an external party to complete a durable operation.

    Obtain a handle by awaiting create_callback(). Give callback_id to the external
    worker, then await result() to consume its outcome.

    Attributes:
        callback_id (str): Backend identifier used by callback completion APIs.
        operation_id (str): Identifier of the callback operation in this execution.
        state (Any): Opaque SDK-managed state for the owning invocation.
        serdes (SerDes | None): Result decoder; None returns the supplied string as-is.
    """

    def __init__(self, callback_id, operation_id, state, serdes=None):
        """Associate a callback identifier with its owning execution state.

        Args:
            callback_id (str): Identifier returned by the backend.
            operation_id (str): Owning callback operation ID.
            state (Any): SDK-managed execution state; normally supplied by
                create_callback().
            serdes (SerDes | None): Optional decoder for the external result string.
        """
        self.callback_id, self.operation_id, self.state, self.serdes = (
            callback_id,
            operation_id,
            state,
            serdes,
        )

    async def result(self):
        """Read the delivered result, suspending the invocation while it is pending.

        Returns:
            (Any): The decoded result, the raw string when no codec is configured, or None
                when the callback supplies no result payload.

        Raises:
            CallbackError: The callback failed, timed out, was cancelled, or was
                stopped.
        """
        saved = self.state.entries[self.operation_id]
        if saved.status in ("FAILED", "TIMED_OUT", "CANCELLED", "STOPPED"):
            raise CallbackError(
                (saved.error or {}).get("ErrorMessage", "Callback failed"),
                self.callback_id,
            )
        if saved.status != "SUCCEEDED":
            raise Pause(saved.due)
        if saved.payload is None or self.serdes is None:
            return saved.payload
        from ._scope import SerDesContext
        from ._serde import convert

        context = SerDesContext(
            saved.key,
            self.state.durable_execution_arn,
            self.state.recursive_level,
            f"operation/{saved.key}",
            saved.name,
            saved.parent,
            OperationType.CALLBACK,
            saved.subtype,
        )
        return await convert(self.serdes, "deserialize", saved.payload, context)


async def callback_effect(ticket, *, timeout=None, heartbeat_timeout=None, serdes=None):
    if ticket.entry is None:
        await ticket.write(
            "START",
            CallbackOptions={
                "TimeoutSeconds": 0 if timeout is None else seconds(timeout),
                "HeartbeatTimeoutSeconds": 0
                if heartbeat_timeout is None
                else seconds(heartbeat_timeout),
            },
        )
    if ticket.entry is None or not ticket.entry.callback:
        raise CallbackError("Backend did not provide a callback identifier")
    return Callback(ticket.entry.callback, ticket.key, ticket.journal, serdes)


def create_callback(
    *,
    name: str | None = None,
    timeout: Duration | None = None,
    heartbeat_timeout: Duration | None = None,
    serdes: SerDes[T] | None = None,
) -> asyncio.Task[Callback[T]]:
    """Create or replay a durable callback and obtain its external identifier.

    Creating the handle does not wait for external completion. Await the handle's
    result() when the workflow needs the response.

    Args:
        name: Optional stable callback operation name.
        timeout: Overall callback deadline in seconds or timedelta; None uses the
            backend default represented by zero.
        heartbeat_timeout: Required heartbeat interval; None uses the backend default.
        serdes: Optional result decoder; None leaves result strings unchanged.

    Returns:
        (asyncio.Task[Callback[T]]): A handle containing the stable callback identifier.

    Raises:
        CallbackError: The backend did not return a callback identifier.
    """
    ticket = reserve("CALLBACK", OperationSubType.CALLBACK, name)
    return ticket.spawn(
        callback_effect(
            ticket, timeout=timeout, heartbeat_timeout=heartbeat_timeout, serdes=serdes
        )
    )


async def scope_effect(
    ticket,
    func,
    *,
    serdes=None,
    summary_generator=None,
    is_virtual=False,
    context=None,
    prepare_failure=None,
    before_commit=None,
):
    saved = ticket.entry
    if saved and saved.status == "SUCCEEDED":
        return (
            await ticket.decode(ticket.journal.restore(saved.payload), serdes)
            if saved.payload is not None
            else None
        )
    if saved and saved.status == "FAILED":
        raise_record(saved.error)
    # Snapshot replay state before this invocation creates the scope's record.
    child = context or ticket.child(virtual=is_virtual)
    if not is_virtual and saved is None:
        await ticket.write("START")
    try:
        with binding(child):
            result = await func()
            if is_virtual:
                return result
            try:
                encoded = await ticket.encode(result, serdes)
                if len(encoded.encode()) > 200_000 and summary_generator:
                    summary_generator(result)
                payload, chunks = await ticket.journal.store(ticket, encoded)
            except Exception as error:
                if prepare_failure is not None:
                    await prepare_failure(error)
                raise
            if before_commit is not None:
                await before_commit()
        await ticket.write(
            "SUCCEED", Payload=payload, ContextOptions={"ReplayChildren": chunks}
        )
        return await ticket.decode(encoded, serdes)
    except InvocationError as error:
        if not error.is_retryable() and not is_virtual:
            await ticket.write("FAIL", Error=error_record(error))
        raise
    except Exception as error:
        if not is_virtual:
            await ticket.write("FAIL", Error=error_record(error))
        if isinstance(error, (ExecutionError, SerDesError)):
            raise
        raise_record(error_record(error))


def run_in_child_context(
    func: Callable[[], Awaitable[T]],
    *,
    name: str | None = None,
    serdes: SerDes[T] | None = None,
    summary_generator: SummaryGenerator[T] | None = None,
    is_virtual: bool = False,
) -> asyncio.Task[T]:
    """Run a sub-workflow with its own durable operation namespace.

    The callable may compose steps, waits, callbacks, and further child contexts.
    Completed non-virtual children replay their saved result without running the
    body again. Large results are stored in chunks and restored directly.

    Args:
        func: Bound zero-argument async sub-workflow.
        name: Stable scope name; defaults to the callable's name when available.
        serdes: Result codec; None uses the default typed codec.
        summary_generator: Optional callback invoked during oversized-result
            preparation.
        is_virtual: Isolate operation identities without persisting a child result.
            Virtual bodies replay, so their side effects must remain in durable steps.

    Returns:
        (asyncio.Task[T]): The child workflow result.
    """
    ticket = reserve(
        "CONTEXT",
        OperationSubType.RUN_IN_CHILD_CONTEXT,
        name or getattr(func, "__name__", None),
    )
    return ticket.spawn(
        scope_effect(
            ticket,
            func,
            serdes=serdes,
            summary_generator=summary_generator,
            is_virtual=is_virtual,
        )
    )


def wait_for_condition(
    check: Callable[[T | None], Awaitable[T]],
    *,
    initial_state: T | None = None,
    name: str | None = None,
    polling_strategy: Callable[[T, int], Duration | None] | None = None,
    serdes: SerDes[T] | None = None,
) -> asyncio.Task[T]:
    """Run a checkpointed condition check repeatedly until its policy completes.

    Each check receives initial_state on its first attempt and its saved prior
    result on subsequent attempts. The polling strategy receives that result and
    one-based attempt count; None completes with the current value, while a delay
    schedules another durable attempt.

    Args:
        check: Async function accepting the previous state or None.
        initial_state: State passed to the first check.
        name: Optional stable polling operation name.
        polling_strategy: Completion/backoff callback; None selects PollingStrategy.
        serdes: Codec used for both retry state and the final value.

    Returns:
        (asyncio.Task[T]): The value from the check that satisfied the policy.

    Raises:
        WaitForConditionError: The default polling strategy exhausts its attempts.
    """
    policy: Callable[[T, int], Duration | None] = (
        polling_strategy if polling_strategy is not None else PollingStrategy[T]()
    )

    async def poll(state):
        value = await check(state)
        delay = policy(value, get_step_context().attempt)
        return (
            ExtensionStepResult.succeed(value)
            if delay is None
            else ExtensionStepResult.retry(value, delay)
        )

    ticket = reserve("STEP", OperationSubType.WAIT_FOR_CONDITION, name)
    return ticket.spawn(
        step_effect(
            ticket,
            poll,
            stateful=True,
            initial_state=initial_state,
            original_error=True,
            serdes=serdes,
            context_type=WaitForConditionCheckContext,
        )
    )


def wait_for_callback(
    submitter: Callable[[], Awaitable[Any]],
    *,
    name: str | None = None,
    timeout: Duration | None = None,
    heartbeat_timeout: Duration | None = None,
    serdes: SerDes | None = None,
    retry_strategy: Callable[[Exception, int], Duration | None] | None = None,
) -> asyncio.Task[Any]:
    """Checkpoint an external-work submission and await its durable callback.

    The submitter runs as a step and obtains the callback ID through
    get_wait_for_callback_context(). It must hand that identifier to the external
    worker. Replay skips an already-completed submission.

    Args:
        submitter: Bound zero-argument async function that submits the work.
        name: Stable scope name, also used to name the callback and submitter step.
        timeout: Optional overall callback timeout in seconds or timedelta.
        heartbeat_timeout: Optional callback heartbeat interval.
        serdes: Codec shared by the submitter result, callback result, and scope result.
        retry_strategy: Retry policy for submission failures; None selects the default.

    Returns:
        (asyncio.Task[Any]): The external callback result.

    Raises:
        CallbackError: External completion reports failure or the callback times out.
    """
    label = name or getattr(submitter, "__name__", None)

    async def body():
        callback = await create_callback(
            name=f"{label}-callback" if label else "callback",
            timeout=timeout,
            heartbeat_timeout=heartbeat_timeout,
            serdes=serdes,
        )

        async def submit():
            context = get_step_context()
            with binding(
                WaitForCallbackContext(
                    context.execution_state,
                    context.operation_identifier,
                    callback.callback_id,
                )
            ):
                return await submitter()

        await step(
            submit,
            name=f"{label}-submitter" if label else "submitter",
            serdes=serdes,
            retry_strategy=retry_strategy,
        )
        return await callback.result()

    ticket = reserve("CONTEXT", OperationSubType.WAIT_FOR_CALLBACK, label)
    return ticket.spawn(scope_effect(ticket, body, serdes=serdes))


def with_retry(
    func: Callable[[], Awaitable[T]],
    *,
    name: str | None = None,
    retry_strategy: Callable[[Exception, int], Duration | None] | None = None,
    serdes: SerDes[T] | None = None,
    summary_generator: SummaryGenerator[T] | None = None,
    is_virtual: bool = False,
) -> asyncio.Task[T]:
    """Retry a durable sub-workflow with checkpointed backoff waits.

    The body can compose durable operations and read its one-based attempt with
    get_with_retry_context(). Invocation errors bypass this application retry loop.

    Args:
        func: Zero-argument async body to retry.
        name: Scope name; defaults to with-retry.
        retry_strategy: Exception/attempt callback returning a delay or None to stop.
            None selects RetryStrategy.default().
        serdes: Codec for the completed body result.
        summary_generator: Optional callback for oversized-result preparation.
        is_virtual: Use an identity-only scope without persisting the combined result.

    Returns:
        (asyncio.Task[T]): Result of the successful body attempt.
    """

    async def body():
        base = get_durable_context()
        counter = 1
        while True:
            view = WithRetryContext(
                base.execution_state,
                base.operation_identifier,
                base.step_id_prefix,
                base.is_replaying(),
                counter,
            )
            view.__dict__["_cursor"] = cursor(base)
            try:
                with binding(view):
                    return await func()
            except InvocationError:
                raise
            except Exception as error:
                delay = (retry_strategy or RetryStrategy.default())(error, counter)
                if delay is None:
                    raise
                await wait(delay, name=f"{name or 'with-retry'}-backoff-{counter}")
                counter += 1

    return run_in_child_context(
        body,
        name=name or "with-retry",
        serdes=serdes,
        summary_generator=summary_generator,
        is_virtual=is_virtual,
    )


def recurse(
    payload: Any,
    *,
    name: str | None = None,
    function_name: str | None = None,
    with_recursive_level: bool = False,
    serdes_payload: SerDes | None = None,
    serdes_result: SerDes[T] | None = None,
    tenant_id: str | None = None,
) -> asyncio.Task[T]:
    """Invoke this durable function again with a changed application input.

    The recursive call is a separate durable execution. When no target is given,
    resolve the function and qualifier from the active Lambda context.

    Args:
        payload: Input that differs from the current execution input.
        name: Optional stable operation name.
        function_name: Optional explicit qualified function name or ARN.
        with_recursive_level: Copy a dictionary payload and increment its propagated
            recursion depth. Read the depth through the public context property.
        serdes_payload: Input codec; defaults to JSON through invoke().
        serdes_result: Target result codec; defaults to JSON through invoke().
        tenant_id: Explicit tenant, or the current Lambda tenant when omitted.

    Returns:
        (asyncio.Task[T]): Result of the recursive durable invocation.

    Raises:
        ValueError: Input is unchanged or depth propagation requires a dictionary.
        RuntimeError: No explicit target or usable Lambda function metadata is
            available.
    """
    context = get_durable_context()
    invocation = context.lambda_context
    if with_recursive_level:
        if not isinstance(payload, dict):
            raise ValueError("Recursive level requires a dictionary payload")
        payload = dict(payload, __recursive_level=context.recursive_level + 1)
    if payload == json.loads(context.execution_state.input or "null"):
        raise ValueError("Recursive invocation requires a different input")
    if function_name is None:
        function_name = getattr(invocation, "invoked_function_arn", None) or getattr(
            invocation, "function_name", None
        )
        if not function_name:
            raise RuntimeError("Cannot determine the current function")
        qualified = (
            len(function_name.split(":")) >= 8
            if function_name.startswith("arn:")
            else ":" in function_name
        )
        version = getattr(invocation, "function_version", None)
        if not qualified and version:
            function_name += ":" + version
    return invoke(
        function_name,
        payload,
        name=name,
        serdes_payload=serdes_payload,
        serdes_result=serdes_result,
        tenant_id=tenant_id
        if tenant_id is not None
        else getattr(invocation, "tenant_id", None),
    )


def now(*, name: str | None = None) -> asyncio.Task[datetime]:
    """Return a task containing the checkpointed current UTC datetime; replay reuses it."""

    async def read():
        return datetime.now(timezone.utc)

    return step(read, name=name or "now")


def timestamp(*, name: str | None = None) -> asyncio.Task[float]:
    """Return a task containing checkpointed Unix time in seconds; replay reuses it."""

    async def read():
        return time.time()

    return step(read, name=name or "timestamp")


def random(*, name: str | None = None) -> asyncio.Task[float]:
    """Return a task containing a checkpointed random float in [0, 1); replay reuses it."""

    async def read():
        return _random.random()

    return step(read, name=name or "random")


def uuid(*, name: str | None = None) -> asyncio.Task[_uuid.UUID]:
    """Return a task containing a checkpointed random UUID; replay reuses it."""

    async def read():
        return _uuid.uuid4()

    return step(read, name=name or "uuid")


async def invoke_workflow(func, event, context, backend):
    records = [
        Entry.read(record)
        for record in event.get("InitialExecutionState", {}).get("Operations", ())
    ]
    marker = event.get("InitialExecutionState", {}).get("NextMarker")
    while marker:
        page = await backend.page(event["CheckpointToken"], marker)
        records.extend(Entry.read(record) for record in page.get("Operations", ()))
        marker = page.get("NextMarker")
    journal = Journal(
        event["DurableExecutionArn"],
        event["CheckpointToken"],
        backend,
        records,
        context,
    )
    root = DurableContext(
        journal, Identity(None), replaying=any(e.kind != "EXECUTION" for e in records)
    )
    install_logging()
    try:
        with binding(root):
            result = await func(json.loads(journal.input or "null"))
        payload = json.dumps(result)
        if len(payload.encode()) > 6 * 1024 * 1024 - 100:
            key = next(e.key for e in records if e.kind == "EXECUTION")
            await journal.commit(
                {
                    "Id": key,
                    "Type": "EXECUTION",
                    "Action": "SUCCEED",
                    "Payload": payload,
                }
            )
            payload = ""
        return {"Status": "SUCCEEDED", "Result": payload}
    except Pause:
        return {"Status": "PENDING"}
    except InvocationError as error:
        if error.is_retryable():
            raise
        return {"Status": "FAILED", "Error": error_record(error)}
    except Exception as error:
        return {"Status": "FAILED", "Error": error_record(error)}
    finally:
        await journal.close(abort=isinstance(sys.exc_info()[1], asyncio.CancelledError))


@overload
def durable_execution(
    func: Callable[..., Awaitable[Any]],
    /,
    *,
    boto3_client: Any = None,
    service_client: DurableServiceClient | None = None,
) -> Callable[[Any, LambdaContext], Any]: ...


@overload
def durable_execution(
    func: None = None,
    /,
    *,
    boto3_client: Any = None,
    service_client: DurableServiceClient | None = None,
) -> Callable[[Callable[..., Awaitable[Any]]], Callable[[Any, LambdaContext], Any]]: ...


def durable_execution(
    func: Callable[..., Awaitable[Any]] | None = None,
    /,
    *,
    boto3_client: Any = None,
    service_client: DurableServiceClient | None = None,
) -> Any:
    """Expose an async workflow as a synchronous durable Lambda handler.

    Use as `@durable_execution` or `@durable_execution(...)`. Lambda passes the
    handler an invocation envelope and context; the workflow receives its decoded
    application input. Completed durable operations replay from saved state.

    Args:
        func: Async workflow function, or None when configuring the decorator.
        boto3_client: Optional synchronous or asynchronous Lambda API client.
        service_client: Optional durable service adapter, taking precedence over
            boto3_client. SDK-created clients are closed after the invocation;
            caller-supplied clients remain caller-owned.

    Returns:
        (Callable): A Lambda handler accepting event and context, or a decorator when
            func is omitted. The synchronous handler cannot run inside an active
            event loop; use the public local runner for asynchronous workflow tests.

    Raises:
        ExecutionError: The Lambda invocation envelope is missing required fields.
    """
    if func is None:
        return functools.partial(
            durable_execution, boto3_client=boto3_client, service_client=service_client
        )
    loop = None

    async def invoke(event, context):
        from ._remote import ServiceBackend

        if (
            not isinstance(event, dict)
            or not event.get("DurableExecutionArn")
            or not event.get("CheckpointToken")
        ):
            raise ExecutionError("A durable Lambda invocation envelope is required")
        backend = ServiceBackend(
            event["DurableExecutionArn"],
            service_client=service_client,
            api_client=boto3_client,
        )
        try:
            return await invoke_workflow(func, event, context, backend)
        finally:
            await backend.close()

    @functools.wraps(func)
    def handler(event, context):
        nonlocal loop
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            raise RuntimeError("Use _async_handler while an event loop is running")
        if loop is None or loop.is_closed():
            loop = asyncio.new_event_loop()
        return loop.run_until_complete(invoke(event, context))

    setattr(handler, "_async_handler", invoke)
    setattr(handler, "_ade_workflow", func)
    return handler


class ExtensionOperation:
    """An opaque reservation that may launch exactly one durable primitive.

    Obtain it from ExtensionContext.reserve() and select a primitive only while
    its owning context is active. Each primitive requires an SDK enum subtype or
    a nonblank extension-owned subtype string. Reuse or cross-context selection
    raises RuntimeError.
    """

    _ticket: Any

    def __init__(self):
        """Reject direct construction; obtain a reservation with
        ExtensionContext.reserve().

        Raises:
            TypeError: Always, because the SDK creates reservations on their owning
                context.
        """
        raise TypeError("Use ExtensionContext.reserve() to obtain an operation")

    def _take(self, kind, sub_type):
        if not isinstance(sub_type, OperationSubType):
            if not isinstance(sub_type, str) or not sub_type.strip():
                raise ValueError(
                    "sub_type must be an enum or a nonblank extension string"
                )
            if sub_type in {item.value for item in OperationSubType}:
                raise ValueError("Select reserved SDK subtypes with their enum member")
        return self._ticket.select(kind, sub_type)

    def step(
        self,
        func,
        *,
        sub_type,
        initial_state=None,
        retry_strategy=None,
        step_semantics=StepSemantics.AT_LEAST_ONCE_PER_RETRY,
        serdes=None,
    ):
        """Select a stateful step using this reservation's durable identity.

        Args:
            func (Callable): Async function from prior state to ExtensionStepResult.
            sub_type (OperationSubType | str): SDK enum member or extension-owned label.
            initial_state (Any): State for the first attempt.
            retry_strategy (Callable | None): Exception/state/attempt callback returning
                ExtensionStepResult.retry(...) or None. None makes ordinary failures
                terminal.
            step_semantics (StepSemantics): Policy for interrupted attempts.
            serdes (SerDes | None): Codec for retry state and final results.

        Returns:
            (asyncio.Task): The stateful step's completed value.
        """
        ticket = self._take("STEP", sub_type)
        return ticket.spawn(
            step_effect(
                ticket,
                func,
                stateful=True,
                initial_state=initial_state,
                retry_strategy=retry_strategy,
                step_semantics=step_semantics,
                serdes=serdes,
            )
        )

    def wait(self, duration, *, sub_type):
        """Select a durable wait using this reservation's identity.

        Args:
            duration (Duration): Integer seconds or timedelta, at least one whole
                second.
            sub_type (OperationSubType | str): SDK enum member or extension-owned label.

        Returns:
            (asyncio.Task): A task returning None when the recorded wait completes.
        """
        duration = seconds(duration, positive=True)
        ticket = self._take("WAIT", sub_type)
        return ticket.spawn(wait_effect(ticket, duration))

    def invoke(
        self,
        function_name,
        payload,
        *,
        sub_type,
        serdes_payload=None,
        serdes_result=None,
        tenant_id=None,
    ):
        """Select a chained durable invocation using this reservation.

        Args:
            function_name (str): Qualified target function name or ARN.
            payload (Any): Target application input.
            sub_type (OperationSubType | str): SDK enum member or extension-owned label.
            serdes_payload (SerDes | None): Input codec; defaults to JSON.
            serdes_result (SerDes | None): Result codec; defaults to JSON.
            tenant_id (str | None): Optional tenant forwarded to the target.

        Returns:
            (asyncio.Task): The target's decoded durable result.
        """
        ticket = self._take("CHAINED_INVOKE", sub_type)
        return ticket.spawn(
            invoke_effect(
                ticket,
                function_name,
                payload,
                serdes_payload=serdes_payload,
                serdes_result=serdes_result,
                tenant_id=tenant_id,
            )
        )

    def create_callback(
        self, *, sub_type, timeout=None, heartbeat_timeout=None, serdes=None
    ):
        """Select a durable callback using this reservation.

        Args:
            sub_type (OperationSubType | str): SDK enum member or extension-owned label.
            timeout (Duration | None): Optional overall callback timeout.
            heartbeat_timeout (Duration | None): Optional heartbeat interval.
            serdes (SerDes | None): Result decoder; None preserves result strings.

        Returns:
            (asyncio.Task): A Callback handle whose result() consumes external completion.
        """
        ticket = self._take("CALLBACK", sub_type)
        return ticket.spawn(
            callback_effect(
                ticket,
                timeout=timeout,
                heartbeat_timeout=heartbeat_timeout,
                serdes=serdes,
            )
        )

    def run_in_child_context(
        self, func, *, sub_type, serdes=None, summary_generator=None, is_virtual=False
    ):
        """Select an isolated durable child scope using this reservation.

        Args:
            func (Callable): Zero-argument async sub-workflow.
            sub_type (OperationSubType | str): SDK enum member or extension-owned label.
            serdes (SerDes | None): Child result codec.
            summary_generator (Callable | None): Optional oversized-result preparation
                callback.
            is_virtual (bool): Isolate identities without persisting a child result.

        Returns:
            (asyncio.Task): The child workflow result.
        """
        ticket = self._take("CONTEXT", sub_type)
        return ticket.spawn(
            scope_effect(
                ticket,
                func,
                serdes=serdes,
                summary_generator=summary_generator,
                is_virtual=is_virtual,
            )
        )


class ExtensionContext:
    """Public access to primitive reservations in an active durable scope.

    Use get_extension_context() or get_current() to obtain this interface. It does
    not create an implicit child scope; reservations belong to the current handler
    or child context.
    """

    def __init__(self, context):
        """Wrap a durable context; normally obtain it through get_extension_context().

        Args:
            context (DurableContext): Owning scope, which must be active when reserving
                work.
        """
        self._context = context

    @classmethod
    def get_current(cls):
        """Return the reservation interface for the active durable handler or child scope."""
        return cls(get_durable_context())

    @property
    def lambda_context(self):
        """Return the underlying Lambda invocation metadata for the owning scope."""
        return self._context.lambda_context

    @property
    def recursive_level(self):
        """Return the recursion depth of the owning durable execution."""
        return self._context.recursive_level

    def is_replaying(self):
        """Return whether the owning scope is currently consuming saved operations."""
        return self._context.is_replaying()

    def reserve(self, name=None, *, local_operation_id=None):
        """Reserve an operation identity before choosing and launching a primitive.

        Sequential reservations must be made in the same order on replay. Local IDs
        use a separate namespace and must all be reserved before any primitive is
        selected. Reservations cannot be reused or selected from another context.

        Args:
            name (str | None): Optional nonblank operation name.
            local_operation_id (str | None): Unique, nonblank local ID independent of
                sequential reservation order.

        Returns:
            (ExtensionOperation): A reservation that can launch one primitive.

        Raises:
            RuntimeError: The owning scope is inactive or local IDs are reserved too
                late.
            TypeError: The name is not a string or None.
            ValueError: A name or local ID is blank, or a local ID is duplicated.
        """
        if get_durable_context() is not self._context:
            raise RuntimeError(
                "An extension context may reserve only within its owning scope"
            )
        if name is not None:
            if not isinstance(name, str):
                raise TypeError("name must be a string")
            if not name.strip():
                raise ValueError("name must not be blank")
        result = object.__new__(ExtensionOperation)
        result._ticket = cursor(self._context).reserve(name, local_operation_id)
        return result


def get_extension_context():
    """Return the extension reservation interface for the active durable scope.

    Raises:
        RuntimeError: Called outside a handler or child scope, including inside a step.
    """
    return ExtensionContext.get_current()
