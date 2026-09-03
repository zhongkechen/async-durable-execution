"""Tests for durable terminal scopes."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any
from unittest.mock import Mock

import pytest

from async_durable_execution import (
    DurableContext,
    DurableTerminalActions,
    TerminalFailure,
    TerminalFailurePhase,
    TerminalScopeConfig,
    TerminalScopeError,
    terminal_scope,
)
from async_durable_execution._core import (
    OperationIdentifier,
    OperationSubType,
    _encode_sdk_control_error_data,
    _restore_sdk_control_error,
    bind_current_context,
)
from async_durable_execution._core.exceptions import (
    BackgroundThreadError,
    CallableRuntimeError,
    ExecutionError,
    InvocationError,
    OrphanedChildException,
    SuspendExecution,
    TimedSuspendExecution,
    ValidationError,
)
from async_durable_execution._core.state import ExecutionState
from async_durable_execution._operation.terminal_scope import (
    _DurableTerminalActions,
    _encode_terminal_scope_error_payload,
    _execute_terminal_scope,
    _failure_from_exception,
    _restore_terminal_scope_error,
)


def _create_context(parent_id: str | None = None) -> DurableContext:
    state = Mock(spec=ExecutionState)
    state.durable_execution_arn = "arn:test:durable-execution"
    return DurableContext(
        execution_state=state,
        operation_identifier=OperationIdentifier(
            operation_id=None,
            sub_type=OperationSubType.EXECUTION,
            parent_id=parent_id,
        ),
    )


class _ImmediateOperation:
    def __init__(
        self,
        owner: _ImmediateExtensionContext,
        name: str | None,
    ) -> None:
        self.owner = owner
        self.name = name

    def _run_step(
        self,
        func: Callable[[], Awaitable[Any]],
        *,
        sub_type: OperationSubType,
        **_kwargs: Any,
    ) -> asyncio.Task[Any]:
        self.owner.calls.append(("step", self.name, sub_type))

        async def run() -> Any:
            return await func()

        return asyncio.create_task(run())

    def _run_in_child_context(
        self,
        func: Callable[[], Awaitable[Any]],
        *,
        sub_type: OperationSubType,
        **_kwargs: Any,
    ) -> asyncio.Task[Any]:
        self.owner.calls.append(("context", self.name, sub_type))

        async def run() -> Any:
            return await func()

        return asyncio.create_task(run())


class _ImmediateExtensionContext:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str | None, OperationSubType]] = []

    def _reserve_sdk_operation(self, name: str | None) -> _ImmediateOperation:
        return _ImmediateOperation(self, name)


@pytest.fixture
def immediate_extension(
    monkeypatch: pytest.MonkeyPatch,
) -> _ImmediateExtensionContext:
    extension = _ImmediateExtensionContext()
    monkeypatch.setattr(
        "async_durable_execution._operation.terminal_scope.get_extension_context",
        lambda: extension,
    )
    return extension


async def test_success_runs_cleanup_in_reverse_order(
    immediate_extension: _ImmediateExtensionContext,
) -> None:
    context = _create_context()
    events: list[str] = []

    async def record(value: str) -> None:
        events.append(value)

    async def body(actions: DurableTerminalActions) -> int:
        actions.compensate(lambda: record("compensate"), name="compensate")
        actions.cleanup(lambda: record("cleanup-1"), name="cleanup-1")
        actions.cleanup(lambda: record("cleanup-2"), name="cleanup-2")
        return 42

    with bind_current_context(context):
        result = await _execute_terminal_scope(body, TerminalScopeConfig())

    assert result == 42
    assert events == ["cleanup-2", "cleanup-1"]
    assert immediate_extension.calls == [
        ("step", "cleanup-2", OperationSubType.TERMINAL_CLEANUP),
        ("step", "cleanup-1", OperationSubType.TERMINAL_CLEANUP),
    ]


async def test_failure_runs_compensation_then_cleanup_in_reverse_order(
    immediate_extension: _ImmediateExtensionContext,
) -> None:
    context = _create_context()
    events: list[str] = []

    async def record(value: str) -> None:
        events.append(value)

    async def body(actions: DurableTerminalActions) -> None:
        actions.compensate(lambda: record("compensate-1"), name="compensate-1")
        actions.compensate(lambda: record("compensate-2"), name="compensate-2")
        actions.cleanup(lambda: record("cleanup-1"), name="cleanup-1")
        actions.cleanup(lambda: record("cleanup-2"), name="cleanup-2")
        msg = "body failed"
        raise ValueError(msg)

    with bind_current_context(context), pytest.raises(ValueError, match="body failed"):
        await _execute_terminal_scope(body, TerminalScopeConfig())

    assert events == [
        "compensate-2",
        "compensate-1",
        "cleanup-2",
        "cleanup-1",
    ]
    assert [call[2] for call in immediate_extension.calls] == [
        OperationSubType.TERMINAL_COMPENSATION,
        OperationSubType.TERMINAL_COMPENSATION,
        OperationSubType.TERMINAL_CLEANUP,
        OperationSubType.TERMINAL_CLEANUP,
    ]


async def test_action_failures_are_collected_without_skipping_later_actions(
    immediate_extension: _ImmediateExtensionContext,
) -> None:
    context = _create_context()
    events: list[str] = []

    async def fail(value: str) -> None:
        events.append(value)
        raise RuntimeError(value)

    async def succeed(value: str) -> None:
        events.append(value)

    async def body(actions: DurableTerminalActions) -> None:
        actions.compensate(lambda: succeed("compensate-1"), name="compensate-1")
        actions.compensate(lambda: fail("compensate-2"), name="compensate-2")
        actions.cleanup(lambda: succeed("cleanup-1"), name="cleanup-1")
        actions.cleanup(lambda: fail("cleanup-2"), name="cleanup-2")
        msg = "body failed"
        raise ValueError(msg)

    with bind_current_context(context), pytest.raises(TerminalScopeError) as raised:
        await _execute_terminal_scope(body, TerminalScopeConfig())

    error = raised.value
    assert error.body_error is raised.value.__cause__
    assert isinstance(error.body_error, ValueError)
    assert error.body_failure == TerminalFailure(
        phase=TerminalFailurePhase.BODY,
        error_type="ValueError",
        message="body failed",
    )
    assert error.action_failures == (
        TerminalFailure(
            phase=TerminalFailurePhase.COMPENSATION,
            error_type="RuntimeError",
            message="compensate-2",
            name="compensate-2",
        ),
        TerminalFailure(
            phase=TerminalFailurePhase.CLEANUP,
            error_type="RuntimeError",
            message="cleanup-2",
            name="cleanup-2",
        ),
    )
    assert events == [
        "compensate-2",
        "compensate-1",
        "cleanup-2",
        "cleanup-1",
    ]


@pytest.mark.parametrize(
    "signal",
    [
        SuspendExecution("suspend"),
        TimedSuspendExecution("timed suspend", 123.0),
        OrphanedChildException("orphaned", "operation-1"),
        BackgroundThreadError("background failed", RuntimeError("source")),
        KeyboardInterrupt(),
        SystemExit(),
        GeneratorExit(),
    ],
)
async def test_non_terminal_base_exceptions_run_no_actions(
    signal: BaseException,
    immediate_extension: _ImmediateExtensionContext,
) -> None:
    context = _create_context()
    events: list[str] = []

    async def cleanup() -> None:
        events.append("cleanup")

    async def body(actions: DurableTerminalActions) -> None:
        actions.cleanup(cleanup, name="cleanup")
        raise signal

    with bind_current_context(context), pytest.raises(type(signal)):
        await _execute_terminal_scope(body, TerminalScopeConfig())

    assert events == []
    assert immediate_extension.calls == []


async def test_retryable_invocation_error_runs_no_actions(
    immediate_extension: _ImmediateExtensionContext,
) -> None:
    context = _create_context()
    events: list[str] = []

    async def cleanup() -> None:
        events.append("cleanup")

    error = InvocationError("retry invocation")

    async def body(actions: DurableTerminalActions) -> None:
        actions.cleanup(cleanup, name="cleanup")
        raise error

    with bind_current_context(context), pytest.raises(InvocationError) as raised:
        await _execute_terminal_scope(body, TerminalScopeConfig())

    assert raised.value is error
    assert events == []
    assert immediate_extension.calls == []


async def test_cancellation_skips_actions_by_default(
    immediate_extension: _ImmediateExtensionContext,
) -> None:
    context = _create_context()
    started = asyncio.Event()
    events: list[str] = []

    async def cleanup() -> None:
        events.append("cleanup")

    async def body(actions: DurableTerminalActions) -> None:
        actions.cleanup(cleanup, name="cleanup")
        started.set()
        await asyncio.Event().wait()

    async def run() -> None:
        with bind_current_context(context):
            await _execute_terminal_scope(body, TerminalScopeConfig())

    task = asyncio.create_task(run())
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert events == []
    assert immediate_extension.calls == []


async def test_cancellation_policy_can_run_compensation_and_cleanup(
    immediate_extension: _ImmediateExtensionContext,
) -> None:
    context = _create_context()
    started = asyncio.Event()
    events: list[str] = []

    async def record(value: str) -> None:
        events.append(value)

    async def body(actions: DurableTerminalActions) -> None:
        actions.compensate(lambda: record("compensate"), name="compensate")
        actions.cleanup(lambda: record("cleanup"), name="cleanup")
        started.set()
        await asyncio.Event().wait()

    async def run() -> None:
        with bind_current_context(context):
            await _execute_terminal_scope(
                body,
                TerminalScopeConfig(
                    compensate_on_cancellation=True,
                    cleanup_on_cancellation=True,
                ),
            )

    task = asyncio.create_task(run())
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert events == ["compensate", "cleanup"]
    assert [call[2] for call in immediate_extension.calls] == [
        OperationSubType.TERMINAL_COMPENSATION,
        OperationSubType.TERMINAL_CLEANUP,
    ]


async def test_terminal_action_suspension_stops_the_current_terminal_pass(
    immediate_extension: _ImmediateExtensionContext,
) -> None:
    context = _create_context()
    events: list[str] = []

    async def earlier_cleanup() -> None:
        events.append("earlier")

    async def suspending_cleanup() -> None:
        events.append("suspend")
        raise SuspendExecution("terminal action suspended")

    async def body(actions: DurableTerminalActions) -> None:
        actions.cleanup(earlier_cleanup, name="earlier")
        actions.cleanup(suspending_cleanup, name="suspending")

    with (
        bind_current_context(context),
        pytest.raises(SuspendExecution, match="terminal action suspended"),
    ):
        await _execute_terminal_scope(body, TerminalScopeConfig())

    assert events == ["suspend"]


async def test_registry_rejects_registration_from_another_context() -> None:
    context = _create_context("parent")
    other_context = _create_context("other")
    with bind_current_context(context):
        actions = _DurableTerminalActions(context)

    async def cleanup() -> None:
        return None

    with (
        bind_current_context(other_context),
        pytest.raises(RuntimeError, match="registered directly"),
    ):
        actions.cleanup(cleanup)


async def test_registry_rejects_registration_after_body_closes() -> None:
    context = _create_context()
    with bind_current_context(context):
        actions = _DurableTerminalActions(context)
        actions._close()

        async def cleanup() -> None:
            return None

        with pytest.raises(RuntimeError, match="while the scope body runs"):
            actions.cleanup(cleanup)


def test_terminal_scope_config_requires_bool_values() -> None:
    with pytest.raises(TypeError, match="compensate_on_cancellation"):
        TerminalScopeConfig(compensate_on_cancellation=1)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="cleanup_on_cancellation"):
        TerminalScopeConfig(cleanup_on_cancellation="yes")  # type: ignore[arg-type]


def test_terminal_scope_error_codec_round_trips_failure_details() -> None:
    body_failure = TerminalFailure(
        phase=TerminalFailurePhase.BODY,
        error_type="ValueError",
        message="body failed",
    )
    action_failure = TerminalFailure(
        phase=TerminalFailurePhase.CLEANUP,
        error_type="RuntimeError",
        message="cleanup failed",
        name="cleanup",
        stack_trace=("line one",),
    )
    error = TerminalScopeError(
        "terminal scope failed",
        body_failure=body_failure,
        action_failures=(action_failure,),
    )

    encoded = _encode_sdk_control_error_data(error)
    restored = _restore_sdk_control_error(
        str(error),
        "TerminalScopeError",
        encoded,
    )

    assert isinstance(restored, TerminalScopeError)
    assert restored.body_failure == body_failure
    assert restored.action_failures == (action_failure,)


async def test_terminal_scope_uses_dedicated_child_context_subtype(
    immediate_extension: _ImmediateExtensionContext,
) -> None:
    context = _create_context()

    async def body(_actions: DurableTerminalActions) -> str:
        return "done"

    with bind_current_context(context):
        task: asyncio.Task[str] = terminal_scope(body, name="scope")
        result = await task

    assert result == "done"
    assert immediate_extension.calls == [
        ("context", "scope", OperationSubType.TERMINAL_SCOPE)
    ]


@pytest.mark.parametrize(
    "payload",
    [
        None,
        "not-json",
        '{"version":2}',
        '{"version":1,"actionFailures":{}}',
        '{"version":1,"bodyFailure":[]}',
        ('{"version":1,"bodyFailure":{"phase":"BODY","errorType":1,"message":"bad"}}'),
    ],
)
def test_terminal_scope_error_restore_fails_closed(payload: str | None) -> None:
    restored = _restore_terminal_scope_error("failed", payload)

    assert restored.body_failure is None
    assert restored.action_failures == ()


@pytest.mark.parametrize(
    "value",
    [
        [],
        {"phase": "BODY", "errorType": 1, "message": "bad"},
        {"phase": "BODY", "errorType": "Type", "message": "bad", "name": 1},
        {
            "phase": "BODY",
            "errorType": "Type",
            "message": "bad",
            "stackTrace": [1],
        },
    ],
)
def test_terminal_failure_rejects_malformed_payload(value: Any) -> None:
    with pytest.raises(ValueError):
        TerminalFailure._from_dict(value)


def test_terminal_scope_error_encoder_rejects_other_execution_errors() -> None:
    with pytest.raises(TypeError, match="incompatible"):
        _encode_terminal_scope_error_payload(ExecutionError("wrong"))


async def test_registry_validates_action_arguments() -> None:
    context = _create_context()

    async def cleanup() -> None:
        return None

    with bind_current_context(context):
        actions = _DurableTerminalActions(context)
        with pytest.raises(TypeError, match="async zero-argument callable"):
            actions.cleanup(None)  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="StepSemantics"):
            actions.cleanup(
                cleanup,
                step_semantics="AT_LEAST_ONCE_PER_RETRY",  # type: ignore[arg-type]
            )
        with pytest.raises(TypeError, match="name must be"):
            actions.cleanup(cleanup, name=1)  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="must not be blank"):
            actions.cleanup(cleanup, name=" ")


async def test_registry_uses_callable_name_by_default(
    immediate_extension: _ImmediateExtensionContext,
) -> None:
    context = _create_context()

    async def named_cleanup() -> None:
        return None

    async def body(actions: DurableTerminalActions) -> None:
        actions.cleanup(named_cleanup)

    with bind_current_context(context):
        await _execute_terminal_scope(body, TerminalScopeConfig())

    assert immediate_extension.calls == [
        ("step", "named_cleanup", OperationSubType.TERMINAL_CLEANUP)
    ]


async def test_registry_rejects_body_as_terminal_action_phase() -> None:
    context = _create_context()
    with bind_current_context(context):
        actions = _DurableTerminalActions(context)
        with pytest.raises(ValidationError, match="Unsupported"):
            await actions._run(TerminalFailurePhase.BODY)


async def test_retryable_invocation_error_from_action_interrupts_terminal_pass(
    immediate_extension: _ImmediateExtensionContext,
) -> None:
    context = _create_context()
    error = InvocationError("retry action")

    async def cleanup() -> None:
        raise error

    async def body(actions: DurableTerminalActions) -> None:
        actions.cleanup(cleanup, name="cleanup")

    with bind_current_context(context), pytest.raises(InvocationError) as raised:
        await _execute_terminal_scope(body, TerminalScopeConfig())

    assert raised.value is error


async def test_retryable_invocation_error_during_cancellation_takes_precedence(
    immediate_extension: _ImmediateExtensionContext,
) -> None:
    context = _create_context()
    error = InvocationError("retry cancellation cleanup")

    async def cleanup() -> None:
        raise error

    async def body(actions: DurableTerminalActions) -> None:
        actions.cleanup(cleanup, name="cleanup")
        raise asyncio.CancelledError

    with bind_current_context(context), pytest.raises(InvocationError) as raised:
        await _execute_terminal_scope(
            body,
            TerminalScopeConfig(cleanup_on_cancellation=True),
        )

    assert raised.value is error


async def test_non_retryable_invocation_error_from_action_is_recorded(
    immediate_extension: _ImmediateExtensionContext,
) -> None:
    context = _create_context()

    class NonRetryableInvocationError(InvocationError):
        def is_retryable(self) -> bool:
            return False

    async def cleanup() -> None:
        raise NonRetryableInvocationError("terminal action")

    async def body(actions: DurableTerminalActions) -> None:
        actions.cleanup(cleanup, name="cleanup")

    with bind_current_context(context), pytest.raises(TerminalScopeError) as raised:
        await _execute_terminal_scope(body, TerminalScopeConfig())

    assert raised.value.body_failure is None
    assert raised.value.action_failures == (
        TerminalFailure(
            phase=TerminalFailurePhase.CLEANUP,
            error_type="NonRetryableInvocationError",
            message="terminal action",
            name="cleanup",
        ),
    )


async def test_cancellation_action_failure_is_attached_as_cause(
    immediate_extension: _ImmediateExtensionContext,
) -> None:
    context = _create_context()

    async def cleanup() -> None:
        msg = "cleanup failed"
        raise RuntimeError(msg)

    async def body(actions: DurableTerminalActions) -> None:
        actions.cleanup(cleanup, name="cleanup")
        raise asyncio.CancelledError

    with bind_current_context(context), pytest.raises(asyncio.CancelledError) as raised:
        await _execute_terminal_scope(
            body,
            TerminalScopeConfig(cleanup_on_cancellation=True),
        )

    assert isinstance(raised.value.__cause__, TerminalScopeError)
    assert raised.value.__cause__.body_failure is not None
    assert raised.value.__cause__.body_failure.error_type == "CancelledError"


def test_failure_from_callable_runtime_error_uses_checkpoint_details() -> None:
    failure = _failure_from_exception(
        CallableRuntimeError(
            message=None,
            error_type=None,
            data=None,
            stack_trace=["line"],
        ),
        phase=TerminalFailurePhase.CLEANUP,
        name="cleanup",
    )

    assert failure == TerminalFailure(
        phase=TerminalFailurePhase.CLEANUP,
        error_type="CallableRuntimeError",
        message="None",
        name="cleanup",
        stack_trace=("line",),
    )


def test_terminal_scope_validates_body_and_config_before_context_lookup() -> None:
    with pytest.raises(TypeError, match="body must be"):
        terminal_scope(None)  # type: ignore[arg-type]

    async def body(_actions: DurableTerminalActions) -> None:
        return None

    with pytest.raises(TypeError, match="config must be"):
        terminal_scope(body, config=object())  # type: ignore[arg-type]
