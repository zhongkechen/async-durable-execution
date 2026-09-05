"""Durable terminal scopes for cleanup and compensation."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum
from typing import Any, NoReturn, Protocol, TypeVar

from .._core import (
    CallableRuntimeError,
    Duration,
    DurableContext,
    ExecutionError,
    InvocationError,
    OperationSubType,
    SerDes,
    TerminationReason,
    ValidationError,
    _register_sdk_control_error_type,
    get_durable_context,
)
from .._primitive.child import SummaryGenerator
from .._primitive.step import StepSemantics
from ..extension import get_extension_context


T = TypeVar("T")
_TERMINAL_SCOPE_ERROR_PAYLOAD_VERSION = 1


class TerminalFailurePhase(str, Enum):
    """Phase in which a terminal-scope failure occurred."""

    BODY = "BODY"
    COMPENSATION = "COMPENSATION"
    CLEANUP = "CLEANUP"


@dataclass(frozen=True)
class TerminalFailure:
    """Serializable details for one terminal-scope failure."""

    phase: TerminalFailurePhase
    error_type: str
    message: str
    name: str | None = None
    stack_trace: tuple[str, ...] = ()

    def _to_dict(self) -> dict[str, Any]:
        return {
            "phase": self.phase.value,
            "errorType": self.error_type,
            "message": self.message,
            "name": self.name,
            "stackTrace": list(self.stack_trace),
        }

    @classmethod
    def _from_dict(cls, value: Any) -> TerminalFailure:
        if not isinstance(value, dict):
            msg = "Terminal failure payload must be an object."
            raise ValueError(msg)

        phase = TerminalFailurePhase(value["phase"])
        error_type = value["errorType"]
        message = value["message"]
        name = value.get("name")
        stack_trace = value.get("stackTrace", [])
        if not isinstance(error_type, str) or not isinstance(message, str):
            msg = "Terminal failure type and message must be strings."
            raise ValueError(msg)
        if name is not None and not isinstance(name, str):
            msg = "Terminal failure name must be a string or null."
            raise ValueError(msg)
        if not isinstance(stack_trace, list) or not all(
            isinstance(line, str) for line in stack_trace
        ):
            msg = "Terminal failure stackTrace must be a list of strings."
            raise ValueError(msg)
        return cls(
            phase=phase,
            error_type=error_type,
            message=message,
            name=name,
            stack_trace=tuple(stack_trace),
        )


class TerminalScopeError(ExecutionError):
    """Terminal-scope failure with preserved body and action details."""

    def __init__(
        self,
        message: str,
        *,
        body_failure: TerminalFailure | None = None,
        action_failures: tuple[TerminalFailure, ...] = (),
        body_error: BaseException | None = None,
    ) -> None:
        super().__init__(message, TerminationReason.UNHANDLED_ERROR)
        self.body_failure = body_failure
        self.action_failures = action_failures
        self.body_error = body_error


def _encode_terminal_scope_error_payload(error: ExecutionError) -> str | None:
    if not isinstance(error, TerminalScopeError):
        msg = "TerminalScopeError codec received an incompatible exception."
        raise TypeError(msg)
    return json.dumps(
        {
            "version": _TERMINAL_SCOPE_ERROR_PAYLOAD_VERSION,
            "bodyFailure": (
                error.body_failure._to_dict()
                if error.body_failure is not None
                else None
            ),
            "actionFailures": [failure._to_dict() for failure in error.action_failures],
        },
        separators=(",", ":"),
        sort_keys=True,
    )


def _restore_terminal_scope_error(
    message: str,
    payload: str | None,
) -> TerminalScopeError:
    body_failure: TerminalFailure | None = None
    action_failures: tuple[TerminalFailure, ...] = ()
    try:
        decoded = json.loads(payload) if payload is not None else None
        if (
            isinstance(decoded, dict)
            and decoded.get("version") == _TERMINAL_SCOPE_ERROR_PAYLOAD_VERSION
        ):
            encoded_body = decoded.get("bodyFailure")
            if encoded_body is not None:
                body_failure = TerminalFailure._from_dict(encoded_body)
            encoded_actions = decoded.get("actionFailures", [])
            if not isinstance(encoded_actions, list):
                raise ValueError
            action_failures = tuple(
                TerminalFailure._from_dict(failure) for failure in encoded_actions
            )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        body_failure = None
        action_failures = ()

    return TerminalScopeError(
        message,
        body_failure=body_failure,
        action_failures=action_failures,
    )


_register_sdk_control_error_type(
    TerminalScopeError,
    encode_payload=_encode_terminal_scope_error_payload,
    restore=_restore_terminal_scope_error,
)


@dataclass(frozen=True)
class TerminalScopeConfig:
    """Policy for cancellation that reaches a terminal scope.

    Cancellation is skipped by default because map, parallel, and flow use
    ``asyncio`` cancellation to abandon unfinished branches after an aggregate
    operation reaches an early terminal result.
    """

    compensate_on_cancellation: bool = False
    cleanup_on_cancellation: bool = False

    def __post_init__(self) -> None:
        if type(self.compensate_on_cancellation) is not bool:
            msg = "compensate_on_cancellation must be a bool"
            raise TypeError(msg)
        if type(self.cleanup_on_cancellation) is not bool:
            msg = "cleanup_on_cancellation must be a bool"
            raise TypeError(msg)


@dataclass(frozen=True)
class _TerminalAction:
    func: Callable[[], Awaitable[None]]
    name: str | None
    retry_strategy: Callable[[Exception, int], Duration | None] | None
    step_semantics: StepSemantics


class DurableTerminalActions(Protocol):
    """Registration interface supplied to a terminal-scope body.

    Registrations are deterministic declarations. The scope reconstructs them
    during replay and runs them only after a logical terminal outcome.
    """

    def cleanup(
        self,
        func: Callable[[], Awaitable[None]],
        *,
        name: str | None = None,
        retry_strategy: Callable[[Exception, int], Duration | None] | None = None,
        step_semantics: StepSemantics = StepSemantics.AT_LEAST_ONCE_PER_RETRY,
    ) -> None:
        """Register an unconditional action for logical scope completion."""
        ...

    def compensate(
        self,
        func: Callable[[], Awaitable[None]],
        *,
        name: str | None = None,
        retry_strategy: Callable[[Exception, int], Duration | None] | None = None,
        step_semantics: StepSemantics = StepSemantics.AT_LEAST_ONCE_PER_RETRY,
    ) -> None:
        """Register a failure-only compensation action."""
        ...


class _DurableTerminalActions:
    """Runtime implementation of the terminal-action registry."""

    def __init__(self, context: DurableContext) -> None:
        self._context = context
        self._compensations: list[_TerminalAction] = []
        self._cleanups: list[_TerminalAction] = []
        self._accepting = True

    def cleanup(
        self,
        func: Callable[[], Awaitable[None]],
        *,
        name: str | None = None,
        retry_strategy: Callable[[Exception, int], Duration | None] | None = None,
        step_semantics: StepSemantics = StepSemantics.AT_LEAST_ONCE_PER_RETRY,
    ) -> None:
        """Register an unconditional action for logical scope completion."""
        self._register(
            self._cleanups,
            func,
            name=name,
            retry_strategy=retry_strategy,
            step_semantics=step_semantics,
        )

    def compensate(
        self,
        func: Callable[[], Awaitable[None]],
        *,
        name: str | None = None,
        retry_strategy: Callable[[Exception, int], Duration | None] | None = None,
        step_semantics: StepSemantics = StepSemantics.AT_LEAST_ONCE_PER_RETRY,
    ) -> None:
        """Register a failure-only compensation action."""
        self._register(
            self._compensations,
            func,
            name=name,
            retry_strategy=retry_strategy,
            step_semantics=step_semantics,
        )

    def _register(
        self,
        registrations: list[_TerminalAction],
        func: Callable[[], Awaitable[None]],
        *,
        name: str | None,
        retry_strategy: Callable[[Exception, int], Duration | None] | None,
        step_semantics: StepSemantics,
    ) -> None:
        if not self._accepting:
            msg = "Terminal actions can only be registered while the scope body runs."
            raise RuntimeError(msg)
        if get_durable_context() is not self._context:
            msg = (
                "Terminal actions must be registered directly in the terminal "
                "scope where the registry was created."
            )
            raise RuntimeError(msg)
        if not callable(func):
            msg = "terminal action must be an async zero-argument callable"
            raise TypeError(msg)
        if not isinstance(step_semantics, StepSemantics):
            msg = "step_semantics must be a StepSemantics value"
            raise TypeError(msg)

        action_name = name if name is not None else getattr(func, "__name__", None)
        if action_name is not None:
            if not isinstance(action_name, str):
                msg = "name must be a string or None"
                raise TypeError(msg)
            if not action_name.strip():
                msg = "name must not be blank"
                raise ValueError(msg)

        registrations.append(
            _TerminalAction(
                func=func,
                name=action_name,
                retry_strategy=retry_strategy,
                step_semantics=step_semantics,
            )
        )

    def _close(self) -> None:
        self._accepting = False

    async def _run(
        self,
        phase: TerminalFailurePhase,
    ) -> list[TerminalFailure]:
        if phase is TerminalFailurePhase.COMPENSATION:
            actions = self._compensations
            sub_type = OperationSubType.TERMINAL_COMPENSATION
        elif phase is TerminalFailurePhase.CLEANUP:
            actions = self._cleanups
            sub_type = OperationSubType.TERMINAL_CLEANUP
        else:
            msg = f"Unsupported terminal action phase: {phase.value}"
            raise ValidationError(msg)

        failures: list[TerminalFailure] = []
        for action in reversed(actions):
            try:
                await (
                    get_extension_context()
                    ._reserve_sdk_operation(action.name)  # noqa: SLF001
                    ._run_step(  # noqa: SLF001
                        action.func,
                        sub_type=sub_type,
                        retry_strategy=action.retry_strategy,
                        step_semantics=action.step_semantics,
                    )
                )
            except InvocationError as error:
                if error.is_retryable():
                    # Checkpoint/SerDes interruptions take precedence over an
                    # in-flight asyncio cancellation. Lambda must retry before
                    # the scope can claim a durable terminal outcome.
                    raise
                failures.append(
                    _failure_from_exception(
                        error,
                        phase=phase,
                        name=action.name,
                    )
                )
            except Exception as error:  # noqa: BLE001
                failures.append(
                    _failure_from_exception(
                        error,
                        phase=phase,
                        name=action.name,
                    )
                )
        return failures


def _failure_from_exception(
    error: BaseException,
    *,
    phase: TerminalFailurePhase,
    name: str | None = None,
) -> TerminalFailure:
    if isinstance(error, CallableRuntimeError):
        error_type = error.error_type or type(error).__name__
        message = error.message or str(error)
        stack_trace = tuple(error.stack_trace or ())
    else:
        error_type = type(error).__name__
        message = str(error)
        stack_trace = ()
    return TerminalFailure(
        phase=phase,
        error_type=error_type,
        message=message,
        name=name,
        stack_trace=stack_trace,
    )


async def _run_terminal_actions(
    actions: _DurableTerminalActions,
    *,
    compensate: bool,
    cleanup: bool,
) -> tuple[TerminalFailure, ...]:
    failures: list[TerminalFailure] = []
    if compensate:
        failures.extend(await actions._run(TerminalFailurePhase.COMPENSATION))
    if cleanup:
        failures.extend(await actions._run(TerminalFailurePhase.CLEANUP))
    return tuple(failures)


def _terminal_scope_error_message(
    body_failure: TerminalFailure | None,
    action_failures: tuple[TerminalFailure, ...],
) -> str:
    if body_failure is None:
        prefix = "Terminal scope completed, but terminal actions failed"
    else:
        prefix = (
            "Terminal scope body failed with "
            f"{body_failure.error_type}: {body_failure.message}"
        )
    count = len(action_failures)
    suffix = "terminal action failed" if count == 1 else "terminal actions failed"
    return f"{prefix}; {count} {suffix}."


class _TerminalScopeLifecycle:
    """Coordinate body, result preparation, and terminal action phases."""

    def __init__(self, config: TerminalScopeConfig) -> None:
        self.config = config
        self.actions: _DurableTerminalActions | None = None

    def _require_actions(self) -> _DurableTerminalActions:
        if self.actions is None:
            msg = "Terminal scope body has not initialized its action registry."
            raise RuntimeError(msg)
        return self.actions

    async def _raise_cancellation(
        self,
        cancellation: asyncio.CancelledError,
    ) -> NoReturn:
        actions = self._require_actions()
        failures = await _run_terminal_actions(
            actions,
            compensate=self.config.compensate_on_cancellation,
            cleanup=self.config.cleanup_on_cancellation,
        )
        if failures:
            cancellation_failure = _failure_from_exception(
                cancellation,
                phase=TerminalFailurePhase.BODY,
            )
            terminal_error = TerminalScopeError(
                _terminal_scope_error_message(cancellation_failure, failures),
                body_failure=cancellation_failure,
                action_failures=failures,
                body_error=cancellation,
            )
            raise cancellation from terminal_error
        raise cancellation

    async def run_body(
        self,
        body: Callable[[DurableTerminalActions], Awaitable[T]],
    ) -> T:
        actions = _DurableTerminalActions(get_durable_context())
        self.actions = actions
        try:
            result = await body(actions)
        except asyncio.CancelledError as cancellation:
            actions._close()
            await self._raise_cancellation(cancellation)
        except Exception as body_error:
            actions._close()
            if isinstance(body_error, InvocationError) and body_error.is_retryable():
                raise

            failures = await _run_terminal_actions(
                actions,
                compensate=True,
                cleanup=True,
            )
            if failures:
                body_failure = _failure_from_exception(
                    body_error,
                    phase=TerminalFailurePhase.BODY,
                )
                raise TerminalScopeError(
                    _terminal_scope_error_message(body_failure, failures),
                    body_failure=body_failure,
                    action_failures=failures,
                    body_error=body_error,
                ) from body_error
            raise
        else:
            actions._close()
            return result

    async def before_result_checkpoint(self, _result: object) -> None:
        actions = self._require_actions()
        failures = await _run_terminal_actions(
            actions,
            compensate=False,
            cleanup=True,
        )
        if failures:
            raise TerminalScopeError(
                _terminal_scope_error_message(None, failures),
                action_failures=failures,
            )

    async def on_result_preparation_error(self, error: BaseException) -> None:
        actions = self._require_actions()
        if isinstance(error, asyncio.CancelledError):
            await self._raise_cancellation(error)
        elif isinstance(error, InvocationError) and error.is_retryable():
            raise error

        failures = await _run_terminal_actions(
            actions,
            compensate=True,
            cleanup=True,
        )
        if failures:
            body_failure = _failure_from_exception(
                error,
                phase=TerminalFailurePhase.BODY,
            )
            raise TerminalScopeError(
                _terminal_scope_error_message(body_failure, failures),
                body_failure=body_failure,
                action_failures=failures,
                body_error=error,
            ) from error
        raise error


async def _execute_terminal_scope(
    body: Callable[[DurableTerminalActions], Awaitable[T]],
    config: TerminalScopeConfig,
) -> T:
    """Execute a complete terminal lifecycle without a child result boundary."""
    lifecycle = _TerminalScopeLifecycle(config)
    result = await lifecycle.run_body(body)
    await lifecycle.before_result_checkpoint(result)
    return result


def terminal_scope(
    body: Callable[[DurableTerminalActions], Awaitable[T]],
    *,
    name: str | None = None,
    config: TerminalScopeConfig | None = None,
    serdes: SerDes[T] | None = None,
    summary_generator: SummaryGenerator[T] | None = None,
) -> asyncio.Task[T]:
    """Run a durable scope with logical-terminal cleanup and compensation.

    Suspension is a non-terminal outcome: registered actions do not run when
    the current invocation unwinds for a wait, callback, retry, or replay
    boundary.
    """
    if not callable(body):
        msg = "body must be an async callable accepting DurableTerminalActions"
        raise TypeError(msg)
    if config is not None and not isinstance(config, TerminalScopeConfig):
        msg = "config must be a TerminalScopeConfig or None"
        raise TypeError(msg)

    scope_name = name if name is not None else getattr(body, "__name__", None)
    active_config = config or TerminalScopeConfig()
    lifecycle = _TerminalScopeLifecycle(active_config)

    async def run_scope() -> T:
        return await lifecycle.run_body(body)

    return (
        get_extension_context()
        ._reserve_sdk_operation(scope_name)  # noqa: SLF001
        ._run_in_child_context(  # noqa: SLF001
            run_scope,
            sub_type=OperationSubType.TERMINAL_SCOPE,
            serdes=serdes,
            summary_generator=summary_generator,
            before_result_checkpoint=lifecycle.before_result_checkpoint,
            on_result_preparation_error=lifecycle.on_result_preparation_error,
            deserialize_result_before_checkpoint=True,
        )
    )


__all__ = [
    "DurableTerminalActions",
    "TerminalFailure",
    "TerminalFailurePhase",
    "TerminalScopeConfig",
    "TerminalScopeError",
    "terminal_scope",
]
