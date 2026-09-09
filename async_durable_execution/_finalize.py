"""Durable terminal actions compiled into sequential journal effects."""

from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Awaitable, Callable, Protocol, TypeVar
from ._serde import SerDes

T = TypeVar("T")

from ._effects import _failure_readers, reserve, scope_effect, step_effect
from ._scope import get_durable_context
from ._types import (
    CallableRuntimeError,
    ExecutionError,
    InvocationError,
    OperationSubType,
    StepSemantics,
)


class TerminalFailurePhase(str, Enum):
    """Phase in which a terminal scope body, compensation, or cleanup failed."""

    BODY = "BODY"
    COMPENSATION = "COMPENSATION"
    CLEANUP = "CLEANUP"


@dataclass(frozen=True)
class TerminalFailure:
    """Serializable description of a body or terminal-action failure.

    Attributes:
        phase (TerminalFailurePhase): BODY, COMPENSATION, or CLEANUP.
        error_type (str): Original error type name when available.
        message (str): Failure explanation.
        name (str | None): Associated action name, when supplied.
        stack_trace (tuple[str, ...]): Recorded trace lines, when available.
    """

    phase: TerminalFailurePhase
    error_type: str
    message: str
    name: str | None = None
    stack_trace: tuple[str, ...] = ()


class TerminalScopeError(ExecutionError):
    """A terminal scope whose cleanup or compensation contains failures.

    Attributes:
        body_failure (TerminalFailure | None): Structured body failure, if any.
        action_failures (tuple[TerminalFailure, ...]): Action failures in execution
            order.
        body_error (BaseException | None): Original in-memory body error; not restored
            on replay.
    """

    def __init__(
        self, message, *, body_failure=None, action_failures=(), body_error=None
    ):
        """Describe the body failure and failed terminal actions.

        Args:
            message (str): Summary of the scope failure.
            body_failure (TerminalFailure | None): Persistable body failure details.
            action_failures (tuple[TerminalFailure, ...]): Persistable action failures.
            body_error (BaseException | None): Original exception from this invocation.
        """
        super().__init__(message)
        self.body_failure, self.action_failures, self.body_error = (
            body_failure,
            action_failures,
            body_error,
        )

    def _journal_error(self):
        return {
            "body": asdict(self.body_failure) if self.body_failure else None,
            "actions": [asdict(failure) for failure in self.action_failures],
        }


def _restore(message, data):
    def read(raw):
        return TerminalFailure(
            TerminalFailurePhase(raw["phase"]),
            raw["error_type"],
            raw["message"],
            raw.get("name"),
            tuple(raw.get("stack_trace", ())),
        )

    data = data or {}
    return TerminalScopeError(
        message,
        body_failure=read(data["body"]) if data.get("body") else None,
        action_failures=tuple(read(raw) for raw in data.get("actions", ())),
    )


_failure_readers["TerminalScopeError"] = _restore


@dataclass(frozen=True)
class TerminalScopeConfig:
    """Control whether task cancellation triggers terminal actions.

    Both flags default to False. Durable suspension and retryable invocation
    interruptions never become cancellation cleanup merely because these are set.

    Attributes:
        compensate_on_cancellation (bool): Run registered compensation on task
            cancellation.
        cleanup_on_cancellation (bool): Run registered cleanup on task cancellation.
    """

    compensate_on_cancellation: bool = False
    cleanup_on_cancellation: bool = False

    def __post_init__(self):
        if (
            type(self.compensate_on_cancellation) is not bool
            or type(self.cleanup_on_cancellation) is not bool
        ):
            raise TypeError("Cancellation policies must be booleans")


class DurableTerminalActions(Protocol):
    """Registry passed to a terminal_scope body for durable terminal actions.

    Register actions directly in the owning scope while its body is active.
    Registrations must be deterministic on replay. Compensation runs before cleanup;
    each phase executes actions sequentially in reverse registration order.
    """

    def cleanup(
        self,
        func,
        *,
        name=None,
        retry_strategy=None,
        step_semantics=StepSemantics.AT_LEAST_ONCE_PER_RETRY,
    ):
        """Register an action for logical success or failure of the owning scope.

        Args:
            func (Callable): Bound zero-argument async cleanup action, executed as a
                step.
            name (str | None): Optional stable action name.
            retry_strategy (Callable | None): Exception/attempt retry policy for the
                action.
            step_semantics (StepSemantics): Interruption policy for its step attempts.

        Returns:
            (None): Registration does not execute the action.
        """
        ...

    def compensate(
        self,
        func,
        *,
        name=None,
        retry_strategy=None,
        step_semantics=StepSemantics.AT_LEAST_ONCE_PER_RETRY,
    ):
        """Register an action for logical failure of the owning scope.

        Register after the durable effect the action should compensate has completed.

        Args:
            func (Callable): Bound zero-argument async compensation, executed as a step.
            name (str | None): Optional stable action name.
            retry_strategy (Callable | None): Exception/attempt retry policy for the
                action.
            step_semantics (StepSemantics): Interruption policy for its step attempts.

        Returns:
            (None): Registration does not execute the action.
        """
        ...


class Actions:
    """Action registry supplied to a terminal_scope body; implements
    DurableTerminalActions.
    """

    def __init__(self):
        self.owner = get_durable_context()
        self.open = True
        self.program: dict[TerminalFailurePhase, list[tuple[Any, Any, Any, Any]]] = {
            TerminalFailurePhase.COMPENSATION: [],
            TerminalFailurePhase.CLEANUP: [],
        }

    def _append(self, phase, func, name, retry_strategy, step_semantics):
        if not self.open or get_durable_context() is not self.owner:
            raise RuntimeError(
                "Terminal actions must be registered in their active owning body"
            )
        if not callable(func):
            raise TypeError("Terminal actions require an async callable")
        if name is not None and (not isinstance(name, str) or not name.strip()):
            raise ValueError("Terminal action names must be nonblank strings")
        self.program[phase].append(
            (
                func,
                name or getattr(func, "__name__", None),
                retry_strategy,
                step_semantics,
            )
        )

    def cleanup(
        self,
        func,
        *,
        name=None,
        retry_strategy=None,
        step_semantics=StepSemantics.AT_LEAST_ONCE_PER_RETRY,
    ):
        """Register an action for logical success or failure of the owning scope.

        Args:
            func (Callable): Bound zero-argument async cleanup action, executed as a
                step.
            name (str | None): Optional stable action name.
            retry_strategy (Callable | None): Exception/attempt retry policy for the
                action.
            step_semantics (StepSemantics): Interruption policy for its step attempts.

        Returns:
            (None): Registration does not execute the action.
        """
        self._append(
            TerminalFailurePhase.CLEANUP, func, name, retry_strategy, step_semantics
        )

    def compensate(
        self,
        func,
        *,
        name=None,
        retry_strategy=None,
        step_semantics=StepSemantics.AT_LEAST_ONCE_PER_RETRY,
    ):
        """Register an action for logical failure of the owning scope.

        Register after the durable effect the action should compensate has completed.

        Args:
            func (Callable): Bound zero-argument async compensation, executed as a step.
            name (str | None): Optional stable action name.
            retry_strategy (Callable | None): Exception/attempt retry policy for the
                action.
            step_semantics (StepSemantics): Interruption policy for its step attempts.

        Returns:
            (None): Registration does not execute the action.
        """
        self._append(
            TerminalFailurePhase.COMPENSATION,
            func,
            name,
            retry_strategy,
            step_semantics,
        )

    async def run(self, phases):
        failures = []
        for phase in phases:
            subtype = (
                OperationSubType.TERMINAL_CLEANUP
                if phase is TerminalFailurePhase.CLEANUP
                else OperationSubType.TERMINAL_COMPENSATION
            )
            for func, name, policy, semantics in reversed(self.program[phase]):
                try:
                    ticket = reserve("STEP", subtype, name)
                    await ticket.spawn(
                        step_effect(
                            ticket,
                            func,
                            retry_strategy=policy,
                            step_semantics=semantics,
                        )
                    )
                except InvocationError:
                    raise
                except Exception as error:
                    failures.append(describe(error, phase, name))
        return tuple(failures)


def describe(error, phase, name=None):
    return TerminalFailure(
        phase,
        error.error_type
        if isinstance(error, CallableRuntimeError) and error.error_type
        else type(error).__name__,
        str(error),
        name,
        tuple(getattr(error, "stack_trace", ()) or ()),
    )


def terminal_scope(
    body: Callable[[DurableTerminalActions], Awaitable[T]],
    *,
    name: str | None = None,
    config: TerminalScopeConfig | None = None,
    serdes: SerDes[T] | None = None,
    summary_generator: Callable[[T], str] | None = None,
) -> asyncio.Task[T]:
    """Run a durable body with cleanup and failure-only compensation.

    The body receives a registry for deterministic action registration. Success
    runs cleanup; logical failure runs compensation and then cleanup. Each phase
    runs in reverse registration order and attempts remaining ordinary actions
    after an action fails. Durable suspension and retryable invocation errors skip
    terminal actions. Cancellation behavior is controlled by config.

    Result serialization and oversized-result preparation precede success cleanup,
    so a permanent preparation failure follows the compensation path.

    Args:
        body: Async callable accepting a DurableTerminalActions registry.
        name: Stable scope name; defaults to the body's name when available.
        config: Cancellation policy; defaults to skipping both action phases on
            cancellation.
        serdes: Codec for the body result.
        summary_generator: Optional oversized-result preparation callback.

    Returns:
        (asyncio.Task[T]): Body result after successful cleanup and durable completion.

    Raises:
        TerminalScopeError: One or more compensation or cleanup actions fail.
        TypeError: The body is not callable or config has the wrong type.
    """
    if not callable(body):
        raise TypeError("terminal_scope requires an async body")
    policy = config or TerminalScopeConfig()
    if not isinstance(policy, TerminalScopeConfig):
        raise TypeError("config must be a TerminalScopeConfig")
    registry: Actions | None = None

    async def failed(error):
        if isinstance(error, InvocationError) and error.is_retryable():
            raise error
        assert registry is not None
        failures = await registry.run(
            (TerminalFailurePhase.COMPENSATION, TerminalFailurePhase.CLEANUP)
        )
        if failures:
            raise TerminalScopeError(
                f"Body and {len(failures)} terminal action(s) failed",
                body_failure=describe(error, TerminalFailurePhase.BODY),
                action_failures=failures,
                body_error=error,
            ) from error

    async def run():
        nonlocal registry
        registry = Actions()
        try:
            try:
                return await body(registry)
            finally:
                registry.open = False
        except asyncio.CancelledError as error:
            phases = []
            if policy.compensate_on_cancellation:
                phases.append(TerminalFailurePhase.COMPENSATION)
            if policy.cleanup_on_cancellation:
                phases.append(TerminalFailurePhase.CLEANUP)
            failures = await registry.run(phases)
            if failures:
                raise error from TerminalScopeError(
                    "Cancellation actions failed",
                    body_failure=describe(error, TerminalFailurePhase.BODY),
                    action_failures=failures,
                    body_error=error,
                )
            raise
        except Exception as error:
            await failed(error)
            raise

    async def cleanup():
        assert registry is not None
        failures = await registry.run((TerminalFailurePhase.CLEANUP,))
        if failures:
            raise TerminalScopeError(
                f"{len(failures)} cleanup action(s) failed", action_failures=failures
            )

    ticket = reserve(
        "CONTEXT",
        OperationSubType.TERMINAL_SCOPE,
        name or getattr(body, "__name__", None),
    )
    return ticket.spawn(
        scope_effect(
            ticket,
            run,
            serdes=serdes,
            summary_generator=summary_generator,
            prepare_failure=failed,
            before_commit=cleanup,
        )
    )
