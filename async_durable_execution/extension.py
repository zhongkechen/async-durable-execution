"""Stable contracts for authoring third-party durable operations."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Generic, TypeAlias, TypeVar, cast

from ._core import (
    Duration,
    DurableContext,
    OperationIdentifier,
    OperationSubType,
    OperationSubTypeValue,
    OperationType,
    SerDes,
    ValidationError,
    create_eager_task,
    duration_to_seconds,
    get_durable_context,
)
from ._primitive.callback import Callback, _create_callback
from ._primitive.child import SummaryGenerator, _run_child_context
from ._primitive.invoke import _invoke
from ._primitive.step import (
    StepSemantics,
    _stateful_step,
    _step,
)
from ._primitive.wait import _wait

T = TypeVar("T")
P = TypeVar("P")
R = TypeVar("R")


@dataclass(frozen=True)
class ExtensionStepResult(Generic[T]):
    """Outcome returned by one attempt of a stateful extension step."""

    value: T
    retry_delay: Duration | None = None

    @classmethod
    def succeed(cls, value: T) -> ExtensionStepResult[T]:
        """Complete the extension step with ``value``."""
        return cls(value=value)

    @classmethod
    def retry(cls, state: T | None, delay: Duration) -> ExtensionStepResult[T]:
        """Checkpoint ``state`` and retry after ``delay``."""
        duration_to_seconds(delay, "retry delay")
        return cls(value=cast("T", state), retry_delay=delay)

    @property
    def is_retry(self) -> bool:
        """Return whether this outcome schedules another attempt."""
        return self.retry_delay is not None


ExtensionStepFunction: TypeAlias = Callable[
    [T | None],
    Awaitable[ExtensionStepResult[T]],
]
ExtensionStepRetryStrategy: TypeAlias = Callable[
    [Exception, T | None, int],
    ExtensionStepResult[T] | None,
]


def _normalize_operation_name(name: str | None) -> str | None:
    if name is None:
        return None
    if not isinstance(name, str):
        msg = "name must be a string or None"
        raise TypeError(msg)
    if not name.strip():
        msg = "name must not be blank"
        raise ValueError(msg)
    return name


def _normalize_sub_type(sub_type: str | OperationSubType) -> OperationSubTypeValue:
    if isinstance(sub_type, OperationSubType):
        return sub_type
    if not isinstance(sub_type, str):
        msg = "sub_type must be a string or OperationSubType"
        raise TypeError(msg)
    if not sub_type.strip():
        msg = "sub_type must not be blank"
        raise ValueError(msg)
    try:
        return OperationSubType(sub_type)
    except ValueError:
        return sub_type


class ExtensionOperation:
    """Opaque one-shot reservation for one SDK-owned durable primitive."""

    __slots__ = ("_claimed", "_context", "_identifier", "_name", "_operation_id")

    def __init__(
        self,
        context: DurableContext,
        operation_id: str,
        name: str | None,
    ) -> None:
        self._context = context
        self._operation_id = operation_id
        self._name = name
        self._claimed = False
        self._identifier: OperationIdentifier | None = None

    def step(
        self,
        func: ExtensionStepFunction[T],
        *,
        sub_type: str | OperationSubType,
        initial_state: T | None = None,
        retry_strategy: ExtensionStepRetryStrategy[T] | None = None,
        step_semantics: StepSemantics = StepSemantics.AT_LEAST_ONCE_PER_RETRY,
        serdes: SerDes[T] | None = None,
    ) -> asyncio.Task[T]:
        """Use this reservation for a stateful STEP primitive."""
        identifier = self._claim(OperationType.STEP, sub_type)
        return create_eager_task(
            lambda: _stateful_step(
                func=func,
                context=self._context,
                operation_identifier=identifier,
                initial_state=initial_state,
                retry_strategy=retry_strategy,
                step_semantics=step_semantics,
                serdes=serdes,
            )
        )

    def _run_step(
        self,
        func: Callable[[], Awaitable[T]],
        *,
        sub_type: str | OperationSubType,
        retry_strategy: Callable[[Exception, int], Duration | None] | None = None,
        step_semantics: StepSemantics = StepSemantics.AT_LEAST_ONCE_PER_RETRY,
        serdes: SerDes[T] | None = None,
    ) -> asyncio.Task[T]:
        """Use this reservation for an SDK-owned standard STEP primitive."""
        identifier = self._claim(
            OperationType.STEP,
            sub_type,
            include_operation_type=False,
        )
        return create_eager_task(
            lambda: _step(
                func=func,
                context=self._context,
                operation_identifier=identifier,
                retry_strategy=retry_strategy,
                step_semantics=step_semantics,
                serdes=serdes,
            )
        )

    def wait(
        self,
        duration: Duration,
        *,
        sub_type: str | OperationSubType,
    ) -> asyncio.Task[None]:
        """Use this reservation for a WAIT primitive."""
        seconds = duration_to_seconds(duration)
        if seconds < 1:
            msg = "duration must be at least 1 second"
            raise ValidationError(msg)
        identifier = self._claim(OperationType.WAIT, sub_type)
        return create_eager_task(
            lambda: _wait(
                seconds=seconds,
                context=self._context,
                operation_identifier=identifier,
            )
        )

    def _run_wait(
        self,
        duration: Duration,
        *,
        sub_type: str | OperationSubType,
    ) -> asyncio.Task[None]:
        """Use this reservation for an SDK-owned WAIT primitive."""
        seconds = duration_to_seconds(duration)
        if seconds < 1:
            msg = "duration must be at least 1 second"
            raise ValidationError(msg)
        identifier = self._claim(
            OperationType.WAIT,
            sub_type,
            include_operation_type=False,
        )
        return create_eager_task(
            lambda: _wait(
                seconds=seconds,
                context=self._context,
                operation_identifier=identifier,
            )
        )

    def invoke(
        self,
        function_name: str,
        payload: P,
        *,
        sub_type: str | OperationSubType,
        serdes_payload: SerDes[P] | None = None,
        serdes_result: SerDes[R] | None = None,
        tenant_id: str | None = None,
    ) -> asyncio.Task[R]:
        """Use this reservation for a CHAINED_INVOKE primitive."""
        identifier = self._claim(OperationType.CHAINED_INVOKE, sub_type)
        return create_eager_task(
            lambda: _invoke(
                function_name=function_name,
                payload=payload,
                context=self._context,
                operation_identifier=identifier,
                serdes_payload=serdes_payload,
                serdes_result=serdes_result,
                tenant_id=tenant_id,
            )
        )

    def _run_invoke(
        self,
        function_name: str,
        payload: P,
        *,
        sub_type: str | OperationSubType,
        serdes_payload: SerDes[P] | None = None,
        serdes_result: SerDes[R] | None = None,
        tenant_id: str | None = None,
    ) -> asyncio.Task[R]:
        """Use this reservation for an SDK-owned CHAINED_INVOKE primitive."""
        identifier = self._claim(
            OperationType.CHAINED_INVOKE,
            sub_type,
            include_operation_type=False,
        )
        return create_eager_task(
            lambda: _invoke(
                function_name=function_name,
                payload=payload,
                context=self._context,
                operation_identifier=identifier,
                serdes_payload=serdes_payload,
                serdes_result=serdes_result,
                tenant_id=tenant_id,
            )
        )

    def create_callback(
        self,
        *,
        sub_type: str | OperationSubType,
        timeout: Duration | None = None,
        heartbeat_timeout: Duration | None = None,
        serdes: SerDes[T] | None = None,
    ) -> asyncio.Task[Callback[T]]:
        """Use this reservation for a CALLBACK primitive."""
        identifier = self._claim(OperationType.CALLBACK, sub_type)
        return create_eager_task(
            lambda: _create_callback(
                context=self._context,
                operation_identifier=identifier,
                operation_id=self._operation_id,
                timeout=timeout,
                heartbeat_timeout=heartbeat_timeout,
                serdes=serdes,
            )
        )

    def _run_create_callback(
        self,
        *,
        sub_type: str | OperationSubType,
        timeout: Duration | None = None,
        heartbeat_timeout: Duration | None = None,
        serdes: SerDes[T] | None = None,
    ) -> asyncio.Task[Callback[T]]:
        """Use this reservation for an SDK-owned CALLBACK primitive."""
        identifier = self._claim(
            OperationType.CALLBACK,
            sub_type,
            include_operation_type=False,
        )
        return create_eager_task(
            lambda: _create_callback(
                context=self._context,
                operation_identifier=identifier,
                operation_id=self._operation_id,
                timeout=timeout,
                heartbeat_timeout=heartbeat_timeout,
                serdes=serdes,
            )
        )

    def run_in_child_context(
        self,
        func: Callable[[], Awaitable[T]],
        *,
        sub_type: str | OperationSubType,
        serdes: SerDes[T] | None = None,
        summary_generator: SummaryGenerator[T] | None = None,
        is_virtual: bool = False,
    ) -> asyncio.Task[T]:
        """Use this reservation for a CONTEXT primitive."""
        identifier = self._claim(OperationType.CONTEXT, sub_type)
        return self._create_child_context_task(
            identifier,
            func,
            serdes=serdes,
            summary_generator=summary_generator,
            is_virtual=is_virtual,
        )

    def _run_in_child_context(
        self,
        func: Callable[[], Awaitable[T]],
        *,
        sub_type: str | OperationSubType,
        serdes: SerDes[T] | None = None,
        summary_generator: SummaryGenerator[T] | None = None,
        is_virtual: bool = False,
    ) -> asyncio.Task[T]:
        """Use this reservation for an SDK-owned CONTEXT primitive."""
        identifier = self._claim(
            OperationType.CONTEXT,
            sub_type,
            include_operation_type=False,
        )
        return self._create_child_context_task(
            identifier,
            func,
            serdes=serdes,
            summary_generator=summary_generator,
            is_virtual=is_virtual,
            replay_aware=True,
        )

    def _restart_child_context(
        self,
        func: Callable[[], Awaitable[T]],
        *,
        serdes: SerDes[T] | None = None,
        summary_generator: SummaryGenerator[T] | None = None,
        is_virtual: bool = False,
    ) -> asyncio.Task[T]:
        """Re-enter an SDK-owned child operation after an in-process suspension."""
        identifier = self._identifier
        if identifier is None or identifier.operation_type is not OperationType.CONTEXT:
            msg = "Only a claimed child-context reservation can be restarted"
            raise RuntimeError(msg)
        return self._create_child_context_task(
            identifier,
            func,
            serdes=serdes,
            summary_generator=summary_generator,
            is_virtual=is_virtual,
        )

    def _create_child_context_task(
        self,
        identifier: OperationIdentifier,
        func: Callable[[], Awaitable[T]],
        *,
        serdes: SerDes[T] | None,
        summary_generator: SummaryGenerator[T] | None,
        is_virtual: bool,
        replay_aware: bool = False,
    ) -> asyncio.Task[T]:
        child_context = self._context.create_child_context(
            operation_id=self._operation_id,
            is_virtual=is_virtual,
        )

        async def run_child_context() -> T:
            if replay_aware:
                with self._context._replay_aware(
                    operation_id=self._operation_id,
                ):
                    return await _run_child_context(
                        func,
                        context=self._context,
                        child_context=child_context,
                        operation_identifier=identifier,
                        serdes=serdes,
                        summary_generator=summary_generator,
                        is_virtual=is_virtual,
                    )
            return await _run_child_context(
                func,
                context=self._context,
                child_context=child_context,
                operation_identifier=identifier,
                serdes=serdes,
                summary_generator=summary_generator,
                is_virtual=is_virtual,
            )

        return create_eager_task(run_child_context)

    def _claim(
        self,
        operation_type: OperationType,
        sub_type: str | OperationSubType,
        *,
        include_operation_type: bool = True,
    ) -> OperationIdentifier:
        self._require_active_context()
        normalized_sub_type = _normalize_sub_type(sub_type)
        if self._claimed:
            msg = "An extension operation reservation can only be used once"
            raise RuntimeError(msg)
        self._claimed = True
        self._identifier = OperationIdentifier(
            operation_id=self._operation_id,
            sub_type=normalized_sub_type,
            parent_id=self._context.parent_id,
            name=self._name,
            operation_type=operation_type if include_operation_type else None,
        )
        return self._identifier

    def _require_active_context(self) -> None:
        current_context = get_durable_context()
        if current_context is not self._context:
            msg = (
                "An extension operation reservation can only be used in the "
                "durable context where it was created"
            )
            raise RuntimeError(msg)


class ExtensionContext:
    """Stable extension-author view of the current durable execution scope."""

    __slots__ = ("_context",)

    def __init__(self, context: DurableContext) -> None:
        self._context = context

    @classmethod
    def get_current(cls) -> ExtensionContext:
        """Return the extension context for the active handler or child scope."""
        return cls(get_durable_context())

    @property
    def lambda_context(self):
        """Return the active AWS Lambda context, when available."""
        return self._context.lambda_context

    @property
    def recursive_level(self) -> int:
        """Return the durable self-invocation recursion level."""
        return self._context.recursive_level

    def is_replaying(self) -> bool:
        """Return whether the current scope is replaying checkpointed work."""
        return self._context.is_replaying()

    def reserve(
        self,
        name: str | None = None,
        *,
        local_operation_id: str | None = None,
    ) -> ExtensionOperation:
        """Reserve a stable one-shot primitive identity.

        Sequential reservations depend on deterministic reservation order.
        A caller-provided local id remains stable when reservation order changes,
        but it must be unique within the current durable context.
        """
        return self._reserve_sdk_operation(
            name,
            local_operation_id=local_operation_id,
            executes_user_code=True,
        )

    def _reserve_sdk_operation(
        self,
        name: str | None = None,
        *,
        local_operation_id: str | None = None,
        executes_user_code: bool,
    ) -> ExtensionOperation:
        """Reserve an SDK-owned primitive with its established replay transition."""
        self._require_active_context()
        normalized_name = _normalize_operation_name(name)
        operation_id = self._reserve_operation_id(local_operation_id)
        with self._context._replay_aware(
            operation_id=operation_id,
            executes_user_code=executes_user_code,
            check_next_operation=local_operation_id is None,
        ):
            pass
        return ExtensionOperation(self._context, operation_id, normalized_name)

    def _reserve_without_replay_transition(
        self,
        name: str | None = None,
        *,
        local_operation_id: str | None = None,
    ) -> ExtensionOperation:
        """Reserve an SDK-owned concurrent child without changing parent replay state."""
        self._require_active_context()
        normalized_name = _normalize_operation_name(name)
        operation_id = self._reserve_operation_id(local_operation_id)
        return ExtensionOperation(self._context, operation_id, normalized_name)

    def _reserve_operation_id(self, local_operation_id: str | None) -> str:
        if local_operation_id is None:
            return self._context.step_counter.create_step_id()
        return self._context.step_counter.create_step_id_for_local_id(
            local_operation_id
        )

    def _require_active_context(self) -> None:
        current_context = get_durable_context()
        if current_context is not self._context:
            msg = (
                "An extension context can only reserve operations in the "
                "durable context where it was created"
            )
            raise RuntimeError(msg)


def get_extension_context() -> ExtensionContext:
    """Return the stable extension-author context for the active durable scope."""
    return ExtensionContext.get_current()


__all__ = [
    "ExtensionContext",
    "ExtensionOperation",
    "ExtensionStepFunction",
    "ExtensionStepResult",
    "ExtensionStepRetryStrategy",
    "get_extension_context",
]
