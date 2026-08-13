"""Stable contracts for authoring third-party durable operations."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Generic, TypeAlias, TypeVar, cast

from ._core import (
    CallableRuntimeError,
    Duration,
    DurableContext,
    ErrorObject,
    ExecutionError,
    ExecutionState,
    Operation,
    OperationIdentifier,
    OperationStatus,
    OperationSubType,
    OperationSubTypeValue,
    OperationType,
    OperationUpdate,
    SerDes,
    ValidationError,
    bind_current_context,
    create_eager_task,
    duration_to_seconds,
    get_durable_context,
    suspend_with_optional_resume_delay,
    suspend_with_optional_resume_timestamp,
)
from ._primitive.base import OperationExecutor
from ._primitive.callback import Callback, _create_callback
from ._primitive.child import SummaryGenerator, _run_child_context
from ._primitive.invoke import _invoke
from ._primitive.step import (
    StepContext,
    StepInterruptedError,
    StepSemantics,
    _error_object_from_exception,
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
    def retry(cls, state: T, delay: Duration) -> ExtensionStepResult[T]:
        """Checkpoint ``state`` and retry after ``delay``."""
        duration_to_seconds(delay, "retry delay")
        return cls(value=state, retry_delay=delay)

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


class _ExtensionStepOperationExecutor(OperationExecutor[T]):
    def __init__(
        self,
        func: ExtensionStepFunction[T],
        state: ExecutionState,
        operation_identifier: OperationIdentifier,
        *,
        initial_state: T | None,
        retry_strategy: ExtensionStepRetryStrategy[T] | None,
        step_semantics: StepSemantics,
        serdes: SerDes[T] | None,
    ) -> None:
        super().__init__(state=state, operation_identifier=operation_identifier)
        self.func = func
        self.initial_state = initial_state
        self.retry_strategy = retry_strategy
        self.step_semantics = step_semantics
        self.serdes = serdes

    async def start(self) -> T:
        start = OperationUpdate.create_step_start(self.operation_identifier)
        await self.create_checkpoint(
            start,
            is_sync=self.step_semantics is StepSemantics.AT_MOST_ONCE_PER_RETRY,
        )
        return await self._execute(None)

    async def replay(self, operation: Operation) -> T:
        if operation.status is OperationStatus.SUCCEEDED:
            payload = operation.step_details.result if operation.step_details else None
            if payload is None:
                return cast("T", None)
            return await self.deserialize_value(payload, self.serdes)

        if operation.status is OperationStatus.FAILED:
            self._raise_failed_operation(operation)

        if operation.status is OperationStatus.PENDING:
            resume_at = (
                operation.step_details.next_attempt_timestamp
                if operation.step_details
                else None
            )
            suspend_with_optional_resume_timestamp(
                msg=f"Extension step {self.operation_name or self.operation_id} is pending",
                datetime_timestamp=resume_at,
            )

        if (
            operation.status is OperationStatus.STARTED
            and self.step_semantics is StepSemantics.AT_MOST_ONCE_PER_RETRY
        ):
            msg = (
                f"Extension step operation_id={self.operation_id} "
                f"name={self.operation_name} was previously interrupted"
            )
            state = await self._load_state(operation)
            attempt = self._attempt(operation)
            return await self._handle_failure(
                StepInterruptedError(msg, self.operation_id),
                state,
                attempt,
            )

        if operation.status is OperationStatus.READY:
            start = OperationUpdate.create_step_start(self.operation_identifier)
            await self.create_checkpoint(
                start,
                is_sync=self.step_semantics is StepSemantics.AT_MOST_ONCE_PER_RETRY,
            )

        return await self._execute(operation)

    async def _execute(self, operation: Operation | None) -> T:
        state = await self._load_state(operation)
        attempt = self._attempt(operation)
        step_context = StepContext(
            attempt=attempt,
            execution_state=self.state,
            operation_identifier=self.operation_identifier,
        )
        try:
            with bind_current_context(step_context):
                outcome = await self.func(state)
        except Exception as error:
            if isinstance(error, ExecutionError):
                raise
            return await self._handle_failure(error, state, attempt)

        if not isinstance(outcome, ExtensionStepResult):
            msg = (
                "Extension step functions must return "
                "ExtensionStepResult.succeed(...) or ExtensionStepResult.retry(...)"
            )
            return await self._fail(TypeError(msg))

        if outcome.is_retry:
            return await self._schedule_retry(outcome)

        payload = await self.serialize_value(outcome.value, self.serdes)
        await self.create_checkpoint(
            OperationUpdate.create_step_succeed(
                self.operation_identifier,
                payload,
            )
        )
        return await self.deserialize_value(payload, self.serdes)

    async def _load_state(self, operation: Operation | None) -> T | None:
        if (
            operation is not None
            and operation.step_details is not None
            and operation.step_details.result is not None
        ):
            return await self.deserialize_value(
                operation.step_details.result,
                self.serdes,
            )
        return self.initial_state

    @staticmethod
    def _attempt(operation: Operation | None) -> int:
        if operation is None or operation.step_details is None:
            return 1
        return operation.step_details.attempt + 1

    async def _handle_failure(
        self,
        error: Exception,
        state: T | None,
        attempt: int,
    ) -> T:
        if self.retry_strategy is None:
            return await self._fail(error)

        try:
            decision = self.retry_strategy(error, state, attempt)
        except Exception as retry_error:
            return await self._fail(retry_error)

        if decision is None:
            return await self._fail(error)
        if not isinstance(decision, ExtensionStepResult) or not decision.is_retry:
            msg = (
                "Extension step retry_strategy must return "
                "ExtensionStepResult.retry(...) or None"
            )
            return await self._fail(TypeError(msg))

        return await self._schedule_retry(decision)

    async def _schedule_retry(
        self,
        outcome: ExtensionStepResult[T],
    ) -> T:
        assert outcome.retry_delay is not None
        delay_seconds = max(
            1,
            duration_to_seconds(outcome.retry_delay, "retry delay"),
        )
        payload = await self.serialize_value(outcome.value, self.serdes)
        await self.create_checkpoint(
            OperationUpdate.create_step_retry(
                self.operation_identifier,
                next_attempt_delay_seconds=delay_seconds,
                payload=payload,
                error=None,
            )
        )
        suspend_with_optional_resume_delay(
            msg=f"Extension step {self.operation_id} will retry",
            delay_seconds=delay_seconds,
        )
        raise AssertionError("suspend_with_optional_resume_delay must raise")

    async def _fail(self, error: Exception) -> T:
        error_object = _error_object_from_exception(
            error,
            invocation_retryable=False,
        )
        await self.create_checkpoint(
            OperationUpdate.create_step_fail(
                self.operation_identifier,
                error_object,
            )
        )
        if isinstance(error, StepInterruptedError):
            raise error
        raise CallableRuntimeError.from_error_object(error_object)

    @staticmethod
    def _raise_failed_operation(operation: Operation) -> None:
        error = operation.step_details.error if operation.step_details else None
        if error is None:
            error = ErrorObject.from_message(
                "Unknown error. No ErrorObject exists on the checkpoint operation."
            )
        raise CallableRuntimeError.from_error_object(error)


class ExtensionOperation:
    """Opaque one-shot reservation for one SDK-owned durable primitive."""

    __slots__ = ("_claimed", "_context", "_name", "_operation_id")

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
            lambda: _ExtensionStepOperationExecutor(
                func=func,
                state=self._context.execution_state,
                operation_identifier=identifier,
                initial_state=initial_state,
                retry_strategy=retry_strategy,
                step_semantics=step_semantics,
                serdes=serdes,
            ).process()
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
        child_context = self._context.create_child_context(
            operation_id=self._operation_id,
            is_virtual=is_virtual,
        )
        return create_eager_task(
            lambda: _run_child_context(
                func,
                context=self._context,
                child_context=child_context,
                operation_identifier=identifier,
                serdes=serdes,
                summary_generator=summary_generator,
                is_virtual=is_virtual,
            )
        )

    def _claim(
        self,
        operation_type: OperationType,
        sub_type: str | OperationSubType,
    ) -> OperationIdentifier:
        normalized_sub_type = _normalize_sub_type(sub_type)
        if self._claimed:
            msg = "An extension operation reservation can only be used once"
            raise RuntimeError(msg)
        self._claimed = True
        return OperationIdentifier(
            operation_id=self._operation_id,
            sub_type=normalized_sub_type,
            parent_id=self._context.parent_id,
            name=self._name,
            operation_type=operation_type,
        )


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
        with self._context._replay_aware(executes_user_code=True):
            if local_operation_id is None:
                operation_id = self._context.step_counter.create_step_id()
            else:
                operation_id = self._context.step_counter.create_step_id_for_local_id(
                    local_operation_id
                )
        return ExtensionOperation(self._context, operation_id, name)


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
