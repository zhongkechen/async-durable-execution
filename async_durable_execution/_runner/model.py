"""Shared runner models."""

from __future__ import annotations

import datetime
import json
import logging
from dataclasses import dataclass, field, replace
from enum import Enum
from typing import Any, Protocol, TYPE_CHECKING

from .._core import (
    BotoSerializableModel,
    CallbackDetails,
    CallbackOptions,
    ChainedInvokeDetails,
    ContextDetails,
    DurableExecutionInvocationOutput,
    ErrorObject,
    ExecutionDetails,
    ExtendedTypeSerDes,
    InvocationStatus,
    Operation,
    OperationAction,
    OperationPayload,
    OperationStatus,
    OperationSubType,
    OperationType,
    OperationUpdate,
    StepDetails,
    WaitDetails,
)
from .exceptions import (
    DurableFunctionsTestError,
    InvalidParameterValueException,
)

if TYPE_CHECKING:
    from .local.model import StartDurableExecutionInput


logger = logging.getLogger(__name__)


class EventType(Enum):
    """Event types for durable execution events."""

    EXECUTION_STARTED = "ExecutionStarted"
    EXECUTION_SUCCEEDED = "ExecutionSucceeded"
    EXECUTION_FAILED = "ExecutionFailed"
    EXECUTION_TIMED_OUT = "ExecutionTimedOut"
    EXECUTION_STOPPED = "ExecutionStopped"
    CONTEXT_STARTED = "ContextStarted"
    CONTEXT_SUCCEEDED = "ContextSucceeded"
    CONTEXT_FAILED = "ContextFailed"
    WAIT_STARTED = "WaitStarted"
    WAIT_SUCCEEDED = "WaitSucceeded"
    WAIT_CANCELLED = "WaitCancelled"
    STEP_STARTED = "StepStarted"
    STEP_SUCCEEDED = "StepSucceeded"
    STEP_FAILED = "StepFailed"
    CHAINED_INVOKE_STARTED = "ChainedInvokeStarted"
    CHAINED_INVOKE_SUCCEEDED = "ChainedInvokeSucceeded"
    CHAINED_INVOKE_FAILED = "ChainedInvokeFailed"
    CHAINED_INVOKE_TIMED_OUT = "ChainedInvokeTimedOut"
    CHAINED_INVOKE_STOPPED = "ChainedInvokeStopped"
    CALLBACK_STARTED = "CallbackStarted"
    CALLBACK_SUCCEEDED = "CallbackSucceeded"
    CALLBACK_FAILED = "CallbackFailed"
    CALLBACK_TIMED_OUT = "CallbackTimedOut"
    INVOCATION_COMPLETED = "InvocationCompleted"


TERMINAL_STATUSES: set[OperationStatus] = {
    OperationStatus.SUCCEEDED,
    OperationStatus.FAILED,
    OperationStatus.TIMED_OUT,
    OperationStatus.STOPPED,
    OperationStatus.CANCELLED,
}


@dataclass(frozen=True)
class GetDurableExecutionResponse(BotoSerializableModel):
    """Response containing durable execution details."""

    durable_execution_arn: str = field(metadata={"alias": "DurableExecutionArn"})
    durable_execution_name: str = field(metadata={"alias": "DurableExecutionName"})
    function_arn: str = field(metadata={"alias": "FunctionArn"})
    status: str = field(metadata={"alias": "Status"})
    start_timestamp: datetime.datetime = field(
        metadata={"alias": "StartTimestamp", "is_timestamp": True}
    )
    input_payload: str | None = field(default=None, metadata={"alias": "InputPayload"})
    result: str | None = field(default=None, metadata={"alias": "Result"})
    error: ErrorObject | None = field(default=None, metadata={"alias": "Error"})
    end_timestamp: datetime.datetime | None = field(
        default=None, metadata={"alias": "EndTimestamp", "is_timestamp": True}
    )
    version: str | None = field(default=None, metadata={"alias": "Version"})


# Event-related structures from Smithy model
@dataclass(frozen=True)
class EventInput(BotoSerializableModel):
    """Event input structure."""

    payload: str | None = field(default=None, metadata={"alias": "Payload"})
    truncated: bool = field(default=False, metadata={"alias": "Truncated"})

    @classmethod
    def from_details(
        cls,
        details: ExecutionDetails,
        include: bool = False,  # noqa: FBT001, FBT002
    ) -> EventInput:
        details_input: str | None = details.input_payload if details else None
        payload: str | None = details_input if include else None
        truncated: bool = not include
        return cls(payload=payload, truncated=truncated)

    @classmethod
    def from_start_durable_execution_input(
        cls,
        start_durable_execution_input: StartDurableExecutionInput,
        include: bool = False,  # noqa: FBT001, FBT002
    ) -> EventInput:
        input: str | None = start_durable_execution_input.input
        truncated: bool = not include
        return cls(input, truncated)


@dataclass(frozen=True)
class EventResult(BotoSerializableModel):
    """Event result structure."""

    payload: str | None = field(default=None, metadata={"alias": "Payload"})
    truncated: bool = field(default=False, metadata={"alias": "Truncated"})

    @classmethod
    def from_details(
        cls,
        details: CallbackDetails | StepDetails | ChainedInvokeDetails | ContextDetails,
        include: bool = False,  # noqa: FBT001, FBT002
    ) -> EventResult:
        details_result: str | None = details.result if details else None
        payload: str | None = details_result if include else None
        truncated: bool = not include
        return cls(payload=payload, truncated=truncated)

    @classmethod
    def from_durable_execution_invocation_output(
        cls,
        durable_execution_invocation_output: DurableExecutionInvocationOutput,
        include: bool = False,  # noqa: FBT001, FBT002
    ) -> EventResult:
        truncated: bool = not include
        return cls(durable_execution_invocation_output.result, truncated)


@dataclass(frozen=True)
class EventError(BotoSerializableModel):
    """Event error structure."""

    payload: ErrorObject | None = field(default=None, metadata={"alias": "Payload"})
    truncated: bool = field(default=False, metadata={"alias": "Truncated"})

    @classmethod
    def from_details(
        cls,
        details: CallbackDetails | StepDetails | ChainedInvokeDetails | ContextDetails,
        include: bool = False,  # noqa: FBT001, FBT002
    ) -> EventError:
        error_object: ErrorObject | None = details.error if details else None
        truncated: bool = not include
        return cls(error_object, truncated)

    @classmethod
    def from_durable_execution_invocation_output(
        cls,
        durable_execution_invocation_output: DurableExecutionInvocationOutput,
        include: bool = False,  # noqa: FBT001, FBT002
    ) -> EventError:
        truncated: bool = not include
        return cls(durable_execution_invocation_output.error, truncated)


@dataclass(frozen=True)
class RetryDetails(BotoSerializableModel):
    """Retry details structure."""

    current_attempt: int = field(default=0, metadata={"alias": "CurrentAttempt"})
    next_attempt_delay_seconds: int | None = field(
        default=None, metadata={"alias": "NextAttemptDelaySeconds"}
    )


# Event detail structures
@dataclass(frozen=True)
class ExecutionStartedDetails(BotoSerializableModel):
    """Execution started event details."""

    input: EventInput | None = field(default=None, metadata={"alias": "Input"})
    execution_timeout: int | None = field(
        default=None, metadata={"alias": "ExecutionTimeout"}
    )


@dataclass(frozen=True)
class ExecutionSucceededDetails(BotoSerializableModel):
    """Execution succeeded event details."""

    result: EventResult | None = field(default=None, metadata={"alias": "Result"})


@dataclass(frozen=True)
class ExecutionFailedDetails(BotoSerializableModel):
    """Execution failed event details."""

    error: EventError | None = field(default=None, metadata={"alias": "Error"})


@dataclass(frozen=True)
class ExecutionTimedOutDetails(BotoSerializableModel):
    """Execution timed out event details."""

    error: EventError | None = field(default=None, metadata={"alias": "Error"})


@dataclass(frozen=True)
class ExecutionStoppedDetails(BotoSerializableModel):
    """Execution stopped event details."""

    error: EventError | None = field(default=None, metadata={"alias": "Error"})


@dataclass(frozen=True)
class ContextStartedDetails(BotoSerializableModel):
    """Context started event details."""


@dataclass(frozen=True)
class ContextSucceededDetails(BotoSerializableModel):
    """Context succeeded event details."""

    result: EventResult | None = field(default=None, metadata={"alias": "Result"})


@dataclass(frozen=True)
class ContextFailedDetails(BotoSerializableModel):
    """Context failed event details."""

    error: EventError | None = field(default=None, metadata={"alias": "Error"})


@dataclass(frozen=True)
class WaitStartedDetails(BotoSerializableModel):
    """Wait started event details."""

    duration: int | None = field(default=None, metadata={"alias": "Duration"})
    scheduled_end_timestamp: datetime.datetime | None = field(
        default=None,
        metadata={"alias": "ScheduledEndTimestamp", "is_timestamp": True},
    )


@dataclass(frozen=True)
class WaitSucceededDetails(BotoSerializableModel):
    """Wait succeeded event details."""

    duration: int | None = field(default=None, metadata={"alias": "Duration"})


@dataclass(frozen=True)
class WaitCancelledDetails(BotoSerializableModel):
    """Wait cancelled event details."""

    error: EventError | None = field(default=None, metadata={"alias": "Error"})


@dataclass(frozen=True)
class StepStartedDetails(BotoSerializableModel):
    """Step started event details."""


@dataclass(frozen=True)
class StepSucceededDetails(BotoSerializableModel):
    """Step succeeded event details."""

    result: EventResult | None = field(default=None, metadata={"alias": "Result"})
    retry_details: RetryDetails | None = field(
        default=None, metadata={"alias": "RetryDetails"}
    )


@dataclass(frozen=True)
class StepFailedDetails(BotoSerializableModel):
    """Step failed event details."""

    error: EventError | None = field(default=None, metadata={"alias": "Error"})
    retry_details: RetryDetails | None = field(
        default=None, metadata={"alias": "RetryDetails"}
    )


@dataclass(frozen=True)
class ChainedInvokePendingDetails(BotoSerializableModel):
    """Chained Invoke Pending event details."""

    input: EventInput | None = field(default=None, metadata={"alias": "Input"})
    function_name: str | None = field(default=None, metadata={"alias": "FunctionName"})


@dataclass(frozen=True)
class ChainedInvokeStartedDetails(BotoSerializableModel):
    """Chained invoke started event details."""

    durable_execution_arn: str | None = field(
        default=None, metadata={"alias": "DurableExecutionArn"}
    )


@dataclass(frozen=True)
class ChainedInvokeSucceededDetails(BotoSerializableModel):
    """Chained invoke succeeded event details."""

    result: EventResult | None = field(default=None, metadata={"alias": "Result"})


@dataclass(frozen=True)
class ChainedInvokeFailedDetails(BotoSerializableModel):
    """Chained invoke failed event details."""

    error: EventError | None = field(default=None, metadata={"alias": "Error"})


@dataclass(frozen=True)
class ChainedInvokeTimedOutDetails(BotoSerializableModel):
    """Chained invoke timed out event details."""

    error: EventError | None = field(default=None, metadata={"alias": "Error"})


@dataclass(frozen=True)
class ChainedInvokeStoppedDetails(BotoSerializableModel):
    """Chained invoke stopped event details."""

    error: EventError | None = field(default=None, metadata={"alias": "Error"})


@dataclass(frozen=True)
class CallbackStartedDetails(BotoSerializableModel):
    """Callback started event details."""

    callback_id: str | None = field(default=None, metadata={"alias": "CallbackId"})
    heartbeat_timeout: int | None = field(
        default=None, metadata={"alias": "HeartbeatTimeout"}
    )
    timeout: int | None = field(default=None, metadata={"alias": "Timeout"})


@dataclass(frozen=True)
class CallbackSucceededDetails(BotoSerializableModel):
    """Callback succeeded event details."""

    result: EventResult | None = field(default=None, metadata={"alias": "Result"})


@dataclass(frozen=True)
class CallbackFailedDetails(BotoSerializableModel):
    """Callback failed event details."""

    error: EventError | None = field(default=None, metadata={"alias": "Error"})


@dataclass(frozen=True)
class CallbackTimedOutDetails(BotoSerializableModel):
    """Callback timed out event details."""

    error: EventError | None = field(default=None, metadata={"alias": "Error"})


@dataclass(frozen=True)
class InvocationCompletedDetails(BotoSerializableModel):
    """Invocation completed event details."""

    start_timestamp: datetime.datetime = field(
        metadata={"alias": "StartTimestamp", "is_timestamp": True}
    )
    end_timestamp: datetime.datetime = field(
        metadata={"alias": "EndTimestamp", "is_timestamp": True}
    )
    request_id: str = field(metadata={"alias": "RequestId"})


@dataclass(frozen=True)
class EventCreationContext:
    operation: Operation
    event_id: int
    durable_execution_arn: str
    start_durable_execution_input: StartDurableExecutionInput
    durable_execution_invocation_output: DurableExecutionInvocationOutput | None = None
    operation_update: OperationUpdate | None = None
    include_execution_data: bool = False

    @property
    def sub_type(self) -> str | None:
        sub_type = self.operation.sub_type
        if isinstance(sub_type, OperationSubType):
            return sub_type.value
        return sub_type

    def get_retry_details(self) -> RetryDetails | None:
        if not self.operation.step_details or not self.operation_update:
            return None

        delay = 0
        if (
            self.operation_update.operation_type == OperationType.STEP
            and self.operation_update.step_options
        ):
            delay = self.operation_update.step_options.next_attempt_delay_seconds

        return RetryDetails(
            current_attempt=self.operation.step_details.attempt,
            next_attempt_delay_seconds=delay,
        )

    @property
    def start_timestamp(self) -> datetime.datetime:
        return (
            self.operation.start_timestamp
            if self.operation.start_timestamp is not None
            else datetime.datetime.now(datetime.timezone.utc)
        )

    @property
    def end_timestamp(self) -> datetime.datetime:
        return (
            self.operation.end_timestamp
            if self.operation.end_timestamp is not None
            else datetime.datetime.now(datetime.timezone.utc)
        )


@dataclass(frozen=True)
class Event(BotoSerializableModel):
    """Event structure from Smithy model."""

    event_type: str = field(metadata={"alias": "EventType"})
    event_timestamp: datetime.datetime = field(
        metadata={"alias": "EventTimestamp", "is_timestamp": True}
    )
    sub_type: str | None = field(default=None, metadata={"alias": "SubType"})
    event_id: int = field(default=1, metadata={"alias": "EventId"})
    operation_id: str | None = field(default=None, metadata={"alias": "Id"})
    name: str | None = field(default=None, metadata={"alias": "Name"})
    parent_id: str | None = field(default=None, metadata={"alias": "ParentId"})
    execution_started_details: ExecutionStartedDetails | None = field(
        default=None, metadata={"alias": "ExecutionStartedDetails"}
    )
    execution_succeeded_details: ExecutionSucceededDetails | None = field(
        default=None, metadata={"alias": "ExecutionSucceededDetails"}
    )
    execution_failed_details: ExecutionFailedDetails | None = field(
        default=None, metadata={"alias": "ExecutionFailedDetails"}
    )
    execution_timed_out_details: ExecutionTimedOutDetails | None = field(
        default=None, metadata={"alias": "ExecutionTimedOutDetails"}
    )
    execution_stopped_details: ExecutionStoppedDetails | None = field(
        default=None, metadata={"alias": "ExecutionStoppedDetails"}
    )
    context_started_details: ContextStartedDetails | None = field(
        default=None, metadata={"alias": "ContextStartedDetails"}
    )
    context_succeeded_details: ContextSucceededDetails | None = field(
        default=None, metadata={"alias": "ContextSucceededDetails"}
    )
    context_failed_details: ContextFailedDetails | None = field(
        default=None, metadata={"alias": "ContextFailedDetails"}
    )
    wait_started_details: WaitStartedDetails | None = field(
        default=None, metadata={"alias": "WaitStartedDetails"}
    )
    wait_succeeded_details: WaitSucceededDetails | None = field(
        default=None, metadata={"alias": "WaitSucceededDetails"}
    )
    wait_cancelled_details: WaitCancelledDetails | None = field(
        default=None, metadata={"alias": "WaitCancelledDetails"}
    )
    step_started_details: StepStartedDetails | None = field(
        default=None, metadata={"alias": "StepStartedDetails"}
    )
    step_succeeded_details: StepSucceededDetails | None = field(
        default=None, metadata={"alias": "StepSucceededDetails"}
    )
    step_failed_details: StepFailedDetails | None = field(
        default=None, metadata={"alias": "StepFailedDetails"}
    )
    chained_invoke_pending_details: ChainedInvokePendingDetails | None = field(
        default=None, metadata={"alias": "ChainedInvokePendingDetails"}
    )
    chained_invoke_started_details: ChainedInvokeStartedDetails | None = field(
        default=None, metadata={"alias": "ChainedInvokeStartedDetails"}
    )
    chained_invoke_succeeded_details: ChainedInvokeSucceededDetails | None = field(
        default=None, metadata={"alias": "ChainedInvokeSucceededDetails"}
    )
    chained_invoke_failed_details: ChainedInvokeFailedDetails | None = field(
        default=None, metadata={"alias": "ChainedInvokeFailedDetails"}
    )
    chained_invoke_timed_out_details: ChainedInvokeTimedOutDetails | None = field(
        default=None, metadata={"alias": "ChainedInvokeTimedOutDetails"}
    )
    chained_invoke_stopped_details: ChainedInvokeStoppedDetails | None = field(
        default=None, metadata={"alias": "ChainedInvokeStoppedDetails"}
    )
    callback_started_details: CallbackStartedDetails | None = field(
        default=None, metadata={"alias": "CallbackStartedDetails"}
    )
    callback_succeeded_details: CallbackSucceededDetails | None = field(
        default=None, metadata={"alias": "CallbackSucceededDetails"}
    )
    callback_failed_details: CallbackFailedDetails | None = field(
        default=None, metadata={"alias": "CallbackFailedDetails"}
    )
    callback_timed_out_details: CallbackTimedOutDetails | None = field(
        default=None, metadata={"alias": "CallbackTimedOutDetails"}
    )
    invocation_completed_details: InvocationCompletedDetails | None = field(
        default=None, metadata={"alias": "InvocationCompletedDetails"}
    )

    @classmethod
    def create_execution_event_started(cls, context: EventCreationContext) -> Event:
        execution_details: ExecutionDetails | None = context.operation.execution_details
        event_input: EventInput | None = (
            EventInput.from_details(execution_details, context.include_execution_data)
            if execution_details
            else None
        )
        execution_timeout: int | None = (
            context.start_durable_execution_input.execution_timeout_seconds
        )

        return cls(
            event_type=EventType.EXECUTION_STARTED.value,
            event_timestamp=context.start_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            execution_started_details=ExecutionStartedDetails(
                input=event_input,
                execution_timeout=execution_timeout,
            ),
        )

    @classmethod
    def create_execution_event_succeeded(cls, context: EventCreationContext) -> Event:
        result: EventResult | None = (
            EventResult.from_durable_execution_invocation_output(
                context.durable_execution_invocation_output,
                context.include_execution_data,
            )
            if context.durable_execution_invocation_output
            else None
        )
        return cls(
            event_type=EventType.EXECUTION_SUCCEEDED.value,
            event_timestamp=context.end_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            execution_succeeded_details=ExecutionSucceededDetails(result=result),
        )

    @classmethod
    def create_execution_event_failed(cls, context: EventCreationContext) -> Event:
        error: EventError | None = (
            EventError.from_durable_execution_invocation_output(
                context.durable_execution_invocation_output,
                include=context.include_execution_data,
            )
            if context.durable_execution_invocation_output
            else None
        )
        return cls(
            event_type=EventType.EXECUTION_FAILED.value,
            event_timestamp=context.end_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            execution_failed_details=ExecutionFailedDetails(error=error),
        )

    @classmethod
    def create_execution_event_timed_out(cls, context: EventCreationContext) -> Event:
        error: EventError | None = (
            EventError.from_durable_execution_invocation_output(
                context.durable_execution_invocation_output,
                include=context.include_execution_data,
            )
            if context.durable_execution_invocation_output
            else None
        )
        return cls(
            event_type=EventType.EXECUTION_TIMED_OUT.value,
            event_timestamp=context.end_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            execution_timed_out_details=ExecutionTimedOutDetails(error=error),
        )

    @classmethod
    def create_execution_event_stopped(cls, context: EventCreationContext) -> Event:
        error: EventError | None = (
            EventError.from_durable_execution_invocation_output(
                context.durable_execution_invocation_output,
                include=context.include_execution_data,
            )
            if context.durable_execution_invocation_output
            else None
        )
        return cls(
            event_type=EventType.EXECUTION_STOPPED.value,
            event_timestamp=context.end_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            execution_stopped_details=ExecutionStoppedDetails(error=error),
        )

    @classmethod
    def create_execution_event(cls, context: EventCreationContext) -> Event:
        """Create execution event based on action."""
        match context.operation.status:
            case OperationStatus.STARTED:
                return cls.create_execution_event_started(context)
            case OperationStatus.SUCCEEDED:
                return cls.create_execution_event_succeeded(context)
            case OperationStatus.FAILED:
                return cls.create_execution_event_failed(context)
            case OperationStatus.TIMED_OUT:
                return cls.create_execution_event_timed_out(context)
            case OperationStatus.STOPPED:
                return cls.create_execution_event_stopped(context)
            case _:
                msg = f"Operation status {context.operation.status} is not valid for execution operations. Valid statuses are: STARTED, SUCCEEDED, FAILED, TIMED_OUT, STOPPED"
                raise InvalidParameterValueException(msg)

    @classmethod
    def create_context_event_started(cls, context: EventCreationContext) -> Event:
        return cls(
            event_type=EventType.CONTEXT_STARTED.value,
            event_timestamp=context.start_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            context_started_details=ContextStartedDetails(),
        )

    @classmethod
    def create_context_event_succeeded(cls, context: EventCreationContext) -> Event:
        context_details: ContextDetails | None = context.operation.context_details
        event_result: EventResult | None = (
            EventResult.from_details(context_details, context.include_execution_data)
            if context_details
            else None
        )
        return cls(
            event_type=EventType.CONTEXT_SUCCEEDED.value,
            event_timestamp=context.end_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            context_succeeded_details=ContextSucceededDetails(result=event_result),
        )

    @classmethod
    def create_context_event_failed(cls, context: EventCreationContext) -> Event:
        context_details: ContextDetails | None = context.operation.context_details
        event_error: EventError | None = (
            EventError.from_details(context_details) if context_details else None
        )
        return cls(
            event_type=EventType.CONTEXT_FAILED.value,
            event_timestamp=context.end_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            context_failed_details=ContextFailedDetails(error=event_error),
        )

    @classmethod
    def create_context_event(cls, context: EventCreationContext) -> Event:
        """Create context event based on action."""
        match context.operation.status:
            case OperationStatus.STARTED:
                return cls.create_context_event_started(context)
            case OperationStatus.SUCCEEDED:
                return cls.create_context_event_succeeded(context)
            case OperationStatus.FAILED:
                return cls.create_context_event_failed(context)
            case _:
                msg = (
                    f"Operation status {context.operation.status} is not valid for context operations. "
                    f"Valid statuses are: STARTED, SUCCEEDED, FAILED"
                )
                raise InvalidParameterValueException(msg)

    @classmethod
    def create_wait_event_started(cls, context: EventCreationContext) -> Event:
        wait_details: WaitDetails | None = context.operation.wait_details
        scheduled_end_timestamp: datetime.datetime | None = (
            wait_details.scheduled_end_timestamp if wait_details else None
        )
        duration: int | None = None
        if (
            wait_details
            and wait_details.scheduled_end_timestamp
            and context.operation.start_timestamp
        ):
            duration = round(
                (
                    wait_details.scheduled_end_timestamp
                    - context.operation.start_timestamp
                ).total_seconds()
            )
        return cls(
            event_type=EventType.WAIT_STARTED.value,
            event_timestamp=context.start_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            wait_started_details=WaitStartedDetails(
                duration=duration,
                scheduled_end_timestamp=scheduled_end_timestamp,
            ),
        )

    @classmethod
    def create_wait_event_succeeded(cls, context: EventCreationContext) -> Event:
        wait_details: WaitDetails | None = context.operation.wait_details
        duration: int | None = None
        if (
            wait_details
            and wait_details.scheduled_end_timestamp
            and context.operation.start_timestamp
        ):
            duration = round(
                (
                    wait_details.scheduled_end_timestamp - context.start_timestamp
                ).total_seconds()
            )
        return cls(
            event_type=EventType.WAIT_SUCCEEDED.value,
            event_timestamp=context.end_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            wait_succeeded_details=WaitSucceededDetails(duration=duration),
        )

    @classmethod
    def create_wait_event_cancelled(cls, context: EventCreationContext) -> Event:
        error: EventError | None = None
        if (
            context.operation_update
            and context.operation_update.operation_type == OperationType.WAIT
            and context.operation_update.action == OperationAction.CANCEL
        ):
            error = EventError(
                context.operation_update.error, not context.include_execution_data
            )
        return cls(
            event_type=EventType.WAIT_CANCELLED.value,
            event_timestamp=context.end_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            wait_cancelled_details=WaitCancelledDetails(error=error),
        )

    @classmethod
    def create_wait_event(cls, context: EventCreationContext) -> Event:
        """Create wait event based on action."""
        match context.operation.status:
            case OperationStatus.STARTED:
                return cls.create_wait_event_started(context)
            case OperationStatus.SUCCEEDED:
                return cls.create_wait_event_succeeded(context)
            case OperationStatus.CANCELLED:
                return cls.create_wait_event_cancelled(context)
            case _:
                msg = (
                    f"Operation status {context.operation.status} is not valid for wait operations. "
                    f"Valid statuses are: STARTED, SUCCEEDED, CANCELLED"
                )
                raise InvalidParameterValueException(msg)

    @classmethod
    def create_step_event_started(cls, context: EventCreationContext) -> Event:
        return cls(
            event_type=EventType.STEP_STARTED.value,
            event_timestamp=context.start_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            step_started_details=StepStartedDetails(),
        )

    @classmethod
    def create_step_event_succeeded(cls, context: EventCreationContext) -> Event:
        step_details: StepDetails | None = context.operation.step_details
        event_result: EventResult | None = (
            EventResult.from_details(step_details, context.include_execution_data)
            if step_details
            else None
        )
        return cls(
            event_type=EventType.STEP_SUCCEEDED.value,
            event_timestamp=context.end_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            step_succeeded_details=StepSucceededDetails(
                result=event_result,
                retry_details=context.get_retry_details(),
            ),
        )

    @classmethod
    def create_step_event_failed(cls, context: EventCreationContext) -> Event:
        step_details: StepDetails | None = context.operation.step_details
        event_error: EventError | None = (
            EventError.from_details(
                step_details, include=context.include_execution_data
            )
            if step_details
            else None
        )
        return cls(
            event_type=EventType.STEP_FAILED.value,
            event_timestamp=context.end_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            step_failed_details=StepFailedDetails(
                error=event_error,
                retry_details=context.get_retry_details(),
            ),
        )

    @classmethod
    def create_step_event(cls, context: EventCreationContext) -> Event:
        """Create step event based on action."""
        match context.operation.status:
            case OperationStatus.STARTED:
                return cls.create_step_event_started(context)
            case OperationStatus.SUCCEEDED:
                return cls.create_step_event_succeeded(context)
            case OperationStatus.FAILED:
                return cls.create_step_event_failed(context)
            case _:
                msg = (
                    f"Operation status {context.operation.status} is not valid for step operations. "
                    f"Valid statuses are: STARTED, SUCCEEDED, FAILED"
                )
                raise InvalidParameterValueException(msg)

    @classmethod
    def create_chained_invoke_event_pending(
        cls, context: EventCreationContext
    ) -> Event:
        input: EventInput = EventInput.from_start_durable_execution_input(
            context.start_durable_execution_input, context.include_execution_data
        )
        return cls(
            event_type=EventType.CHAINED_INVOKE_STARTED.value,
            event_timestamp=context.start_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            chained_invoke_pending_details=ChainedInvokePendingDetails(
                input=input,
                function_name=context.start_durable_execution_input.function_name,
            ),
        )

    @classmethod
    def create_chained_invoke_event_started(
        cls, context: EventCreationContext
    ) -> Event:
        return cls(
            event_type=EventType.CHAINED_INVOKE_STARTED.value,
            event_timestamp=context.start_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            chained_invoke_started_details=ChainedInvokeStartedDetails(
                durable_execution_arn=context.durable_execution_arn
            ),
        )

    @classmethod
    def create_chained_invoke_event_succeeded(
        cls, context: EventCreationContext
    ) -> Event:
        chained_invoke_details: ChainedInvokeDetails | None = (
            context.operation.chained_invoke_details
        )
        event_result: EventResult | None = (
            EventResult.from_details(
                chained_invoke_details, context.include_execution_data
            )
            if chained_invoke_details
            else None
        )
        return cls(
            event_type=EventType.CHAINED_INVOKE_SUCCEEDED.value,
            event_timestamp=context.end_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            chained_invoke_succeeded_details=ChainedInvokeSucceededDetails(
                result=event_result
            ),
        )

    @classmethod
    def create_chained_invoke_event_failed(cls, context: EventCreationContext) -> Event:
        chained_invoke_details: ChainedInvokeDetails | None = (
            context.operation.chained_invoke_details
        )
        event_error: EventError | None = (
            EventError.from_details(
                chained_invoke_details, include=context.include_execution_data
            )
            if chained_invoke_details
            else None
        )
        return cls(
            event_type=EventType.CHAINED_INVOKE_FAILED.value,
            event_timestamp=context.end_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            chained_invoke_failed_details=ChainedInvokeFailedDetails(error=event_error),
        )

    @classmethod
    def create_chained_invoke_event_timed_out(
        cls, context: EventCreationContext
    ) -> Event:
        chained_invoke_details: ChainedInvokeDetails | None = (
            context.operation.chained_invoke_details
        )
        event_error: EventError | None = (
            EventError.from_details(
                chained_invoke_details, include=context.include_execution_data
            )
            if chained_invoke_details
            else None
        )
        return cls(
            event_type=EventType.CHAINED_INVOKE_TIMED_OUT.value,
            event_timestamp=context.end_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            chained_invoke_timed_out_details=ChainedInvokeTimedOutDetails(
                error=event_error
            ),
        )

    @classmethod
    def create_chained_invoke_event_stopped(
        cls, context: EventCreationContext
    ) -> Event:
        chained_invoke_details: ChainedInvokeDetails | None = (
            context.operation.chained_invoke_details
        )
        event_error: EventError | None = (
            EventError.from_details(
                chained_invoke_details, include=context.include_execution_data
            )
            if chained_invoke_details
            else None
        )
        return cls(
            event_type=EventType.CHAINED_INVOKE_STOPPED.value,
            event_timestamp=context.end_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            chained_invoke_stopped_details=ChainedInvokeStoppedDetails(
                error=event_error
            ),
        )

    @classmethod
    def create_chained_invoke_event(cls, context: EventCreationContext) -> Event:
        """Create chained invoke event based on action."""
        match context.operation.status:
            case OperationStatus.PENDING:
                return cls.create_chained_invoke_event_pending(context)
            case OperationStatus.STARTED:
                return cls.create_chained_invoke_event_started(context)
            case OperationStatus.SUCCEEDED:
                return cls.create_chained_invoke_event_succeeded(context)
            case OperationStatus.FAILED:
                return cls.create_chained_invoke_event_failed(context)
            case OperationStatus.TIMED_OUT:
                return cls.create_chained_invoke_event_timed_out(context)
            case OperationStatus.STOPPED:
                return cls.create_chained_invoke_event_stopped(context)
            case _:
                msg = (
                    f"Operation status {context.operation.status} is not valid for chained invoke operations. Valid statuses are: "
                    f"STARTED, SUCCEEDED, FAILED, TIMED_OUT, STOPPED"
                )
                raise InvalidParameterValueException(msg)

    @classmethod
    def create_callback_event_started(cls, context: EventCreationContext) -> Event:
        callback_details: CallbackDetails | None = context.operation.callback_details
        callback_id: str | None = (
            callback_details.callback_id if callback_details else None
        )
        callback_options: CallbackOptions | None = (
            context.operation_update.callback_options
            if context.operation_update
            else None
        )
        timeout: int | None = (
            callback_options.timeout_seconds if callback_options else None
        )
        heartbeat_timeout: int | None = (
            callback_options.heartbeat_timeout_seconds if callback_options else None
        )
        return cls(
            event_type=EventType.CALLBACK_STARTED.value,
            event_timestamp=context.start_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            callback_started_details=CallbackStartedDetails(
                callback_id=callback_id,
                timeout=timeout,
                heartbeat_timeout=heartbeat_timeout,
            ),
        )

    @classmethod
    def create_callback_event_succeeded(cls, context: EventCreationContext) -> Event:
        callback_details: CallbackDetails | None = context.operation.callback_details
        event_result: EventResult | None = (
            EventResult.from_details(callback_details, context.include_execution_data)
            if callback_details
            else None
        )
        return cls(
            event_type=EventType.CALLBACK_SUCCEEDED.value,
            event_timestamp=context.end_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            callback_succeeded_details=CallbackSucceededDetails(result=event_result),
        )

    @classmethod
    def create_callback_event_failed(cls, context: EventCreationContext) -> Event:
        callback_details: CallbackDetails | None = context.operation.callback_details
        event_error: EventError | None = (
            EventError.from_details(callback_details) if callback_details else None
        )
        return cls(
            event_type=EventType.CALLBACK_FAILED.value,
            event_timestamp=context.end_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            callback_failed_details=CallbackFailedDetails(error=event_error),
        )

    @classmethod
    def create_callback_event_timed_out(cls, context: EventCreationContext) -> Event:
        callback_details: CallbackDetails | None = context.operation.callback_details
        event_error: EventError | None = (
            EventError.from_details(callback_details) if callback_details else None
        )
        return cls(
            event_type=EventType.CALLBACK_TIMED_OUT.value,
            event_timestamp=context.end_timestamp,
            sub_type=context.sub_type,
            event_id=context.event_id,
            operation_id=context.operation.operation_id,
            name=context.operation.name,
            parent_id=context.operation.parent_id,
            callback_timed_out_details=CallbackTimedOutDetails(error=event_error),
        )

    @classmethod
    def create_callback_event(cls, context: EventCreationContext) -> Event:
        """Create callback event based on action."""
        match context.operation.status:
            case OperationStatus.STARTED:
                return cls.create_callback_event_started(context)
            case OperationStatus.SUCCEEDED:
                return cls.create_callback_event_succeeded(context)
            case OperationStatus.FAILED:
                return cls.create_callback_event_failed(context)
            case OperationStatus.TIMED_OUT:
                return cls.create_callback_event_timed_out(context)
            case _:
                msg = (
                    f"Operation status {context.operation.status} is not valid for callback operations. "
                    f"Valid statuses are: STARTED, SUCCEEDED, FAILED, TIMED_OUT"
                )
                raise InvalidParameterValueException(msg)

    @classmethod
    def create_invocation_completed(
        cls,
        event_id: int,
        event_timestamp: datetime.datetime,
        start_timestamp: datetime.datetime,
        end_timestamp: datetime.datetime,
        request_id: str,
    ) -> Event:
        """Create invocation completed event."""
        return cls(
            event_type=EventType.INVOCATION_COMPLETED.value,
            event_timestamp=event_timestamp,
            event_id=event_id,
            invocation_completed_details=InvocationCompletedDetails(
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
                request_id=request_id,
            ),
        )

    @classmethod
    def create_event_started(cls, context: EventCreationContext) -> Event:
        """Convert operation to started event."""
        if context.operation.start_timestamp is None:
            msg: str = "Operation start timestamp cannot be None when converting to started event"
            raise InvalidParameterValueException(msg)

        match context.operation.operation_type:
            case OperationType.EXECUTION:
                return cls.create_execution_event_started(context)
            case OperationType.CONTEXT:
                return cls.create_context_event_started(context)
            case OperationType.WAIT:
                return cls.create_wait_event_started(context)
            case OperationType.STEP:
                return cls.create_step_event_started(context)
            case OperationType.CHAINED_INVOKE:
                return cls.create_chained_invoke_event_started(context)
            case OperationType.CALLBACK:
                return cls.create_callback_event_started(context)
            case _:
                msg = f"Unknown operation type: {context.operation.operation_type}"
                raise InvalidParameterValueException(msg)

    @classmethod
    def from_event_with_id(cls, event: Event, event_id: int) -> Event:
        """Create a new Event from an existing event with updated event_id."""
        return cls(
            event_type=event.event_type,
            event_timestamp=event.event_timestamp,
            sub_type=event.sub_type,
            event_id=event_id,
            operation_id=event.operation_id,
            name=event.name,
            parent_id=event.parent_id,
            execution_started_details=event.execution_started_details,
            execution_succeeded_details=event.execution_succeeded_details,
            execution_failed_details=event.execution_failed_details,
            execution_timed_out_details=event.execution_timed_out_details,
            execution_stopped_details=event.execution_stopped_details,
            context_started_details=event.context_started_details,
            context_succeeded_details=event.context_succeeded_details,
            context_failed_details=event.context_failed_details,
            wait_started_details=event.wait_started_details,
            wait_succeeded_details=event.wait_succeeded_details,
            wait_cancelled_details=event.wait_cancelled_details,
            step_started_details=event.step_started_details,
            step_succeeded_details=event.step_succeeded_details,
            step_failed_details=event.step_failed_details,
            chained_invoke_pending_details=event.chained_invoke_pending_details,
            chained_invoke_started_details=event.chained_invoke_started_details,
            chained_invoke_succeeded_details=event.chained_invoke_succeeded_details,
            chained_invoke_failed_details=event.chained_invoke_failed_details,
            chained_invoke_timed_out_details=event.chained_invoke_timed_out_details,
            chained_invoke_stopped_details=event.chained_invoke_stopped_details,
            callback_started_details=event.callback_started_details,
            callback_succeeded_details=event.callback_succeeded_details,
            callback_failed_details=event.callback_failed_details,
            callback_timed_out_details=event.callback_timed_out_details,
        )

    @classmethod
    def create_event_terminated(cls, context: EventCreationContext) -> Event:
        """Convert operation to finished event."""
        operation: Operation = context.operation
        if operation.end_timestamp is None:
            msg: str = "Operation end timestamp cannot be None when converting to finished event"
            raise InvalidParameterValueException(msg)

        if operation.status not in TERMINAL_STATUSES:
            msg = f"Operation status must be one of SUCCEEDED, FAILED, TIMED_OUT, STOPPED, or CANCELLED. Got: {operation.status}"
            raise InvalidParameterValueException(msg)

        match operation.operation_type:
            case OperationType.EXECUTION:
                return cls.create_execution_event(context)
            case OperationType.CONTEXT:
                return cls.create_context_event(context)
            case OperationType.WAIT:
                return cls.create_wait_event(context)
            case OperationType.STEP:
                return cls.create_step_event(context)
            case OperationType.CHAINED_INVOKE:
                return cls.create_chained_invoke_event(context)
            case OperationType.CALLBACK:
                return cls.create_callback_event(context)
            case _:
                msg = f"Unknown operation type: {operation.operation_type}"
                raise InvalidParameterValueException(msg)


@dataclass(frozen=True)
class HistoryEventTypeConfig:
    """Configuration for how to process a specific event type."""

    operation_type: OperationType | None
    operation_status: OperationStatus | None
    is_start_event: bool
    is_end_event: bool
    has_result: bool  # Whether this event type contains result/error data


# Mapping of event types to their processing configuration
# This matches the TypeScript historyEventTypes constant
HISTORY_EVENT_TYPES: dict[str, HistoryEventTypeConfig] = {
    "ExecutionStarted": HistoryEventTypeConfig(
        operation_type=OperationType.EXECUTION,
        operation_status=OperationStatus.STARTED,
        is_start_event=True,
        is_end_event=False,
        has_result=False,
    ),
    "ExecutionFailed": HistoryEventTypeConfig(
        operation_type=OperationType.EXECUTION,
        operation_status=OperationStatus.FAILED,
        is_start_event=False,
        is_end_event=True,
        has_result=False,
    ),
    "ExecutionStopped": HistoryEventTypeConfig(
        operation_type=OperationType.EXECUTION,
        operation_status=OperationStatus.STOPPED,
        is_start_event=False,
        is_end_event=True,
        has_result=False,
    ),
    "ExecutionSucceeded": HistoryEventTypeConfig(
        operation_type=OperationType.EXECUTION,
        operation_status=OperationStatus.SUCCEEDED,
        is_start_event=False,
        is_end_event=True,
        has_result=False,
    ),
    "ExecutionTimedOut": HistoryEventTypeConfig(
        operation_type=OperationType.EXECUTION,
        operation_status=OperationStatus.TIMED_OUT,
        is_start_event=False,
        is_end_event=True,
        has_result=False,
    ),
    "CallbackStarted": HistoryEventTypeConfig(
        operation_type=OperationType.CALLBACK,
        operation_status=OperationStatus.STARTED,
        is_start_event=True,
        is_end_event=False,
        has_result=False,
    ),
    "CallbackFailed": HistoryEventTypeConfig(
        operation_type=OperationType.CALLBACK,
        operation_status=OperationStatus.FAILED,
        is_start_event=False,
        is_end_event=True,
        has_result=True,
    ),
    "CallbackSucceeded": HistoryEventTypeConfig(
        operation_type=OperationType.CALLBACK,
        operation_status=OperationStatus.SUCCEEDED,
        is_start_event=False,
        is_end_event=True,
        has_result=True,
    ),
    "CallbackTimedOut": HistoryEventTypeConfig(
        operation_type=OperationType.CALLBACK,
        operation_status=OperationStatus.TIMED_OUT,
        is_start_event=False,
        is_end_event=True,
        has_result=True,
    ),
    "ContextStarted": HistoryEventTypeConfig(
        operation_type=OperationType.CONTEXT,
        operation_status=OperationStatus.STARTED,
        is_start_event=True,
        is_end_event=False,
        has_result=False,
    ),
    "ContextFailed": HistoryEventTypeConfig(
        operation_type=OperationType.CONTEXT,
        operation_status=OperationStatus.FAILED,
        is_start_event=False,
        is_end_event=True,
        has_result=True,
    ),
    "ContextSucceeded": HistoryEventTypeConfig(
        operation_type=OperationType.CONTEXT,
        operation_status=OperationStatus.SUCCEEDED,
        is_start_event=False,
        is_end_event=True,
        has_result=True,
    ),
    "ChainedInvokeStarted": HistoryEventTypeConfig(
        operation_type=OperationType.CHAINED_INVOKE,
        operation_status=OperationStatus.STARTED,
        is_start_event=True,
        is_end_event=False,
        has_result=False,
    ),
    "ChainedInvokeFailed": HistoryEventTypeConfig(
        operation_type=OperationType.CHAINED_INVOKE,
        operation_status=OperationStatus.FAILED,
        is_start_event=False,
        is_end_event=True,
        has_result=True,
    ),
    "ChainedInvokeSucceeded": HistoryEventTypeConfig(
        operation_type=OperationType.CHAINED_INVOKE,
        operation_status=OperationStatus.SUCCEEDED,
        is_start_event=False,
        is_end_event=True,
        has_result=True,
    ),
    "ChainedInvokeTimedOut": HistoryEventTypeConfig(
        operation_type=OperationType.CHAINED_INVOKE,
        operation_status=OperationStatus.TIMED_OUT,
        is_start_event=False,
        is_end_event=True,
        has_result=True,
    ),
    "ChainedInvokeCancelled": HistoryEventTypeConfig(
        operation_type=OperationType.CHAINED_INVOKE,
        operation_status=OperationStatus.CANCELLED,
        is_start_event=False,
        is_end_event=True,
        has_result=True,
    ),
    "StepStarted": HistoryEventTypeConfig(
        operation_type=OperationType.STEP,
        operation_status=OperationStatus.STARTED,
        is_start_event=True,
        is_end_event=False,
        has_result=False,
    ),
    "StepFailed": HistoryEventTypeConfig(
        operation_type=OperationType.STEP,
        operation_status=OperationStatus.FAILED,
        is_start_event=False,
        is_end_event=True,
        has_result=True,
    ),
    "StepSucceeded": HistoryEventTypeConfig(
        operation_type=OperationType.STEP,
        operation_status=OperationStatus.SUCCEEDED,
        is_start_event=False,
        is_end_event=True,
        has_result=True,
    ),
    "WaitStarted": HistoryEventTypeConfig(
        operation_type=OperationType.WAIT,
        operation_status=OperationStatus.STARTED,
        is_start_event=True,
        is_end_event=False,
        has_result=True,
    ),
    "WaitSucceeded": HistoryEventTypeConfig(
        operation_type=OperationType.WAIT,
        operation_status=OperationStatus.SUCCEEDED,
        is_start_event=False,
        is_end_event=True,
        has_result=True,
    ),
    "WaitCancelled": HistoryEventTypeConfig(
        operation_type=OperationType.WAIT,
        operation_status=OperationStatus.CANCELLED,
        is_start_event=False,
        is_end_event=True,
        has_result=True,
    ),
    # TODO: add support for populating invocation information from InvocationCompleted event
    "InvocationCompleted": HistoryEventTypeConfig(
        operation_type=None,
        operation_status=None,
        is_start_event=False,
        is_end_event=False,
        has_result=True,
    ),
}


def events_to_operations(events: list[Event]) -> list[Operation]:
    """Convert a list of history events into operations.

    This function processes raw history events and groups them by operation ID,
    creating comprehensive operation objects following the TypeScript pattern from
    aws-durable-execution-sdk-js-testing.

    Multiple events for the same operation_id are merged together, with each event
    contributing its specific fields (e.g., CallbackStarted provides callback_id,
    CallbackSucceeded provides result).

    Args:
        events: List of history events to process

    Returns:
        List of operations, one per unique operation ID

    Raises:
        InvalidParameterValueException: When required fields are missing from an event

    Note:
        InvocationCompleted events are currently skipped as they don't represent
        operations. Future enhancement: populate invocation information from these
        events (TODO).
    """
    operations_map: dict[str, Operation] = {}

    for event in events:
        if not event.event_type:
            msg = "Missing required 'event_type' field in event"
            raise InvalidParameterValueException(msg)

        # Get event type configuration
        event_config: HistoryEventTypeConfig | None = HISTORY_EVENT_TYPES.get(
            event.event_type
        )
        if not event_config:
            msg = f"Unknown event type: {event.event_type}"
            raise InvalidParameterValueException(msg)

        # TODO: add support for populating invocation information from InvocationCompleted event
        if event.event_type == "InvocationCompleted":
            continue

        if not event.operation_id:
            msg = f"Missing required 'operation_id' field in event {event.event_id}"
            raise InvalidParameterValueException(msg)

        # Get previous operation if it exists
        previous_operation: Operation | None = operations_map.get(event.operation_id)

        # Get operation type and status from configuration
        operation_type: OperationType = (
            event_config.operation_type or OperationType.EXECUTION
        )
        status: OperationStatus = (
            event_config.operation_status or OperationStatus.PENDING
        )

        # Parse sub_type
        sub_type: OperationSubType | str | None = None
        if event.sub_type:
            try:
                sub_type = OperationSubType(event.sub_type)
            except ValueError:
                sub_type = event.sub_type

        # Create base operation
        operation = Operation(
            operation_id=event.operation_id,
            operation_type=operation_type,
            status=status,
            name=event.name,
            parent_id=event.parent_id,
            sub_type=sub_type,
            start_timestamp=datetime.datetime.now(tz=datetime.timezone.utc),
        )

        # Merge with previous operation if it exists
        # Most fields are immutable, so they get preserved from previous events
        if previous_operation:
            operation = replace(
                operation,
                name=operation.name or previous_operation.name,
                parent_id=operation.parent_id or previous_operation.parent_id,
                sub_type=operation.sub_type or previous_operation.sub_type,
                start_timestamp=previous_operation.start_timestamp,
                end_timestamp=previous_operation.end_timestamp,
                execution_details=previous_operation.execution_details,
                context_details=previous_operation.context_details,
                step_details=previous_operation.step_details,
                wait_details=previous_operation.wait_details,
                callback_details=previous_operation.callback_details,
                chained_invoke_details=previous_operation.chained_invoke_details,
            )

        # Set timestamps based on event configuration
        if event_config.is_start_event:
            operation = replace(operation, start_timestamp=event.event_timestamp)
        if event_config.is_end_event:
            operation = replace(operation, end_timestamp=event.event_timestamp)

        # Add operation-specific details incrementally
        # Each event type contributes only the fields it has

        # EXECUTION details
        if (
            operation_type == OperationType.EXECUTION
            and event.execution_started_details
            and event.execution_started_details.input
        ):
            operation = replace(
                operation,
                execution_details=ExecutionDetails(
                    input_payload=event.execution_started_details.input.payload
                ),
            )

        # CALLBACK details - merge callback_id, result, and error from different events
        if operation_type == OperationType.CALLBACK:
            existing_cb: CallbackDetails | None = operation.callback_details
            callback_id: str = existing_cb.callback_id if existing_cb else ""
            result: str | None = existing_cb.result if existing_cb else None
            error: ErrorObject | None = existing_cb.error if existing_cb else None

            # CallbackStarted provides callback_id
            if event.callback_started_details:
                callback_id = event.callback_started_details.callback_id or callback_id

            # CallbackSucceeded provides result
            if (
                event.callback_succeeded_details
                and event.callback_succeeded_details.result
            ):
                result = event.callback_succeeded_details.result.payload

            # CallbackFailed provides error
            if event.callback_failed_details and event.callback_failed_details.error:
                error = event.callback_failed_details.error.payload

            # CallbackTimedOut provides error
            if (
                event.callback_timed_out_details
                and event.callback_timed_out_details.error
            ):
                error = event.callback_timed_out_details.error.payload

            operation = replace(
                operation,
                callback_details=CallbackDetails(
                    callback_id=callback_id,
                    result=result,
                    error=error,
                ),
            )

        # STEP details - only update if this event type has result data
        if operation_type == OperationType.STEP and event_config.has_result:
            existing_step: StepDetails | None = operation.step_details
            result_val: str | None = existing_step.result if existing_step else None
            error_val: ErrorObject | None = (
                existing_step.error if existing_step else None
            )
            attempt: int = existing_step.attempt if existing_step else 0
            next_attempt_ts: datetime.datetime | None = (
                existing_step.next_attempt_timestamp if existing_step else None
            )

            # StepSucceeded provides result
            if event.step_succeeded_details:
                if event.step_succeeded_details.result:
                    result_val = event.step_succeeded_details.result.payload
                if event.step_succeeded_details.retry_details:
                    attempt = event.step_succeeded_details.retry_details.current_attempt

            # StepFailed provides error and retry details
            if event.step_failed_details:
                if event.step_failed_details.error:
                    error_val = event.step_failed_details.error.payload
                if event.step_failed_details.retry_details:
                    attempt = event.step_failed_details.retry_details.current_attempt
                    if (
                        event.step_failed_details.retry_details.next_attempt_delay_seconds
                        is not None
                    ):
                        next_attempt_ts = event.event_timestamp + datetime.timedelta(
                            seconds=event.step_failed_details.retry_details.next_attempt_delay_seconds
                        )

            operation = replace(
                operation,
                step_details=StepDetails(
                    result=result_val,
                    error=error_val,
                    attempt=attempt,
                    next_attempt_timestamp=next_attempt_ts,
                ),
            )

        # WAIT details
        if operation_type == OperationType.WAIT and event.wait_started_details:
            operation = replace(
                operation,
                wait_details=WaitDetails(
                    scheduled_end_timestamp=event.wait_started_details.scheduled_end_timestamp
                ),
            )

        # CONTEXT details - only update if this event type has result data (matching TypeScript hasResult)
        if operation_type == OperationType.CONTEXT and event_config.has_result:
            if (
                event.context_succeeded_details
                and event.context_succeeded_details.result
            ):
                operation = replace(
                    operation,
                    context_details=ContextDetails(
                        result=event.context_succeeded_details.result.payload,
                        error=None,
                    ),
                )
            elif event.context_failed_details and event.context_failed_details.error:
                operation = replace(
                    operation,
                    context_details=ContextDetails(
                        result=None,
                        error=event.context_failed_details.error.payload,
                    ),
                )

        # CHAINED_INVOKE details - only update if this event type has result data (matching TypeScript hasResult)
        if operation_type == OperationType.CHAINED_INVOKE and event_config.has_result:
            if (
                event.chained_invoke_succeeded_details
                and event.chained_invoke_succeeded_details.result
            ):
                operation = replace(
                    operation,
                    chained_invoke_details=ChainedInvokeDetails(
                        result=event.chained_invoke_succeeded_details.result.payload,
                        error=None,
                    ),
                )
            elif (
                event.chained_invoke_failed_details
                and event.chained_invoke_failed_details.error
            ):
                operation = replace(
                    operation,
                    chained_invoke_details=ChainedInvokeDetails(
                        result=None,
                        error=event.chained_invoke_failed_details.error.payload,
                    ),
                )

        # Store in map
        operations_map[event.operation_id] = operation

    return list(operations_map.values())


@dataclass(frozen=True)
class GetDurableExecutionHistoryResponse(BotoSerializableModel):
    """Response containing durable execution history events."""

    events: list[Event] = field(default_factory=list, metadata={"alias": "Events"})
    next_marker: str | None = field(default=None, metadata={"alias": "NextMarker"})


class _ExecutionResultSource(Protocol):
    operations: list[Operation]
    result: DurableExecutionInvocationOutput | None


@dataclass(frozen=True)
class DurableFunctionTestResult:
    status: InvocationStatus
    operations: list[Operation]
    result: OperationPayload | None = None
    error: ErrorObject | None = None
    _all_operations: list[Operation] = field(
        default_factory=list,
        repr=False,
        compare=False,
    )

    @classmethod
    def create(cls, execution: _ExecutionResultSource) -> DurableFunctionTestResult:
        operations = []
        for operation in execution.operations:
            if operation.operation_type is OperationType.EXECUTION:
                # don't want the EXECUTION operations in the list test code asserts against
                continue

            if operation.parent_id is None:
                operations.append(operation)

        if execution.result is None:
            msg: str = "Execution result must exist to create test result."
            raise DurableFunctionsTestError(msg)

        return cls(
            status=execution.result.status,
            operations=operations,
            result=execution.result.result,
            error=execution.result.error,
            _all_operations=execution.operations,
        )

    @classmethod
    def from_execution_history(
        cls,
        execution_response: GetDurableExecutionResponse,
        history_response: GetDurableExecutionHistoryResponse,
    ) -> DurableFunctionTestResult:
        """Create test result from execution history responses.

        Factory method for cloud runner that builds DurableFunctionTestResult
        from GetDurableExecution and GetDurableExecutionHistory API responses.
        """
        # Map status string to InvocationStatus enum
        try:
            status = InvocationStatus[execution_response.status]
        except KeyError:
            logger.warning(
                "Unknown status: %s, defaulting to FAILED", execution_response.status
            )
            status = InvocationStatus.FAILED

        # Convert Events to Operations - group by operation_id and merge
        try:
            svc_operations = events_to_operations(history_response.events)
        except Exception as e:
            logger.warning("Failed to convert events to operations: %s", e)
            svc_operations = []

        # Build top-level operation list (exclude EXECUTION type)
        operations = []
        for svc_op in svc_operations:
            if svc_op.operation_type == OperationType.EXECUTION:
                continue
            if svc_op.parent_id is None:
                operations.append(svc_op)

        return cls(
            status=status,
            operations=operations,
            result=execution_response.result,
            error=execution_response.error,
            _all_operations=svc_operations,
        )

    def get_operation_by_name(self, name: str) -> Operation:
        for operation in self.operations:
            if operation.name == name:
                return operation
        msg: str = f"Operation with name '{name}' not found"
        raise DurableFunctionsTestError(msg)

    def get_step(self, name: str) -> Operation:
        return self._get_operation_by_name_and_type(name, OperationType.STEP)

    def get_wait(self, name: str) -> Operation:
        return self._get_operation_by_name_and_type(name, OperationType.WAIT)

    def get_context(self, name: str) -> Operation:
        return self._get_operation_by_name_and_type(name, OperationType.CONTEXT)

    def get_callback(self, name: str) -> Operation:
        return self._get_operation_by_name_and_type(name, OperationType.CALLBACK)

    def get_invoke(self, name: str) -> Operation:
        return self._get_operation_by_name_and_type(name, OperationType.CHAINED_INVOKE)

    def get_execution(self, name: str) -> Operation:
        return self._get_operation_by_name_and_type(name, OperationType.EXECUTION)

    def get_deserialized_result(self, serdes: ExtendedTypeSerDes | None = None) -> Any:
        """Return the deserialized execution result."""
        return _deserialize_operation_payload(self.result, serdes)

    def get_operation_deserialized_result(
        self,
        operation: Operation,
        serdes: ExtendedTypeSerDes | None = None,
    ) -> Any:
        """Return the deserialized result payload for a service operation."""
        match operation.operation_type:
            case OperationType.CONTEXT:
                result = (
                    operation.context_details.result
                    if operation.context_details
                    else None
                )
            case OperationType.STEP:
                result = (
                    operation.step_details.result if operation.step_details else None
                )
            case OperationType.CALLBACK:
                result = (
                    operation.callback_details.result
                    if operation.callback_details
                    else None
                )
            case OperationType.CHAINED_INVOKE:
                result = (
                    operation.chained_invoke_details.result
                    if operation.chained_invoke_details
                    else None
                )
            case _:
                result = None
        return _deserialize_operation_payload(result, serdes)

    def get_child_operations(self, operation: Operation) -> list[Operation]:
        """Return direct child operations for a service operation."""
        return [
            candidate
            for candidate in self._operation_source()
            if candidate.parent_id == operation.operation_id
        ]

    def get_all_operations(self) -> list[Operation]:
        """Return all non-execution operations, including nested operations."""
        return [
            operation
            for operation in self._operation_source()
            if operation.operation_type != OperationType.EXECUTION
        ]

    def _operation_source(self) -> list[Operation]:
        return self._all_operations or self.operations

    def _get_operation_by_name_and_type(
        self, name: str, operation_type: OperationType
    ) -> Operation:
        operation = self.get_operation_by_name(name)
        if operation.operation_type != operation_type:
            msg = (
                f"Operation with name '{name}' has type "
                f"{operation.operation_type}, expected {operation_type}"
            )
            raise DurableFunctionsTestError(msg)
        return operation


def _deserialize_operation_payload(
    payload: OperationPayload | None,
    serdes: ExtendedTypeSerDes | None = None,
) -> Any:
    """Deserialize an operation payload using the provided or default serializer."""
    if not payload:
        return None

    if serdes is None:
        serdes = ExtendedTypeSerDes()

    try:
        return serdes.deserialize_sync(payload)
    except Exception:
        return json.loads(payload)


def _get_callback_id_from_events(
    events: list[Event], name: str | None = None
) -> str | None:
    """
    Get callback ID from execution history for callbacks that haven't completed.

    Args:
        execution_arn: The ARN of the execution to query.
        name: Optional callback name to search for. If not provided, returns the latest callback.

    Returns:
        The callback ID string for a non-completed callback whose creating
        invocation has completed, or None if not found.

    Raises:
        DurableFunctionsTestError: If the named callback has already succeeded/failed/timed out.
    """
    callback_started_events = [
        event for event in events if event.event_type == "CallbackStarted"
    ]

    if not callback_started_events:
        return None

    completed_callback_ids = {
        event.event_id
        for event in events
        if event.event_type
        in ["CallbackSucceeded", "CallbackFailed", "CallbackTimedOut"]
    }

    def is_callback_ready(callback_started_event: Event) -> bool:
        for event in events[events.index(callback_started_event) + 1 :]:
            if event.event_type == "InvocationCompleted":
                return True
        return False

    if name is not None:
        for event in callback_started_events:
            if event.name == name:
                callback_id = event.event_id
                if callback_id in completed_callback_ids:
                    raise DurableFunctionsTestError(
                        f"Callback {name} has already completed (succeeded/failed/timed out)"
                    )
                if not is_callback_ready(event):
                    return None
                return (
                    event.callback_started_details.callback_id
                    if event.callback_started_details
                    else None
                )
        return None

    # If name is not provided, find the latest non-completed callback event
    active_callbacks = [
        event
        for event in callback_started_events
        if event.event_id not in completed_callback_ids and is_callback_ready(event)
    ]

    if not active_callbacks:
        return None

    latest_event = active_callbacks[-1]
    return (
        latest_event.callback_started_details.callback_id
        if latest_event.callback_started_details
        else None
    )


@dataclass(frozen=True)
class InvokeResponse:
    """Response from invoking a durable function."""

    invocation_output: DurableExecutionInvocationOutput
    request_id: str
