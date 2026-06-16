from __future__ import annotations

import copy
import datetime
from collections.abc import Mapping, MutableMapping
from dataclasses import MISSING, dataclass, field, fields
from enum import Enum
from typing import Any, TypeAlias, get_args, get_origin, get_type_hints

from async_durable_execution.exceptions import CallableRuntimeError

# Replace with `type` it when dropping support to Python 3.11
ReplayChildren: TypeAlias = bool
OperationPayload: TypeAlias = str
TimeoutSeconds: TypeAlias = int


@dataclass(frozen=True)
class RetryDecision:
    """Decision about whether to retry an operation and with what delay."""

    should_retry: bool
    delay: datetime.timedelta

    def __post_init__(self):
        if self.delay.total_seconds() < 0:
            msg = "delay must be non-negative"
            raise ValueError(msg)

    @property
    def delay_seconds(self) -> int:
        """Get delay in seconds."""
        return int(self.delay.total_seconds())

    @classmethod
    def retry(cls, delay: datetime.timedelta) -> "RetryDecision":
        """Create a retry decision."""
        return cls(should_retry=True, delay=delay)

    @classmethod
    def retry_after_delay(cls, delay_seconds: int | float) -> "RetryDecision":
        """Create a retry decision from a delay in seconds."""
        return cls.retry(datetime.timedelta(seconds=delay_seconds))

    @classmethod
    def no_retry(cls) -> "RetryDecision":
        """Create a no-retry decision."""
        return cls(should_retry=False, delay=datetime.timedelta())


@dataclass(frozen=True)
class WaitDecision:
    """Decision about whether to wait and with what delay."""

    should_wait: bool
    delay: datetime.timedelta

    def __post_init__(self):
        if self.delay.total_seconds() < 0:
            msg = "delay must be non-negative"
            raise ValueError(msg)

    @property
    def delay_seconds(self) -> int:
        """Get delay in seconds."""
        return int(self.delay.total_seconds())

    @classmethod
    def wait(cls, delay: datetime.timedelta) -> "WaitDecision":
        """Create a wait decision."""
        return cls(should_wait=True, delay=delay)

    @classmethod
    def no_wait(cls) -> "WaitDecision":
        """Create a no-wait decision."""
        return cls(should_wait=False, delay=datetime.timedelta())


@dataclass(frozen=True)
class WaitForConditionDecision:
    """Decision about whether to continue waiting."""

    should_continue: bool
    delay: datetime.timedelta

    def __post_init__(self):
        if self.delay.total_seconds() < 0:
            msg = "delay must be non-negative"
            raise ValueError(msg)

    @property
    def delay_seconds(self) -> int:
        """Get delay in seconds."""
        return int(self.delay.total_seconds())

    @classmethod
    def continue_waiting(cls, delay: datetime.timedelta) -> "WaitForConditionDecision":
        """Create a decision to continue waiting."""
        return cls(should_continue=True, delay=delay)

    @classmethod
    def stop_polling(cls) -> "WaitForConditionDecision":
        """Create a decision to stop polling."""
        return cls(should_continue=False, delay=datetime.timedelta())


def _metadata(
    *,
    alias: str,
    serializer: Any = None,
    deserializer: Any = None,
    omit_if_none: bool = True,
    omit_if_falsey: bool = False,
) -> dict[str, Any]:
    return {
        "alias": alias,
        "serializer": serializer,
        "deserializer": deserializer,
        "omit_if_none": omit_if_none,
        "omit_if_falsey": omit_if_falsey,
    }


def _enum_type(annotation: Any) -> type[Enum] | None:
    if isinstance(annotation, type) and issubclass(annotation, Enum):
        return annotation

    origin = get_origin(annotation)
    if origin is None:
        return None

    for arg in get_args(annotation):
        enum_cls = _enum_type(arg)
        if enum_cls is not None:
            return enum_cls

    return None


def _model_type(annotation: Any) -> type[SerializableModel] | None:
    if isinstance(annotation, type) and issubclass(annotation, SerializableModel):
        return annotation

    origin = get_origin(annotation)
    if origin is None:
        return None

    for arg in get_args(annotation):
        model_cls = _model_type(arg)
        if model_cls is not None:
            return model_cls

    return None


def _deserialize_value(value: Any, annotation: Any, metadata: Mapping[str, Any]) -> Any:
    if value is None:
        return None

    custom_deserializer = metadata.get("deserializer")
    if custom_deserializer is not None:
        return custom_deserializer(value)

    model_cls = _model_type(annotation)
    if model_cls is not None and isinstance(value, Mapping):
        return model_cls.from_dict(value)

    enum_cls = _enum_type(annotation)
    if enum_cls is not None:
        return enum_cls(value)

    origin = get_origin(annotation)
    if origin is list:
        args = get_args(annotation)
        item_annotation = args[0] if args else Any
        return [_deserialize_value(item, item_annotation, {}) for item in value]

    return value


def _serialize_value(value: Any, metadata: Mapping[str, Any]) -> Any:
    if value is None:
        return None

    custom_serializer = metadata.get("serializer")
    if custom_serializer is not None:
        return custom_serializer(value)

    if isinstance(value, Enum):
        return value.value

    if isinstance(value, SerializableModel):
        return value.to_dict()

    if isinstance(value, list):
        return [_serialize_value(item, {}) for item in value]

    return value


@dataclass(frozen=True)
class SerializableModel:
    @classmethod
    def from_dict(cls, data: Mapping[str, Any]):
        kwargs: dict[str, Any] = {}
        type_hints = get_type_hints(cls)

        for model_field in fields(cls):
            alias = model_field.metadata.get("alias", model_field.name)
            annotation = type_hints.get(model_field.name, model_field.type)

            if alias in data:
                kwargs[model_field.name] = _deserialize_value(
                    data[alias], annotation, model_field.metadata
                )
                continue

            if (
                model_field.default is not MISSING
                or model_field.default_factory is not MISSING
            ):
                continue

            raise KeyError(alias)

        return cls(**kwargs)

    def to_dict(self) -> MutableMapping[str, Any]:
        result: MutableMapping[str, Any] = {}

        for model_field in fields(self):
            alias = model_field.metadata.get("alias", model_field.name)
            value = getattr(self, model_field.name)

            if value is None and model_field.metadata.get("omit_if_none", True):
                continue
            if not value and model_field.metadata.get("omit_if_falsey", False):
                continue

            result[alias] = _serialize_value(value, model_field.metadata)

        return result


class OperationAction(Enum):
    START = "START"
    SUCCEED = "SUCCEED"
    FAIL = "FAIL"
    RETRY = "RETRY"
    CANCEL = "CANCEL"


class OperationStatus(Enum):
    STARTED = "STARTED"
    PENDING = "PENDING"
    READY = "READY"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    TIMED_OUT = "TIMED_OUT"
    STOPPED = "STOPPED"


class CallbackTimeoutType(Enum):
    TIMEOUT = "Callback.Timeout"
    HEARTBEAT = "Callback.Heartbeat"


class ChainedInvokeFailedToStartType(Enum):
    FAILED_TO_START = "ChainedInvoke.FailedToStart"


class ChainedInvokeTimeoutType(Enum):
    TIMEOUT = "ChainedInvoke.Timeout"


class ChainedInvokeStopType(Enum):
    STOPPED = "ChainedInvoke.Stopped"


class OperationSubType(Enum):
    STEP = "Step"
    WAIT = "Wait"
    CALLBACK = "Callback"
    RUN_IN_CHILD_CONTEXT = "RunInChildContext"
    MAP = "Map"
    MAP_ITERATION = "MapIteration"
    PARALLEL = "Parallel"
    PARALLEL_BRANCH = "ParallelBranch"
    WAIT_FOR_CALLBACK = "WaitForCallback"
    WAIT_FOR_CONDITION = "WaitForCondition"
    CHAINED_INVOKE = "ChainedInvoke"


class OperationType(Enum):
    EXECUTION = "EXECUTION"
    CONTEXT = "CONTEXT"
    STEP = "STEP"
    WAIT = "WAIT"
    CALLBACK = "CALLBACK"
    CHAINED_INVOKE = "CHAINED_INVOKE"

    @classmethod
    def from_sub_type(cls, sub_type: OperationSubType) -> OperationType:
        match sub_type:
            case OperationSubType.STEP | OperationSubType.WAIT_FOR_CONDITION:
                return OperationType.STEP
            case OperationSubType.WAIT:
                return OperationType.WAIT
            case OperationSubType.CHAINED_INVOKE:
                return OperationType.CHAINED_INVOKE
            case OperationSubType.CALLBACK:
                return OperationType.CALLBACK
            case (
                OperationSubType.WAIT_FOR_CALLBACK
                | OperationSubType.RUN_IN_CHILD_CONTEXT
                | OperationSubType.MAP
                | OperationSubType.MAP_ITERATION
                | OperationSubType.PARALLEL
                | OperationSubType.PARALLEL_BRANCH
            ):
                return OperationType.CONTEXT
            case _:
                raise ValueError(f"Unknown operation sub-type {sub_type}")


@dataclass(frozen=True)
class OperationIdentifier:
    """Container for operation id, parent id, and name."""

    operation_id: str
    sub_type: OperationSubType
    parent_id: str | None = None
    name: str | None = None

    @property
    def type(self) -> OperationType:
        return OperationType.from_sub_type(self.sub_type)


class InvocationStatus(Enum):
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    PENDING = "PENDING"

    # Used internally only: the invocation failed and the backend will retry
    RETRY = "RETRY"


@dataclass(frozen=True)
class ErrorObject(SerializableModel):
    message: str | None = field(default=None, metadata=_metadata(alias="ErrorMessage"))
    type: str | None = field(default=None, metadata=_metadata(alias="ErrorType"))
    data: str | None = field(default=None, metadata=_metadata(alias="ErrorData"))
    stack_trace: list[str] | None = field(
        default=None, metadata=_metadata(alias="StackTrace")
    )

    @classmethod
    def from_exception(cls, exception: Exception) -> ErrorObject:
        return cls(
            message=str(exception),
            type=type(exception).__name__,
            data=None,
            stack_trace=None,
        )

    @classmethod
    def from_message(cls, message: str) -> ErrorObject:
        return cls(
            message=message,
            type=None,
            data=None,
            stack_trace=None,
        )

    def to_callable_runtime_error(self) -> CallableRuntimeError:
        return CallableRuntimeError(
            message=self.message,
            error_type=self.type,
            data=self.data,
            stack_trace=self.stack_trace,
        )


@dataclass(frozen=True)
class DurableExecutionInvocationOutput(SerializableModel):
    """Representation the DurableExecutionInvocationOutput. This is what the Durable lambda handler returns.

    If the execution has been already completed via an update to the EXECUTION operation via CheckpointDurableExecution,
    payload must be empty for SUCCEEDED/FAILED status.
    """

    status: InvocationStatus = field(metadata=_metadata(alias="Status"))
    result: str | None = field(default=None, metadata=_metadata(alias="Result"))
    error: ErrorObject | None = field(default=None, metadata=_metadata(alias="Error"))

    @classmethod
    def create_succeeded(cls, result: str) -> DurableExecutionInvocationOutput:
        return cls(status=InvocationStatus.SUCCEEDED, result=result)

    @classmethod
    def create_retry(cls, error: ErrorObject) -> DurableExecutionInvocationOutput:
        return cls(status=InvocationStatus.RETRY, error=error)


@dataclass(frozen=True)
class ExecutionDetails(SerializableModel):
    input_payload: str | None = field(
        default=None,
        metadata=_metadata(alias="InputPayload", omit_if_none=False),
    )


@dataclass(frozen=True)
class ContextDetails(SerializableModel):
    replay_children: ReplayChildren = field(
        default=False, metadata=_metadata(alias="ReplayChildren")
    )
    result: OperationPayload | None = field(
        default=None, metadata=_metadata(alias="Result")
    )
    error: ErrorObject | None = field(default=None, metadata=_metadata(alias="Error"))


@dataclass(frozen=True)
class StepDetails(SerializableModel):
    attempt: int = field(default=0, metadata=_metadata(alias="Attempt"))
    next_attempt_timestamp: datetime.datetime | None = field(
        default=None,
        metadata=_metadata(alias="NextAttemptTimestamp"),
    )
    result: OperationPayload | None = field(
        default=None,
        metadata=_metadata(alias="Result"),
    )
    error: ErrorObject | None = field(
        default=None,
        metadata=_metadata(alias="Error"),
    )


@dataclass(frozen=True)
class WaitDetails(SerializableModel):
    scheduled_end_timestamp: datetime.datetime | None = field(
        default=None,
        metadata=_metadata(alias="ScheduledEndTimestamp"),
    )


@dataclass(frozen=True)
class CallbackDetails(SerializableModel):
    callback_id: str = field(metadata=_metadata(alias="CallbackId"))
    result: str | None = field(default=None, metadata=_metadata(alias="Result"))
    error: ErrorObject | None = field(default=None, metadata=_metadata(alias="Error"))


@dataclass(frozen=True)
class ChainedInvokeDetails(SerializableModel):
    result: str | None = field(default=None, metadata=_metadata(alias="Result"))
    error: ErrorObject | None = field(default=None, metadata=_metadata(alias="Error"))


@dataclass(frozen=True)
class StepOptions(SerializableModel):
    next_attempt_delay_seconds: int = field(
        default=0,
        metadata=_metadata(alias="NextAttemptDelaySeconds"),
    )


@dataclass(frozen=True)
class WaitOptions(SerializableModel):
    """
    Wait Options provides details regarding suspension.

    As of 2025/10/27:

    - `wait_seconds` accepts values between 1, and 31622400
    - When wait_second seconds does not exist,then we default to 1

    """

    wait_seconds: int = field(default=1, metadata=_metadata(alias="WaitSeconds"))


@dataclass(frozen=True)
class CallbackOptions(SerializableModel):
    """
    Callback options provides details about the callback, wrt timeout
    and heartbeat checks.

    As of 2025/10/27:
    - When timeout_seconds == 0, then the callback has no timeout
    - When heartbeat_timeout_seconds == 0, then the callback has no timeout

    - When timeout_seconds is not present, then default is 0
    - When heartbeat_timeout_seconds, then default is 0

    """

    timeout_seconds: TimeoutSeconds = field(
        default=0, metadata=_metadata(alias="TimeoutSeconds")
    )
    heartbeat_timeout_seconds: int = field(
        default=0,
        metadata=_metadata(alias="HeartbeatTimeoutSeconds"),
    )


@dataclass(frozen=True)
class ChainedInvokeOptions(SerializableModel):
    """
    As of 2025/10/27:
     - Chained invoke options only contains a function name
    """

    function_name: str = field(metadata=_metadata(alias="FunctionName"))
    tenant_id: str | None = field(default=None, metadata=_metadata(alias="TenantId"))


@dataclass(frozen=True)
class ContextOptions(SerializableModel):
    replay_children: ReplayChildren = field(
        default=False,
        metadata=_metadata(alias="ReplayChildren"),
    )


@dataclass(frozen=True)
class OperationUpdate(SerializableModel):
    """Update an Operation. Use this to create a checkpoint.

    See the various create_ factory class methods to instantiate me.
    """

    operation_id: str = field(metadata=_metadata(alias="Id"))
    operation_type: OperationType = field(metadata=_metadata(alias="Type"))
    action: OperationAction = field(metadata=_metadata(alias="Action"))
    parent_id: str | None = field(
        default=None,
        metadata=_metadata(alias="ParentId", omit_if_falsey=True),
    )
    name: str | None = field(
        default=None,
        metadata=_metadata(alias="Name", omit_if_falsey=True),
    )
    sub_type: OperationSubType | None = field(
        default=None,
        metadata=_metadata(alias="SubType"),
    )
    payload: str | None = field(
        default=None,
        metadata=_metadata(alias="Payload", omit_if_falsey=True),
    )
    error: ErrorObject | None = field(default=None, metadata=_metadata(alias="Error"))
    context_options: ContextOptions | None = field(
        default=None,
        metadata=_metadata(alias="ContextOptions"),
    )
    step_options: StepOptions | None = field(
        default=None,
        metadata=_metadata(alias="StepOptions"),
    )
    wait_options: WaitOptions | None = field(
        default=None,
        metadata=_metadata(alias="WaitOptions"),
    )
    callback_options: CallbackOptions | None = field(
        default=None,
        metadata=_metadata(alias="CallbackOptions"),
    )
    chained_invoke_options: ChainedInvokeOptions | None = field(
        default=None,
        metadata=_metadata(alias="ChainedInvokeOptions"),
    )

    @classmethod
    def create_callback(
        cls, identifier: OperationIdentifier, callback_options: CallbackOptions
    ) -> OperationUpdate:
        """Create an instance of OperationUpdate for type:CALLBACK, action:START"""
        return cls(
            operation_id=identifier.operation_id,
            parent_id=identifier.parent_id,
            operation_type=OperationType.CALLBACK,
            sub_type=OperationSubType.CALLBACK,
            action=OperationAction.START,
            name=identifier.name,
            callback_options=callback_options,
        )

    @classmethod
    def create_context_start(
        cls, identifier: OperationIdentifier, sub_type: OperationSubType
    ) -> OperationUpdate:
        """Create an instance of OperationUpdate for type: CONTEXT, action: START."""
        return cls(
            operation_id=identifier.operation_id,
            parent_id=identifier.parent_id,
            operation_type=OperationType.CONTEXT,
            sub_type=sub_type,
            action=OperationAction.START,
            name=identifier.name,
        )

    @classmethod
    def create_context_succeed(
        cls,
        identifier: OperationIdentifier,
        payload: str,
        sub_type: OperationSubType,
        context_options: ContextOptions | None = None,
    ) -> OperationUpdate:
        """Create an instance of OperationUpdate for type: CONTEXT, action: SUCCEED."""
        return cls(
            operation_id=identifier.operation_id,
            parent_id=identifier.parent_id,
            operation_type=OperationType.CONTEXT,
            sub_type=sub_type,
            action=OperationAction.SUCCEED,
            name=identifier.name,
            payload=payload,
            context_options=context_options,
        )

    @classmethod
    def create_context_fail(
        cls,
        identifier: OperationIdentifier,
        error: ErrorObject,
        sub_type: OperationSubType,
    ) -> OperationUpdate:
        """Create an instance of OperationUpdate for type: CONTEXT, action: FAIL."""
        return cls(
            operation_id=identifier.operation_id,
            parent_id=identifier.parent_id,
            operation_type=OperationType.CONTEXT,
            sub_type=sub_type,
            action=OperationAction.FAIL,
            name=identifier.name,
            error=error,
        )

    @classmethod
    def create_execution_succeed(cls, payload: str) -> OperationUpdate:
        """Create an instance of OperationUpdate for type: EXECUTION, action: SUCCEED."""
        return cls(
            operation_id=f"execution-result-{int(datetime.datetime.now(tz=datetime.UTC).timestamp() * 1000)}",
            operation_type=OperationType.EXECUTION,
            action=OperationAction.SUCCEED,
            payload=payload,
        )

    @classmethod
    def create_execution_fail(cls, error: ErrorObject) -> OperationUpdate:
        """Create an instance of OperationUpdate for type: EXECUTION, action: FAIL."""
        return cls(
            operation_id=f"execution-result-{int(datetime.datetime.now(tz=datetime.UTC).timestamp() * 1000)}",
            operation_type=OperationType.EXECUTION,
            action=OperationAction.FAIL,
            error=error,
        )

    @classmethod
    def create_step_succeed(
        cls, identifier: OperationIdentifier, payload: str
    ) -> OperationUpdate:
        """Create an instance of OperationUpdate for type: STEP, action: SUCCEED."""
        return cls(
            operation_id=identifier.operation_id,
            parent_id=identifier.parent_id,
            operation_type=OperationType.STEP,
            sub_type=OperationSubType.STEP,
            action=OperationAction.SUCCEED,
            name=identifier.name,
            payload=payload,
        )

    @classmethod
    def create_step_fail(
        cls, identifier: OperationIdentifier, error: ErrorObject
    ) -> OperationUpdate:
        """Create an instance of OperationUpdate for type: STEP, action: FAIL."""
        return cls(
            operation_id=identifier.operation_id,
            parent_id=identifier.parent_id,
            operation_type=OperationType.STEP,
            sub_type=OperationSubType.STEP,
            action=OperationAction.FAIL,
            name=identifier.name,
            error=error,
        )

    @classmethod
    def create_step_start(cls, identifier: OperationIdentifier) -> OperationUpdate:
        """Create an instance of OperationUpdate for type: STEP, action: START."""
        return cls(
            operation_id=identifier.operation_id,
            parent_id=identifier.parent_id,
            operation_type=OperationType.STEP,
            sub_type=OperationSubType.STEP,
            action=OperationAction.START,
            name=identifier.name,
        )

    @classmethod
    def create_step_retry(
        cls,
        identifier: OperationIdentifier,
        error: ErrorObject,
        next_attempt_delay_seconds: int,
    ) -> OperationUpdate:
        """Create an instance of OperationUpdate for type: STEP, action: RETRY."""
        return cls(
            operation_id=identifier.operation_id,
            parent_id=identifier.parent_id,
            operation_type=OperationType.STEP,
            sub_type=OperationSubType.STEP,
            action=OperationAction.RETRY,
            name=identifier.name,
            error=error,
            step_options=StepOptions(
                next_attempt_delay_seconds=next_attempt_delay_seconds
            ),
        )

    @classmethod
    def create_invoke_start(
        cls,
        identifier: OperationIdentifier,
        payload: str,
        chained_invoke_options: ChainedInvokeOptions,
    ) -> OperationUpdate:
        """Create an instance of OperationUpdate for type: INVOKE, action: START."""
        return cls(
            operation_id=identifier.operation_id,
            parent_id=identifier.parent_id,
            operation_type=OperationType.CHAINED_INVOKE,
            sub_type=OperationSubType.CHAINED_INVOKE,
            action=OperationAction.START,
            name=identifier.name,
            payload=payload,
            chained_invoke_options=chained_invoke_options,
        )

    @classmethod
    def create_wait_for_condition_start(
        cls, identifier: OperationIdentifier
    ) -> OperationUpdate:
        """Create an instance of OperationUpdate for type: STEP, action: START."""
        return cls(
            operation_id=identifier.operation_id,
            parent_id=identifier.parent_id,
            operation_type=OperationType.STEP,
            sub_type=OperationSubType.WAIT_FOR_CONDITION,
            action=OperationAction.START,
            name=identifier.name,
        )

    @classmethod
    def create_wait_for_condition_succeed(
        cls, identifier: OperationIdentifier, payload: str
    ) -> OperationUpdate:
        """Create an instance of OperationUpdate for type: STEP, action: SUCCEED."""
        return cls(
            operation_id=identifier.operation_id,
            parent_id=identifier.parent_id,
            operation_type=OperationType.STEP,
            sub_type=OperationSubType.WAIT_FOR_CONDITION,
            action=OperationAction.SUCCEED,
            name=identifier.name,
            payload=payload,
        )

    @classmethod
    def create_wait_for_condition_retry(
        cls,
        identifier: OperationIdentifier,
        payload: str,
        next_attempt_delay_seconds: int,
    ) -> OperationUpdate:
        """Create an instance of OperationUpdate for type: STEP, action: RETRY."""
        return cls(
            operation_id=identifier.operation_id,
            parent_id=identifier.parent_id,
            operation_type=OperationType.STEP,
            sub_type=OperationSubType.WAIT_FOR_CONDITION,
            action=OperationAction.RETRY,
            name=identifier.name,
            payload=payload,
            step_options=StepOptions(
                next_attempt_delay_seconds=next_attempt_delay_seconds
            ),
        )

    @classmethod
    def create_wait_for_condition_fail(
        cls, identifier: OperationIdentifier, error: ErrorObject
    ) -> OperationUpdate:
        """Create an instance of OperationUpdate for type: STEP, action: FAIL."""
        return cls(
            operation_id=identifier.operation_id,
            parent_id=identifier.parent_id,
            operation_type=OperationType.STEP,
            sub_type=OperationSubType.WAIT_FOR_CONDITION,
            action=OperationAction.FAIL,
            name=identifier.name,
            error=error,
        )

    @classmethod
    def create_wait_start(
        cls, identifier: OperationIdentifier, wait_options: WaitOptions
    ) -> OperationUpdate:
        """Create an instance of OperationUpdate for type: WAIT, action: START."""
        return cls(
            operation_id=identifier.operation_id,
            parent_id=identifier.parent_id,
            operation_type=OperationType.WAIT,
            sub_type=OperationSubType.WAIT,
            action=OperationAction.START,
            name=identifier.name,
            wait_options=wait_options,
        )


class TimestampConverter:
    """Converter for datetime/Unix timestamp conversions."""

    @staticmethod
    def to_unix_millis(dt: datetime.datetime | None) -> int | None:
        """Convert datetime to Unix timestamp in milliseconds."""
        return int(dt.timestamp() * 1000) if dt else None

    @staticmethod
    def from_unix_millis(ms: int | None) -> datetime.datetime | None:
        """Convert Unix timestamp in milliseconds to datetime."""
        return (
            datetime.datetime.fromtimestamp(ms / 1000, tz=datetime.UTC)
            if ms is not None
            else None
        )


@dataclass(frozen=True)
class Operation(SerializableModel):
    """Represent the Operation type for GetDurableExecutionState and CheckpointDurableExecution."""

    operation_id: str = field(metadata=_metadata(alias="Id"))
    operation_type: OperationType = field(metadata=_metadata(alias="Type"))
    status: OperationStatus = field(metadata=_metadata(alias="Status"))
    parent_id: str | None = field(
        default=None,
        metadata=_metadata(alias="ParentId", omit_if_falsey=True),
    )
    name: str | None = field(
        default=None,
        metadata=_metadata(alias="Name", omit_if_falsey=True),
    )
    start_timestamp: datetime.datetime | None = field(
        default=None,
        metadata=_metadata(alias="StartTimestamp"),
    )
    end_timestamp: datetime.datetime | None = field(
        default=None,
        metadata=_metadata(alias="EndTimestamp"),
    )
    sub_type: OperationSubType | None = field(
        default=None,
        metadata=_metadata(alias="SubType"),
    )
    execution_details: ExecutionDetails | None = field(
        default=None,
        metadata=_metadata(alias="ExecutionDetails"),
    )
    context_details: ContextDetails | None = field(
        default=None,
        metadata=_metadata(alias="ContextDetails"),
    )
    step_details: StepDetails | None = field(
        default=None,
        metadata=_metadata(alias="StepDetails"),
    )
    wait_details: WaitDetails | None = field(
        default=None,
        metadata=_metadata(alias="WaitDetails"),
    )
    callback_details: CallbackDetails | None = field(
        default=None,
        metadata=_metadata(alias="CallbackDetails"),
    )
    chained_invoke_details: ChainedInvokeDetails | None = field(
        default=None,
        metadata=_metadata(alias="ChainedInvokeDetails"),
    )

    def to_json_dict(self) -> MutableMapping[str, Any]:
        """Convert the Operation to a JSON-serializable dictionary.

        Converts datetime objects to millisecond timestamps for JSON compatibility.

        Returns:
            A dictionary with JSON-serializable values
        """
        result = self.to_dict()

        if ts := result.get("StartTimestamp"):
            result["StartTimestamp"] = TimestampConverter.to_unix_millis(ts)

        if ts := result.get("EndTimestamp"):
            result["EndTimestamp"] = TimestampConverter.to_unix_millis(ts)

        if (step_details := result.get("StepDetails")) and (
            ts := step_details.get("NextAttemptTimestamp")
        ):
            result["StepDetails"]["NextAttemptTimestamp"] = (
                TimestampConverter.to_unix_millis(ts)
            )

        if (wait_details := result.get("WaitDetails")) and (
            ts := wait_details.get("ScheduledEndTimestamp")
        ):
            result["WaitDetails"]["ScheduledEndTimestamp"] = (
                TimestampConverter.to_unix_millis(ts)
            )

        return result

    @classmethod
    def from_json_dict(cls, data: MutableMapping[str, Any]) -> Operation:
        """Create an Operation from a JSON-serializable dictionary.

        Converts millisecond timestamps back to datetime objects.

        Args:
            data: Dictionary with JSON-serializable values (millisecond timestamps)

        Returns:
            An Operation instance with datetime objects
        """
        data_copy = copy.deepcopy(data)

        if ms := data_copy.get("StartTimestamp"):
            data_copy["StartTimestamp"] = TimestampConverter.from_unix_millis(ms)

        if ms := data_copy.get("EndTimestamp"):
            data_copy["EndTimestamp"] = TimestampConverter.from_unix_millis(ms)

        if (step_details := data_copy.get("StepDetails")) and (
            ms := step_details.get("NextAttemptTimestamp")
        ):
            step_details["NextAttemptTimestamp"] = TimestampConverter.from_unix_millis(
                ms
            )

        if (wait_details := data_copy.get("WaitDetails")) and (
            ms := wait_details.get("ScheduledEndTimestamp")
        ):
            wait_details["ScheduledEndTimestamp"] = TimestampConverter.from_unix_millis(
                ms
            )

        return cls.from_dict(data_copy)


@dataclass(frozen=True)
class CheckpointUpdatedExecutionState(SerializableModel):
    """Representation of the CheckpointUpdatedExecutionState structure of the DEX API."""

    operations: list[Operation] = field(
        default_factory=list,
        metadata=_metadata(alias="Operations"),
    )
    next_marker: str | None = field(
        default=None, metadata=_metadata(alias="NextMarker")
    )


@dataclass(frozen=True)
class CheckpointOutput(SerializableModel):
    """Representation of the CheckpointDurableExecutionOutput structure of the DEX CheckpointDurableExecution API."""

    checkpoint_token: str = field(
        default="",
        metadata=_metadata(alias="CheckpointToken", omit_if_none=False),
    )
    new_execution_state: CheckpointUpdatedExecutionState = field(
        default_factory=CheckpointUpdatedExecutionState,
        metadata=_metadata(alias="NewExecutionState", omit_if_none=False),
    )


@dataclass(frozen=True)
class StateOutput(SerializableModel):
    """Representation of the GetDurableExecutionStateOutput structure of the DEX GetDurableExecutionState API."""

    operations: list[Operation] = field(
        default_factory=list,
        metadata=_metadata(alias="Operations"),
    )
    next_marker: str | None = field(
        default=None, metadata=_metadata(alias="NextMarker")
    )


__all__ = [
    "CallbackDetails",
    "CallbackOptions",
    "CallbackTimeoutType",
    "ChainedInvokeDetails",
    "ChainedInvokeFailedToStartType",
    "ChainedInvokeOptions",
    "ChainedInvokeStopType",
    "ChainedInvokeTimeoutType",
    "CheckpointOutput",
    "CheckpointUpdatedExecutionState",
    "ContextDetails",
    "ContextOptions",
    "DurableExecutionInvocationOutput",
    "ErrorObject",
    "ExecutionDetails",
    "InvocationStatus",
    "Operation",
    "OperationAction",
    "OperationPayload",
    "OperationStatus",
    "OperationSubType",
    "OperationType",
    "OperationUpdate",
    "ReplayChildren",
    "StateOutput",
    "StepDetails",
    "StepOptions",
    "TimeoutSeconds",
    "TimestampConverter",
    "WaitDetails",
    "WaitOptions",
]
