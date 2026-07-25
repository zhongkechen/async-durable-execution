"""Exceptions for the Durable Executions SDK.

Avoid any non-stdlib references in this module, it is at the bottom of the dependency chain.
"""

from __future__ import annotations

import datetime
import json
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, NoReturn, TypedDict, cast

BAD_REQUEST_ERROR: int = 400
TOO_MANY_REQUESTS_ERROR: int = 429
SERVICE_ERROR: int = 500
INVALID_PARAMETER_VALUE_EXCEPTION: str = "InvalidParameterValueException"
INVALID_CHECKPOINT_TOKEN_PREFIX: str = "Invalid Checkpoint Token"
_SDK_ERROR_DATA_KEY: str = "__async_durable_execution_error__"
_SDK_ERROR_DATA_VERSION: int = 1
_SDK_INVOCATION_ERROR_PAYLOAD_VERSION: int = 1

# Non-retryable customer error codes that arrive as non-4xx (e.g. HTTP 502) from Lambda.
# Unlike typical 5xx errors, these require customer intervention (e.g., fixing
# a KMS key configuration) and will never succeed on retry.
# Add new non-retryable error codes here — they are automatically classified
# as EXECUTION (non-retryable) by _classify_error_category().
_NON_RETRYABLE_CUSTOMER_ERROR_CODES: frozenset[str] = frozenset(
    {
        "KMSAccessDeniedException",
        "KMSDisabledException",
        "KMSInvalidStateException",
        "KMSNotFoundException",
    }
)


def _encode_sdk_error_data(
    exception_type: type[Exception],
    payload: str | None = None,
) -> str:
    """Encode SDK-owned exception metadata for durable replay."""
    return json.dumps(
        {
            _SDK_ERROR_DATA_KEY: _SDK_ERROR_DATA_VERSION,
            "exception_type": (
                f"{exception_type.__module__}.{exception_type.__qualname__}"
            ),
            "payload": payload,
        },
        separators=(",", ":"),
        sort_keys=True,
    )


def _decode_sdk_error_data(
    data: str | None,
    expected_exception_type: type[Exception],
) -> tuple[bool, str | None]:
    """Return whether data identifies the expected SDK exception and its payload."""
    if data is None:
        return False, None

    try:
        decoded = json.loads(data)
    except (TypeError, ValueError):
        return False, None

    if not isinstance(decoded, dict):
        return False, None

    version = decoded.get(_SDK_ERROR_DATA_KEY)
    if type(version) is not int or version != _SDK_ERROR_DATA_VERSION:
        return False, None

    expected_name = (
        f"{expected_exception_type.__module__}.{expected_exception_type.__qualname__}"
    )
    if decoded.get("exception_type") != expected_name:
        return False, None

    payload = decoded.get("payload")
    if payload is not None and not isinstance(payload, str):
        return False, None

    return True, payload


def _encode_sdk_control_error_data(error: Exception) -> str | None:
    """Encode the SDK-owned control category of an exception, if any."""
    if isinstance(error, InvocationError):
        return _encode_sdk_error_data(
            InvocationError,
            _encode_sdk_invocation_error_payload(error),
        )
    if isinstance(error, ExecutionError):
        return _encode_sdk_error_data(ExecutionError)
    if isinstance(error, SerDesError):
        return _encode_sdk_error_data(SerDesError)
    return None


class AwsErrorObj(TypedDict):
    """Subset of a boto-style AWS error payload."""

    Code: str | None
    Message: str | None


class AwsErrorMetadata(TypedDict, total=False):
    """Subset of boto response metadata used for retry classification."""

    RequestId: str | None
    HostId: str | None
    HTTPStatusCode: int | None
    HTTPHeaders: str | None
    RetryAttempts: str | None


class TerminationReason(Enum):
    """Reasons why a durable execution terminated."""

    UNHANDLED_ERROR = "UNHANDLED_ERROR"
    INVOCATION_ERROR = "INVOCATION_ERROR"
    EXECUTION_ERROR = "EXECUTION_ERROR"
    CHECKPOINT_FAILED = "CHECKPOINT_FAILED"
    NON_DETERMINISTIC_EXECUTION = "NON_DETERMINISTIC_EXECUTION"
    STEP_INTERRUPTED = "STEP_INTERRUPTED"
    CALLBACK_ERROR = "CALLBACK_ERROR"
    SERIALIZATION_ERROR = "SERIALIZATION_ERROR"


class DurableExecutionsError(Exception):
    """Base class for Durable Executions exceptions"""


class UnrecoverableError(DurableExecutionsError):
    """Base class for errors that terminate execution."""

    def __init__(self, message: str, termination_reason: TerminationReason):
        super().__init__(message)
        self.termination_reason = termination_reason


class ExecutionError(UnrecoverableError):
    """Error that returns FAILED status without retry."""

    def __init__(
        self,
        message: str,
        termination_reason: TerminationReason = TerminationReason.EXECUTION_ERROR,
    ):
        super().__init__(message, termination_reason)


class WaitForConditionError(ExecutionError):
    """Raised when a wait_for_condition operation exhausts its attempts."""


class CallbackError(ExecutionError):
    """Error in callback handling."""

    def __init__(self, message: str, callback_id: str | None = None):
        super().__init__(message, TerminationReason.CALLBACK_ERROR)
        self.callback_id = callback_id


class InvocationError(UnrecoverableError):
    """Error that should cause Lambda retry by throwing from handler."""

    def __init__(
        self,
        message: str,
        termination_reason: TerminationReason = TerminationReason.INVOCATION_ERROR,
    ):
        super().__init__(message, termination_reason)

    def is_retryable(self) -> bool:
        """Whether this error is retryable. Returns True by default.

        Subclasses override to implement classification logic based on
        error codes and HTTP status codes.
        """
        return True

    def build_logger_extras(self) -> dict:
        """Return structured logging extras for retryable invocation errors."""
        return {}


class DurableApiErrorCategory(Enum):
    """Whether a durable API failure should retry the Lambda or fail execution."""

    INVOCATION = "INVOCATION"
    EXECUTION = "EXECUTION"


# Backward-compatible alias
CheckpointErrorCategory = DurableApiErrorCategory


class BotoClientError(InvocationError):
    """Error from a Lambda API call (e.g., CheckpointDurableExecution, GetDurableExecutionState).

    Extends InvocationError because the default behavior for API failures is to retry
    the Lambda invocation. However, some errors are non-retryable (e.g., 4xx client errors,
    KMS key misconfiguration) and should fail the execution instead. The error_category field
    and is_retryable() method distinguish these cases at runtime.
    """

    def __init__(
        self,
        message: str,
        error_category: DurableApiErrorCategory = DurableApiErrorCategory.INVOCATION,
        error: AwsErrorObj | None = None,
        response_metadata: AwsErrorMetadata | None = None,
        termination_reason=TerminationReason.INVOCATION_ERROR,
    ):
        super().__init__(message=message, termination_reason=termination_reason)
        self.error: AwsErrorObj | None = error
        self.response_metadata: AwsErrorMetadata | None = response_metadata
        self.error_category: DurableApiErrorCategory = error_category

    @classmethod
    def from_exception(cls, exception: Exception) -> BotoClientError:
        response = getattr(exception, "response", {})
        response_metadata = response.get("ResponseMetadata")
        error = response.get("Error")
        error_category = BotoClientError._classify_error_category(
            error, response_metadata
        )
        return cls(
            message=str(exception),
            error_category=error_category,
            error=error,
            response_metadata=response_metadata,
        )

    @staticmethod
    def _classify_error_category(
        error: AwsErrorObj | None,
        response_metadata: AwsErrorMetadata | None,
    ) -> DurableApiErrorCategory:
        """Classify a Durable API error as retryable (INVOCATION) or non-retryable (EXECUTION).

        Classification rules:
        - Non-retryable customer error codes (e.g., KMS key issues) → EXECUTION
          These arrive as HTTP 502 but require customer intervention to fix.
        - 4xx errors → EXECUTION, except:
          - 429 (TooManyRequests) → INVOCATION (throttling is transient)
          - InvalidParameterValueException with "Invalid Checkpoint Token" → INVOCATION
            (stale token from a concurrent checkpoint; next invocation gets a fresh token)
        - 5xx, network errors → INVOCATION
        """
        error_code: str | None = error.get("Code") if error else None
        if error_code and error_code in _NON_RETRYABLE_CUSTOMER_ERROR_CODES:
            return DurableApiErrorCategory.EXECUTION

        status_code: int | None = (
            response_metadata.get("HTTPStatusCode") if response_metadata else None
        )

        if (
            status_code
            and BAD_REQUEST_ERROR <= status_code < SERVICE_ERROR
            and status_code != TOO_MANY_REQUESTS_ERROR
            and error
            and not (
                (error.get("Code") or "") == INVALID_PARAMETER_VALUE_EXCEPTION
                and (error.get("Message") or "").startswith(
                    INVALID_CHECKPOINT_TOKEN_PREFIX
                )
            )
        ):
            return DurableApiErrorCategory.EXECUTION

        return DurableApiErrorCategory.INVOCATION

    def is_retryable(self) -> bool:
        """Whether this error is retryable based on error_category."""
        return self.error_category == DurableApiErrorCategory.INVOCATION

    # Backward-compatible alias
    is_retriable = is_retryable

    def build_logger_extras(self) -> dict:
        extras: dict = {}
        # preserve PascalCase to be consistent with other langauges
        if error := self.error:
            extras["Error"] = error
        if response_metadata := self.response_metadata:
            extras["ResponseMetadata"] = response_metadata
        return extras


class _RestoredInvocationError(InvocationError):
    """Invocation control error restored without importing its original class."""

    def __init__(
        self,
        message: str,
        *,
        original_error_type: str,
        retryable: bool,
        termination_reason: TerminationReason,
        error_category: DurableApiErrorCategory | None = None,
        error: AwsErrorObj | None = None,
        response_metadata: AwsErrorMetadata | None = None,
    ):
        super().__init__(message, termination_reason)
        self.original_error_type = original_error_type
        self.retryable = retryable
        self.error_category = error_category
        self.error = error
        self.response_metadata = response_metadata

    def is_retryable(self) -> bool:
        return self.retryable

    def build_logger_extras(self) -> dict:
        extras: dict = {}
        if self.error is not None:
            extras["Error"] = self.error
        if self.response_metadata is not None:
            extras["ResponseMetadata"] = self.response_metadata
        return extras


def _sdk_error_type_name(error: InvocationError) -> str:
    """Return the original type name for a restored invocation error."""
    if isinstance(error, _RestoredInvocationError):
        return error.original_error_type
    return type(error).__name__


def _encode_sdk_invocation_error_payload(error: InvocationError) -> str:
    """Encode retry behavior needed to safely restore an invocation error."""
    details: dict[str, Any] = {
        "version": _SDK_INVOCATION_ERROR_PAYLOAD_VERSION,
        "error_type": _sdk_error_type_name(error),
        "retryable": bool(error.is_retryable()),
        "termination_reason": error.termination_reason.value,
    }
    if isinstance(error, BotoClientError | _RestoredInvocationError):
        if error.error_category is not None:
            details["error_category"] = error.error_category.value
        details["error"] = error.error
        details["response_metadata"] = error.response_metadata

    try:
        return json.dumps(details, separators=(",", ":"), sort_keys=True)
    except (TypeError, ValueError):
        # Retry behavior is control data; diagnostic boto payloads are best effort.
        details.pop("error", None)
        details.pop("response_metadata", None)
        return json.dumps(details, separators=(",", ":"), sort_keys=True)


def _restore_sdk_invocation_error(
    message: str,
    error_type: str | None,
    payload: str | None,
) -> InvocationError:
    """Restore replay-critical invocation semantics from SDK-owned metadata."""
    original_error_type = error_type or InvocationError.__name__
    # Legacy envelopes had no payload and represented retryable InvocationError.
    # A present but invalid payload fails closed to avoid an unbounded retry loop.
    retryable = payload is None
    termination_reason = TerminationReason.INVOCATION_ERROR
    error_category: DurableApiErrorCategory | None = None
    error: AwsErrorObj | None = None
    response_metadata: AwsErrorMetadata | None = None

    try:
        decoded = json.loads(payload) if payload is not None else None
    except (TypeError, ValueError):
        decoded = None

    if (
        isinstance(decoded, dict)
        and decoded.get("version") == _SDK_INVOCATION_ERROR_PAYLOAD_VERSION
    ):
        encoded_error_type = decoded.get("error_type")
        if isinstance(encoded_error_type, str) and encoded_error_type:
            original_error_type = encoded_error_type

        encoded_retryable = decoded.get("retryable")
        if type(encoded_retryable) is bool:
            retryable = encoded_retryable

        try:
            termination_reason = TerminationReason(decoded.get("termination_reason"))
        except (TypeError, ValueError):
            pass

        try:
            error_category = DurableApiErrorCategory(decoded.get("error_category"))
        except (TypeError, ValueError):
            pass

        encoded_error = decoded.get("error")
        if isinstance(encoded_error, dict):
            error = cast("AwsErrorObj", encoded_error)
        encoded_response_metadata = decoded.get("response_metadata")
        if isinstance(encoded_response_metadata, dict):
            response_metadata = cast(
                "AwsErrorMetadata",
                encoded_response_metadata,
            )

    return _RestoredInvocationError(
        message,
        original_error_type=original_error_type,
        retryable=retryable,
        termination_reason=termination_reason,
        error_category=error_category,
        error=error,
        response_metadata=response_metadata,
    )


class NonDeterministicExecutionError(ExecutionError):
    """Error when execution is non-deterministic."""

    def __init__(self, message: str, step_id: str | None = None):
        super().__init__(message, TerminationReason.NON_DETERMINISTIC_EXECUTION)
        self.step_id = step_id


class CheckpointError(BotoClientError):
    """Failure to checkpoint. Will terminate the lambda."""

    def __init__(
        self,
        message: str,
        error_category: DurableApiErrorCategory = DurableApiErrorCategory.INVOCATION,
        error: AwsErrorObj | None = None,
        response_metadata: AwsErrorMetadata | None = None,
    ):
        super().__init__(
            message,
            error_category,
            error,
            response_metadata,
            termination_reason=TerminationReason.CHECKPOINT_FAILED,
        )


class ValidationError(DurableExecutionsError):
    """Incorrect arguments to a Durable Function operation."""


class FlowDefinitionError(ValidationError):
    """Raised when a declarative flow definition is invalid."""


class FlowExecutionError(DurableExecutionsError):
    """Raised after a flow checkpoints a result with unhandled node failures."""

    def __init__(self, message: str, result: Any):
        super().__init__(message)
        self.result = result


class GetExecutionStateError(BotoClientError):
    """Raised when failing to retrieve execution state"""

    def __init__(
        self,
        message: str,
        error_category: DurableApiErrorCategory = DurableApiErrorCategory.INVOCATION,
        error: AwsErrorObj | None = None,
        response_metadata: AwsErrorMetadata | None = None,
    ):
        super().__init__(
            message,
            error_category,
            error,
            response_metadata,
            termination_reason=TerminationReason.INVOCATION_ERROR,
        )


class InvalidStateError(DurableExecutionsError):
    """Raised when an operation is attempted on an object in an invalid state."""


class UserlandError(DurableExecutionsError):
    """Failure in user-land - i.e code passed into durable executions from the caller."""


class CallableRuntimeError(UserlandError):
    """This error wraps any failure from inside the callable code that you pass to a Durable Function operation."""

    def __init__(
        self,
        message: str | None,
        error_type: str | None,
        data: str | None,
        stack_trace: list[str] | None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.error_type = error_type
        self.data = data
        self.stack_trace = stack_trace

    @classmethod
    def from_error_object(cls, error_object: Any) -> CallableRuntimeError:
        return cls(
            message=error_object.message,
            error_type=error_object.type,
            data=error_object.data,
            stack_trace=error_object.stack_trace,
        )


class BackgroundThreadError(BaseException):
    """Critical error from background checkpoint thread.

    Derives from BaseException to bypass normal exception handlers.
    Similar to KeyboardInterrupt or SystemExit - this is a system-level
    error that should terminate execution immediately without attempting
    to checkpoint or process the error.

    This exception is raised in the user thread when the background
    checkpoint processing thread encounters a fatal error. It propagates
    through the awaiting checkpoint future to interrupt blocked user code.

    Attributes:
        source_exception: The original exception from the background thread
    """

    def __init__(self, message: str, source_exception: Exception):
        super().__init__(message)
        self.source_exception = source_exception


class SuspendExecution(BaseException):
    """Raise this exception to suspend the current execution by returning PENDING to DAR.

    Note this derives from BaseException - in keeping with system-exiting exceptions like
    KeyboardInterrupt or SystemExit.
    """

    def __init__(self, message: str):
        super().__init__(message)


class TimedSuspendExecution(SuspendExecution):
    """Suspend execution until a specific timestamp.

    This is a specialized form of SuspendExecution that includes a scheduled resume time.

    Attributes:
        scheduled_timestamp (float): Unix timestamp in seconds at which to resume.
    """

    def __init__(self, message: str, scheduled_timestamp: float):
        super().__init__(message)
        self.scheduled_timestamp = scheduled_timestamp

    @classmethod
    def from_delay(cls, message: str, delay_seconds: int) -> TimedSuspendExecution:
        """Create a timed suspension with the delay calculated from now.

        Args:
            message: Descriptive message for the suspension
            delay_seconds: Number of seconds to suspend from current time

        Returns:
            TimedSuspendExecution: Instance with calculated resume time

        Example:
            >>> exception = TimedSuspendExecution.from_delay("Waiting for callback", 30)
            >>> # Will suspend for 30 seconds from now
        """
        resume_time = time.time() + delay_seconds
        return cls(message, scheduled_timestamp=resume_time)

    @classmethod
    def from_datetime(
        cls, message: str, datetime_timestamp: datetime.datetime
    ) -> TimedSuspendExecution:
        """Create a timed suspension with the delay calculated from now.

        Args:
            message: Descriptive message for the suspension
            datetime_timestamp: Unix datetime timestamp in seconds at which to resume

        Returns:
            TimedSuspendExecution: Instance with calculated resume time
        """
        return cls(message, scheduled_timestamp=datetime_timestamp.timestamp())


def suspend_with_optional_resume_timestamp(
    msg: str, datetime_timestamp: datetime.datetime | None = None
) -> NoReturn:
    """Suspend execution with an optional target resume timestamp."""

    if datetime_timestamp is None:
        msg = f"No timestamp provided. Suspending without retry timestamp. Original operation: [{msg}]"
        raise SuspendExecution(msg)

    if datetime_timestamp < datetime.datetime.now(tz=datetime.timezone.utc):
        msg = f"Invalid timestamp {datetime_timestamp}, suspending with immediate retry, original operation: [{msg}]"
        raise TimedSuspendExecution.from_datetime(
            msg, datetime.datetime.now(tz=datetime.timezone.utc)
        )

    raise TimedSuspendExecution.from_datetime(msg, datetime_timestamp)


def suspend_with_optional_resume_delay(
    msg: str, delay_seconds: int | None = None
) -> NoReturn:
    """Suspend execution with an optional delay before resuming."""

    if delay_seconds is None:
        msg = f"No delay_seconds provided, suspending without retry timestamp, original operation: [{msg}]"
        raise SuspendExecution(msg)

    if delay_seconds < 0:
        msg = f"Invalid delay_seconds {delay_seconds}, suspending with delay 0, original operation: [{msg}]"
        raise TimedSuspendExecution.from_delay(msg, 0)

    raise TimedSuspendExecution.from_delay(msg, delay_seconds)


@dataclass(frozen=True)
class CallableRuntimeErrorSerializableDetails:
    """Serializable error details."""

    type: str
    message: str

    @classmethod
    def from_exception(
        cls, exception: Exception
    ) -> CallableRuntimeErrorSerializableDetails:
        """Create an instance from an Exception, using its type and message.

        Args:
            exception: An Exception instance

        Returns:
            A CallableRuntimeErrorDetails instance with the exception's type name and message
        """
        return cls(type=exception.__class__.__name__, message=str(exception))

    def __str__(self) -> str:
        """
        Return a string representation of the object.

        Returns:
            A string in the format "type: message"
        """
        return f"{self.type}: {self.message}"


class SerDesError(DurableExecutionsError):
    """Raised when serialization fails."""
