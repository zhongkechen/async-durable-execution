"""Exceptions for the Durable Executions SDK.

Avoid any non-stdlib references in this module, it is at the bottom of the dependency chain.
"""

from __future__ import annotations

import datetime
import time
from dataclasses import dataclass
from enum import Enum
from typing import NoReturn, TypedDict


BAD_REQUEST_ERROR: int = 400
TOO_MANY_REQUESTS_ERROR: int = 429
SERVICE_ERROR: int = 500
INVALID_PARAMETER_VALUE_EXCEPTION: str = "InvalidParameterValueException"
INVALID_CHECKPOINT_TOKEN_PREFIX: str = "Invalid Checkpoint Token"

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


class AwsErrorObj(TypedDict):
    """Subset of a boto-style AWS error payload."""

    Code: str | None
    Message: str | None


class AwsErrorMetadata(TypedDict):
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
        error_code: str | None = (error and error.get("Code")) or None
        if error_code and error_code in _NON_RETRYABLE_CUSTOMER_ERROR_CODES:
            return DurableApiErrorCategory.EXECUTION

        status_code: int | None = (
            response_metadata and response_metadata.get("HTTPStatusCode")
        ) or None
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
