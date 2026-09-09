"""Public value types. These carry data; they never schedule or execute work."""

from __future__ import annotations

import math
import random as _random
import re
from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum
from typing import Any, Awaitable, Callable, Generic, Protocol, TypeVar

T = TypeVar("T")
Duration = int | timedelta


class InvocationStatus(Enum):
    """Outcome of one durable Lambda invocation.

    SUCCEEDED and FAILED finish the execution; PENDING reports durable work that
    requires a later invocation. RETRY denotes a request to retry an invocation.
    """

    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    PENDING = "PENDING"
    RETRY = "RETRY"


class OperationType(Enum):
    """Backend categories used to identify operations in execution history."""

    EXECUTION = "EXECUTION"
    CONTEXT = "CONTEXT"
    STEP = "STEP"
    WAIT = "WAIT"
    CALLBACK = "CALLBACK"
    CHAINED_INVOKE = "CHAINED_INVOKE"


class OperationStatus(Enum):
    """Persisted lifecycle state of a durable operation.

    STARTED denotes work in progress. PENDING and READY describe scheduled and
    runnable retries. SUCCEEDED, FAILED, CANCELLED, TIMED_OUT, and STOPPED are
    terminal states.
    """

    STARTED = "STARTED"
    PENDING = "PENDING"
    READY = "READY"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    TIMED_OUT = "TIMED_OUT"
    STOPPED = "STOPPED"


class OperationSubType(Enum):
    """SDK operation labels that distinguish uses of the same backend category.

    Extension reservations may use these enum members explicitly or supply their
    own nonblank subtype strings. Strings matching SDK values are reserved.
    """

    STEP = "Step"
    WAIT = "Wait"
    CALLBACK = "Callback"
    CHAINED_INVOKE = "ChainedInvoke"
    RUN_IN_CHILD_CONTEXT = "RunInChildContext"
    MAP = "Map"
    MAP_ITERATION = "MapIteration"
    PARALLEL = "Parallel"
    PARALLEL_BRANCH = "ParallelBranch"
    WAIT_FOR_CALLBACK = "WaitForCallback"
    WAIT_FOR_CONDITION = "WaitForCondition"
    TERMINAL_SCOPE = "TerminalScope"
    TERMINAL_COMPENSATION = "TerminalCompensation"
    TERMINAL_CLEANUP = "TerminalCleanup"
    EXECUTION = "Execution"


class StepSemantics(Enum):
    """Controls whether an interrupted attempt may run its callable again.

    AT_LEAST_ONCE_PER_RETRY permits re-execution of interrupted work.
    AT_MOST_ONCE_PER_RETRY routes an already-started attempt through interruption
    handling instead. Neither mode guarantees exactly-once external side effects.
    """

    AT_MOST_ONCE_PER_RETRY = "AT_MOST_ONCE_PER_RETRY"
    AT_LEAST_ONCE_PER_RETRY = "AT_LEAST_ONCE_PER_RETRY"


class TerminationReason(Enum):
    UNHANDLED_ERROR = "UNHANDLED_ERROR"
    INVOCATION_ERROR = "INVOCATION_ERROR"
    EXECUTION_ERROR = "EXECUTION_ERROR"
    CHECKPOINT_FAILED = "CHECKPOINT_FAILED"
    NON_DETERMINISTIC_EXECUTION = "NON_DETERMINISTIC_EXECUTION"
    STEP_INTERRUPTED = "STEP_INTERRUPTED"
    CALLBACK_ERROR = "CALLBACK_ERROR"
    SERIALIZATION_ERROR = "SERIALIZATION_ERROR"


class DurableExecutionsError(Exception):
    """Base exception for public SDK errors."""


class ExecutionError(DurableExecutionsError):
    """A logical execution failure that should not trigger an invocation retry.

    Attributes:
        termination_reason (TerminationReason): Category of the execution failure.
    """

    def __init__(
        self, message: str, termination_reason=TerminationReason.EXECUTION_ERROR
    ):
        """Describe an execution failure and its termination category.

        Args:
            message (str): Human-readable explanation.
            termination_reason (TerminationReason): Failure category for diagnostics.
        """
        self.termination_reason = termination_reason
        super().__init__(message)


class InvocationError(DurableExecutionsError):
    """An invocation interruption, retryable by default.

    Override `is_retryable()` to report a permanent failure. Retryable invocation
    errors bypass terminal cleanup and compensation so execution can resume safely.

    Attributes:
        termination_reason (TerminationReason): Diagnostic interruption category.
    """

    def __init__(
        self, message: str, termination_reason=TerminationReason.INVOCATION_ERROR
    ):
        """Describe an invocation interruption and its diagnostic category.

        Args:
            message (str): Human-readable explanation.
            termination_reason (TerminationReason): Interruption category.
        """
        self.termination_reason = termination_reason
        super().__init__(message)

    def is_retryable(self) -> bool:
        """Return whether another invocation may recover; the default is True."""
        return True

    def build_logger_extras(self) -> dict:
        """Return additional structured log fields; the default is an empty dictionary."""
        return {}


class UserlandError(DurableExecutionsError):
    """Base class for failures originating in user-supplied callables."""

    pass


class ValidationError(DurableExecutionsError):
    """An operation or configuration value violates an SDK requirement."""

    pass


class InvalidStateError(DurableExecutionsError):
    """A durable action is incompatible with the active scope or recorded state."""

    pass


class SerDesError(DurableExecutionsError):
    """A permanent failure to encode, decode, or validate a persisted value."""

    pass


class RetryableSerDesError(InvocationError):
    """A transient serialization or storage failure that may succeed on retry.

    Step retry policies can handle these failures during result preparation.
    Elsewhere they interrupt the invocation without running terminal actions.
    """

    def __init__(self, message):
        """Describe a transient serialization failure with the serialization termination
        reason.
        """
        super().__init__(message, TerminationReason.SERIALIZATION_ERROR)


class StepInterruptedError(InvocationError):
    """An at-most-once step attempt started but did not finish durably.

    Attributes:
        step_id (str | None): Identifier of the interrupted step, when available.
    """

    def __init__(self, message, step_id=None):
        """Describe an interrupted attempt and optionally identify its step."""
        self.step_id = step_id
        super().__init__(message, TerminationReason.STEP_INTERRUPTED)


class CallbackError(ExecutionError):
    """A callback failed, timed out, stopped, or could not be initialized.

    Attributes:
        callback_id (str | None): Identifier of the affected callback, when known.
    """

    def __init__(self, message, callback_id=None):
        """Describe a callback failure and optionally attach its callback identifier."""
        self.callback_id = callback_id
        super().__init__(message, TerminationReason.CALLBACK_ERROR)


class WaitForConditionError(ExecutionError):
    """Polling exhausted its allowed attempts before the condition became true."""

    pass


@dataclass(frozen=True)
class ErrorObject:
    """Serializable failure details carried by durable operations and callbacks.

    Attributes:
        message (str | None): Human-readable failure message.
        type (str | None): Original error type name.
        data (str | None): Additional serialized error metadata.
        stack_trace (list[str] | None): Captured stack trace lines, if supplied.
    """

    message: str | None = None
    type: str | None = None
    data: str | None = None
    stack_trace: list[str] | None = None

    @classmethod
    def from_exception(cls, exception):
        """Capture an exception's message and type name.

        Existing `CallableRuntimeError` details are preserved, including optional data
        and trace lines. This method does not capture a new Python stack trace.
        """
        if isinstance(exception, CallableRuntimeError):
            return cls(
                exception.message,
                exception.error_type,
                exception.data,
                exception.stack_trace,
            )
        return cls(str(exception), type(exception).__name__)

    @classmethod
    def from_message(cls, message):
        """Create failure details containing only a human-readable message."""
        return cls(message)

    @classmethod
    def from_dict(cls, data):
        """Read AWS ErrorMessage, ErrorType, ErrorData, and StackTrace fields.

        Missing optional fields become None.
        """
        return cls(
            *(
                data.get(key)
                for key in ("ErrorMessage", "ErrorType", "ErrorData", "StackTrace")
            )
        )

    def to_dict(self):
        """Return AWS error-field names, omitting fields whose value is None."""
        return {
            k: v
            for k, v in zip(
                ("ErrorMessage", "ErrorType", "ErrorData", "StackTrace"),
                (self.message, self.type, self.data, self.stack_trace),
            )
            if v is not None
        }


class CallableRuntimeError(UserlandError):
    """A user-callable failure represented by its persisted error details.

    The original exception type is available as `error_type`; replay does not
    instantiate arbitrary user exception classes from checkpoint data.

    Attributes:
        message (str | None): Original error message.
        error_type (str | None): Original exception type name.
        data (str | None): Optional serialized error metadata.
        stack_trace (list[str] | None): Optional recorded trace lines.
    """

    def __init__(self, message, error_type, data, stack_trace):
        """Construct an error from persisted callable-failure fields.

        Args:
            message (str | None): Original failure message.
            error_type (str | None): Original exception type name.
            data (str | None): Serialized error metadata.
            stack_trace (list[str] | None): Recorded trace lines.
        """
        self.message, self.error_type, self.data, self.stack_trace = (
            message,
            error_type,
            data,
            stack_trace,
        )
        super().__init__(message)

    @classmethod
    def from_error_object(cls, error_object):
        """Build a callable failure preserving all fields from an ErrorObject."""
        return cls(
            error_object.message,
            error_object.type,
            error_object.data,
            error_object.stack_trace,
        )


class LambdaContext(Protocol):
    """Lambda invocation metadata exposed through the active SDK context.

    Optional metadata may be None in local execution or when Lambda does not
    supply it. Runtime state should be read through context getters.

    Attributes:
        aws_request_id (str): Identifier of the Lambda invocation.
        log_group_name (str | None): CloudWatch Logs group.
        log_stream_name (str | None): CloudWatch Logs stream.
        memory_limit_in_mb (str | None): Configured memory limit, as a string.
        client_context (Any): Optional client-supplied invocation context.
        identity (Any): Optional caller identity metadata.
        function_name (str | None): Invoked Lambda function name.
        function_version (str | None): Invoked version or qualifier.
        invoked_function_arn (str | None): Function ARN used for invocation.
        tenant_id (str | None): Tenant identifier, when supplied.
    """

    aws_request_id: str
    log_group_name: str | None = None
    log_stream_name: str | None = None
    memory_limit_in_mb: str | None = None
    client_context: Any | None = None
    identity: Any | None = None
    function_name: str | None
    function_version: str | None
    invoked_function_arn: str | None
    tenant_id: str | None

    def get_remaining_time_in_millis(self) -> int:
        """Return the remaining invocation time budget in milliseconds."""
        ...

    def log(self, msg) -> None:
        """Write a message through the Lambda context logging interface."""
        ...


class DurableServiceClient(Protocol):
    """Asynchronous persistence interface accepted by durable_execution().

    Implement checkpoint writes and paginated state reads. Responses may be AWS
    mappings or objects exposing `to_dict()` with the same field names.
    """

    async def checkpoint(
        self, durable_execution_arn, checkpoint_token, updates, client_token
    ):
        """Persist operation updates using the current checkpoint token.

        Args:
            durable_execution_arn (str): Execution receiving the updates.
            checkpoint_token (str): Current backend token authorizing the write.
            updates (list): Update objects exposing AWS mappings through `to_dict()`.
            client_token (str | None): Idempotency token for this checkpoint request.

        Returns:
            (Any): A response containing NewExecutionState and the next CheckpointToken.
                The token may be absent when the execution finishes.
        """
        ...

    async def get_execution_state(
        self, durable_execution_arn, checkpoint_token, next_marker, max_items=1000
    ):
        """Fetch another page of the execution state for the supplied token.

        Args:
            durable_execution_arn (str): Execution whose history is being read.
            checkpoint_token (str): Token identifying the state being read.
            next_marker (str): Pagination marker from the preceding response.
            max_items (int): Maximum number of operations requested in the page.

        Returns:
            (Any): An AWS-style response with Operations and an optional NextMarker.
        """
        ...


def seconds(value: Duration, label="duration", *, positive=False) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, timedelta)):
        raise ValidationError(f"{label} must be integer seconds or timedelta")
    raw = value.total_seconds() if isinstance(value, timedelta) else value
    if raw < 0 or (positive and int(raw) < 1):
        raise ValidationError(
            f"{label} must be {'positive' if positive else 'nonnegative'}"
        )
    return int(raw)


class JitterStrategy(str, Enum):
    """Randomization applied to a capped retry or polling delay.

    NONE keeps the full delay, FULL samples from zero up to that delay, and HALF
    samples from half the delay up to the full delay.
    """

    NONE = "NONE"
    FULL = "FULL"
    HALF = "HALF"

    def apply_jitter(self, delay):
        """Randomize a delay according to this strategy without rounding it."""
        fraction = {self.NONE: 1.0, self.FULL: 0.0, self.HALF: 0.5}[self]
        return (
            delay
            if fraction == 1
            else delay * (fraction + (1 - fraction) * _random.random())
        )

    def finalize_delay(self, base_delay):
        """Apply jitter, round upward to whole seconds, and enforce a one-second minimum."""
        return max(1, math.ceil(self.apply_jitter(base_delay)))


@dataclass
class DelayPolicy:
    max_attempts: int = 6
    initial_delay: Duration = 5
    max_delay: Duration = 60
    backoff_rate: int | float = 2
    jitter_strategy: JitterStrategy = JitterStrategy.FULL
    increment: Duration | None = None

    def __post_init__(self):
        self.initial_delay = seconds(self.initial_delay)
        self.max_delay = seconds(self.max_delay)
        if self.increment is not None:
            self.increment = seconds(self.increment)

    @property
    def initial_delay_seconds(self):
        """Return the initial delay normalized to whole seconds."""
        return seconds(self.initial_delay)

    @property
    def max_delay_seconds(self):
        """Return the delay cap normalized to whole seconds."""
        return seconds(self.max_delay)

    @property
    def increment_seconds(self):
        """Return the linear increment in whole seconds, or None for exponential backoff."""
        return None if self.increment is None else seconds(self.increment)

    def calculate_delay(self, attempts_made):
        """Calculate the capped and jittered delay after an attempt.

        Args:
            attempts_made (int): Number of attempts already made, starting at one.

        Returns:
            (int): Delay in whole seconds, with a minimum of one second.
        """
        index = max(0, attempts_made - 1)
        try:
            delay = (
                self.initial_delay_seconds * self.backoff_rate**index
                if self.increment is None
                else self.initial_delay_seconds + index * self.increment_seconds
            )
        except OverflowError:
            delay = self.max_delay_seconds
        return self.jitter_strategy.finalize_delay(min(self.max_delay_seconds, delay))


@dataclass
class RetryStrategy(DelayPolicy):
    """Select retries by attempt count, error messages, and exception types.

    Call the strategy with an exception and the number of attempts already made.
    It returns a delay or None to stop. Message and type filters are combined
    with OR; omitting both filters allows any error eligible for step retries.

    Attributes:
        max_attempts (int): Maximum total attempts, including the first; default 6.
        initial_delay (Duration): Initial delay in seconds or timedelta; default 5.
        max_delay (Duration): Delay cap before jitter; default 60 seconds.
        backoff_rate (int | float): Exponential multiplier; default 2.
        jitter_strategy (JitterStrategy): Delay randomization; default FULL.
        increment (Duration | None): Linear increment replacing exponential growth.
        retryable_errors (list | None): Message substrings or compiled regex patterns.
        retryable_error_types (list[type[Exception]] | None): Accepted exception types.
    """

    retryable_errors: list[str | re.Pattern] | None = None
    retryable_error_types: list[type[Exception]] | None = None

    def __call__(self, error, attempts_made):
        """Return the next delay, or None when filters or the attempt limit reject retry.

        Args:
            error (Exception): Failure from the attempted operation.
            attempts_made (int): Number of completed attempts, starting at one.
        """
        if attempts_made >= self.max_attempts:
            return None
        accept = self.retryable_errors is None and self.retryable_error_types is None
        accept |= any(
            isinstance(error, kind) for kind in self.retryable_error_types or ()
        )
        accept |= any(
            bool(pattern.search(str(error)))
            if hasattr(pattern, "search")
            else pattern in str(error)
            for pattern in self.retryable_errors or ()
        )
        return self.calculate_delay(attempts_made) if accept else None

    @classmethod
    def default(cls):
        """Create the default six-attempt exponential policy with full jitter."""
        return cls()

    @classmethod
    def none(cls):
        """Create a policy that permits the initial attempt and no retries."""
        return cls(max_attempts=1, max_delay=300, backoff_rate=2.0)

    @classmethod
    def transient(cls):
        """Create a three-attempt policy with half jitter and a 300-second delay cap."""
        return cls(max_attempts=3, max_delay=300, jitter_strategy=JitterStrategy.HALF)

    @classmethod
    def resource_availability(cls):
        """Create a five-attempt policy with full jitter and a 300-second delay cap."""
        return cls(max_attempts=5, max_delay=300)

    @classmethod
    def critical(cls):
        """Create a ten-attempt policy starting at one second with 1.5 backoff and no
        jitter.
        """
        return cls(
            max_attempts=10,
            initial_delay=1,
            backoff_rate=1.5,
            jitter_strategy=JitterStrategy.NONE,
        )

    @classmethod
    def linear(cls):
        """Create a six-attempt policy with one-second initial delay and linear increments."""
        return cls(
            initial_delay=1,
            increment=1,
            max_delay=300,
            jitter_strategy=JitterStrategy.NONE,
        )


@dataclass
class PollingStrategy(DelayPolicy, Generic[T]):
    """Poll until a result is truthy or the attempt limit is reached.

    A truthy result completes even on the final attempt. False results schedule
    another capped, jittered delay, or raise WaitForConditionError at exhaustion.

    Attributes:
        max_attempts (int): Maximum checks, including the first; default 6.
        initial_delay (Duration): Initial polling delay; default 5 seconds.
        max_delay (Duration): Delay cap before jitter; default 60 seconds.
        backoff_rate (int | float): Exponential delay multiplier; default 2.
        jitter_strategy (JitterStrategy): Delay randomization; default FULL.
        increment (Duration | None): Optional linear increment instead of backoff.
    """

    def __call__(self, result: T, attempts_made: int):
        """Return None for a truthy result, otherwise schedule the next check.

        Args:
            result (T): Value returned by the most recent condition check.
            attempts_made (int): Number of checks performed, starting at one.

        Raises:
            WaitForConditionError: The final permitted check returned a false value.
        """
        if result:
            return None
        if attempts_made >= self.max_attempts:
            raise WaitForConditionError(
                f"Condition not satisfied after {self.max_attempts} attempts"
            )
        return self.calculate_delay(attempts_made)


class NestingType(Enum):
    """Choose whether aggregate branches have persisted child-context records.

    NESTED records each branch context. FLAT keeps isolated operation identities
    while attaching branch operations directly to the enclosing context.
    """

    NESTED = "NESTED"
    FLAT = "FLAT"


class BatchItemStatus(Enum):
    """Outcome of a started map item or parallel branch in a BatchResult."""

    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    STARTED = "STARTED"


class CompletionReason(Enum):
    """Reason a map or parallel batch stopped accepting more work.

    ALL_COMPLETED, MIN_SUCCESSFUL_REACHED, and CUSTOM_COMPLETION_SUCCEEDED indicate
    successful policy completion. A successful policy may still tolerate failed
    items; inspect the individual results when that distinction matters.
    """

    ALL_COMPLETED = "ALL_COMPLETED"
    MIN_SUCCESSFUL_REACHED = "MIN_SUCCESSFUL_REACHED"
    FAILURE_TOLERANCE_EXCEEDED = "FAILURE_TOLERANCE_EXCEEDED"
    CUSTOM_COMPLETION_SUCCEEDED = "CUSTOM_COMPLETION_SUCCEEDED"
    CUSTOM_COMPLETION_FAILED = "CUSTOM_COMPLETION_FAILED"

    @property
    def is_succeeded(self):
        """Return whether the completion policy finished successfully."""
        return self not in (
            self.FAILURE_TOLERANCE_EXCEEDED,
            self.CUSTOM_COMPLETION_FAILED,
        )


@dataclass(frozen=True)
class CompletionStatus:
    """Counts supplied to a custom aggregate completion policy.

    Attributes:
        success_count (int): Successfully completed branches.
        failure_count (int): Branches completed with failure.
        total_count (int): Total submitted branches, including those not yet started.
    """

    success_count: int
    failure_count: int
    total_count: int

    def __post_init__(self):
        if (
            min(self.success_count, self.failure_count, self.total_count) < 0
            or self.completed_count > self.total_count
        ):
            raise ValueError("Invalid completion counts")

    @property
    def completed_count(self):
        """Return the sum of successful and failed branches, excluding cancellations."""
        return self.success_count + self.failure_count

    @property
    def all_completed(self):
        """Return whether successful and failed branches account for the entire batch."""
        return self.completed_count == self.total_count


@dataclass(frozen=True)
class CompletionDecision:
    """A completion policy's decision to finish or continue an aggregate.

    Attributes:
        should_complete (bool): Whether execution should stop collecting results.
        completion_reason (CompletionReason | None): Required exactly when complete.
    """

    should_complete: bool
    completion_reason: CompletionReason | None = None

    def __post_init__(self):
        if self.should_complete != (self.completion_reason is not None):
            raise ValueError(
                "A completion decision requires a reason exactly when complete"
            )

    @staticmethod
    def complete(completion_reason):
        """Finish an aggregate with the supplied completion reason."""
        return CompletionDecision(True, completion_reason)

    @staticmethod
    def continue_execution():
        """Keep collecting branch outcomes without declaring a completion reason."""
        return CompletionDecision(False)

    @property
    def is_succeeded(self):
        """Return True only for a completed decision with a successful reason."""
        return bool(
            self.should_complete
            and self.completion_reason is not None
            and self.completion_reason.is_succeeded
        )


@dataclass(frozen=True)
class CompletionConfig:
    """Thresholds or a custom policy controlling aggregate completion.

    The default configuration requires all work to succeed and tolerates no
    failures. A custom callback cannot be combined with thresholds. Policies run
    as replayed workflow code and must be deterministic and free of side effects.

    Attributes:
        min_successful (int | None): Success count sufficient for early completion.
        tolerated_failure_count (int | None): Allowed failures; None acts as zero.
        should_complete (Callable | None): Callback from CompletionStatus to a decision.
    """

    min_successful: int | None = None
    tolerated_failure_count: int | None = None
    should_complete: Callable[[CompletionStatus], CompletionDecision] | None = None

    def __post_init__(self):
        if self.should_complete is not None:
            if not callable(self.should_complete):
                raise TypeError("should_complete must be callable")
            if (
                self.min_successful is not None
                or self.tolerated_failure_count is not None
            ):
                raise ValueError("A custom policy cannot also have thresholds")

    @classmethod
    def thresholds(cls, *, min_successful=None, tolerated_failure_count=None):
        """Create a policy from optional success and failure-count thresholds.

        Args:
            min_successful (int | None): Successes sufficient to finish early.
            tolerated_failure_count (int | None): Failures allowed before stopping.
        """
        return cls(min_successful, tolerated_failure_count)

    @classmethod
    def first_successful(cls):
        """Finish after one success, retaining the default zero-failure tolerance."""
        return cls(min_successful=1)

    @classmethod
    def all_completed(cls):
        """Use the default policy: all successes finish; any failure exceeds tolerance."""
        return cls()

    @classmethod
    def all_successful(cls):
        """Require every branch to succeed, stopping when the first failure is observed."""
        return cls(tolerated_failure_count=0)

    @classmethod
    def custom(cls, should_complete):
        """Create a policy from a deterministic CompletionStatus-to-CompletionDecision
        callback.
        """
        return cls(should_complete=should_complete)

    @property
    def has_custom_should_complete(self):
        """Return whether a custom completion callback is configured."""
        return self.should_complete is not None

    def completion_decision(self, status):
        """Evaluate a custom callback or the configured thresholds against current counts.

        Args:
            status (CompletionStatus): Current success, failure, and total counts.

        Returns:
            (CompletionDecision): Whether to finish and, if so, the completion reason.

        Raises:
            TypeError: A custom callback returned something other than
                CompletionDecision.
        """
        if self.should_complete is not None:
            decision = self.should_complete(status)
            if not isinstance(decision, CompletionDecision):
                raise TypeError("Completion policies must return CompletionDecision")
            return decision
        if (
            self.min_successful is not None
            and status.success_count >= self.min_successful
        ):
            reason = CompletionReason.MIN_SUCCESSFUL_REACHED
        elif status.failure_count > (self.tolerated_failure_count or 0):
            reason = CompletionReason.FAILURE_TOLERANCE_EXCEEDED
        elif status.all_completed:
            reason = CompletionReason.ALL_COMPLETED
        else:
            return CompletionDecision.continue_execution()
        return CompletionDecision.complete(reason)


@dataclass(frozen=True)
class BatchItem(Generic[T]):
    """Recorded outcome of one started map item or parallel branch.

    Attributes:
        index (int): Original zero-based position in the input sequence.
        status (BatchItemStatus): Completion, failure, cancellation, or started state.
        result (T | None): Successful value, which may itself be None.
        error (ErrorObject | None): Failure details when available.
    """

    index: int
    status: BatchItemStatus
    result: T | None = None
    error: ErrorObject | None = None

    def to_dict(self):
        """Return index, status, result, and serialized error fields as a mapping."""
        return dict(
            index=self.index,
            status=self.status.value,
            result=self.result,
            error=None if self.error is None else self.error.to_dict(),
        )

    @classmethod
    def from_dict(cls, data):
        """Restore a BatchItem from its mapping representation."""
        return cls(
            data["index"],
            BatchItemStatus(data["status"]),
            data.get("result"),
            ErrorObject.from_dict(data["error"]) if data.get("error") else None,
        )


@dataclass(frozen=True)
class BatchResult(Generic[T]):
    """Collected branch outcomes and the policy decision that ended a batch.

    Aggregates order items by their original index and omit work that never
    started. Started unfinished branches are retained as CANCELLED after early
    completion. A tolerated failure still contributes to failure_count and status.

    Attributes:
        all (list[BatchItem]): All recorded items, including None results and
            cancellations.
        completion_reason (CompletionReason): Reason the aggregate finished.
    """

    all: list[BatchItem[T]]
    completion_reason: CompletionReason

    def to_dict(self):
        """Return item mappings and the completionReason field for this batch."""
        return {
            "all": [item.to_dict() for item in self.all],
            "completionReason": self.completion_reason.value,
        }

    @classmethod
    def from_dict(cls, data, completion_config=None):
        """Restore items and their completion reason from a mapping.

        When completionReason is absent, infer it with the supplied completion_config
        or the default policy.
        """
        items = [BatchItem.from_dict(value) for value in data["all"]]
        return (
            cls(items, CompletionReason(data["completionReason"]))
            if data.get("completionReason")
            else cls.from_items(items, completion_config)
        )

    @classmethod
    def from_items(cls, items, completion_config=None):
        """Build a batch from item records and infer its completion reason.

        A policy that does not yet complete falls back to ALL_COMPLETED for this
        constructed result. Supplied items retain their order.
        """
        state = CompletionStatus(
            sum(x.status is BatchItemStatus.SUCCEEDED for x in items),
            sum(x.status is BatchItemStatus.FAILED for x in items),
            len(items),
        )
        decision = (completion_config or CompletionConfig()).completion_decision(state)
        return cls(items, decision.completion_reason or CompletionReason.ALL_COMPLETED)

    def succeeded(self):
        """Return successful item records whose result is not None."""
        return [
            x
            for x in self.all
            if x.status is BatchItemStatus.SUCCEEDED and x.result is not None
        ]

    def failed(self):
        """Return failed item records that contain error details."""
        return [
            x
            for x in self.all
            if x.status is BatchItemStatus.FAILED and x.error is not None
        ]

    def started(self):
        """Return item records still marked STARTED."""
        return [x for x in self.all if x.status is BatchItemStatus.STARTED]

    def cancelled(self):
        """Return item records marked CANCELLED."""
        return [x for x in self.all if x.status is BatchItemStatus.CANCELLED]

    def get_results(self):
        """Return non-None successful values in recorded item order."""
        return [x.result for x in self.succeeded()]

    def get_errors(self):
        """Return the available error objects from failed items in recorded order."""
        return [x.error for x in self.failed()]

    def throw_if_error(self):
        """Raise CallableRuntimeError for the first failed item containing error details."""
        for item in self.failed():
            raise CallableRuntimeError.from_error_object(item.error)

    @property
    def success_count(self):
        """Count all successful items, including those whose result is None."""
        return sum(x.status is BatchItemStatus.SUCCEEDED for x in self.all)

    @property
    def failure_count(self):
        """Count failed items, whether or not error details are present."""
        return sum(x.status is BatchItemStatus.FAILED for x in self.all)

    @property
    def started_count(self):
        """Count item records still marked STARTED."""
        return len(self.started())

    @property
    def cancelled_count(self):
        """Count cancelled item records."""
        return len(self.cancelled())

    @property
    def total_count(self):
        """Count recorded items; work omitted because it never started is not included."""
        return len(self.all)

    @property
    def has_failure(self):
        """Return whether any recorded item failed, including tolerated failures."""
        return bool(self.failure_count)

    @property
    def status(self):
        """Return FAILED when any item failed, otherwise SUCCEEDED."""
        return BatchItemStatus.FAILED if self.has_failure else BatchItemStatus.SUCCEEDED


@dataclass(frozen=True)
class ExtensionStepResult(Generic[T]):
    """A stateful extension step's successful value or retry state.

    Attributes:
        value (T): Final result, or state passed to the next retry attempt.
        retry_delay (Duration | None): Delay requesting a retry; None means success.
    """

    value: T
    retry_delay: Duration | None = None

    @classmethod
    def succeed(cls, value):
        """Finish a stateful step with the supplied value."""
        return cls(value)

    @classmethod
    def retry(cls, state, delay):
        """Request another attempt with durable state and a nonnegative delay.

        Args:
            state (T | None): State to serialize and pass to the next attempt.
            delay (Duration): Integer seconds or timedelta; execution uses at least one
                second.
        """
        seconds(delay)
        return cls(state, delay)

    @property
    def is_retry(self):
        """Return whether this outcome requests another attempt."""
        return self.retry_delay is not None


ExtensionStepFunction = Callable[[T | None], Awaitable[ExtensionStepResult[T]]]
"""Async callback from optional prior state to ExtensionStepResult, used by reserved steps.
"""
ExtensionStepRetryStrategy = Callable[
    [Exception, T | None, int], ExtensionStepResult[T] | None
]
"""Callback from exception, prior state, and attempt count to a retry result or None."""
