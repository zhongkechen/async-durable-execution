"""Types and Protocols. Don't import anything other than config here - the reason it exists is to avoid circular references."""

from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar, Mapping

from .models import (
    OperationUpdate,
    CheckpointOutput,
    StateOutput,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping, Sequence

    from .config import (
        BatchedInput,
        CallbackConfig,
        ChildConfig,
        MapConfig,
        ParallelBranch,
        ParallelConfig,
        StepConfig,
        WaitForCallbackConfig,
    )
    from .state import ExecutionState

T = TypeVar("T")
U = TypeVar("U")
C_co = TypeVar("C_co", covariant=True)
C_contra = TypeVar("C_contra", contravariant=True)


class LoggerInterface(Protocol):
    def debug(
        self, msg: object, *args: object, extra: Mapping[str, object] | None = None
    ) -> None: ...  # pragma: no cover

    def info(
        self, msg: object, *args: object, extra: Mapping[str, object] | None = None
    ) -> None: ...  # pragma: no cover

    def warning(
        self, msg: object, *args: object, extra: Mapping[str, object] | None = None
    ) -> None: ...  # pragma: no cover

    def error(
        self, msg: object, *args: object, extra: Mapping[str, object] | None = None
    ) -> None: ...  # pragma: no cover

    def exception(
        self, msg: object, *args: object, extra: Mapping[str, object] | None = None
    ) -> None: ...  # pragma: no cover


@dataclass(frozen=True)
class StepContext:
    attempt: int | None = None
    execution_state: ExecutionState | None = None
    execution_arn: str | None = None
    parent_id: str | None = None
    operation_id: str | None = None
    operation_name: str | None = None


@dataclass(frozen=True)
class WaitForCallbackContext:
    """Context available during wait_for_callback submitter execution."""

    callback_id: str
    execution_state: ExecutionState | None = None
    execution_arn: str | None = None
    parent_id: str | None = None
    operation_id: str | None = None
    operation_name: str | None = None


@dataclass(frozen=True)
class WaitForConditionCheckContext:
    """Context available during wait_for_condition checker execution."""

    attempt: int | None = None
    execution_state: ExecutionState | None = None
    execution_arn: str | None = None
    parent_id: str | None = None
    operation_id: str | None = None
    operation_name: str | None = None


class Callback(Protocol, Generic[C_co]):
    """Protocol for callback futures."""

    callback_id: str

    @abstractmethod
    async def result(self) -> C_co | None:
        """Return the result of the future."""
        ...  # pragma: no cover


class BatchResult(Protocol, Generic[T]):
    """Protocol for batch operation results."""

    @abstractmethod
    def get_results(self) -> list[T]:
        """Get all successful results."""
        ...  # pragma: no cover


class DurableContext(Protocol):
    """Protocol defining the interface for durable execution contexts."""

    execution_state: ExecutionState | None
    execution_arn: str | None
    parent_id: str | None
    operation_id: str | None
    operation_name: str | None

    @abstractmethod
    async def step(
        self,
        func: Callable[[], Awaitable[T]],
        name: str | None = None,
        config: StepConfig | None = None,
    ) -> T:
        """Execute a step durably."""
        ...  # pragma: no cover

    @abstractmethod
    async def run_in_child_context(
        self,
        func: Callable[[], Awaitable[T]],
        name: str | None = None,
        config: ChildConfig | None = None,
    ) -> T:
        """Run callable in a child context."""
        ...  # pragma: no cover

    @abstractmethod
    async def map(
        self,
        inputs: Sequence[U],
        func: Callable[[U | BatchedInput[Any, U], int, Sequence[U]], Awaitable[T]],
        name: str | None = None,
        config: MapConfig | None = None,
    ) -> BatchResult[T]:
        """Apply function durably to each item in inputs."""
        ...  # pragma: no cover

    @abstractmethod
    async def parallel(
        self,
        functions: Sequence[Callable[[], Awaitable[T]] | ParallelBranch[T]],
        name: str | None = None,
        config: ParallelConfig | None = None,
    ) -> BatchResult[T]:
        """Execute callables durably in parallel."""
        ...  # pragma: no cover

    @abstractmethod
    async def wait(self, duration: timedelta, name: str | None = None) -> None:
        """Wait for a specified amount of time."""
        ...  # pragma: no cover

    @abstractmethod
    async def create_callback(
        self, name: str | None = None, config: CallbackConfig | None = None
    ) -> Callback:
        """Create a callback."""
        ...  # pragma: no cover

    @abstractmethod
    async def wait_for_callback(
        self,
        submitter: Callable[[str], Awaitable[Any]],
        name: str | None = None,
        config: WaitForCallbackConfig | None = None,
    ) -> Any:
        """Wait for an external callback using a submitter that receives callback_id."""
        ...  # pragma: no cover


class LambdaContext(Protocol):  # pragma: no cover
    aws_request_id: str
    log_group_name: str | None = None
    log_stream_name: str | None = None
    function_name: str | None = None
    memory_limit_in_mb: str | None = None
    function_version: str | None = None
    invoked_function_arn: str | None = None
    tenant_id: str | None = None
    client_context: Any | None = None
    identity: Any | None = None

    def get_remaining_time_in_millis(self) -> int: ...
    def log(self, msg) -> None: ...


"""Summary generators for concurrent operations.

Summary generators create compact JSON representations of large BatchResult objects
when the serialized result exceeds the 256KB checkpoint size limit. This prevents
large payloads from being stored in checkpoints while maintaining operation metadata.

When a summary is used, the operation is marked with ReplayChildren=true, causing
the child context to be re-executed during replay to reconstruct the full result.
"""


class SummaryGenerator(Protocol[C_contra]):
    def __call__(self, result: C_contra) -> str: ...  # pragma: no cover


Context = (
    StepContext | WaitForCallbackContext | WaitForConditionCheckContext | DurableContext
)


class DurableServiceClient(Protocol):
    """Durable Service clients must implement this interface."""

    async def checkpoint(
        self,
        durable_execution_arn: str,
        checkpoint_token: str,
        updates: list[OperationUpdate],
        client_token: str | None,
    ) -> CheckpointOutput: ...  # pragma: no cover

    async def get_execution_state(
        self,
        durable_execution_arn: str,
        checkpoint_token: str,
        next_marker: str,
        max_items: int = 1000,
    ) -> StateOutput: ...  # pragma: no cover


class LambdaApiClient(Protocol):
    """Minimal Lambda client surface needed by durable execution."""

    def checkpoint_durable_execution(
        self, **kwargs: Any
    ) -> Mapping[str, Any]: ...  # pragma: no cover

    def get_durable_execution_state(
        self, **kwargs: Any
    ) -> Mapping[str, Any]: ...  # pragma: no cover
