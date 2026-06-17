"""Types and Protocols. Don't import anything other than config here - the reason it exists is to avoid circular references."""

from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar, runtime_checkable

from .models import (
    OperationUpdate,
    CheckpointOutput,
    StateOutput,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

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


@runtime_checkable
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


@runtime_checkable
class DurableContext(Protocol):
    execution_state: ExecutionState | None
    durable_execution_arn: str | None
    parent_id: str | None
    operation_id: str | None
    operation_name: str | None


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
