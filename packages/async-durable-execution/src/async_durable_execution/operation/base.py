"""Base classes and shared helpers for operation executors."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, TypeVar

from ..models import OperationIdentifier
from ..serdes import SerDes, deserialize, serialize


if TYPE_CHECKING:
    from ..models import OperationUpdate
    from ..state import CheckpointedResult, ExecutionState

T = TypeVar("T")
S = TypeVar("S")


@dataclass(frozen=True)
class OperationContext:
    """Protocol defining the interface for durable execution contexts."""

    execution_state: ExecutionState
    operation_identifier: OperationIdentifier

    @property
    def durable_execution_arn(self) -> str:
        """Get the ARN of the Durable Execution."""
        return self.execution_state.durable_execution_arn

    @property
    def parent_id(self) -> str | None:
        return self.operation_identifier.parent_id

    @property
    def operation_id(self) -> str | None:
        return self.operation_identifier.operation_id

    @property
    def operation_name(self) -> str | None:
        return self.operation_identifier.name


class OperationExecutor(ABC, Generic[T]):
    """Base class for durable operations with shared state and serdes helpers."""

    def __init__(
        self,
        state: ExecutionState,
        operation_identifier: OperationIdentifier,
    ) -> None:
        self.state = state
        self.operation_identifier = operation_identifier

    @property
    def operation_id(self) -> str:
        """Return the required operation id for this executor."""
        return self.operation_identifier.require_operation_id()

    @property
    def operation_name(self) -> str | None:
        """Return the human-readable operation name, if provided."""
        return self.operation_identifier.name

    @property
    def durable_execution_arn(self) -> str:
        """Return the durable execution ARN for serialization helpers."""
        return self.state.durable_execution_arn

    def get_checkpointed_result(self) -> CheckpointedResult:
        """Load the current checkpoint state for this operation."""
        return self.state.get_checkpoint_result(self.operation_id)

    async def create_checkpoint(
        self,
        operation_update: OperationUpdate,
        *,
        is_sync: bool | None = None,
    ) -> None:
        """Persist a checkpoint update for this operation."""
        if is_sync is None:
            await self.state._create_checkpoint_async(
                operation_update=operation_update,
            )
            return

        await self.state._create_checkpoint_async(
            operation_update=operation_update,
            is_sync=is_sync,
        )

    def serialize_value(self, value: S, serdes: SerDes[S] | None) -> str:
        """Serialize a value using operation-scoped metadata."""
        return serialize(
            serdes=serdes,
            value=value,
            operation_id=self.operation_id,
            durable_execution_arn=self.durable_execution_arn,
        )

    def deserialize_value(self, data: str, serdes: SerDes[S] | None) -> S:
        """Deserialize a value using operation-scoped metadata."""
        return deserialize(
            serdes=serdes,
            data=data,
            operation_id=self.operation_id,
            durable_execution_arn=self.durable_execution_arn,
        )

    @abstractmethod
    async def execute(self, checkpointed_result: CheckpointedResult) -> T:
        """Execute operation logic with checkpoint data."""
        ...  # pragma: no cover

    @abstractmethod
    async def process(self) -> T:
        """Process the operation, including replay and checkpoint handling."""
        ...  # pragma: no cover
