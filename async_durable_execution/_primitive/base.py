"""Base classes and shared helpers for operation executors."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from .._core import (
    ExecutionState,
    Operation,
    OperationContext,
    OperationIdentifier,
    OperationUpdate,
    SerDesLike,
    deserialize,
    serialize,
)

T = TypeVar("T")
S = TypeVar("S")


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

    async def create_checkpoint(
        self,
        operation_update: OperationUpdate,
        *,
        is_sync: bool | None = None,
    ) -> Operation | None:
        """Persist a checkpoint update for this operation."""
        if is_sync is None:
            return await self.state.create_checkpoint(
                operation_update=operation_update,
            )

        return await self.state.create_checkpoint(
            operation_update=operation_update,
            is_sync=is_sync,
        )

    async def serialize_value(self, value: S, serdes: SerDesLike[S] | None) -> str:
        """Serialize a value using operation-scoped metadata."""
        return await serialize(
            serdes=serdes,
            value=value,
            operation_id=self.operation_id,
            durable_execution_arn=self.durable_execution_arn,
            recursive_level=self.state.recursive_level,
        )

    async def deserialize_value(self, data: str, serdes: SerDesLike[S] | None) -> S:
        """Deserialize a value using operation-scoped metadata."""
        return await deserialize(
            serdes=serdes,
            data=data,
            operation_id=self.operation_id,
            durable_execution_arn=self.durable_execution_arn,
            recursive_level=self.state.recursive_level,
        )

    @abstractmethod
    async def start(self) -> T:
        """Start a new operation with no existing checkpoint."""
        ...

    @abstractmethod
    async def replay(self, operation: Operation) -> T:
        """Replay an operation from an existing checkpoint."""
        ...

    async def process(self) -> T:
        """Process the operation, including replay and checkpoint handling."""
        operation = self.state.operations.get(self.operation_id)
        if operation is None:
            return await self.start()
        return await self.replay(operation)
