"""Base classes and shared helpers for operation executors."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import ClassVar, Generic, TypeVar

from .._core import (
    ExecutionState,
    InvalidStateError,
    Operation,
    OperationContext,
    OperationIdentifier,
    OperationType,
    OperationUpdate,
    SerDes,
    deserialize,
    serialize,
)

T = TypeVar("T")
S = TypeVar("S")


class OperationExecutor(ABC, Generic[T]):
    """Base class for durable operations with shared state and serdes helpers."""

    SERDES_OPERATION_TYPE: ClassVar[OperationType | None] = None

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

    async def serialize_value(
        self,
        value: S,
        serdes: SerDes[S] | None,
        *,
        attempt: int | None = None,
    ) -> str:
        """Serialize a value using operation-scoped metadata."""
        return await serialize(
            serdes=serdes,
            value=value,
            operation_id=self.operation_id,
            durable_execution_arn=self.durable_execution_arn,
            recursive_level=self.state.recursive_level,
            operation_name=self.operation_identifier.name,
            parent_id=self.operation_identifier.parent_id,
            operation_type=(
                self.operation_identifier.operation_type or self.SERDES_OPERATION_TYPE
            ),
            operation_sub_type=self.operation_identifier.sub_type,
            attempt=attempt,
        )

    async def deserialize_value(
        self,
        data: str,
        serdes: SerDes[S] | None,
        *,
        operation: Operation | None = None,
        attempt: int | None = None,
    ) -> S:
        """Deserialize a value using operation-scoped metadata."""
        return await deserialize(
            serdes=serdes,
            data=data,
            operation_id=self.operation_id,
            durable_execution_arn=self.durable_execution_arn,
            recursive_level=self.state.recursive_level,
            operation_name=(
                operation.name
                if operation is not None
                else self.operation_identifier.name
            ),
            parent_id=(
                operation.parent_id
                if operation is not None
                else self.operation_identifier.parent_id
            ),
            operation_type=(
                operation.operation_type
                if operation is not None
                else (
                    self.operation_identifier.operation_type
                    or self.SERDES_OPERATION_TYPE
                )
            ),
            operation_sub_type=(
                operation.sub_type
                if operation is not None and operation.sub_type is not None
                else self.operation_identifier.sub_type
            ),
            attempt=attempt,
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
        expected_type = self.operation_identifier.operation_type
        if expected_type is not None:
            expected = self.operation_identifier
            mismatches = []
            if operation.operation_type is not expected_type:
                mismatches.append(
                    f"type={operation.operation_type.value!r}, "
                    f"expected {expected_type.value!r}"
                )
            if operation.sub_type != expected.sub_type:
                mismatches.append(
                    f"sub_type={operation.sub_type!r}, expected {expected.sub_type!r}"
                )
            if operation.name != expected.name:
                mismatches.append(
                    f"name={operation.name!r}, expected {expected.name!r}"
                )
            if operation.parent_id != expected.parent_id:
                mismatches.append(
                    f"parent_id={operation.parent_id!r}, "
                    f"expected {expected.parent_id!r}"
                )
            if mismatches:
                details = "; ".join(mismatches)
                msg = (
                    f"Reserved extension operation {self.operation_id!r} does "
                    f"not match its checkpoint: {details}"
                )
                raise InvalidStateError(msg)
        return await self.replay(operation)
