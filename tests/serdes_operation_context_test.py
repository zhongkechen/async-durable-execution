"""Integration coverage for SerDes context populated by durable operations."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest

from async_durable_execution import (
    FileSystemSerDesStage,
    FileSystemSerDesStageConfig,
    JsonSerDes,
    OperationStatus,
    OperationSubType,
    OperationType,
    RetryableSerDesError,
    SerDesContext,
)
from async_durable_execution._core.exceptions import TimedSuspendExecution
from async_durable_execution._core.models import (
    CallbackDetails,
    ChainedInvokeDetails,
    Operation,
    OperationAction,
    OperationIdentifier,
    StepDetails,
)
from async_durable_execution._core.state import ExecutionState
from async_durable_execution._primitive.callback import Callback
from async_durable_execution._primitive.child import ChildOperationExecutor
from async_durable_execution._primitive.invoke import InvokeOperationExecutor
from async_durable_execution._primitive.step import StepOperationExecutor


class CapturingStage:
    """Identity stage that records explicit runtime context."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, SerDesContext]] = []

    async def serialize(self, value: str, context: SerDesContext) -> str:
        self.calls.append(("serialize", context))
        return value

    async def deserialize(self, data: str, context: SerDesContext) -> str:
        self.calls.append(("deserialize", context))
        return data


class RetryableStage:
    """Stage that simulates a transient storage failure."""

    async def serialize(self, value: str, context: SerDesContext) -> str:
        raise RetryableSerDesError("transient storage failure")

    async def deserialize(self, data: str, context: SerDesContext) -> str:
        return data


class RetryableDeserializeStage:
    """Stage that fails only while reading a checkpointed value."""

    async def serialize(self, value: str, context: SerDesContext) -> str:
        return value

    async def deserialize(self, data: str, context: SerDesContext) -> str:
        raise RetryableSerDesError("transient checkpoint read failure")


def _state(execution_arn: str = "arn:test") -> Mock:
    state = Mock(spec=ExecutionState)
    state.durable_execution_arn = execution_arn
    state.recursive_level = 0
    state.create_checkpoint = AsyncMock(return_value=None)
    return state


async def test_step_serdes_context_uses_effective_type_and_attempt() -> None:
    stage = CapturingStage()
    serdes = JsonSerDes[dict[str, int]]().then(stage)
    executor = StepOperationExecutor(
        lambda: _return_value({"value": 1}),
        _state(),
        OperationIdentifier(
            "step-id",
            OperationSubType.STEP,
            "parent-id",
            "step-name",
        ),
        serdes=serdes,
    )

    assert await executor.execute(None) == {"value": 1}
    assert [action for action, _context in stage.calls] == [
        "serialize",
        "deserialize",
    ]
    for _action, context in stage.calls:
        assert context.operation_type is OperationType.STEP
        assert context.operation_sub_type is OperationSubType.STEP
        assert context.operation_name == "step-name"
        assert context.parent_id == "parent-id"
        assert context.attempt == 1

    stage.calls.clear()
    replayed_operation = Operation(
        operation_id="step-id",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        sub_type=None,
        parent_id="checkpoint-parent",
        name="checkpoint-name",
        step_details=StepDetails(
            result=json.dumps({"value": 2}),
            attempt=3,
        ),
    )

    assert await executor.replay(replayed_operation) == {"value": 2}
    action, context = stage.calls.pop()
    assert action == "deserialize"
    assert context.operation_type is OperationType.STEP
    assert context.operation_sub_type is None
    assert context.operation_name == "checkpoint-name"
    assert context.parent_id == "checkpoint-parent"
    assert context.attempt == 3


async def test_child_context_serdes_context_uses_context_metadata() -> None:
    stage = CapturingStage()
    serdes = JsonSerDes[dict[str, int]]().then(stage)
    executor = ChildOperationExecutor(
        lambda: _return_value({"value": 1}),
        _state(),
        OperationIdentifier(
            "child-id",
            OperationSubType.RUN_IN_CHILD_CONTEXT,
            "parent-id",
            "child-name",
        ),
        serdes=serdes,
    )

    assert await executor.execute(None) == {"value": 1}
    for _action, context in stage.calls:
        assert context.operation_type is OperationType.CONTEXT
        assert context.operation_sub_type is OperationSubType.RUN_IN_CHILD_CONTEXT
        assert context.operation_name == "child-name"
        assert context.parent_id == "parent-id"
        assert context.attempt is None


async def test_step_retry_strategy_receives_retryable_serdes_failure() -> None:
    retry_calls: list[tuple[Exception, int]] = []

    def retry_strategy(error: Exception, attempt: int) -> int:
        retry_calls.append((error, attempt))
        return 1

    executor = StepOperationExecutor(
        lambda: _return_value({"value": 1}),
        _state(),
        OperationIdentifier(
            "step-id",
            OperationSubType.STEP,
            None,
            "step-name",
        ),
        retry_strategy=retry_strategy,
        serdes=JsonSerDes[dict[str, int]]().then(RetryableStage()),
    )

    with pytest.raises(TimedSuspendExecution):
        await executor.execute(None)

    assert len(retry_calls) == 1
    error, attempt = retry_calls[0]
    assert isinstance(error, RetryableSerDesError)
    assert attempt == 1


async def test_step_does_not_checkpoint_retry_after_successful_transition() -> None:
    retry_calls: list[tuple[Exception, int]] = []
    state = _state()

    def retry_strategy(error: Exception, attempt: int) -> int:
        retry_calls.append((error, attempt))
        return 1

    executor = StepOperationExecutor(
        lambda: _return_value({"value": 1}),
        state,
        OperationIdentifier(
            "step-id",
            OperationSubType.STEP,
            None,
            "step-name",
        ),
        retry_strategy=retry_strategy,
        serdes=JsonSerDes[dict[str, int]]().then(RetryableDeserializeStage()),
    )

    with pytest.raises(RetryableSerDesError, match="failed to deserialize"):
        await executor.execute(None)

    assert retry_calls == []
    state.create_checkpoint.assert_awaited_once()
    update = state.create_checkpoint.await_args.kwargs["operation_update"]
    assert update.action is OperationAction.SUCCEED


async def test_callback_result_serdes_context_uses_checkpoint_metadata() -> None:
    stage = CapturingStage()
    state = _state()
    state.operations = {
        "callback-id": Operation(
            operation_id="callback-id",
            operation_type=OperationType.CALLBACK,
            status=OperationStatus.SUCCEEDED,
            sub_type=OperationSubType.CALLBACK,
            parent_id="parent-id",
            name="callback-name",
            callback_details=CallbackDetails(
                callback_id="external-callback-id",
                result=json.dumps({"value": 1}),
            ),
        )
    }
    callback = Callback[dict[str, int]](
        callback_id="external-callback-id",
        operation_id="callback-id",
        state=state,
        serdes=JsonSerDes[dict[str, int]]().then(stage),
    )

    assert await callback.result() == {"value": 1}
    action, context = stage.calls.pop()
    assert action == "deserialize"
    assert context.operation_type is OperationType.CALLBACK
    assert context.operation_sub_type is OperationSubType.CALLBACK
    assert context.operation_name == "callback-name"
    assert context.parent_id == "parent-id"


async def test_invoke_result_can_follow_cross_execution_filesystem_reference(
    tmp_path: Path,
) -> None:
    filesystem_stage = FileSystemSerDesStage(
        tmp_path,
        FileSystemSerDesStageConfig(
            cross_execution_reference_policy=(
                lambda owner_arn, _owner_entity_id, _context: (
                    owner_arn == "arn:producer"
                )
            ),
        ),
    )
    producer_context = SerDesContext(
        operation_id="producer-id",
        durable_execution_arn="arn:producer",
        entity_id="operation/producer-id/result",
        operation_type=OperationType.STEP,
    )
    envelope = await filesystem_stage.serialize(
        json.dumps({"value": 1}),
        producer_context,
    )

    executor = InvokeOperationExecutor[dict[str, int]](
        function_name="child-function",
        payload={"input": 1},
        state=_state("arn:consumer"),
        operation_identifier=OperationIdentifier(
            "invoke-id",
            OperationSubType.CHAINED_INVOKE,
            "parent-id",
            "invoke-name",
        ),
        serdes_result=JsonSerDes[dict[str, int]]().then(filesystem_stage),
    )
    operation = Operation(
        operation_id="invoke-id",
        operation_type=OperationType.CHAINED_INVOKE,
        status=OperationStatus.SUCCEEDED,
        sub_type=OperationSubType.CHAINED_INVOKE,
        parent_id="parent-id",
        name="invoke-name",
        chained_invoke_details=ChainedInvokeDetails(result=envelope),
    )

    assert await executor.replay(operation) == {"value": 1}


async def _return_value(value: Any) -> Any:
    return value
