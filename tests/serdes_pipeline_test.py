"""Tests for composable SerDes pipelines."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from async_durable_execution import (
    ComposableSerDes,
    JsonSerDes,
    OperationType,
    SerDesContext,
    SerDesPipelineError,
    create_serdes_pipeline,
    is_composable_serdes,
)
from async_durable_execution._core.serdes import deserialize, serialize


class FramedStage:
    def __init__(self, name: str, calls: list[str]) -> None:
        self.name = name
        self.calls = calls
        self.original_values: list[Any] = []

    async def serialize(self, value: str, context: SerDesContext) -> str:
        self.calls.append(f"serialize:{self.name}")
        self.original_values.append(context.original_value)
        return f"{self.name}[{value}]"

    async def deserialize(self, data: str, context: SerDesContext) -> str:
        self.calls.append(f"deserialize:{self.name}")
        self.original_values.append(context.original_value)
        prefix = f"{self.name}["
        if data.startswith(prefix) and data.endswith("]"):
            return data[len(prefix) : -1]
        return data


async def test_pipeline_serializes_forward_and_deserializes_in_reverse() -> None:
    calls: list[str] = []
    first = FramedStage("first", calls)
    second = FramedStage("second", calls)
    value = {"value": 1}
    pipeline = JsonSerDes[dict[str, int]]().then(first).then(second)

    serialized = await serialize(
        pipeline,
        value,
        "operation-1",
        "arn:test",
    )
    assert serialized == 'second[first[{"value": 1}]]'

    deserialized = await deserialize(
        pipeline,
        serialized,
        "operation-1",
        "arn:test",
    )
    assert deserialized == value
    assert calls == [
        "serialize:first",
        "serialize:second",
        "deserialize:second",
        "deserialize:first",
    ]
    assert first.original_values == [value, None]
    assert second.original_values == [value, None]


def test_pipeline_creation_is_immutable_and_flattens_existing_pipeline() -> None:
    calls: list[str] = []
    first_stage = FramedStage("first", calls)
    second_stage = FramedStage("second", calls)
    first: ComposableSerDes[Any] = create_serdes_pipeline(
        JsonSerDes[Any](),
        first_stage,
    )
    second: ComposableSerDes[Any] = create_serdes_pipeline(first, second_stage)

    assert isinstance(first, ComposableSerDes)
    assert is_composable_serdes(second)
    assert second.value_codec is first.value_codec
    assert first.stages == (first_stage,)
    assert second.stages == (first_stage, second_stage)


def test_pipeline_rejects_missing_components() -> None:
    with pytest.raises(TypeError, match="value_codec"):
        create_serdes_pipeline(None)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="stages"):
        JsonSerDes[str]().then(None)  # type: ignore[arg-type]


async def test_pipeline_passes_explicit_operation_context_to_stages() -> None:
    contexts: list[SerDesContext] = []

    class ContextStage:
        async def serialize(
            self,
            value: str,
            context: SerDesContext,
        ) -> str:
            contexts.append(context)
            return value

        async def deserialize(
            self,
            data: str,
            context: SerDesContext,
        ) -> str:
            contexts.append(context)
            return data

    pipeline = JsonSerDes[str]().then(ContextStage())
    serialized = await serialize(
        pipeline,
        "value",
        "operation-1",
        "arn:test",
        entity_id="operation/operation-1/result",
        operation_name="custom",
        parent_id="parent",
        operation_type=OperationType.STEP,
        operation_sub_type="CustomStep",
        attempt=2,
    )
    await deserialize(
        pipeline,
        serialized,
        "operation-1",
        "arn:test",
        entity_id="operation/operation-1/result",
        operation_name="custom",
        parent_id="parent",
        operation_type=OperationType.STEP,
        operation_sub_type="CustomStep",
        attempt=2,
    )

    assert contexts[0] == SerDesContext(
        operation_id="operation-1",
        durable_execution_arn="arn:test",
        entity_id="operation/operation-1/result",
        operation_name="custom",
        parent_id="parent",
        operation_type=OperationType.STEP,
        operation_sub_type="CustomStep",
        attempt=2,
        original_value="value",
    )
    assert contexts[1].original_value is None


async def test_pipeline_error_identifies_component_and_action() -> None:
    class FailingStage:
        async def serialize(
            self,
            value: str,
            context: SerDesContext,
        ) -> str:
            raise ValueError("boom")

        async def deserialize(
            self,
            data: str,
            context: SerDesContext,
        ) -> str:
            return data

    pipeline: ComposableSerDes[str] = create_serdes_pipeline(
        JsonSerDes[str](),
        FailingStage(),
    )

    with pytest.raises(SerDesPipelineError) as exc_info:
        await pipeline.serialize("value")

    assert exc_info.value.stage_index == 1
    assert exc_info.value.action == "serialize"
    assert isinstance(exc_info.value.__cause__, ValueError)


async def test_pipeline_rejects_non_string_stage_output() -> None:
    class InvalidStage:
        async def serialize(
            self,
            value: str,
            context: SerDesContext,
        ) -> str:
            return 42  # type: ignore[return-value]

        async def deserialize(
            self,
            data: str,
            context: SerDesContext,
        ) -> str:
            return data

    with pytest.raises(SerDesPipelineError, match="stage 1"):
        await JsonSerDes[str]().then(InvalidStage()).serialize("value")


async def test_pipeline_does_not_wrap_task_cancellation() -> None:
    class CancelledStage:
        async def serialize(
            self,
            value: str,
            context: SerDesContext,
        ) -> str:
            raise asyncio.CancelledError

        async def deserialize(
            self,
            data: str,
            context: SerDesContext,
        ) -> str:
            return data

    with pytest.raises(asyncio.CancelledError):
        await JsonSerDes[str]().then(CancelledStage()).serialize("value")
