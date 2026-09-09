"""Public serializers preserve values, ownership, and immutable payloads."""

import asyncio
import json
import os
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID
import pytest
from async_durable_execution import *


@pytest.mark.parametrize(
    "value",
    [
        None,
        False,
        0,
        3.5,
        "text",
        b"bytes",
        bytearray(b"data"),
        UUID(int=42),
        Decimal("1.230"),
        date(2026, 9, 9),
        datetime(2026, 9, 9, tzinfo=timezone.utc),
        (1, "two"),
        [1, {"three": (4,)}],
        {4: "key", None: "nil"},
        ErrorObject("bad", "Type", "data", ["trace"]),
    ],
)
async def test_extended_values(value):
    codec: ExtendedTypeSerDes = ExtendedTypeSerDes()
    actual = await codec.deserialize(await codec.serialize(value))
    assert actual == value
    assert codec.deserialize_sync(codec.serialize_sync(value)) == value


async def test_cycles_rejected_and_shared_values_accepted():
    codec: ExtendedTypeSerDes = ExtendedTypeSerDes()
    shared = [1]
    assert await codec.deserialize(await codec.serialize([shared, shared])) == [
        [1],
        [1],
    ]
    cycle: list[Any] = []
    cycle.append(cycle)
    with pytest.raises(SerDesError):
        await codec.serialize(cycle)


async def test_pipeline_order_immutability_and_context():
    seen = []

    class Stage:
        def __init__(self, name):
            self.name = name

        async def serialize(self, value, context):
            seen.append(("write", self.name, context.original_value))
            return self.name + value

        async def deserialize(self, data, context):
            seen.append(("read", self.name, context.original_value))
            return data[len(self.name) :] if data.startswith(self.name) else data

    original = JsonSerDes().then(Stage("A"))
    both = original.then(Stage("B"))
    assert len(original.stages) == 1 and len(both.stages) == 2
    assert await both.deserialize(await both.serialize({"value": 3})) == {"value": 3}
    assert [(a, b) for a, b, _ in seen] == [
        ("write", "A"),
        ("write", "B"),
        ("read", "B"),
        ("read", "A"),
    ]
    assert seen[0][2] == {"value": 3} and seen[-1][2] is None


async def test_pipeline_bad_output_is_attributed():
    class Bad:
        async def serialize(self, value, context):
            return 42

        async def deserialize(self, data, context):
            return data

    with pytest.raises(SerDesPipelineError) as failure:
        await JsonSerDes().then(Bad()).serialize(1)
    assert failure.value.stage_index == 1 and failure.value.action == "serialize"


@pytest.mark.parametrize("mode", list(FileSystemSerDesMode))
@pytest.mark.parametrize("encoding", list(FileSystemPathEncoding))
async def test_filesystem_roundtrip_modes_and_independent_writes(
    tmp_path, mode, encoding
):
    stage = FileSystemSerDesStage(
        tmp_path,
        FileSystemSerDesStageConfig(
            storage_mode=mode,
            path_encoding=encoding,
            checkpoint_envelope_limit_bytes=1500,
        ),
    )
    context = SerDesContext(
        operation_id="one",
        durable_execution_arn="arn:aws:lambda:us-west-2:123456789012:function:test:1/durable-execution/job/run",
    )
    for value in ("small", "x" * 3000):
        first = await stage.serialize(value, context)
        second = await stage.serialize(value, context)
        assert await stage.deserialize(first, context) == value
        assert await stage.deserialize(second, context) == value
        if mode is FileSystemSerDesMode.ALWAYS or len(value) > 1500:
            assert first != second
    assert stage.execution_directory(context.durable_execution_arn).is_relative_to(
        tmp_path
    )


async def test_filesystem_replay_in_public_workflow(tmp_path):
    calls = []
    codec = JsonSerDes().then(FileSystemSerDesStage(tmp_path))

    async def work():
        calls.append(1)
        return {"answer": 42}

    @durable_execution
    async def handler(event):
        result = await step(work, serdes=codec, name="file")
        await wait(1)
        return result

    async with create_local_runner(handler=handler) as r:
        result = await r.run()
    assert result.status is InvocationStatus.SUCCEEDED, result.error
    assert result.get_deserialized_result() == {"answer": 42} and calls == [1]


async def test_filesystem_owner_and_policy(tmp_path):
    first = SerDesContext("first", "producer")
    second = SerDesContext(
        "second", "consumer", operation_type=OperationType.CHAINED_INVOKE
    )
    stage = FileSystemSerDesStage(tmp_path)
    envelope = await stage.serialize("payload", first)
    with pytest.raises(SerDesError):
        await stage.deserialize(envelope, second)
    allowed = FileSystemSerDesStage(
        tmp_path,
        FileSystemSerDesStageConfig(
            cross_execution_reference_policy=lambda arn, entity, ctx: arn == "producer"
            and entity == "operation/first"
        ),
    )
    assert await allowed.deserialize(envelope, second) == "payload"
    wrong = SerDesContext("second", "consumer", operation_type=OperationType.STEP)
    with pytest.raises(SerDesError):
        await allowed.deserialize(envelope, wrong)


async def test_file_corruption_and_symlinks_are_rejected(tmp_path):
    stage = FileSystemSerDesStage(tmp_path)
    ctx = SerDesContext("operation", "execution")
    envelope = await stage.serialize("payload", ctx)
    path = next(tmp_path.rglob("*.payload"))
    path.write_text("corrupt")
    with pytest.raises(SerDesError):
        await stage.deserialize(envelope, ctx)
    path.unlink()
    outside = tmp_path / "elsewhere"
    outside.write_text("payload")
    path.symlink_to(outside)
    with pytest.raises(SerDesError):
        await stage.deserialize(envelope, ctx)


async def test_missing_base_is_not_created(tmp_path):
    missing = tmp_path / "missing"
    stage = FileSystemSerDesStage(missing)
    with pytest.raises(SerDesError):
        await stage.serialize("payload", SerDesContext("operation", "execution"))
    assert not missing.exists()


async def test_unknown_payload_passes_through(tmp_path):
    stage = FileSystemSerDesStage(tmp_path)
    for payload in ("hello", '{"other":true}', "[1,2,3]"):
        assert await stage.deserialize(payload, SerDesContext()) == payload


@pytest.mark.parametrize("mode", list(PreviewMode))
def test_preview_exclusion_masking_and_bounds(mode):
    config = PreviewConfig(
        mode,
        include=(PreviewField("name"),),
        exclude=(PreviewField("secret"),),
        mask=(PreviewField("email"),),
        max_preview_bytes=100,
    )
    result = build_preview(
        {
            "name": "Ada",
            "secret": "never",
            "email": "hidden",
            "users": [{"name": "one"}, {"name": "two"}],
        },
        config,
    )
    assert (
        result["name"] == "Ada" and "secret" not in result and result["email"] == "***"
    )
    assert result["users"]["name"] == "two"
    assert (
        len(json.dumps(result, ensure_ascii=False, separators=(",", ":")).encode())
        <= 100
    )


def test_preview_iterative_depth_and_cycle_bounds():
    value: dict[str, Any] = {}
    value["cycle"] = value
    assert (
        build_preview(value, PreviewConfig(PreviewMode.INCLUDE_ALL, max_depth=4))
        is None
    )
    assert (
        build_preview(
            {"a": "x" * 100},
            PreviewConfig(PreviewMode.INCLUDE_ALL, max_preview_bytes=8),
        )
        is None
    )


async def test_custom_type_codec_uses_public_recursive_callbacks():
    from dataclasses import dataclass

    @dataclass
    class Box:
        value: int

    class Codec:
        tag = "box"

        def can_encode(self, value):
            return isinstance(value, Box)

        def encode(self, value, encode_value):
            return {"nested": encode_value(value.value)}

        def decode(self, value, decode_value):
            encoded = value["nested"]
            return Box(decode_value(encoded["t"], encoded["v"]))

    codec: ExtendedTypeSerDes = ExtendedTypeSerDes((Codec(),))
    assert await codec.deserialize(await codec.serialize({"value": Box(9)})) == {
        "value": Box(9)
    }


def test_public_primitive_predicate_handles_shared_lists():
    shared: list[Any] = [1, False, None]
    assert SerDes.is_primitive([shared, shared])
    shared.append(shared)
    assert not SerDes.is_primitive(shared)


async def test_readable_paths_cannot_alias_different_owner_strings(tmp_path):
    stage = FileSystemSerDesStage(tmp_path)
    first = SerDesContext("item", "owner:one")
    second = SerDesContext("item", "owner/one")
    assert stage.execution_directory(
        first.durable_execution_arn
    ) != stage.execution_directory(second.durable_execution_arn)
    envelope = json.loads(await stage.serialize("secret", second))
    envelope["owner"] = first.durable_execution_arn
    with pytest.raises(SerDesError):
        await stage.deserialize(json.dumps(envelope), first)
