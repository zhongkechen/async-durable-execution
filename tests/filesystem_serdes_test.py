"""Tests for the filesystem-backed SerDes pipeline stage."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from async_durable_execution import (
    FileSystemPathEncoding,
    FileSystemSerDesMode,
    FileSystemSerDesStage,
    FileSystemSerDesStageConfig,
    JsonSerDes,
    PreviewConfig,
    PreviewField,
    PreviewMode,
    SerDesContext,
    SerDesError,
    OperationType,
    create_file_system_serdes_stage,
)
from async_durable_execution._core.serdes import deserialize, serialize

EXECUTION_ARN = (
    "arn:aws:lambda:us-east-1:123456789012:function:test:1/"
    "durable-execution/run/invocation"
)


def _context(
    *,
    entity_id: str = "operation/operation-1/result",
    arn: str = EXECUTION_ARN,
    original_value: Any = None,
) -> SerDesContext:
    return SerDesContext(
        operation_id="operation-1",
        durable_execution_arn=arn,
        entity_id=entity_id,
        original_value=original_value,
    )


def test_filesystem_stage_validates_configuration() -> None:
    with pytest.raises(TypeError, match="storage_mode"):
        FileSystemSerDesStageConfig(storage_mode="ALWAYS")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="path_encoding"):
        FileSystemSerDesStageConfig(path_encoding="URI")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="positive"):
        FileSystemSerDesStageConfig(checkpoint_envelope_limit_bytes=0)
    with pytest.raises(ValueError, match="either"):
        FileSystemSerDesStageConfig(
            generate_preview=lambda _value, _context: None,
            preview_config=PreviewConfig(mode=PreviewMode.INCLUDE_ALL),
        )
    with pytest.raises(ValueError, match="base_path"):
        FileSystemSerDesStage("")


async def test_filesystem_stage_roundtrips_immutable_file_with_preview(
    tmp_path: Path,
) -> None:
    seen_originals: list[Any] = []

    async def preview(
        value: str,
        context: SerDesContext,
    ) -> dict[str, Any]:
        seen_originals.append(context.original_value)
        return {"id": json.loads(value)["id"]}

    stage = FileSystemSerDesStage(
        tmp_path,
        FileSystemSerDesStageConfig(generate_preview=preview),
    )
    pipeline = JsonSerDes[dict[str, Any]]().then(stage)
    value = {"id": "order-1", "payload": "data"}

    serialized = await serialize(
        pipeline,
        value,
        "operation-1",
        EXECUTION_ARN,
        entity_id="operation/operation-1/result",
    )
    envelope = json.loads(serialized)
    file_path = Path(envelope["file"])

    assert envelope["__durable_execution_filesystem_serdes"] == 1
    assert envelope["preview"] == {"id": "order-1"}
    assert file_path.read_text() == json.dumps(value)
    assert envelope["payloadSizeBytes"] == len(file_path.read_bytes())
    assert (
        envelope["payloadDigest"] == hashlib.sha256(file_path.read_bytes()).hexdigest()
    )
    assert seen_originals == [value]

    deserialized = await deserialize(
        pipeline,
        serialized,
        "operation-1",
        EXECUTION_ARN,
        entity_id="operation/operation-1/result",
    )
    assert deserialized == value

    second = await serialize(
        pipeline,
        value,
        "operation-1",
        EXECUTION_ARN,
        entity_id="operation/operation-1/result",
    )
    assert json.loads(second)["file"] != str(file_path)
    assert file_path.exists()


async def test_overflow_mode_keeps_small_values_inline(tmp_path: Path) -> None:
    stage = create_file_system_serdes_stage(
        tmp_path,
        FileSystemSerDesStageConfig(
            storage_mode=FileSystemSerDesMode.OVERFLOW,
        ),
    )
    context = _context()

    serialized = await stage.serialize('{"small":true}', context)
    envelope = json.loads(serialized)

    assert envelope["data"] == '{"small":true}'
    assert "file" not in envelope
    assert await stage.deserialize(serialized, context) == '{"small":true}'
    assert not list(tmp_path.rglob("*.payload"))


async def test_overflow_mode_offloads_large_values(tmp_path: Path) -> None:
    stage = FileSystemSerDesStage(
        tmp_path,
        FileSystemSerDesStageConfig(
            storage_mode=FileSystemSerDesMode.OVERFLOW,
            checkpoint_envelope_limit_bytes=1000,
        ),
    )
    context = _context()

    serialized = await stage.serialize("x" * 2000, context)
    envelope = json.loads(serialized)

    assert "file" in envelope
    assert await stage.deserialize(serialized, context) == "x" * 2000


async def test_stage_passes_unrecognized_input_through(tmp_path: Path) -> None:
    stage = FileSystemSerDesStage(tmp_path)

    assert await stage.deserialize('{"external":true}', _context()) == (
        '{"external":true}'
    )
    assert await stage.deserialize("not json", _context()) == "not json"
    malformed_nested = '{"nested":{"__durable_execution_filesystem_serdes":1'
    assert await stage.deserialize(malformed_nested, _context()) == malformed_nested


async def test_stage_rejects_malformed_or_unsupported_envelopes(
    tmp_path: Path,
) -> None:
    stage = FileSystemSerDesStage(tmp_path)
    context = _context()

    with pytest.raises(SerDesError, match="Unsupported"):
        await stage.deserialize(
            '{"__durable_execution_filesystem_serdes":2}',
            context,
        )

    with pytest.raises(SerDesError, match="Invalid"):
        await stage.deserialize(
            '{"__durable_execution_filesystem_serdes":1',
            context,
        )

    duplicate_marker = (
        '{"__durable_execution_filesystem_serdes":1,'
        '"__durable_execution_filesystem_serdes":1}'
    )
    with pytest.raises(SerDesError, match="Invalid"):
        await stage.deserialize(duplicate_marker, context)


async def test_stage_requires_durable_context_and_bounded_file_envelope(
    tmp_path: Path,
) -> None:
    with pytest.raises(SerDesError, match="SDK-managed"):
        await FileSystemSerDesStage(tmp_path).serialize(
            "value",
            SerDesContext(),
        )

    stage = FileSystemSerDesStage(
        tmp_path,
        FileSystemSerDesStageConfig(checkpoint_envelope_limit_bytes=1),
    )
    with pytest.raises(SerDesError, match="exceeds"):
        await stage.serialize("value", _context())


async def test_stage_rejects_tampered_file_and_wrong_owner(
    tmp_path: Path,
) -> None:
    stage = FileSystemSerDesStage(tmp_path)
    context = _context()
    serialized = await stage.serialize("trusted", context)
    envelope = json.loads(serialized)
    Path(envelope["file"]).write_text("corrupt")

    with pytest.raises(SerDesError, match="digest"):
        await stage.deserialize(serialized, context)

    serialized = await stage.serialize("trusted", context)
    envelope = json.loads(serialized)
    Path(envelope["file"]).write_bytes(b"x" * (1024 * 1024))

    with pytest.raises(SerDesError, match="size"):
        await stage.deserialize(serialized, context)

    serialized = await stage.serialize("trusted", context)
    with pytest.raises(SerDesError, match="different durable entity"):
        await stage.deserialize(
            serialized,
            _context(entity_id="operation/other/result"),
        )

    inline_stage = FileSystemSerDesStage(
        tmp_path,
        FileSystemSerDesStageConfig(
            storage_mode=FileSystemSerDesMode.OVERFLOW,
        ),
    )
    inline = await inline_stage.serialize("small", context)
    with pytest.raises(SerDesError, match="different durable entity"):
        await inline_stage.deserialize(
            inline,
            _context(entity_id="operation/other/result"),
        )


async def test_chained_invoke_can_resolve_cross_execution_owner(
    tmp_path: Path,
) -> None:
    stage = FileSystemSerDesStage(tmp_path)
    serialized = await stage.serialize("trusted", _context())
    consumer = _context(
        entity_id="operation/consumer/result",
        arn="arn:consumer",
    )
    consumer = replace(
        consumer,
        operation_type=OperationType.CHAINED_INVOKE,
    )

    assert await stage.deserialize(serialized, consumer) == "trusted"


async def test_stage_rejects_symlinked_execution_directory(
    tmp_path: Path,
) -> None:
    stage = FileSystemSerDesStage(tmp_path)
    context = _context()
    serialized = await stage.serialize("trusted", context)
    envelope = json.loads(serialized)
    file_path = Path(envelope["file"])
    outside = tmp_path / "outside"
    outside.mkdir()
    outside_file = outside / file_path.name
    outside_file.write_text("trusted")
    file_path.unlink()
    file_path.parent.rmdir()
    file_path.parent.symlink_to(outside, target_is_directory=True)

    with pytest.raises(SerDesError):
        await stage.deserialize(serialized, context)


async def test_hash_path_encoding_uses_fixed_length_segments(
    tmp_path: Path,
) -> None:
    stage = FileSystemSerDesStage(
        tmp_path,
        FileSystemSerDesStageConfig(
            path_encoding=FileSystemPathEncoding.HASH,
        ),
    )
    envelope = json.loads(
        await stage.serialize("value", _context(entity_id="x" * 10000))
    )
    file_path = Path(envelope["file"])

    assert len(file_path.parent.name) == 64
    assert file_path.name.startswith(
        f"{hashlib.sha256(('x' * 10000).encode()).hexdigest()}-"
    )


async def test_structured_preview_uses_preceding_json_stage(tmp_path: Path) -> None:
    stage = FileSystemSerDesStage(
        tmp_path,
        FileSystemSerDesStageConfig(
            preview_config=PreviewConfig(
                mode=PreviewMode.INCLUDE_ALL,
                exclude=(PreviewField("payload"),),
                mask=(PreviewField("secret"),),
            )
        ),
    )

    serialized = await stage.serialize(
        '{"id":"order-1","secret":"token","payload":"large"}',
        _context(),
    )

    assert json.loads(serialized)["preview"] == {
        "id": "order-1",
        "secret": "***",
    }


async def test_preview_generator_failures_are_serdes_errors(tmp_path: Path) -> None:
    async def invalid_preview(
        value: str,
        context: SerDesContext,
    ) -> Any:
        return "not a mapping"

    stage = FileSystemSerDesStage(
        tmp_path,
        FileSystemSerDesStageConfig(generate_preview=invalid_preview),
    )

    with pytest.raises(SerDesError, match="dict or None"):
        await stage.serialize("value", _context())
