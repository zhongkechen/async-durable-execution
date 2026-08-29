"""Tests for the filesystem-backed SerDes pipeline stage."""

from __future__ import annotations

import errno
import hashlib
import json
import os
import stat
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

import async_durable_execution.filesystem_serdes as filesystem_serdes_module
from async_durable_execution import (
    FileSystemPathEncoding,
    FileSystemSerDesMode,
    FileSystemSerDesStage,
    FileSystemSerDesStageConfig,
    JsonSerDes,
    OperationType,
    PreviewConfig,
    PreviewField,
    PreviewMode,
    RetryableSerDesError,
    SerDesContext,
    SerDesError,
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
    with pytest.raises(TypeError, match="cross_execution_reference_policy"):
        FileSystemSerDesStageConfig(
            cross_execution_reference_policy=True,  # type: ignore[arg-type]
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
    assert len(list(tmp_path.rglob("*.payload"))) == 2


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


async def test_filesystem_io_errors_are_classified_for_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stage = FileSystemSerDesStage(tmp_path)

    def fail_write(_base_path: Path, _path: Path, _payload: bytes) -> None:
        raise OSError(errno.EIO, "transient mount failure")

    monkeypatch.setattr(filesystem_serdes_module, "_write_payload", fail_write)
    with pytest.raises(RetryableSerDesError, match="Failed to store"):
        await stage.serialize("trusted", _context())

    def deny_write(_base_path: Path, _path: Path, _payload: bytes) -> None:
        raise OSError(errno.EACCES, "permission denied")

    monkeypatch.setattr(filesystem_serdes_module, "_write_payload", deny_write)
    with pytest.raises(SerDesError, match="Failed to store"):
        await stage.serialize("trusted", _context())

    def reject_symlink(_base_path: Path, _path: Path, _payload: bytes) -> None:
        raise OSError(errno.ELOOP, "symbolic link rejected")

    monkeypatch.setattr(filesystem_serdes_module, "_write_payload", reject_symlink)
    with pytest.raises(SerDesError, match="Failed to store"):
        await stage.serialize("trusted", _context())


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


async def test_stage_does_not_create_missing_configured_base_path(
    tmp_path: Path,
) -> None:
    missing_base = tmp_path / "missing-mount"
    stage = FileSystemSerDesStage(missing_base)

    with pytest.raises(SerDesError, match="base path does not exist"):
        await stage.serialize("trusted", _context())

    assert not missing_base.exists()


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
    Path(envelope["file"]).unlink()

    serialized = await stage.serialize("trusted", context)
    envelope = json.loads(serialized)
    Path(envelope["file"]).write_bytes(b"x" * (1024 * 1024))

    with pytest.raises(SerDesError, match="size"):
        await stage.deserialize(serialized, context)
    Path(envelope["file"]).unlink()

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

    chained_consumer = replace(
        _context(
            entity_id="operation/consumer/result",
            arn="arn:consumer",
        ),
        operation_type=OperationType.CHAINED_INVOKE,
    )
    with pytest.raises(SerDesError, match="different durable entity"):
        await stage.deserialize(serialized, chained_consumer)


async def test_stage_treats_missing_checkpoint_payload_as_permanent(
    tmp_path: Path,
) -> None:
    stage = FileSystemSerDesStage(tmp_path)
    context = _context()
    serialized = await stage.serialize("trusted", context)
    Path(json.loads(serialized)["file"]).unlink()

    with pytest.raises(SerDesError, match="Failed to load"):
        await stage.deserialize(serialized, context)


async def test_chained_invoke_can_resolve_cross_execution_owner(
    tmp_path: Path,
) -> None:
    trusted_owners: list[tuple[str, str]] = []

    def trust_producer(
        owner_arn: str,
        owner_entity_id: str,
        _context: SerDesContext,
    ) -> bool:
        trusted_owners.append((owner_arn, owner_entity_id))
        return (
            owner_arn == EXECUTION_ARN
            and owner_entity_id == "operation/operation-1/result"
        )

    stage = FileSystemSerDesStage(
        tmp_path,
        FileSystemSerDesStageConfig(
            cross_execution_reference_policy=trust_producer,
        ),
    )
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
    assert trusted_owners == [
        (EXECUTION_ARN, "operation/operation-1/result"),
    ]


def test_execution_directory_supports_retention_cleanup(tmp_path: Path) -> None:
    stage = FileSystemSerDesStage(tmp_path)

    assert stage.execution_directory(EXECUTION_ARN) == (
        tmp_path
        / "aws"
        / "us-east-1"
        / "123456789012"
        / "test"
        / "1"
        / "run"
        / "invocation"
    )
    with pytest.raises(SerDesError, match="must not be empty"):
        stage.execution_directory(" ")

    arn_variants = (
        EXECUTION_ARN.replace("arn:aws:", "arn:aws-cn:"),
        EXECUTION_ARN.replace("us-east-1", "us-west-2"),
        EXECUTION_ARN.replace("123456789012", "210987654321"),
        EXECUTION_ARN.replace("function:test:1", "function:other:1"),
        EXECUTION_ARN.replace("function:test:1", "function:test:2"),
    )
    assert all(
        stage.execution_directory(variant) != stage.execution_directory(EXECUTION_ARN)
        for variant in arn_variants
    )

    hostile_arn = (
        "arn:aws:lambda:us-east-1:123456789012:function:..:1/durable-execution/../.."
    )
    hostile_directory = stage.execution_directory(hostile_arn)
    assert hostile_directory.is_relative_to(tmp_path)
    assert hostile_directory != tmp_path
    assert ".." not in hostile_directory.relative_to(tmp_path).parts


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


async def test_stage_syncs_payload_and_directory_before_returning_envelope(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stage = FileSystemSerDesStage(tmp_path)
    execution_directory = stage.execution_directory(EXECUTION_ARN)
    execution_directory.mkdir(parents=True)
    sync_targets: list[str] = []
    real_fsync = os.fsync

    def record_sync(file_descriptor: int) -> None:
        mode = os.fstat(file_descriptor).st_mode
        sync_targets.append("directory" if stat.S_ISDIR(mode) else "file")
        real_fsync(file_descriptor)

    monkeypatch.setattr(filesystem_serdes_module.os, "fsync", record_sync)

    await stage.serialize("trusted", _context())

    traversed_directory_count = len(execution_directory.relative_to(tmp_path).parts)
    assert sync_targets == [
        *(["directory"] * traversed_directory_count),
        "file",
        "directory",
    ]


async def test_stage_syncs_each_new_directory_entry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stage = FileSystemSerDesStage(tmp_path)
    execution_directory = stage.execution_directory(EXECUTION_ARN)
    sync_targets: list[str] = []
    real_fsync = os.fsync

    def record_sync(file_descriptor: int) -> None:
        mode = os.fstat(file_descriptor).st_mode
        sync_targets.append("directory" if stat.S_ISDIR(mode) else "file")
        real_fsync(file_descriptor)

    monkeypatch.setattr(filesystem_serdes_module.os, "fsync", record_sync)

    await stage.serialize("trusted", _context())

    new_directory_count = len(execution_directory.relative_to(tmp_path).parts)
    assert sync_targets == [
        *(["directory"] * new_directory_count),
        "file",
        "directory",
    ]


async def test_stage_resyncs_existing_directory_after_failed_parent_sync(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stage = FileSystemSerDesStage(tmp_path)
    execution_directory = stage.execution_directory(EXECUTION_ARN)
    real_fsync = os.fsync
    sync_calls = 0

    def fail_first_sync(file_descriptor: int) -> None:
        nonlocal sync_calls
        sync_calls += 1
        if sync_calls == 1:
            raise OSError(errno.EIO, "parent sync failed")
        real_fsync(file_descriptor)

    monkeypatch.setattr(filesystem_serdes_module.os, "fsync", fail_first_sync)
    with pytest.raises(RetryableSerDesError, match="Failed to store"):
        await stage.serialize("trusted", _context())

    assert (tmp_path / execution_directory.relative_to(tmp_path).parts[0]).is_dir()

    sync_targets: list[str] = []

    def record_sync(file_descriptor: int) -> None:
        mode = os.fstat(file_descriptor).st_mode
        sync_targets.append("directory" if stat.S_ISDIR(mode) else "file")
        real_fsync(file_descriptor)

    monkeypatch.setattr(filesystem_serdes_module.os, "fsync", record_sync)
    await stage.serialize("trusted", _context())

    traversed_directory_count = len(execution_directory.relative_to(tmp_path).parts)
    assert sync_targets == [
        *(["directory"] * traversed_directory_count),
        "file",
        "directory",
    ]


@pytest.mark.parametrize(
    ("failure_call", "error_number", "expected_error"),
    [
        (1, errno.EIO, RetryableSerDesError),
        (1, errno.EOPNOTSUPP, SerDesError),
        (2, errno.EROFS, SerDesError),
    ],
)
async def test_stage_classifies_sync_failures_and_removes_unpublished_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_call: int,
    error_number: int,
    expected_error: type[Exception],
) -> None:
    stage = FileSystemSerDesStage(tmp_path)
    stage.execution_directory(EXECUTION_ARN).mkdir(parents=True)
    sync_calls = 0
    real_fsync = os.fsync

    def fail_sync(file_descriptor: int) -> None:
        nonlocal sync_calls
        sync_calls += 1
        if sync_calls == failure_call:
            raise OSError(error_number, "sync failed")
        real_fsync(file_descriptor)

    monkeypatch.setattr(filesystem_serdes_module.os, "fsync", fail_sync)

    with pytest.raises(expected_error, match="Failed to store"):
        await stage.serialize("trusted", _context())

    assert list(tmp_path.rglob("*.payload")) == []


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
