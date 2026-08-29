"""Composable SerDes stage backed by a durable shared filesystem."""

from __future__ import annotations

import asyncio
import errno
import hashlib
import inspect
import json
import os
import re
import stat
import uuid
from collections.abc import Awaitable, Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any
from urllib.parse import quote

from ._core.context import SerDesContext
from ._core.exceptions import RetryableSerDesError, SerDesError
from ._core.models import OperationType
from .preview import PreviewConfig, build_preview

_ENVELOPE_MARKER = "__durable_execution_filesystem_serdes"
_ENVELOPE_VERSION = 1
_PAYLOAD_TYPE = "STRING"
_DEFAULT_CHECKPOINT_ENVELOPE_LIMIT_BYTES = 256 * 1024 - 1024
_DURABLE_EXECUTION_ARN_PATTERN = re.compile(
    r"^arn:[^:]*:lambda:[^:]*:[^:]*:function:"
    r"([^:/]+):[^:/]+/durable-execution/([^/]+)/([^/]+)$"
)
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_NON_RETRYABLE_FILESYSTEM_ERRNOS = {
    errno.EACCES,
    getattr(errno, "EDQUOT", errno.ENOSPC),
    errno.ELOOP,
    errno.EINVAL,
    errno.EISDIR,
    errno.ENAMETOOLONG,
    errno.ENOSPC,
    errno.ENOTDIR,
    errno.EPERM,
    errno.EROFS,
}
_FILESYSTEM_EXECUTOR = ThreadPoolExecutor(
    max_workers=4,
    thread_name_prefix="durable-filesystem-serdes",
)

PreviewGenerator = Callable[
    [str, SerDesContext],
    dict[str, Any] | None | Awaitable[dict[str, Any] | None],
]
CrossExecutionReferencePolicy = Callable[[str, str, SerDesContext], bool]


class FileSystemSerDesMode(str, Enum):
    """Controls when payload strings are offloaded to the filesystem."""

    ALWAYS = "ALWAYS"
    OVERFLOW = "OVERFLOW"


class FileSystemPathEncoding(str, Enum):
    """Controls how execution and entity identifiers appear in paths."""

    URI = "URI"
    HASH = "HASH"


@dataclass(frozen=True)
class FileSystemSerDesStageConfig:
    """Configuration for :class:`FileSystemSerDesStage`."""

    storage_mode: FileSystemSerDesMode = FileSystemSerDesMode.ALWAYS
    path_encoding: FileSystemPathEncoding = FileSystemPathEncoding.URI
    checkpoint_envelope_limit_bytes: int = _DEFAULT_CHECKPOINT_ENVELOPE_LIMIT_BYTES
    generate_preview: PreviewGenerator | None = None
    preview_config: PreviewConfig | None = None
    cross_execution_reference_policy: CrossExecutionReferencePolicy | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.storage_mode, FileSystemSerDesMode):
            msg = "storage_mode must be a FileSystemSerDesMode."
            raise TypeError(msg)
        if not isinstance(self.path_encoding, FileSystemPathEncoding):
            msg = "path_encoding must be a FileSystemPathEncoding."
            raise TypeError(msg)
        if self.checkpoint_envelope_limit_bytes <= 0:
            msg = "checkpoint_envelope_limit_bytes must be positive."
            raise ValueError(msg)
        if self.generate_preview is not None and self.preview_config is not None:
            msg = "Configure either generate_preview or preview_config, not both."
            raise ValueError(msg)
        if self.cross_execution_reference_policy is not None and not callable(
            self.cross_execution_reference_policy
        ):
            msg = "cross_execution_reference_policy must be callable."
            raise TypeError(msg)


class FileSystemSerDesStage:
    """Store a pipeline string on a durable shared filesystem.

    Do not use Lambda's ephemeral ``/tmp`` directory. Use a shared durable
    mount such as Amazon EFS or S3 Files.

    Payload files are immutable and uniquely named. The
    versioned checkpoint envelope records ownership and a SHA-256 digest.
    Unrecognized input passes through unchanged.
    """

    def __init__(
        self,
        base_path: str | os.PathLike[str],
        config: FileSystemSerDesStageConfig | None = None,
    ) -> None:
        if not os.fspath(base_path):
            msg = "base_path must not be empty."
            raise ValueError(msg)
        self._base_path = Path(os.path.abspath(os.fspath(base_path)))
        self._config = config or FileSystemSerDesStageConfig()

    def execution_directory(self, durable_execution_arn: str) -> Path:
        """Return the directory eligible for cleanup after history retention."""
        return self._resolve_execution_directory(durable_execution_arn)

    async def serialize(self, value: str, context: SerDesContext) -> str:
        """Store or inline ``value`` and return a versioned envelope."""
        self._require_context(context)
        payload = value.encode("utf-8")
        digest = hashlib.sha256(payload).hexdigest()
        payload_size = len(payload)

        if self._config.storage_mode is FileSystemSerDesMode.OVERFLOW:
            inline_envelope = self._encode_envelope(
                context=context,
                digest=digest,
                payload_size=payload_size,
                data=value,
            )
            if self._fits_checkpoint(inline_envelope):
                return inline_envelope

        file_path = self._resolve_payload_path(context, digest)
        preview = await self._generate_preview(value, context)
        file_envelope = self._encode_envelope(
            context=context,
            digest=digest,
            payload_size=payload_size,
            file_path=file_path,
            preview=preview,
        )
        if not self._fits_checkpoint(file_envelope):
            msg = (
                "Filesystem SerDes envelope exceeds the checkpoint payload "
                f"limit for entity {self._entity_id(context)!r}."
            )
            raise SerDesError(msg)

        try:
            await asyncio.get_running_loop().run_in_executor(
                _FILESYSTEM_EXECUTOR,
                _write_payload,
                file_path,
                payload,
            )
        except OSError as error:
            msg = (
                "Failed to store filesystem payload for entity "
                f"{self._entity_id(context)!r}."
            )
            _raise_filesystem_error(msg, error)
        return file_envelope

    async def deserialize(self, data: str, context: SerDesContext) -> str:
        """Resolve a recognized filesystem envelope or pass input through."""
        try:
            envelope = _strict_json_loads(data)
        except (TypeError, ValueError, json.JSONDecodeError) as error:
            if _contains_top_level_marker(data):
                self._require_context(context)
                raise self._malformed_envelope(context) from error
            return data

        if not isinstance(envelope, dict) or _ENVELOPE_MARKER not in envelope:
            return data

        self._require_context(context)
        version = envelope.get(_ENVELOPE_MARKER)
        if isinstance(version, bool) or not isinstance(version, int):
            raise self._malformed_envelope(context)
        if version != _ENVELOPE_VERSION:
            msg = (
                f"Unsupported filesystem SerDes envelope version {version} "
                f"for entity {self._entity_id(context)!r}."
            )
            raise SerDesError(msg)

        parsed = self._validate_envelope(envelope, context)
        digest = parsed["payloadDigest"]
        payload_size = parsed["payloadSizeBytes"]
        owner_arn = parsed["ownerDurableExecutionArn"]
        owner_entity_id = parsed["ownerEntityId"]
        self._validate_owner(owner_arn, owner_entity_id, context)

        inline_data = parsed.get("data")
        if isinstance(inline_data, str):
            inline_payload = inline_data.encode("utf-8")
            self._verify_payload_size(inline_payload, payload_size, context)
            self._verify_digest(inline_payload, digest, context)
            return inline_data

        file_path = self._validate_file_path(
            parsed["file"],
            owner_arn,
            owner_entity_id,
            digest,
        )
        try:
            payload = await asyncio.get_running_loop().run_in_executor(
                _FILESYSTEM_EXECUTOR,
                _read_payload,
                file_path,
                payload_size,
            )
        except OSError as error:
            msg = (
                "Failed to load filesystem payload for entity "
                f"{self._entity_id(context)!r}."
            )
            _raise_filesystem_error(msg, error)
        self._verify_digest(payload, digest, context)
        try:
            return payload.decode("utf-8")
        except UnicodeDecodeError as error:
            raise self._malformed_envelope(context) from error

    async def _generate_preview(
        self,
        value: str,
        context: SerDesContext,
    ) -> dict[str, Any] | None:
        generator = self._config.generate_preview
        try:
            if generator is not None:
                preview = generator(value, context)
                if inspect.isawaitable(preview):
                    preview = await preview
            elif self._config.preview_config is not None:
                preview = build_preview(
                    json.loads(value),
                    self._config.preview_config,
                )
            else:
                preview = None
        except Exception as error:
            msg = (
                "Failed to generate filesystem payload preview for entity "
                f"{self._entity_id(context)!r}."
            )
            raise SerDesError(msg) from error

        if preview is not None and not isinstance(preview, dict):
            msg = "Filesystem SerDes preview generator must return a dict or None."
            raise SerDesError(msg)
        return preview

    def _encode_envelope(
        self,
        *,
        context: SerDesContext,
        digest: str,
        payload_size: int,
        data: str | None = None,
        file_path: Path | None = None,
        preview: dict[str, Any] | None = None,
    ) -> str:
        envelope: dict[str, Any] = {
            _ENVELOPE_MARKER: _ENVELOPE_VERSION,
            "ownerDurableExecutionArn": context.durable_execution_arn,
            "ownerEntityId": self._entity_id(context),
            "payloadType": _PAYLOAD_TYPE,
            "payloadDigest": digest,
            "payloadSizeBytes": payload_size,
        }
        if data is not None:
            envelope["data"] = data
        elif file_path is not None:
            envelope["file"] = str(file_path)
            if preview is not None:
                envelope["preview"] = preview
        else:
            msg = "Filesystem SerDes envelope requires data or a file."
            raise SerDesError(msg)

        try:
            return json.dumps(
                envelope,
                ensure_ascii=False,
                separators=(",", ":"),
            )
        except (TypeError, ValueError) as error:
            msg = (
                "Failed to encode filesystem payload envelope for entity "
                f"{self._entity_id(context)!r}."
            )
            raise SerDesError(msg) from error

    def _validate_envelope(
        self,
        envelope: dict[str, Any],
        context: SerDesContext,
    ) -> dict[str, Any]:
        has_data = isinstance(envelope.get("data"), str)
        has_file = isinstance(envelope.get("file"), str)
        has_preview = "preview" in envelope
        expected_keys = {
            _ENVELOPE_MARKER,
            "ownerDurableExecutionArn",
            "ownerEntityId",
            "payloadType",
            "payloadDigest",
            "payloadSizeBytes",
            "data" if has_data else "file",
        }
        if has_preview:
            expected_keys.add("preview")

        valid = (
            has_data != has_file
            and set(envelope) == expected_keys
            and isinstance(envelope.get("ownerDurableExecutionArn"), str)
            and bool(envelope.get("ownerDurableExecutionArn"))
            and isinstance(envelope.get("ownerEntityId"), str)
            and bool(envelope.get("ownerEntityId"))
            and envelope.get("payloadType") == _PAYLOAD_TYPE
            and isinstance(envelope.get("payloadDigest"), str)
            and bool(_SHA256_PATTERN.fullmatch(envelope["payloadDigest"]))
            and isinstance(envelope.get("payloadSizeBytes"), int)
            and not isinstance(envelope.get("payloadSizeBytes"), bool)
            and envelope["payloadSizeBytes"] >= 0
            and (
                not has_preview or (has_file and isinstance(envelope["preview"], dict))
            )
        )
        if not valid:
            raise self._malformed_envelope(context)
        return envelope

    def _validate_owner(
        self,
        owner_arn: str,
        owner_entity_id: str,
        context: SerDesContext,
    ) -> None:
        same_owner = (
            owner_arn == context.durable_execution_arn
            and owner_entity_id == self._entity_id(context)
        )
        operation_type = context.operation_type
        operation_type_value = (
            operation_type.value
            if isinstance(operation_type, OperationType)
            else operation_type
        )
        if same_owner:
            return

        policy = self._config.cross_execution_reference_policy
        if (
            operation_type_value == OperationType.CHAINED_INVOKE.value
            and policy is not None
        ):
            try:
                if policy(owner_arn, owner_entity_id, context):
                    return
            except Exception as error:
                msg = "Filesystem SerDes cross-execution policy failed."
                raise SerDesError(msg) from error

        msg = "Filesystem SerDes file belongs to a different durable entity."
        raise SerDesError(msg)

    def _validate_file_path(
        self,
        file_value: str,
        owner_arn: str,
        owner_entity_id: str,
        digest: str,
    ) -> Path:
        file_path = Path(os.path.abspath(file_value))
        expected_directory = self._resolve_execution_directory(owner_arn)
        encoded_entity = self._encode(owner_entity_id)
        expected_name = re.compile(
            rf"^{re.escape(encoded_entity)}-{digest}-"
            r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
            r"[0-9a-f]{4}-[0-9a-f]{12}\.payload$"
        )
        if file_path.parent != expected_directory or not expected_name.fullmatch(
            file_path.name
        ):
            msg = "Filesystem SerDes file is not valid for its declared entity."
            raise SerDesError(msg)
        return file_path

    def _verify_digest(
        self,
        payload: bytes,
        expected_digest: str,
        context: SerDesContext,
    ) -> None:
        if hashlib.sha256(payload).hexdigest() != expected_digest:
            msg = (
                "Filesystem SerDes payload digest does not match stored "
                f"content for entity {self._entity_id(context)!r}."
            )
            raise SerDesError(msg)

    def _verify_payload_size(
        self,
        payload: bytes,
        expected_size: int,
        context: SerDesContext,
    ) -> None:
        if len(payload) != expected_size:
            msg = (
                "Filesystem SerDes payload size does not match the envelope "
                f"for entity {self._entity_id(context)!r}."
            )
            raise SerDesError(msg)

    def _resolve_payload_path(
        self,
        context: SerDesContext,
        digest: str,
    ) -> Path:
        directory = self._resolve_execution_directory(context.durable_execution_arn)
        filename = (
            f"{self._encode(self._entity_id(context))}-{digest}-{uuid.uuid4()}.payload"
        )
        return directory / filename

    def _resolve_execution_directory(self, durable_execution_arn: str) -> Path:
        if not durable_execution_arn.strip():
            msg = "durable_execution_arn must not be empty."
            raise SerDesError(msg)

        if self._config.path_encoding is FileSystemPathEncoding.URI:
            match = _DURABLE_EXECUTION_ARN_PATTERN.fullmatch(durable_execution_arn)
            if match is not None:
                directory = self._base_path.joinpath(
                    *(self._encode(part) for part in match.groups())
                )
                return self._require_strict_descendant(directory)
        directory = self._base_path / self._encode(durable_execution_arn)
        return self._require_strict_descendant(directory)

    def _encode(self, value: str) -> str:
        if self._config.path_encoding is FileSystemPathEncoding.HASH:
            return hashlib.sha256(value.encode("utf-8")).hexdigest()
        encoded = quote(value, safe="-._~")
        if encoded in {".", ".."}:
            return "".join(f"%{byte:02X}" for byte in value.encode("utf-8"))
        return encoded

    def _require_strict_descendant(self, directory: Path) -> Path:
        normalized = Path(os.path.abspath(directory))
        try:
            relative = normalized.relative_to(self._base_path)
        except ValueError as error:
            msg = "Filesystem SerDes execution directory is outside the base path."
            raise SerDesError(msg) from error
        if not relative.parts:
            msg = "Filesystem SerDes execution directory must be below the base path."
            raise SerDesError(msg)
        return normalized

    def _fits_checkpoint(self, envelope: str) -> bool:
        return (
            len(envelope.encode("utf-8"))
            <= self._config.checkpoint_envelope_limit_bytes
        )

    def _require_context(self, context: SerDesContext) -> None:
        if not context.durable_execution_arn or not self._entity_id(context):
            msg = (
                "FileSystemSerDesStage requires an SDK-managed SerDesContext "
                "with durable_execution_arn and entity_id."
            )
            raise SerDesError(msg)

    @staticmethod
    def _entity_id(context: SerDesContext) -> str:
        if context.entity_id:
            return context.entity_id
        if context.operation_id:
            return f"operation/{context.operation_id}"
        return ""

    def _malformed_envelope(self, context: SerDesContext) -> SerDesError:
        return SerDesError(
            "Invalid filesystem SerDes envelope for entity "
            f"{self._entity_id(context)!r}."
        )


def create_file_system_serdes_stage(
    base_path: str | os.PathLike[str],
    config: FileSystemSerDesStageConfig | None = None,
) -> FileSystemSerDesStage:
    """Create a filesystem stage for a composable SerDes pipeline."""
    return FileSystemSerDesStage(base_path, config)


def _raise_filesystem_error(message: str, error: OSError) -> None:
    if error.errno in _NON_RETRYABLE_FILESYSTEM_ERRNOS:
        raise SerDesError(message) from error
    raise RetryableSerDesError(message) from error


def _strict_json_loads(data: str) -> Any:
    def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                msg = f"Duplicate JSON object key: {key}"
                raise ValueError(msg)
            result[key] = value
        return result

    return json.loads(data, object_pairs_hook=reject_duplicate_keys)


def _contains_top_level_marker(data: str) -> bool:
    index = 0
    while index < len(data) and data[index].isspace():
        index += 1
    if index == len(data) or data[index] != "{":
        return False

    depth = 1
    index += 1
    while index < len(data) and depth > 0:
        current = data[index]
        if current in "{[":
            depth += 1
            index += 1
            continue
        if current in "}]":
            depth -= 1
            index += 1
            continue
        if current != '"':
            index += 1
            continue

        literal_start = index
        index += 1
        escaped = False
        while index < len(data):
            current = data[index]
            if escaped:
                escaped = False
            elif current == "\\":
                escaped = True
            elif current == '"':
                break
            index += 1
        if index == len(data):
            return False

        literal_end = index
        delimiter = index + 1
        while delimiter < len(data) and data[delimiter].isspace():
            delimiter += 1
        if depth == 1 and delimiter < len(data) and data[delimiter] == ":":
            try:
                field_name = json.loads(data[literal_start : literal_end + 1])
            except json.JSONDecodeError:
                return False
            if field_name == _ENVELOPE_MARKER:
                return True
        index += 1
    return False


def _directory_open_flags() -> int:
    return os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)


def _open_directory_path(path: Path, *, create: bool) -> int:
    if not path.is_absolute():
        msg = "Filesystem SerDes paths must be absolute."
        raise SerDesError(msg)

    root = Path(path.anchor)
    directory_fd = os.open(root, _directory_open_flags())
    try:
        for part in path.parts[1:]:
            if create:
                try:
                    os.mkdir(part, mode=0o700, dir_fd=directory_fd)
                except FileExistsError:
                    pass
            next_fd = os.open(part, _directory_open_flags(), dir_fd=directory_fd)
            os.close(directory_fd)
            directory_fd = next_fd
        return directory_fd
    except BaseException:
        os.close(directory_fd)
        raise


def _write_payload(file_path: Path, payload: bytes) -> None:
    directory_fd = _open_directory_path(file_path.parent, create=True)
    file_fd: int | None = None
    created = False
    try:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
        file_fd = os.open(
            file_path.name,
            flags,
            mode=0o600,
            dir_fd=directory_fd,
        )
        created = True
        view = memoryview(payload)
        while view:
            written = os.write(file_fd, view)
            view = view[written:]
    except BaseException:
        if created:
            try:
                os.unlink(file_path.name, dir_fd=directory_fd)
            except OSError:
                pass
        raise
    finally:
        if file_fd is not None:
            os.close(file_fd)
        os.close(directory_fd)


def _read_payload(file_path: Path, expected_size: int) -> bytes:
    directory_fd = _open_directory_path(file_path.parent, create=False)
    file_fd: int | None = None
    try:
        file_fd = os.open(
            file_path.name,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=directory_fd,
        )
        file_stat = os.fstat(file_fd)
        if not stat.S_ISREG(file_stat.st_mode):
            msg = "Filesystem SerDes envelope does not reference a regular file."
            raise SerDesError(msg)
        if file_stat.st_size != expected_size:
            msg = "Filesystem SerDes payload size does not match the envelope."
            raise SerDesError(msg)

        chunks: list[bytes] = []
        remaining = expected_size
        while remaining:
            chunk = os.read(file_fd, min(remaining, 1024 * 1024))
            if not chunk:
                msg = "Filesystem SerDes payload ended before its declared size."
                raise SerDesError(msg)
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(file_fd, 1):
            msg = "Filesystem SerDes payload exceeds its declared size."
            raise SerDesError(msg)
        return b"".join(chunks)
    finally:
        if file_fd is not None:
            os.close(file_fd)
        os.close(directory_fd)
