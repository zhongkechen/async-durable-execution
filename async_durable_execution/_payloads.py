"""Payload storage, integrity checks, and bounded structured previews."""

from __future__ import annotations

import asyncio
import errno
import hashlib
import inspect
import json
import os
import stat
import uuid
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable
from urllib.parse import quote

from ._scope import SerDesContext
from ._types import OperationType, RetryableSerDesError, SerDesError


class PreviewMode(str, Enum):
    """Choose whether fields are visible by default or only through inclusion/masking
    rules.
    """

    INCLUDE_ALL = "INCLUDE_ALL"
    EXCLUDE_ALL = "EXCLUDE_ALL"


class FieldMatchMode(str, Enum):
    """Choose how preview selectors match object fields.

    ANYWHERE matches a name in any path component. PATH matches the complete
    dot-separated field path exactly.
    """

    ANYWHERE = "ANYWHERE"
    PATH = "PATH"


@dataclass(frozen=True)
class PreviewField:
    """Selector identifying fields to include, exclude, or mask in a preview.

    Attributes:
        name (str): Nonempty field name or dot-separated path.
        match (FieldMatchMode): Match anywhere in the path, or match the full path.
    """

    name: str
    match: FieldMatchMode = FieldMatchMode.ANYWHERE

    def __post_init__(self):
        if not self.name:
            raise ValueError("Preview field names cannot be empty")
        if not isinstance(self.match, FieldMatchMode):
            raise TypeError("Invalid field match mode")


@dataclass(frozen=True)
class PreviewConfig:
    """Visibility rules and resource bounds for a structured payload preview.

    Exclusion takes precedence over masking and inclusion. Masking makes a field
    visible unless excluded. Traversal never copies fields whose names contain dots.

    Attributes:
        mode (PreviewMode): Default field visibility.
        include (tuple[PreviewField, ...]): Fields visible in EXCLUDE_ALL mode.
        exclude (tuple[PreviewField, ...]): Fields omitted regardless of other rules.
        mask (tuple[PreviewField, ...]): Fields replaced by mask_string.
        mask_string (str): Replacement value; defaults to three asterisks.
        max_preview_bytes (int): Positive UTF-8 JSON byte budget; default 4096.
        max_traversal_nodes (int): Positive visit limit; default 10000.
        max_depth (int): Positive traversal depth limit; default 64.
    """

    mode: PreviewMode
    include: tuple = field(default_factory=tuple)
    exclude: tuple = field(default_factory=tuple)
    mask: tuple = field(default_factory=tuple)
    mask_string: str = "***"
    max_preview_bytes: int = 4096
    max_traversal_nodes: int = 10000
    max_depth: int = 64

    def __post_init__(self):
        if not isinstance(self.mode, PreviewMode):
            raise TypeError("Invalid preview mode")
        if min(self.max_preview_bytes, self.max_traversal_nodes, self.max_depth) <= 0:
            raise ValueError("Preview bounds must be positive")


def matches(path, fields):
    return any(
        (".".join(path) == selector.name)
        if selector.match is FieldMatchMode.PATH
        else selector.name in path
        for selector in fields
    )


def nested(leaves):
    result: dict[str, Any] = {}
    for path, value in leaves.items():
        target = result
        for key in path[:-1]:
            if not isinstance(target.get(key), dict):
                target[key] = {}
            target = target[key]
        target[path[-1]] = value
    return result


def build_preview(value, config):
    """Select and mask object fields within byte, depth, and traversal limits.

    Arrays are traversed using their containing field path; matching object fields
    from later elements may replace values at the same preview path. Traversal stops
    when the next accepted field would exceed the byte budget.

    Args:
        value (Any): JSON-like dictionary to preview; other root types return None.
        config (PreviewConfig): Visibility and resource-limit settings.

    Returns:
        (dict | None): Nested preview fields, or None when no fields are selected.
    """
    if not isinstance(value, dict):
        return None
    leaves: dict[tuple[str, ...], Any] = {}
    stack: list[tuple[tuple[str, ...], Any, int]] = [((), value, 0)]
    budget = config.max_traversal_nodes
    while stack and budget:
        path, item, depth = stack.pop()
        budget -= 1
        if depth > config.max_depth or matches(path, config.exclude):
            continue
        masked = bool(path) and matches(path, config.mask)
        if masked:
            item = config.mask_string
        elif isinstance(item, dict):
            stack.extend(
                ((*path, str(key)), child, depth + 1)
                for key, child in reversed(tuple(item.items()))
                if "." not in str(key)
            )
            continue
        elif isinstance(item, list):
            stack.extend((path, child, depth + 1) for child in reversed(item))
            continue
        if not path or (
            not masked
            and config.mode is PreviewMode.EXCLUDE_ALL
            and not matches(path, config.include)
        ):
            continue
        candidate = dict(leaves)
        candidate[path] = item
        if (
            len(
                json.dumps(
                    nested(candidate), ensure_ascii=False, separators=(",", ":")
                ).encode()
            )
            > config.max_preview_bytes
        ):
            break
        leaves = candidate
    return nested(leaves) if leaves else None


class FileSystemSerDesMode(str, Enum):
    """Choose whether payloads always use files or remain inline when they fit.

    ALWAYS writes an immutable file. OVERFLOW first tries the complete inline
    envelope against the configured checkpoint byte limit.
    """

    ALWAYS = "ALWAYS"
    OVERFLOW = "OVERFLOW"


class FileSystemPathEncoding(str, Enum):
    """Choose readable URI-encoded execution paths or fixed-length hashed path segments."""

    URI = "URI"
    HASH = "HASH"


@dataclass(frozen=True)
class FileSystemSerDesStageConfig:
    """Filesystem offload, preview, and cross-execution ownership settings.

    Attributes:
        storage_mode (FileSystemSerDesMode): ALWAYS or OVERFLOW; defaults to ALWAYS.
        path_encoding (FileSystemPathEncoding): URI or HASH; defaults to URI.
        checkpoint_envelope_limit_bytes (int): Positive UTF-8 envelope limit; default
            261120.
        generate_preview (Callable | None): Optional sync or async callback receiving
            the
            serialized string and context, returning a dictionary or None.
        preview_config (PreviewConfig | None): Built-in preview rules, mutually
            exclusive
            with generate_preview.
        cross_execution_reference_policy (Callable | None): Synchronous predicate
            receiving
            producer ARN, producer entity ID, and consumer context. Must return bool;
            it is consulted for cross-owner chained-invocation results only.
    """

    storage_mode: FileSystemSerDesMode = FileSystemSerDesMode.ALWAYS
    path_encoding: FileSystemPathEncoding = FileSystemPathEncoding.URI
    checkpoint_envelope_limit_bytes: int = 261120
    generate_preview: Callable[..., Any] | None = None
    preview_config: PreviewConfig | None = None
    cross_execution_reference_policy: Callable[..., bool] | None = None

    def __post_init__(self):
        if not isinstance(self.storage_mode, FileSystemSerDesMode) or not isinstance(
            self.path_encoding, FileSystemPathEncoding
        ):
            raise TypeError("Filesystem mode and encoding must be enum values")
        if self.checkpoint_envelope_limit_bytes <= 0:
            raise ValueError("Envelope limit must be positive")
        if self.generate_preview is not None and self.preview_config is not None:
            raise ValueError("Configure only one preview generator")
        if self.cross_execution_reference_policy is not None and not callable(
            self.cross_execution_reference_policy
        ):
            raise TypeError("Reference policy must be callable")


def directory(path, create=False, root=None):
    """Walk every component with directory descriptors, rejecting symlinks."""
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    fd = directory(root) if root is not None else os.open(path.anchor, flags)
    parts = path.relative_to(root).parts if root is not None else path.parts[1:]
    try:
        for part in parts:
            if part in (".", ".."):
                raise SerDesError("Invalid directory component")
            if create:
                try:
                    os.mkdir(part, 0o700, dir_fd=fd)
                except FileExistsError:
                    pass
                os.fsync(fd)
            child = os.open(part, flags, dir_fd=fd)
            os.close(fd)
            fd = child
        return fd
    except BaseException:
        os.close(fd)
        raise


def persist(base, path, payload):
    fd = directory(path.parent, create=True, root=base)
    owned = False
    try:
        target = os.open(
            path.name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
            dir_fd=fd,
        )
        owned = True
        try:
            with os.fdopen(target, "wb") as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            os.fsync(fd)
        except BaseException:
            if owned:
                os.unlink(path.name, dir_fd=fd)
            raise
    finally:
        os.close(fd)


def retrieve(path, size):
    fd = directory(path.parent)
    try:
        target = os.open(
            path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd
        )
        with os.fdopen(target, "rb") as stream:
            metadata = os.fstat(stream.fileno())
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_size != size:
                raise SerDesError("Payload file type or length is invalid")
            payload = stream.read(size + 1)
            if len(payload) != size:
                raise SerDesError("Payload file changed while being read")
            return payload
    finally:
        os.close(fd)


def filesystem_error(error):
    permanent = {
        errno.EACCES,
        errno.EPERM,
        errno.ENOENT,
        errno.ELOOP,
        errno.ENOTDIR,
        errno.EISDIR,
        errno.EINVAL,
        errno.ENOSPC,
        errno.EROFS,
        errno.ENAMETOOLONG,
        errno.EFBIG,
    }
    cls = SerDesError if error.errno in permanent else RetryableSerDesError
    return cls(f"Filesystem payload operation failed: {error}")


class FileSystemSerDesStage:
    """Store serialized strings on an existing durable shared filesystem.

    Compose after a value codec, for example JsonSerDes().then(stage). The returned
    envelope binds the payload to its execution and entity, size, and SHA-256 digest.
    Files are immutable and must outlive every checkpoint that references them.
    The stage does not automatically delete payloads.
    """

    def __init__(self, base_path, config=None):
        """Configure the durable filesystem root and offload policy.

        Args:
            base_path (str | os.PathLike): Existing shared filesystem directory.
                Creating
                the stage does not create the root or access the filesystem.
            config (FileSystemSerDesStageConfig | None): Storage and preview settings;
                None uses default settings.

        Raises:
            ValueError: base_path is empty.
        """
        if not os.fspath(base_path):
            raise ValueError("Filesystem base path cannot be empty")
        self.base_path = Path(os.path.abspath(base_path))
        self.config = config or FileSystemSerDesStageConfig()

    def _component(self, text):
        if self.config.path_encoding is FileSystemPathEncoding.HASH:
            return hashlib.sha256(text.encode()).hexdigest()
        if text in (".", ".."):
            return text.replace(".", "%2E")
        return quote(text, safe="-_~")

    def execution_directory(self, durable_execution_arn):
        """Resolve an execution's payload directory without creating it.

        Use this path for retention cleanup only after all referencing executions and
        their history-retention windows have ended.

        Args:
            durable_execution_arn (str): Nonblank durable owner identity.

        Returns:
            (Path): Owner-specific directory beneath the configured base path.
        """
        if (
            not isinstance(durable_execution_arn, str)
            or not durable_execution_arn.strip()
        ):
            raise SerDesError("A durable execution identity is required")
        parts = (
            durable_execution_arn.replace(":", "/").split("/")
            if self.config.path_encoding is FileSystemPathEncoding.URI
            else [durable_execution_arn]
        )
        readable = self.base_path.joinpath(
            *(self._component(part) for part in parts if part)
        )
        # Preserve full owner identity even when readable separator splitting
        # produces the same components for two accepted identity strings.
        return readable / hashlib.sha256(durable_execution_arn.encode()).hexdigest()

    def _owner(self, context):
        entity = context.entity_id or (
            f"operation/{context.operation_id}" if context.operation_id else ""
        )
        if not context.durable_execution_arn or not entity:
            raise SerDesError("Filesystem stages require an SDK serialization context")
        return context.durable_execution_arn, entity

    def _json(self, value):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))

    async def serialize(self, value, context):
        """Create an inline envelope or durably publish a new immutable payload file.

        The file envelope is returned only after the write and directory metadata have
        been synchronized. Repeating serialization uses a new filename.

        Args:
            value (str): Output of the preceding codec or stage.
            context (SerDesContext): SDK-supplied execution and entity identity.

        Returns:
            (str): Versioned envelope suitable for checkpoint storage.

        Raises:
            SerDesError: Context, preview, filesystem configuration, or envelope size is
                invalid.
            RetryableSerDesError: A transient filesystem failure may succeed on retry.
        """
        owner, entity = self._owner(context)
        raw = value.encode("utf-8")
        digest = hashlib.sha256(raw).hexdigest()
        envelope = {
            "adeFS": 3,
            "owner": owner,
            "entity": entity,
            "sha256": digest,
            "bytes": len(raw),
        }
        inline = self._json(dict(envelope, data=value))
        if (
            self.config.storage_mode is FileSystemSerDesMode.OVERFLOW
            and len(inline.encode()) <= self.config.checkpoint_envelope_limit_bytes
        ):
            return inline
        path = self.execution_directory(owner) / (
            self._component(entity) + "-" + digest + "-" + uuid.uuid4().hex + ".payload"
        )
        envelope["file"] = str(path)
        try:
            preview = None
            if self.config.generate_preview:
                preview = self.config.generate_preview(value, context)
                if inspect.isawaitable(preview):
                    preview = await preview
            elif self.config.preview_config:
                source = (
                    context.original_value
                    if context.original_value is not None
                    else json.loads(value)
                )
                preview = build_preview(source, self.config.preview_config)
            if preview is not None:
                if not isinstance(preview, dict):
                    raise SerDesError(
                        "Preview generator must return a dictionary or None"
                    )
                envelope["preview"] = preview
            encoded = self._json(envelope)
        except RetryableSerDesError:
            raise
        except Exception as error:
            raise SerDesError(
                f"Cannot prepare filesystem payload envelope: {error}"
            ) from error
        if len(encoded.encode()) > self.config.checkpoint_envelope_limit_bytes:
            raise SerDesError("Filesystem envelope exceeds the checkpoint limit")
        try:
            await asyncio.to_thread(persist, self.base_path, path, raw)
        except OSError as error:
            raise filesystem_error(error) from error
        return encoded

    async def deserialize(self, data, context):
        """Validate and read an owned payload, or pass unrecognized strings through.

        Recognized envelopes are checked for version, owner, path, file type, size,
        and digest. Cross-owner reads require explicit approval by the configured policy
        and are restricted to chained-invocation contexts.

        Args:
            data (str): Stored envelope or a string not produced by this stage.
            context (SerDesContext): Consumer execution, entity, and operation metadata.

        Returns:
            (str): Original serialized payload for the preceding codec or stage.

        Raises:
            SerDesError: A recognized envelope, ownership claim, path, or payload is
                invalid.
            RetryableSerDesError: A transient filesystem read failure may succeed on
                retry.
        """

        def unique(pairs):
            result = {}
            for key, value in pairs:
                if key in result:
                    raise ValueError("Duplicate JSON key")
                result[key] = value
            return result

        try:
            envelope = json.loads(data, object_pairs_hook=unique)
        except (ValueError, TypeError) as error:
            if isinstance(data, str) and data.lstrip().startswith('{"adeFS"'):
                raise SerDesError("Malformed filesystem envelope") from error
            return data
        if not isinstance(envelope, dict) or "adeFS" not in envelope:
            return data
        owner, entity = self._owner(context)
        if type(envelope["adeFS"]) is not int or envelope["adeFS"] != 3:
            raise SerDesError("Unsupported filesystem envelope version")
        try:
            stored_owner, stored_entity = envelope["owner"], envelope["entity"]
            digest, size = envelope["sha256"], envelope["bytes"]
            if (
                not isinstance(stored_owner, str)
                or not isinstance(stored_entity, str)
                or not isinstance(digest, str)
                or len(digest) != 64
                or any(ch not in "0123456789abcdef" for ch in digest)
                or type(size) is not int
                or size < 0
            ):
                raise ValueError("Invalid payload metadata")
            if (stored_owner, stored_entity) != (owner, entity):
                policy = self.config.cross_execution_reference_policy
                if (
                    context.operation_type
                    not in (OperationType.CHAINED_INVOKE, "CHAINED_INVOKE")
                    or policy is None
                ):
                    raise ValueError("Payload belongs to another durable entity")
                allowed = policy(stored_owner, stored_entity, context)
                if inspect.iscoroutine(allowed):
                    allowed.close()
                if type(allowed) is not bool or not allowed:
                    raise ValueError("Cross-execution reference denied")
            inline = "data" in envelope
            required = {
                "adeFS",
                "owner",
                "entity",
                "sha256",
                "bytes",
                "data" if inline else "file",
            }
            if not inline and "preview" in envelope:
                required.add("preview")
            if set(envelope) != required:
                raise ValueError("Invalid envelope fields")
            if inline:
                raw = envelope["data"].encode()
            else:
                path = Path(envelope["file"])
                expected = self.execution_directory(stored_owner)
                prefix = self._component(stored_entity) + "-" + digest + "-"
                suffix = path.name.removeprefix(prefix).removesuffix(".payload")
                if (
                    path.parent != expected
                    or not path.name.startswith(prefix)
                    or not path.name.endswith(".payload")
                    or len(suffix) != 32
                    or any(c not in "0123456789abcdef" for c in suffix)
                ):
                    raise ValueError("Payload path is not bound to its owner")
                raw = await asyncio.to_thread(retrieve, path, size)
            if len(raw) != size or hashlib.sha256(raw).hexdigest() != digest:
                raise ValueError("Payload integrity check failed")
            return raw.decode("utf-8")
        except OSError as error:
            raise filesystem_error(error) from error
        except (ValueError, KeyError, TypeError, AttributeError) as error:
            raise SerDesError(f"Invalid filesystem payload: {error}") from error


def create_file_system_serdes_stage(base_path, config=None):
    """Create a filesystem string stage for a serializer pipeline.

    Args:
        base_path (str | os.PathLike): Existing durable shared filesystem root.
        config (FileSystemSerDesStageConfig | None): Offload, preview, and ownership
            policy.

    Returns:
        (FileSystemSerDesStage): Stage to append with SerDes.then().
    """
    return FileSystemSerDesStage(base_path, config)
