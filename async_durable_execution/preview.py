"""Build compact structured previews for externally stored payloads."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class PreviewMode(str, Enum):
    """Controls which fields are visible by default."""

    INCLUDE_ALL = "INCLUDE_ALL"
    EXCLUDE_ALL = "EXCLUDE_ALL"


class FieldMatchMode(str, Enum):
    """Controls how a preview selector matches a field."""

    ANYWHERE = "ANYWHERE"
    PATH = "PATH"


@dataclass(frozen=True)
class PreviewField:
    """A field name or exact dot-separated path used by preview rules."""

    name: str
    match: FieldMatchMode = FieldMatchMode.ANYWHERE

    def __post_init__(self) -> None:
        if not self.name:
            msg = "PreviewField.name must not be empty."
            raise ValueError(msg)


@dataclass(frozen=True)
class PreviewConfig:
    """Configuration for :func:`build_preview`."""

    mode: PreviewMode
    include: tuple[PreviewField, ...] = field(default_factory=tuple)
    exclude: tuple[PreviewField, ...] = field(default_factory=tuple)
    mask: tuple[PreviewField, ...] = field(default_factory=tuple)
    mask_string: str = "***"
    max_preview_bytes: int = 4096
    max_traversal_nodes: int = 10_000
    max_depth: int = 64

    def __post_init__(self) -> None:
        if self.max_preview_bytes <= 0:
            msg = "max_preview_bytes must be positive."
            raise ValueError(msg)
        if self.max_traversal_nodes <= 0:
            msg = "max_traversal_nodes must be positive."
            raise ValueError(msg)
        if self.max_depth <= 0:
            msg = "max_depth must be positive."
            raise ValueError(msg)


def _field_matches(path: str, preview_field: PreviewField) -> bool:
    if preview_field.match is FieldMatchMode.PATH:
        return path == preview_field.name
    return preview_field.name in path.split(".")


def _is_matched(path: str, fields: tuple[PreviewField, ...]) -> bool:
    return any(_field_matches(path, preview_field) for preview_field in fields)


def build_preview(
    value: Any,
    config: PreviewConfig,
) -> dict[str, Any] | None:
    """Build a bounded nested preview from a mapping-like JSON value.

    Exclusion wins over every other rule. Masking implies visibility unless
    the field is excluded. Arrays are traversed and matching object fields are
    merged into their containing preview path.
    """
    if not isinstance(value, dict):
        return None

    accepted: dict[str, Any] = {}
    visited_nodes = 0
    stopped = False

    def visit_node() -> bool:
        nonlocal stopped, visited_nodes
        visited_nodes += 1
        if visited_nodes > config.max_traversal_nodes:
            stopped = True
        return not stopped

    def add_value(path: str, preview_value: Any) -> None:
        nonlocal stopped
        missing = object()
        previous = accepted.get(path, missing)
        accepted[path] = preview_value
        candidate_preview = _paths_to_nested_dict(accepted)
        encoded = json.dumps(
            candidate_preview,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        if len(encoded) <= config.max_preview_bytes:
            return

        if previous is missing:
            del accepted[path]
        else:
            accepted[path] = previous
        stopped = True

    def collect(current: Any, path_prefix: str, depth: int) -> None:
        nonlocal stopped
        if stopped or depth > config.max_depth or not visit_node():
            return
        if isinstance(current, list):
            for item in current:
                collect(item, path_prefix, depth + 1)
                if stopped:
                    break
            return
        if not isinstance(current, dict):
            return

        for raw_key, child in current.items():
            if stopped or not visit_node():
                break
            key = str(raw_key)
            if "." in key:
                continue

            path = f"{path_prefix}.{key}" if path_prefix else key
            excluded = _is_matched(path, config.exclude)
            masked = _is_matched(path, config.mask)
            visible = not excluded and (
                masked
                or config.mode is PreviewMode.INCLUDE_ALL
                or _is_matched(path, config.include)
            )

            if not visible:
                if not excluded and isinstance(child, dict | list):
                    collect(child, path, depth + 1)
                continue
            if masked:
                add_value(path, config.mask_string)
            elif isinstance(child, dict | list):
                collect(child, path, depth + 1)
            else:
                add_value(path, child)

    collect(value, "", 0)
    if not accepted:
        return None
    return _paths_to_nested_dict(accepted)


def _paths_to_nested_dict(paths: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for path, value in paths.items():
        parts = path.split(".")
        node = result
        for part in parts[:-1]:
            existing = node.get(part)
            if not isinstance(existing, dict):
                existing = {}
                node[part] = existing
            node = existing
        node[parts[-1]] = value
    return result
