"""Replay-safe helpers for common non-deterministic values."""

from __future__ import annotations

import asyncio
import random as _random
import time as _time
import uuid as _uuid
from datetime import datetime, timezone

from .._core import OperationSubType
from ..extension import ExtensionStepResult, get_extension_context


async def _random_value() -> float:
    return _random.random()


async def _now_value() -> datetime:
    return datetime.now(tz=timezone.utc)


async def _timestamp_value() -> float:
    return _time.time()


async def _uuid_value() -> _uuid.UUID:
    return _uuid.uuid4()


def random(*, name: str | None = None) -> asyncio.Task[float]:
    """Return a checkpointed `random.random()` value."""

    async def run(_state: float | None) -> ExtensionStepResult[float]:
        return ExtensionStepResult.succeed(await _random_value())

    return (
        get_extension_context()
        .reserve(name or "random")
        .step(run, sub_type=OperationSubType.STEP)
    )


def now(*, name: str | None = None) -> asyncio.Task[datetime]:
    """Return a checkpointed timezone-aware UTC `datetime`."""

    async def run(_state: datetime | None) -> ExtensionStepResult[datetime]:
        return ExtensionStepResult.succeed(await _now_value())

    return (
        get_extension_context()
        .reserve(name or "now")
        .step(run, sub_type=OperationSubType.STEP)
    )


def timestamp(*, name: str | None = None) -> asyncio.Task[float]:
    """Return a checkpointed Unix timestamp in seconds."""

    async def run(_state: float | None) -> ExtensionStepResult[float]:
        return ExtensionStepResult.succeed(await _timestamp_value())

    return (
        get_extension_context()
        .reserve(name or "timestamp")
        .step(run, sub_type=OperationSubType.STEP)
    )


def uuid(*, name: str | None = None) -> asyncio.Task[_uuid.UUID]:
    """Return a checkpointed UUID4 value."""

    async def run(_state: _uuid.UUID | None) -> ExtensionStepResult[_uuid.UUID]:
        return ExtensionStepResult.succeed(await _uuid_value())

    return (
        get_extension_context()
        .reserve(name or "uuid")
        .step(run, sub_type=OperationSubType.STEP)
    )
