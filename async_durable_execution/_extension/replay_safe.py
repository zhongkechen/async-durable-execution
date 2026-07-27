"""Replay-safe helpers for common non-deterministic values."""

from __future__ import annotations

import asyncio
import random as _random
import time as _time
import uuid as _uuid
from datetime import datetime, timezone

from .._primitive.step import step


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
    return step(_random_value, name=name or "random")


def now(*, name: str | None = None) -> asyncio.Task[datetime]:
    """Return a checkpointed timezone-aware UTC `datetime`."""
    return step(_now_value, name=name or "now")


def timestamp(*, name: str | None = None) -> asyncio.Task[float]:
    """Return a checkpointed Unix timestamp in seconds."""
    return step(_timestamp_value, name=name or "timestamp")


def uuid(*, name: str | None = None) -> asyncio.Task[_uuid.UUID]:
    """Return a checkpointed UUID4 value."""
    return step(_uuid_value, name=name or "uuid")
