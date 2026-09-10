"""User-facing durable wait operation."""

from __future__ import annotations

import asyncio

from .._core import (
    Duration,
    OperationSubType,
    ValidationError,
    duration_to_seconds,
)
from .._extension_api import get_extension_context


def wait(duration: Duration, *, name: str | None = None) -> asyncio.Task[None]:
    """Suspend the durable execution for at least the given duration.

    Args:
        duration: Seconds or timedelta to pause. Must be at least one second.
        name: Optional operation name shown in execution history.
    """
    seconds = duration_to_seconds(duration)
    if seconds < 1:
        msg = "duration must be at least 1 second"
        raise ValidationError(msg)

    return (
        get_extension_context()
        ._reserve_sdk_operation(  # noqa: SLF001
            name,
        )
        ._run_wait(  # noqa: SLF001
            duration,
            sub_type=OperationSubType.WAIT,
        )
    )


__all__ = ["wait"]
