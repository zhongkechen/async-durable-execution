"""User-facing durable wait operation."""

from __future__ import annotations

import asyncio

from .._core import Duration, OperationSubType
from ..extension import get_extension_context


def wait(duration: Duration, *, name: str | None = None) -> asyncio.Task[None]:
    """Suspend the durable execution for at least the given duration.

    Args:
        duration: Seconds or timedelta to pause. Must be at least one second.
        name: Optional operation name shown in execution history.
    """
    return (
        get_extension_context()
        ._reserve_sdk_operation(  # noqa: SLF001
            name,
            executes_user_code=False,
        )
        ._run_wait(  # noqa: SLF001
            duration,
            sub_type=OperationSubType.WAIT,
        )
    )


__all__ = ["wait"]
