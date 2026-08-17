"""User-facing durable callback operation."""

from __future__ import annotations

import asyncio

from .._core import Duration, OperationSubType, SerDes
from .._primitive.callback import Callback, CallbackError
from ..extension import get_extension_context


def create_callback(
    *,
    name: str | None = None,
    timeout: Duration | None = None,
    heartbeat_timeout: Duration | None = None,
    serdes: SerDes | None = None,
) -> asyncio.Task[Callback]:
    """Create a durable callback handle that external systems can complete later.

    Args:
        name: Optional durable operation name.
        timeout: Optional maximum time to wait for callback completion.
        heartbeat_timeout: Optional maximum time to wait between callback heartbeats.
        serdes: Optional serializer for callback results.
    """
    return (
        get_extension_context()
        ._reserve_sdk_operation(  # noqa: SLF001
            name,
        )
        ._run_create_callback(  # noqa: SLF001
            sub_type=OperationSubType.CALLBACK,
            timeout=timeout,
            heartbeat_timeout=heartbeat_timeout,
            serdes=serdes,
        )
    )


__all__ = ["Callback", "CallbackError", "create_callback"]
