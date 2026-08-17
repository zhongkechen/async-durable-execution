"""User-facing durable chained-invoke operation."""

from __future__ import annotations

import asyncio
from typing import TypeVar

from .._core import OperationSubType, SerDes
from ..extension import get_extension_context

P = TypeVar("P")
R = TypeVar("R")


def invoke(
    function_name: str,
    payload: P,
    *,
    name: str | None = None,
    serdes_payload: SerDes[P] | None = None,
    serdes_result: SerDes[R] | None = None,
    tenant_id: str | None = None,
) -> asyncio.Task[R]:
    """Invoke another durable Lambda function and wait for its durable result.

    Args:
        function_name: Qualified Lambda function name or ARN to invoke.
        payload: Payload to send to the invoked function.
        name: Optional durable operation name.
        serdes_payload: Optional serializer for the invocation payload.
        serdes_result: Optional deserializer for the invocation result.
        tenant_id: Optional tenant identifier for the chained invocation.
    """
    return (
        get_extension_context()
        ._reserve_sdk_operation(  # noqa: SLF001
            name,
        )
        ._run_invoke(  # noqa: SLF001
            function_name,
            payload,
            sub_type=OperationSubType.CHAINED_INVOKE,
            serdes_payload=serdes_payload,
            serdes_result=serdes_result,
            tenant_id=tenant_id,
        )
    )


__all__ = ["invoke"]
