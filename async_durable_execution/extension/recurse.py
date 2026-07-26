"""Recursive self-invocation built on the backend-supported invoke operation."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, TypeVar, cast

from ..core.exceptions import ValidationError
from ..core.context import get_durable_context
from ..primitive.invoke import invoke
from ..core.state import RECURSIVE_LEVEL_INPUT_FIELD

if TYPE_CHECKING:
    from ..core.context import DurableContext
    from ..core.serdes import SerDes


P = TypeVar("P")
R = TypeVar("R")


def _is_qualified_function_arn(function_name: str) -> bool:
    """Return whether a Lambda function ARN includes a qualifier."""
    parts = function_name.split(":")
    return len(parts) >= 8 and parts[0] == "arn" and parts[5] == "function"


def _is_qualified_function_name(function_name: str) -> bool:
    """Return whether a short Lambda function name includes a qualifier."""
    return ":" in function_name and not function_name.startswith("arn:")


def _append_qualifier(function_name: str, qualifier: str | None) -> str:
    """Append a Lambda qualifier when one is available and needed."""
    if not qualifier:
        return function_name
    if _is_qualified_function_arn(function_name) or _is_qualified_function_name(
        function_name
    ):
        return function_name
    return f"{function_name}:{qualifier}"


def _resolve_recursive_function_name(
    context: DurableContext,
    explicit_function_name: str | None,
) -> str:
    if explicit_function_name:
        return explicit_function_name

    lambda_context = context.lambda_context
    if lambda_context is None:
        msg = "recurse requires a Lambda context or explicit function_name."
        raise RuntimeError(msg)

    function_version = getattr(lambda_context, "function_version", None)
    invoked_function_arn = getattr(lambda_context, "invoked_function_arn", None)
    if invoked_function_arn:
        return _append_qualifier(invoked_function_arn, function_version)

    context_function_name = getattr(lambda_context, "function_name", None)
    if context_function_name:
        return _append_qualifier(context_function_name, function_version)

    msg = "recurse could not determine the current Lambda function name."
    raise RuntimeError(msg)


def _validate_recursive_payload(context: DurableContext, payload: P) -> None:
    current_input = context.execution_state.get_input_event()
    if payload == current_input:
        msg = "recurse payload must differ from the current execution input."
        raise ValidationError(msg)


def _add_recursive_level(context: DurableContext, payload: P) -> P:
    if not isinstance(payload, dict):
        msg = "recurse payload must be a dict when with_recursive_level is enabled."
        raise ValidationError(msg)

    payload_with_level = payload.copy()
    payload_with_level[RECURSIVE_LEVEL_INPUT_FIELD] = context.recursive_level + 1
    return cast("P", payload_with_level)


def recurse(
    payload: P,
    *,
    name: str | None = None,
    function_name: str | None = None,
    with_recursive_level: bool = False,
    serdes_payload: SerDes[P] | None = None,
    serdes_result: SerDes[R] | None = None,
    tenant_id: str | None = None,
) -> asyncio.Task[R]:
    """Invoke the current durable Lambda function and wait for its result.

    This is a convenience wrapper around :func:`invoke` for recursive workflows
    such as divide-and-conquer algorithms. Each recursive call is a separate
    durable execution, so the current execution records a chained invoke instead
    of growing a Python call stack.

    Args:
        payload: Payload to send to the recursive invocation.
        name: Optional durable operation name.
        function_name: Optional qualified Lambda function name or ARN. When omitted,
            the current Lambda context is used.
        with_recursive_level: When true, copy the payload and set
            ``__recursive_level`` to the current context level plus one.
        serdes_payload: Optional serializer for the invocation payload.
        serdes_result: Optional deserializer for the invocation result.
        tenant_id: Optional tenant identifier. Defaults to the current Lambda context
            tenant id when present.
    """
    context = get_durable_context()
    recursive_payload = (
        _add_recursive_level(context, payload) if with_recursive_level else payload
    )
    _validate_recursive_payload(context, recursive_payload)
    target_function_name = _resolve_recursive_function_name(context, function_name)
    recursive_tenant_id = tenant_id
    if recursive_tenant_id is None and context.lambda_context is not None:
        recursive_tenant_id = getattr(context.lambda_context, "tenant_id", None)

    return invoke(
        function_name=target_function_name,
        payload=recursive_payload,
        name=name,
        serdes_payload=serdes_payload,
        serdes_result=serdes_result,
        tenant_id=recursive_tenant_id,
    )
