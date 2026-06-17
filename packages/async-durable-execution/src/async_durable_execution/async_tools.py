from __future__ import annotations

import functools
import inspect
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, TypeVar, cast

from .context import reset_current_context, set_current_context
from .exceptions import ValidationError

if TYPE_CHECKING:
    from . import DurableContext

T = TypeVar("T")
_CONTEXT_PARAM_NAMES = {
    "context",
    "ctx",
    "child_context",
    "child_ctx",
    "durable_context",
    "durable_ctx",
}


def is_async_callable(func: Callable[..., object]) -> bool:
    if inspect.iscoroutinefunction(func):
        return True
    if isinstance(func, functools.partial):
        return is_async_callable(func.func)

    call = getattr(func, "__call__", None)
    return call is not None and inspect.iscoroutinefunction(call)


def get_callable_name(
    func: Callable[..., object],
    *,
    include_original_name: bool = True,
) -> str | None:
    if isinstance(func, functools.partial):
        return get_callable_name(
            func.func,
            include_original_name=include_original_name,
        )

    if include_original_name:
        original_name = getattr(func, "_original_name", None)
        if original_name is not None:
            return original_name

    if inspect.isfunction(func) or inspect.ismethod(func):
        return getattr(func, "__name__", None)

    return None


def assert_async_callable(
    func: Callable[..., object],
    *,
    label: str = "func",
) -> None:
    if is_async_callable(func):
        return

    name = get_callable_name(func)
    if name is None:
        name = type(func).__name__

    msg = (
        f"`{label}` must be an async function. "
        f"Non-async callables are no longer supported: {name}."
    )
    raise ValidationError(msg)


async def invoke_callable(func: Callable[..., Awaitable[T]], *args, **kwargs) -> T:
    assert_async_callable(func)
    return await func(*args, **kwargs)


async def invoke_user_callable(
    context: DurableContext,
    func: Callable[..., Awaitable[T]],
    *args,
    **kwargs,
) -> T:
    token = set_current_context(context)
    try:
        return await invoke_callable(
            func,
            *args,
            **kwargs,
        )
    finally:
        reset_current_context(token)
