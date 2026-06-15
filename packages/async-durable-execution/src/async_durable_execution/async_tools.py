from __future__ import annotations

import functools
import inspect
from collections.abc import Awaitable, Callable
from typing import TypeVar, cast

from async_durable_execution.exceptions import ValidationError


T = TypeVar("T")


def is_async_callable(func: Callable[..., object]) -> bool:
    if inspect.iscoroutinefunction(func):
        return True
    if isinstance(func, functools.partial):
        return is_async_callable(func.func)

    call = getattr(func, "__call__", None)
    return call is not None and inspect.iscoroutinefunction(call)


def assert_async_callable(
    func: Callable[..., object],
    *,
    label: str = "func",
) -> None:
    if is_async_callable(func):
        return

    name = getattr(func, "_original_name", None) or getattr(func, "__name__", None)
    if name is None and isinstance(func, functools.partial):
        name = getattr(func.func, "__name__", None)
    if name is None:
        name = type(func).__name__

    msg = (
        f"`{label}` must be an async function. "
        f"Non-async callables are no longer supported: {name}."
    )
    raise ValidationError(msg)


async def invoke_callable(func: Callable[..., Awaitable[T]], *args, **kwargs) -> T:
    assert_async_callable(func)
    return await cast("Awaitable[T]", func(*args, **kwargs))
