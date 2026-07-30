"""Helpers for invoking synchronous and asynchronous user callables."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable
from typing import Any, ParamSpec, TypeAlias, TypeVar, cast, overload


T = TypeVar("T")
Params = ParamSpec("Params")

CallableResult: TypeAlias = T | Awaitable[T]


@overload
async def call_user_function(
    func: Callable[Params, Awaitable[T]],
    /,
    *args: Params.args,
    **kwargs: Params.kwargs,
) -> T: ...


@overload
async def call_user_function(
    func: Callable[Params, CallableResult[T]],
    /,
    *args: Params.args,
    **kwargs: Params.kwargs,
) -> T: ...


async def call_user_function(
    func: Callable[Params, CallableResult[T]],
    /,
    *args: Params.args,
    **kwargs: Params.kwargs,
) -> T:
    """Invoke async callables directly and sync callables in a worker thread."""
    if inspect.iscoroutinefunction(func):
        result = func(*args, **kwargs)
    else:
        sync_func = cast("Callable[Params, Any]", func)
        result = await asyncio.to_thread(sync_func, *args, **kwargs)

    if inspect.isawaitable(result):
        return await cast("Awaitable[T]", result)
    return cast("T", result)
