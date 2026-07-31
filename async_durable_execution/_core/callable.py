"""Helpers for invoking synchronous and asynchronous user callables."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable
from typing import Any, ParamSpec, TypeAlias, TypeVar, cast, overload

from .context import bind_synchronous_user_callable


T = TypeVar("T")
Params = ParamSpec("Params")

CallableResult: TypeAlias = T | Awaitable[T]


async def _drain_thread_task(task: asyncio.Task[Any]) -> None:
    """Wait until a started thread task settles, ignoring its outcome."""
    while True:
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            if task.done():
                return
        except BaseException:
            return
        else:
            return


def _is_async_callable(func: Callable[..., Any]) -> bool:
    """Return whether a callable's implementation is asynchronous."""
    candidates = (func, getattr(func, "__call__", None))
    for candidate in candidates:
        if candidate is None:
            continue
        if inspect.iscoroutinefunction(candidate):
            return True
        try:
            unwrapped = inspect.unwrap(candidate)
        except ValueError:
            continue
        if inspect.iscoroutinefunction(unwrapped):
            return True
    return False


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
    if _is_async_callable(func):
        result = func(*args, **kwargs)
    else:
        sync_func = cast("Callable[Params, Any]", func)
        with bind_synchronous_user_callable():
            thread_task = asyncio.create_task(
                asyncio.to_thread(sync_func, *args, **kwargs)
            )
            try:
                result = await asyncio.shield(thread_task)
            except asyncio.CancelledError:
                await _drain_thread_task(thread_task)
                raise
            if inspect.isawaitable(result):
                return await cast("Awaitable[T]", result)

    if inspect.isawaitable(result):
        return await cast("Awaitable[T]", result)
    return cast("T", result)
