"""Helpers for invoking synchronous and asynchronous user callables."""

from __future__ import annotations

import asyncio
import functools
import inspect
import threading
from collections.abc import Awaitable, Callable
from typing import Any, ParamSpec, TypeAlias, TypeVar, cast, overload

from .context import bind_synchronous_user_callable


T = TypeVar("T")
Params = ParamSpec("Params")

CallableResult: TypeAlias = T | Awaitable[T]


class _SyncCallState:
    """Coordinate cancellation with a worker function's start boundary."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._started = False
        self._cancelled = False

    def try_start(self) -> bool:
        with self._lock:
            if self._cancelled:
                return False
            self._started = True
            return True

    def cancel_if_queued(self) -> bool:
        with self._lock:
            if self._started:
                return False
            self._cancelled = True
            return True


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


def _is_async_callable_target(target: Any, seen: set[int]) -> bool:
    target_id = id(target)
    if target_id in seen:
        return False
    seen.add(target_id)

    if inspect.iscoroutinefunction(target):
        return True
    if isinstance(target, functools.partial) and _is_async_callable_target(
        target.func, seen
    ):
        return True

    try:
        unwrapped = inspect.unwrap(target)
    except ValueError:
        unwrapped = target
    if unwrapped is not target and _is_async_callable_target(unwrapped, seen):
        return True

    call = getattr(target, "__call__", None)
    if call is None:
        return False
    if inspect.iscoroutinefunction(call):
        return True
    try:
        unwrapped_call = inspect.unwrap(call)
    except ValueError:
        return False
    return inspect.iscoroutinefunction(unwrapped_call)


def _is_async_callable(func: Callable[..., Any]) -> bool:
    """Return whether a callable's implementation is asynchronous."""
    return _is_async_callable_target(func, set())


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
            call_state = _SyncCallState()

            def run_sync_func() -> Any:
                if not call_state.try_start():
                    return None
                return sync_func(*args, **kwargs)

            thread_task = asyncio.create_task(asyncio.to_thread(run_sync_func))
            try:
                result = await asyncio.shield(thread_task)
            except asyncio.CancelledError:
                if call_state.cancel_if_queued():
                    thread_task.cancel()
                else:
                    await _drain_thread_task(thread_task)
                raise
            if inspect.isawaitable(result):
                return await cast("Awaitable[T]", result)

    if inspect.isawaitable(result):
        return await cast("Awaitable[T]", result)
    return cast("T", result)
