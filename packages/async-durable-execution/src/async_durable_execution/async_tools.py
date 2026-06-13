from __future__ import annotations

import asyncio
import functools
import inspect
import queue
import threading
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


def invoke_callable(func: Callable[..., Awaitable[T]], *args, **kwargs) -> T:
    assert_async_callable(func)
    return run_awaitable(cast("Awaitable[T]", func(*args, **kwargs)))


def run_awaitable(awaitable: Awaitable[T]) -> T:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(awaitable)

    return _run_awaitable_in_thread(awaitable)


def _run_awaitable_in_thread(awaitable: Awaitable[T]) -> T:
    result_queue: queue.Queue[tuple[bool, T | BaseException]] = queue.Queue(maxsize=1)

    def runner() -> None:
        try:
            result_queue.put((True, asyncio.run(awaitable)))
        except BaseException as exc:  # noqa: BLE001
            result_queue.put((False, exc))

    thread = threading.Thread(target=runner, name="dex-async-user-code", daemon=True)
    thread.start()
    success, payload = result_queue.get()
    thread.join()

    if success:
        return cast("T", payload)
    raise cast("BaseException", payload)
