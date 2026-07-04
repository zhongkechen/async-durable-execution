from __future__ import annotations

import asyncio
import contextvars
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any, TypeVar, cast


T = TypeVar("T")


def create_eager_task(
    coro_factory: Callable[[], Coroutine[Any, Any, T]],
) -> asyncio.Task[T]:
    """Create an operation task and eagerly run it to its first suspension point."""
    loop = asyncio.get_running_loop()
    coro = coro_factory()

    eager_task_factory = getattr(asyncio, "eager_task_factory", None)
    if eager_task_factory is not None:
        return eager_task_factory(loop, coro)

    task_context = contextvars.copy_context()
    try:
        yielded = task_context.run(coro.send, None)
    except StopIteration as complete:
        result = complete.value

        async def completed_task() -> T:
            return result

        return loop.create_task(completed_task())
    except BaseException as error:
        captured_error = error

        async def failed_task() -> T:
            raise captured_error

        return loop.create_task(failed_task())

    _release_future_blocking(yielded)
    return loop.create_task(_resume_eager_coroutine(coro, yielded, task_context))


def _release_future_blocking(awaitable: object) -> None:
    if isinstance(awaitable, asyncio.Future) and getattr(
        awaitable, "_asyncio_future_blocking", False
    ):
        setattr(awaitable, "_asyncio_future_blocking", False)


async def _resume_eager_coroutine(
    coro: Coroutine[Any, Any, T],
    awaitable: object,
    task_context: contextvars.Context,
) -> T:
    """Resume a manually-started coroutine in the context used for its first step."""
    while True:
        try:
            if awaitable is None:
                await asyncio.sleep(0)
                result = None
            else:
                result = await cast(Awaitable[Any], awaitable)
        except BaseException as error:
            try:
                awaitable = task_context.run(coro.throw, error)
            except StopIteration as complete:
                return complete.value
        else:
            try:
                awaitable = task_context.run(coro.send, result)
            except StopIteration as complete:
                return complete.value

        _release_future_blocking(awaitable)
