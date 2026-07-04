from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine
from typing import Any, TypeVar


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

    return loop.create_task(coro)
