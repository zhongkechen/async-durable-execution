from __future__ import annotations

import functools
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, ParamSpec, TypeVar, cast

from .context import reset_current_context, set_current_context

if TYPE_CHECKING:
    from .primitive.base import OperationContext

T = TypeVar("T")
Params = ParamSpec("Params")


def durable_callable(
    func: Callable[Params, Awaitable[T]],
) -> Callable[Params, Callable[[], Awaitable[T]]]:
    """Wrap an async function so calling it returns a zero-argument durable callable.

    The returned callable can be passed to durable operations such as `step()`
    and `run_in_child_context()`, keeping durable operation creation explicit
    while avoiding manual `functools.partial(...)` wrapping at the callsite.

    Class and static methods are supported with either decorator order:
    `@classmethod`/`@staticmethod` may appear above or below `@durable_callable`.
    """
    if isinstance(func, classmethod):
        return classmethod(durable_callable(func.__func__))  # type: ignore[return-value]
    if isinstance(func, staticmethod):
        return staticmethod(durable_callable(func.__func__))  # type: ignore[return-value]

    @functools.wraps(func)
    def wrapper(
        *args: Params.args, **kwargs: Params.kwargs
    ) -> Callable[[], Awaitable[T]]:
        bound = functools.partial(func, *args, **kwargs)
        setattr(bound, "__name__", func.__name__)
        return bound

    return wrapper


async def invoke_user_callable(
    context: OperationContext,
    func: Callable[..., Awaitable[T]],
    *args,
    **kwargs,
) -> T:
    """Invoke user code while temporarily binding the supplied durable context."""
    token = set_current_context(context)
    try:
        return await func(*args, **kwargs)
    finally:
        reset_current_context(token)
