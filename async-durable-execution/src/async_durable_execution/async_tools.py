from __future__ import annotations

import functools
import inspect
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, ParamSpec, TypeVar, cast

from .context import reset_current_context, set_current_context
from .exceptions import ValidationError

if TYPE_CHECKING:
    from . import DurableContext

T = TypeVar("T")
Params = ParamSpec("Params")


def is_async_callable(func: Callable[..., object]) -> bool:
    """Return whether `func` can be awaited by durable operations."""
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
    """Best-effort name lookup used for default operation names."""
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
    """Validate that a durable-operation callback is asynchronous."""
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


def durable_callable(
    func: Callable[Params, Awaitable[T]],
) -> Callable[Params, Callable[[], Awaitable[T]]]:
    """Wrap an async function so calling it returns a zero-argument durable callable.

    The returned callable can be passed to durable operations such as `step()`
    and `run_in_child_context()`, keeping durable operation creation explicit
    while avoiding manual `functools.partial(...)` wrapping at the callsite.
    """
    assert_async_callable(func)

    @functools.wraps(func)
    def wrapper(
        *args: Params.args, **kwargs: Params.kwargs
    ) -> Callable[[], Awaitable[T]]:
        return functools.partial(func, *args, **kwargs)

    return wrapper


async def invoke_callable(func: Callable[..., Awaitable[T]], *args, **kwargs) -> T:
    """Call an async function after validating it is awaitable."""
    assert_async_callable(func)
    return await func(*args, **kwargs)


async def invoke_user_callable(
    context: DurableContext,
    func: Callable[..., Awaitable[T]],
    *args,
    **kwargs,
) -> T:
    """Invoke user code while temporarily binding the supplied durable context."""
    token = set_current_context(context)
    try:
        return await invoke_callable(
            func,
            *args,
            **kwargs,
        )
    finally:
        reset_current_context(token)
