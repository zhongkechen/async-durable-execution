from __future__ import annotations

import functools
import inspect
from collections.abc import Awaitable, Callable
from typing import TypeVar, cast

from .exceptions import ValidationError


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
    return await cast("Awaitable[T]", func(*args, **kwargs))


def _looks_like_context_parameter(parameter: inspect.Parameter) -> bool:
    annotation = parameter.annotation
    if annotation is not inspect.Signature.empty:
        annotation_name = getattr(annotation, "__name__", str(annotation))
        if "DurableContext" in annotation_name:
            return True
    return parameter.name in _CONTEXT_PARAM_NAMES


def _should_inject_context(
    func: Callable[..., object],
    provided_positional_count: int,
    *,
    context_position: str,
) -> bool:
    try:
        signature = inspect.signature(func)
    except (TypeError, ValueError):
        return False

    if any(
        parameter.kind is inspect.Parameter.KEYWORD_ONLY
        and parameter.default is inspect.Signature.empty
        for parameter in signature.parameters.values()
    ):
        return False

    positional_parameters = [
        parameter
        for parameter in signature.parameters.values()
        if parameter.kind
        in {
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        }
    ]
    required_positional_parameters = [
        parameter
        for parameter in positional_parameters
        if parameter.default is inspect.Signature.empty
    ]

    if len(required_positional_parameters) != provided_positional_count + 1:
        return False

    candidate_index = 0 if context_position == "prepend" else provided_positional_count
    if candidate_index >= len(positional_parameters):
        return False

    return _looks_like_context_parameter(positional_parameters[candidate_index])


async def invoke_callable_with_optional_context(
    func: Callable[..., Awaitable[T]],
    context: object,
    *args,
    context_position: str = "prepend",
    **kwargs,
) -> T:
    assert_async_callable(func)
    if context_position not in {"prepend", "append"}:
        msg = "context_position must be either 'prepend' or 'append'"
        raise ValueError(msg)

    if _should_inject_context(
        func,
        len(args),
        context_position=context_position,
    ):
        if context_position == "prepend":
            return await cast("Awaitable[T]", func(context, *args, **kwargs))
        return await cast("Awaitable[T]", func(*args, context, **kwargs))

    return await cast("Awaitable[T]", func(*args, **kwargs))
