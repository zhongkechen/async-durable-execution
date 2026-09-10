"""User-facing durable child-context operation."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

from .._core import OperationSubType, SerDes
from .._primitive.child import SummaryGenerator
from .._extension_api import get_extension_context

T = TypeVar("T")


def run_in_child_context(
    func: Callable[[], Awaitable[T]],
    *,
    name: str | None = None,
    serdes: SerDes | None = None,
    summary_generator: SummaryGenerator | None = None,
    is_virtual: bool = False,
) -> asyncio.Task[T]:
    """Execute a durable sub-workflow inside its own child context.

    Args:
        func: The child context function to execute.
        name: Optional durable operation name.
        serdes: Optional serializer for the child context result.
        summary_generator: Optional summary generator for large child results.
        is_virtual: Whether this child context should skip lifecycle checkpoints.
    """
    step_name = name if name is not None else getattr(func, "__name__", None)
    return (
        get_extension_context()
        ._reserve_sdk_operation(step_name)  # noqa: SLF001
        ._run_in_child_context(  # noqa: SLF001
            func,
            sub_type=OperationSubType.RUN_IN_CHILD_CONTEXT,
            serdes=serdes,
            summary_generator=summary_generator,
            is_virtual=is_virtual,
        )
    )


__all__ = ["SummaryGenerator", "run_in_child_context"]
