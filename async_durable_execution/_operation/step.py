"""User-facing durable step operation."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

from .._core import Duration, OperationSubType, SerDes
from .._primitive.step import (
    StepContext,
    StepInterruptedError,
    StepSemantics,
    get_step_context,
)
from .._extension_api import get_extension_context

T = TypeVar("T")


def step(
    func: Callable[[], Awaitable[T]],
    *,
    name: str | None = None,
    retry_strategy: Callable[[Exception, int], Duration | None] | None = None,
    step_semantics: StepSemantics = StepSemantics.AT_LEAST_ONCE_PER_RETRY,
    serdes: SerDes | None = None,
) -> asyncio.Task[T]:
    """Run user code as a checkpointed durable step.

    Durable steps are the main way to isolate non-deterministic work such as API
    calls, clock reads, UUID generation, and database access from replayed code.
    """
    step_name = name if name is not None else getattr(func, "__name__", None)
    return (
        get_extension_context()
        ._reserve_sdk_operation(  # noqa: SLF001
            step_name,
        )
        ._run_step(  # noqa: SLF001
            func,
            sub_type=OperationSubType.STEP,
            retry_strategy=retry_strategy,
            step_semantics=step_semantics,
            serdes=serdes,
        )
    )


__all__ = [
    "StepContext",
    "StepInterruptedError",
    "StepSemantics",
    "get_step_context",
    "step",
]
