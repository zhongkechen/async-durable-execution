from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, TypeVar, overload

from .._core import (
    CallableResult,
    Duration,
    DurableContext,
    RetryStrategy,
    SerDes,
    bind_current_context,
    call_user_function,
    get_current_context,
    get_durable_context,
)
from .._primitive.child import run_in_child_context
from .._primitive.wait import wait

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from .._primitive.child import SummaryGenerator

T = TypeVar("T")


@dataclass(frozen=True)
class WithRetryContext(DurableContext):
    """Context available while a with_retry body is executing."""

    attempt: int = 1


def get_with_retry_context() -> WithRetryContext:
    """Return the active `WithRetryContext`."""
    current_context = get_current_context()
    if not isinstance(current_context, WithRetryContext):
        msg = (
            "get_with_retry_context() can only be used while a with_retry body "
            "is executing."
        )
        raise RuntimeError(msg)
    return current_context


@overload
def with_retry(
    func: Callable[[], Awaitable[T]],
    *,
    name: str | None = None,
    retry_strategy: Callable[[Exception, int], Duration | None] | None = None,
    serdes: SerDes | None = None,
    summary_generator: SummaryGenerator | None = None,
    is_virtual: bool = False,
) -> asyncio.Task[T]: ...


@overload
def with_retry(
    func: Callable[[], T],
    *,
    name: str | None = None,
    retry_strategy: Callable[[Exception, int], Duration | None] | None = None,
    serdes: SerDes | None = None,
    summary_generator: SummaryGenerator | None = None,
    is_virtual: bool = False,
) -> asyncio.Task[T]: ...


def with_retry(
    func: Callable[[], CallableResult[T]],
    *,
    name: str | None = None,
    retry_strategy: Callable[[Exception, int], Duration | None] | None = None,
    serdes: SerDes | None = None,
    summary_generator: SummaryGenerator | None = None,
    is_virtual: bool = False,
) -> asyncio.Task[T]:
    """Retry a block of durable logic with configurable backoff.

    Args:
        func: Sync or async callable to retry. Synchronous callables run in the
            event loop's thread executor. Use get_with_retry_context().attempt
            inside the callable to access the current attempt number.
        name: Optional durable operation name.
        retry_strategy: Optional strategy that returns a retry delay or None to stop.
        serdes: Optional serializer for the child context result.
        summary_generator: Optional summary generator for large child results.
        is_virtual: Whether the child context should skip lifecycle checkpoints.
    """

    async def run_loop() -> T:
        retry = retry_strategy or RetryStrategy()
        attempt = 0
        while True:
            attempt += 1
            try:
                context = get_durable_context()
                retry_context = WithRetryContext(
                    execution_state=context.execution_state,
                    operation_identifier=context.operation_identifier,
                    step_id_prefix=context.step_id_prefix,
                    replaying=context.is_replaying(),
                    attempt=attempt,
                )
                if "step_counter" in context.__dict__:
                    retry_context.__dict__["step_counter"] = context.__dict__[
                        "step_counter"
                    ]
                with bind_current_context(retry_context):
                    return await call_user_function(func)
            except Exception as err:
                delay = retry(err, attempt)
                if delay is None:
                    raise

                wait_name = (
                    f"{name}-backoff-{attempt}" if name else f"backoff-{attempt}"
                )
                await wait(duration=delay, name=wait_name)

    return run_in_child_context(
        run_loop,
        name=name or "with-retry",
        serdes=serdes,
        summary_generator=summary_generator,
        is_virtual=is_virtual,
    )
