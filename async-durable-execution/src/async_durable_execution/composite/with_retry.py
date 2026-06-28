from __future__ import annotations

from typing import TYPE_CHECKING, Awaitable, Callable, TypeVar

from ..config import RetryStrategyBuilder
from ..config import RetryDecision
from ..primitive.child import (
    run_in_child_context,
)
from ..primitive.wait import wait

if TYPE_CHECKING:
    from .concurrency import SummaryGenerator
    from ..serdes import SerDes

T = TypeVar("T")


async def with_retry(
    func: Callable[[int], Awaitable[T]],
    *,
    name: str | None = None,
    retry_strategy: Callable[[Exception, int], RetryDecision] | None = None,
    serdes: SerDes | None = None,
    summary_generator: SummaryGenerator | None = None,
    is_virtual: bool = False,
) -> T:
    """Retry a block of durable logic with configurable backoff.

    Args:
        func: Async callable to retry. Receives the current attempt number.
        name: Optional durable operation name.
        retry_strategy: Optional strategy that decides whether and when to retry.
        serdes: Optional serializer for the child context result.
        summary_generator: Optional summary generator for large child results.
        is_virtual: Whether the child context should skip lifecycle checkpoints.
    """

    async def run_loop() -> T:
        retry = retry_strategy or RetryStrategyBuilder().build()
        attempt = 0
        while True:
            attempt += 1
            try:
                return await func(attempt)
            except Exception as err:
                decision = retry(err, attempt)
                if not decision.should_retry:
                    raise
                wait_name = (
                    f"{name}-backoff-{attempt}" if name else f"backoff-{attempt}"
                )
                await wait(duration=decision.delay, name=wait_name)

    return await run_in_child_context(
        run_loop,
        name=name or "with-retry",
        serdes=serdes,
        summary_generator=summary_generator,
        is_virtual=is_virtual,
    )
