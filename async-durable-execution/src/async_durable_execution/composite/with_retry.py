from __future__ import annotations

from typing import TYPE_CHECKING, Awaitable, Callable, TypeVar

from ..config import RetryStrategyBuilder
from ..models import RetryDecision
from ..async_tools import (
    assert_async_callable,
    invoke_user_callable,
)
from ..exceptions import SuspendExecution
from ..primitive.child import (
    get_durable_context,
    run_in_child_context,
)
from ..primitive.wait import wait

if TYPE_CHECKING:
    from ..serdes import SerDes
    from ..types import SummaryGenerator

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
    context = get_durable_context()

    async def run_loop() -> T:
        assert_async_callable(func)
        retry = retry_strategy or RetryStrategyBuilder().build()
        attempt = 0
        while True:
            attempt += 1
            try:
                return await invoke_user_callable(
                    context,
                    func,
                    attempt,
                )
            except SuspendExecution:
                raise
            except Exception as err:
                decision = retry(err, attempt)
                if not decision.should_retry:
                    raise
                wait_name = f"{name}-backoff-{attempt}" if name else None
                await wait(duration=decision.delay, name=wait_name)

    return await run_in_child_context(
        run_loop,
        name=name,
        serdes=serdes,
        summary_generator=summary_generator,
        is_virtual=is_virtual,
    )
