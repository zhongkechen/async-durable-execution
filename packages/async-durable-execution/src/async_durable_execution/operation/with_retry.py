from __future__ import annotations

from typing import Callable, Awaitable, TypeVar

from ..config import WithRetryConfig, RetryStrategyBuilder
from ..async_tools import (
    assert_async_callable,
    invoke_user_callable,
)
from ..exceptions import SuspendExecution
from .child import (
    _run_in_child_context_in_context,
    _get_durable_context,
)
from .wait import _wait_in_context

T = TypeVar("T")


async def with_retry(
    func: Callable[[int], Awaitable[T]],
    config: WithRetryConfig[T],
    name: str | None = None,
) -> T:
    """Retry a block of durable logic with configurable backoff."""
    context = _get_durable_context()

    async def run_loop() -> T:
        assert_async_callable(func)
        retry_strategy = config.retry_strategy or RetryStrategyBuilder().build()
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
                decision = retry_strategy(err, attempt)
                if not decision.should_retry:
                    raise
                wait_name = f"{name}-backoff-{attempt}" if name else None
                await _wait_in_context(
                    context,
                    duration=decision.delay,
                    name=wait_name,
                )

    if config.wrap_with_run_in_child_context:
        return await _run_in_child_context_in_context(
            context,
            run_loop,
            name=name,
            config=config.child_context_config,
        )

    return await invoke_user_callable(context, run_loop)
