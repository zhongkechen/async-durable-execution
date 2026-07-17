"""7-7: Wait-for-callback submitter retry exhaustion."""

from typing import Any

from async_durable_execution import (
    JitterStrategy,
    RetryStrategy,
    durable_execution,
    wait_for_callback,
)


async def submitter() -> None:
    raise RuntimeError("submitter failure")


@durable_execution
async def handler(event: Any) -> str:
    retry_strategy = RetryStrategy(
        max_attempts=2,
        initial_delay=1,
        max_delay=1,
        jitter_strategy=JitterStrategy.NONE,
    )
    return await wait_for_callback(submitter, name=event, retry_strategy=retry_strategy)
