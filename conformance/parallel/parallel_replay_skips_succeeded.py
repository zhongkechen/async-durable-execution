"""8-14: Parallel replay skips succeeded branches."""

from datetime import timedelta
from typing import Any

from async_durable_execution import (
    durable_callable,
    durable_execution,
    parallel,
    step,
    wait,
)


@durable_callable
async def return_value() -> str:
    return "b0"


async def first() -> str:
    return await step(return_value())


async def second() -> str:
    await wait(timedelta(seconds=2))
    return "b1"


@durable_execution
async def handler(_event: Any) -> list:
    return (
        await parallel([first, second], name="replay", max_concurrency=1)
    ).get_results()
