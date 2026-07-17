"""8-12: Parallel with flat nesting."""

from typing import Any

from async_durable_execution import (
    NestingType,
    durable_callable,
    durable_execution,
    parallel,
    step,
)


@durable_callable
async def return_value(value: str) -> str:
    return value


async def first() -> str:
    return await step(return_value("fa"))


async def second() -> str:
    return await step(return_value("fb"))


@durable_execution
async def handler(_event: Any) -> list:
    result = await parallel(
        [first, second],
        name="flat",
        max_concurrency=1,
        nesting_type=NestingType.FLAT,
    )
    return result.get_results()
