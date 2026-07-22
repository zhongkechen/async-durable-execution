"""8-21: Nested parallel operations."""

from typing import Any

from async_durable_execution import durable_callable, durable_execution, parallel, step


@durable_callable
async def return_value(value: str) -> str:
    return value


async def inner0() -> str:
    return await step(return_value("i1"))


async def inner1() -> str:
    return await step(return_value("i2"))


async def outer_branch() -> list:
    return (
        await parallel([inner0, inner1], name="inner", max_concurrency=1)
    ).get_results()


@durable_execution
async def handler(_event: Any) -> list:
    return (
        await parallel([outer_branch], name="outer", max_concurrency=1)
    ).get_results()
