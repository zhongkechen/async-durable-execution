"""8-1: Parallel basic."""

from typing import Any

from async_durable_execution import durable_callable, durable_execution, parallel, step


@durable_callable
async def return_value(value: str) -> str:
    return value


async def branch0() -> str:
    return await step(return_value("task-1"))


async def branch1() -> str:
    return await step(return_value("task-2"))


@durable_execution
async def handler(_event: Any) -> list:
    return (
        await parallel([branch0, branch1], name="parallel", max_concurrency=1)
    ).get_results()
