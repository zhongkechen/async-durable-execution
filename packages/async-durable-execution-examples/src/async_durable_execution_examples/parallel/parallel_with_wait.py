"""Example demonstrating parallel with wait operations."""

from datetime import timedelta
from typing import Any

from async_durable_execution import (
    durable_execution,
    parallel,
    wait,
)


@durable_execution
async def handler(_event: Any) -> str:
    """Execute parallel waits."""

    async def wait_1_second() -> None:
        await wait(timedelta(seconds=1), name="wait_1_second")

    async def wait_2_seconds() -> None:
        await wait(timedelta(seconds=2), name="wait_2_seconds")

    async def wait_5_seconds() -> None:
        await wait(timedelta(seconds=3), name="wait_5_seconds")

    # Call get_results() to extract data and avoid BatchResult serialization
    (
        await parallel(
            functions=[wait_1_second, wait_2_seconds, wait_5_seconds],
            name="parallel_waits",
        )
    ).get_results()

    return "Completed waits"
