"""Example demonstrating parallel with wait operations."""

from datetime import timedelta
from typing import Any

from async_durable_execution.context import DurableContext
from async_durable_execution.execution import durable_execution


@durable_execution
async def handler(_event: Any, context: DurableContext) -> str:
    """Execute parallel waits."""

    async def wait_1_second(ctx: DurableContext) -> None:
        ctx.wait(timedelta(seconds=1), name="wait_1_second")

    async def wait_2_seconds(ctx: DurableContext) -> None:
        ctx.wait(timedelta(seconds=2), name="wait_2_seconds")

    async def wait_5_seconds(ctx: DurableContext) -> None:
        ctx.wait(timedelta(seconds=5), name="wait_5_seconds")

    # Call get_results() to extract data and avoid BatchResult serialization
    context.parallel(
        functions=[wait_1_second, wait_2_seconds, wait_5_seconds],
        name="parallel_waits",
    ).get_results()

    return "Completed waits"
