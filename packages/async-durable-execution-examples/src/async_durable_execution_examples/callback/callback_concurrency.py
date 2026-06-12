"""Demonstrates multiple concurrent createCallback operations using context.parallel."""

from datetime import timedelta
from typing import Any

from async_durable_execution.config import CallbackConfig
from async_durable_execution.context import DurableContext
from async_durable_execution.execution import durable_execution


@durable_execution
async def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    """Handler demonstrating multiple concurrent callback operations."""

    callback_config = CallbackConfig(timeout=timedelta(seconds=30))

    async def callback_branch_1(ctx: DurableContext) -> str:
        """First callback branch."""
        callback = ctx.create_callback(
            name="api-call-1",
            config=callback_config,
        )
        return callback.result()

    async def callback_branch_2(ctx: DurableContext) -> str:
        """Second callback branch."""
        callback = ctx.create_callback(
            name="api-call-2",
            config=callback_config,
        )
        return callback.result()

    async def callback_branch_3(ctx: DurableContext) -> str:
        """Third callback branch."""
        callback = ctx.create_callback(
            name="api-call-3",
            config=callback_config,
        )
        return callback.result()

    parallel_results = context.parallel(
        functions=[callback_branch_1, callback_branch_2, callback_branch_3],
        name="parallel_callbacks",
    )

    # Extract results from parallel execution
    results = parallel_results.get_results()

    return {
        "results": results,
        "allCompleted": True,
    }
