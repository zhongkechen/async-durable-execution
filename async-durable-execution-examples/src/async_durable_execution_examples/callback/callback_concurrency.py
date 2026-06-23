"""Demonstrates multiple concurrent createCallback operations using parallel()."""

from datetime import timedelta
from typing import Any

from async_durable_execution import (
    CallbackConfig,
    durable_execution,
    create_callback,
    parallel,
)


@durable_execution
async def handler(_event: Any) -> dict[str, Any]:
    """Handler demonstrating multiple concurrent callback operations."""
    callback_config = CallbackConfig(timeout=timedelta(seconds=30))

    async def callback_branch_1() -> str:
        """First callback branch."""
        callback = await create_callback(
            name="api-call-1",
            config=callback_config,
        )
        return await callback.result()

    async def callback_branch_2() -> str:
        """Second callback branch."""
        callback = await create_callback(
            name="api-call-2",
            config=callback_config,
        )
        return await callback.result()

    async def callback_branch_3() -> str:
        """Third callback branch."""
        callback = await create_callback(
            name="api-call-3",
            config=callback_config,
        )
        return await callback.result()

    parallel_results = await parallel(
        branches=[callback_branch_1, callback_branch_2, callback_branch_3],
        name="parallel_callbacks",
    )

    # Extract results from parallel execution
    results = parallel_results.get_results()

    return {
        "results": results,
        "allCompleted": True,
    }
