"""3-11: Child context large payload (ReplayChildren mode)."""

from datetime import timedelta
from async_durable_execution import (
    durable_callable,
    durable_execution,
    run_in_child_context,
    step,
    wait,
)
from typing import Any


def generate_large_string(size_in_kb: int) -> str:
    """Generate a string of approximately the specified size in KB."""
    return "A" * 1024 * size_in_kb


@durable_callable
async def generate_data() -> str:
    return generate_large_string(50)


@durable_callable
async def large_data_processor(*, input_1: str) -> str:
    print(input_1, flush=True)
    step_result: str = await step(generate_data())
    # Build a large result (>256KB) from the small step result
    large_result = step_result * 6  # ~300KB
    return large_result


@durable_execution
async def handler(event: Any) -> dict[str, Any]:
    result: str = await run_in_child_context(
        large_data_processor(input_1=str(event)), name="large-data-processor"
    )
    await wait(timedelta(seconds=2))
    return {
        "success": True,
        "dataSize": len(result),
    }
