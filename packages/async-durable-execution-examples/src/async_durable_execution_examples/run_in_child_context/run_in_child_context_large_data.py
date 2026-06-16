"""Test runInChildContext with large data exceeding individual step limits."""

from datetime import timedelta
from typing import Any

from async_durable_execution import (
    durable_step,
    step,
    durable_execution,
    run_in_child_context,
    wait,
)


async def generate_large_string(size_in_kb: int) -> str:
    """Generate a string of approximately the specified size in KB."""
    return "A" * 1024 * size_in_kb


async def large_data_processor() -> dict[str, Any]:
    """Process large data in child context."""
    # Generate data using a loop - each step returns ~50KB of data (under the step limit)
    step_results: list[str] = []
    step_sizes: list[int] = []

    for i in range(1, 6):  # 1 to 5

        @durable_step
        async def build_chunk(size_in_kb: int = 50) -> str:
            return await generate_large_string(size_in_kb)

        step_result: str = await step(build_chunk(), name=f"generate-data-{i}")

        step_results.append(step_result)
        step_sizes.append(len(step_result))

    # Concatenate all results - total should be ~250KB
    concatenated_result = "".join(step_results)

    return {
        "totalSize": len(concatenated_result),
        "sizeInKB": round(len(concatenated_result) / 1024),
        "data": concatenated_result,
        "stepSizes": step_sizes,
    }


@durable_execution
async def handler(_event: Any) -> dict[str, Any]:
    """Handler demonstrating runInChildContext with large data."""
    # Use runInChildContext to handle large data that would exceed 256k step limit
    large_data_result: dict[str, Any] = await run_in_child_context(
        large_data_processor, name="large-data-processor"
    )

    # Add a wait after runInChildContext to test persistence across invocations
    await wait(timedelta(seconds=1), name="post-processing-wait")

    # Verify the data is still intact after the wait
    data_integrity_check = (
        len(large_data_result["data"]) == large_data_result["totalSize"]
        and len(large_data_result["data"]) > 0
    )

    return {
        "success": True,
        "message": "Successfully processed large data exceeding individual step limits using runInChildContext",
        "dataIntegrityCheck": data_integrity_check,
        "summary": {
            "totalDataSize": large_data_result["sizeInKB"],
            "stepsExecuted": 5,
            "childContextUsed": True,
            "waitExecuted": True,
            "dataPreservedAcrossWait": data_integrity_check,
        },
    }
