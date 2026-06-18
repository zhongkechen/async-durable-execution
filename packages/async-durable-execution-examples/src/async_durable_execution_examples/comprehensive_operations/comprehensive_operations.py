"""Complex multi-operation example demonstrating all major operations."""

from datetime import timedelta
from typing import Any

from async_durable_execution import (
    durable_callable,
    step,
    durable_execution,
    map,
    parallel,
    wait,
)


@durable_execution
async def handler(event: dict[str, Any]) -> dict[str, Any]:
    """Comprehensive example demonstrating all major durable operations."""
    print(f"Starting comprehensive operations example with event: {event}")

    @durable_callable
    async def run_step_one() -> str:
        return "Step 1 completed successfully"

    async def map_item(item: int, index: int, _) -> int:
        @durable_callable
        async def get_item() -> int:
            return item

        return await step(get_item(), name=f"map-step-{index}")

    async def fruit_step_1() -> str:
        @durable_callable
        async def get_fruit() -> str:
            return "apple"

        return await step(get_fruit(), name="fruit-step-1")

    async def fruit_step_2() -> str:
        @durable_callable
        async def get_fruit() -> str:
            return "banana"

        return await step(get_fruit(), name="fruit-step-2")

    async def fruit_step_3() -> str:
        @durable_callable
        async def get_fruit() -> str:
            return "orange"

        return await step(get_fruit(), name="fruit-step-3")

    # Step 1: step() - Simple step that returns a result
    step1_result: str = await step(run_step_one(), name="step1")

    # Step 2: wait() - Keep the delay short so example tests stay fast.
    await wait(timedelta(seconds=1))

    # Step 3: map() - Map with 5 iterations returning numbers 1 to 5
    map_input = [1, 2, 3, 4, 5]

    map_results = (
        await map(
            inputs=map_input,
            func=map_item,
            name="map-numbers",
        )
    ).to_dict()

    # Step 4: parallel() - 3 branches, each returning a fruit name

    parallel_results = (
        await parallel(functions=[fruit_step_1, fruit_step_2, fruit_step_3])
    ).to_dict()

    # Final result combining all operations
    return {
        "step1": step1_result,
        "waitCompleted": True,
        "mapResults": map_results,
        "parallelResults": parallel_results,
    }
