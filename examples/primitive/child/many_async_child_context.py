"""Example demonstrating many async child contexts collected with asyncio.gather."""

from __future__ import annotations

import asyncio
from datetime import timedelta
import time
from typing import Any, TypedDict

from async_durable_execution import (
    durable_callable,
    durable_execution,
    run_in_child_context,
    step,
    wait,
)


DEFAULT_STEPS = 500
DEFAULT_MULTIPLIER = 1


class ManyAsyncChildContextOutput(TypedDict):
    result: int
    execution_time_ms: int
    replay_time_ms: int


@durable_callable
async def read_clock_ns() -> int:
    return time.perf_counter_ns()


@durable_callable
async def compute(index: int, multiplier: int) -> int:
    return index * multiplier


@durable_callable
async def elapsed_ms(start_time_ns: int) -> int:
    return int((time.perf_counter_ns() - start_time_ns) / 1_000_000)


@durable_callable
async def child_operation(index: int, multiplier: int) -> int:
    return await step(
        compute(index, multiplier),
        name=f"compute-{index}",
    )


@durable_execution
async def handler(event: dict[str, Any] | None) -> ManyAsyncChildContextOutput:
    """Start many child context tasks and await all results together."""
    input_event = event or {}
    steps = int(input_event.get("steps", DEFAULT_STEPS))
    multiplier = int(input_event.get("multiplier", DEFAULT_MULTIPLIER))

    if steps < 0:
        msg = "steps must be non-negative"
        raise ValueError(msg)

    start_time_ns = await step(read_clock_ns(), name="start-time")

    child_tasks = [
        run_in_child_context(
            child_operation(index, multiplier),
            name=f"child-{index}",
        )
        for index in range(steps)
    ]
    results = await asyncio.gather(*child_tasks)
    total_sum = sum(results)

    execution_time_ms = await step(
        elapsed_ms(start_time_ns),
        name="execution-time",
    )

    await wait(timedelta(seconds=2), name="post-compute-wait")

    replay_time_ms = await step(
        elapsed_ms(start_time_ns),
        name="replay-time",
    )

    return {
        "result": total_sum,
        "execution_time_ms": execution_time_ms,
        "replay_time_ms": replay_time_ms,
    }
