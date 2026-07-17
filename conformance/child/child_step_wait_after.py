"""3-18: Child context with step and wait inside, step and wait after."""

from datetime import timedelta
from async_durable_execution import (
    durable_callable,
    durable_execution,
    run_in_child_context,
    step,
    wait,
)
from typing import Any


@durable_callable
async def inner_step(value: str) -> str:
    return value


@durable_callable
async def step_and_wait_child(value: str) -> str:
    await step(inner_step(value))
    await wait(timedelta(seconds=2))
    return value


@durable_callable
async def outer_step(value: str) -> str:
    return value


@durable_execution
async def handler(event: Any) -> str:
    await run_in_child_context(step_and_wait_child(str(event)), name="step-wait-child")
    result: str = await step(outer_step(str(event)))
    await wait(timedelta(seconds=2))
    return result
