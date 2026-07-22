"""7-10: Wait-for-callback mixed with wait and step."""

from datetime import timedelta
from typing import Any

from async_durable_execution import (
    durable_callable,
    durable_execution,
    step,
    wait,
    wait_for_callback,
)


async def submitter() -> None:
    pass


@durable_callable
async def fixed_data() -> str:
    return "fixed-data"


@durable_execution
async def handler(event: Any) -> str:
    await wait(timedelta(seconds=1))
    await step(fixed_data(), name="step")
    return await wait_for_callback(submitter, name=event)
