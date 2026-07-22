"""7-8: Wait-for-callback inside a child context."""

from typing import Any

from async_durable_execution import (
    durable_callable,
    durable_execution,
    run_in_child_context,
    wait_for_callback,
)


async def submitter() -> None:
    pass


@durable_callable
async def wrapper(name: str) -> str:
    return await wait_for_callback(submitter, name=name)


@durable_execution
async def handler(event: Any) -> str:
    return await run_in_child_context(wrapper(event), name="wrapper")
