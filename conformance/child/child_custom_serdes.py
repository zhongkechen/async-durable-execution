"""3-14: Child context with custom serdes (succeed)."""

from async_durable_execution import (
    SerDes,
    durable_callable,
    durable_execution,
    run_in_child_context,
    step,
)
from typing import Any


class UppercaseSerDes(SerDes[str]):
    async def serialize(self, value: str) -> str:
        return value.upper()

    async def deserialize(self, data: str) -> str:
        return data


@durable_callable
async def return_input(value: str) -> str:
    return value


@durable_callable
async def serdes_child(value: str) -> str:
    return await step(return_input(value))


@durable_execution
async def handler(event: Any) -> str:
    return await run_in_child_context(
        serdes_child(event), name="serdes-child", serdes=UppercaseSerDes()
    )
