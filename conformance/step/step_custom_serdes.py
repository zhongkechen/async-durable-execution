"""1-6: Custom serdes (per-step) - transforms result to uppercase."""

from async_durable_execution import SerDes, durable_callable, durable_execution, step
from typing import Any


class UppercaseSerDes(SerDes[str]):
    """Custom serdes that transforms strings to uppercase on serialization."""

    async def serialize(self, value: str) -> str:
        return value.upper()

    async def deserialize(self, data: str) -> str:
        return data


@durable_callable
async def return_input(value: str) -> str:
    return value


@durable_execution
async def handler(event: Any) -> str:
    return await step(return_input(event), serdes=UppercaseSerDes())
