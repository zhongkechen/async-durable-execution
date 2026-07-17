"""5-16: Invoke with custom result serdes."""

from async_durable_execution import SerDes, durable_execution, invoke
import os
from typing import Any


class UppercaseResultSerDes(SerDes[str]):
    """Custom serdes that uppercases the result on deserialization."""

    async def serialize(self, value: str) -> str:
        return value

    async def deserialize(self, data: str) -> str:
        return data.upper()


@durable_execution
async def handler(event: Any) -> str:
    function_name = os.environ["TARGET_FUNCTION_NAME"]
    result: str = await invoke(
        function_name, event, serdes_result=UppercaseResultSerDes()
    )
    return result
