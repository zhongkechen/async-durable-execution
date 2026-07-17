"""5-15: Invoke with custom payload serdes."""

from async_durable_execution import SerDes, durable_execution, invoke
import json
import os
from typing import Any


class UppercasePayloadSerDes(SerDes[Any]):
    """Custom serdes that uppercases string values in the payload."""

    async def serialize(self, value: Any) -> str:
        if isinstance(value, str):
            return json.dumps(value.upper())
        return json.dumps(value)

    async def deserialize(self, data: str) -> Any:
        return json.loads(data)


@durable_execution
async def handler(event: Any) -> Any:
    function_name = os.environ["TARGET_FUNCTION_NAME"]
    result = await invoke(function_name, event, serdes_payload=UppercasePayloadSerDes())
    return result
