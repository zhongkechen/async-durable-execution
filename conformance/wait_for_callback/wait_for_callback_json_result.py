"""7-11: Wait-for-callback with a structured JSON result."""

import json
from typing import Any

from async_durable_execution import SerDes, durable_execution, wait_for_callback


class JsonObjectSerDes(SerDes[dict | None]):
    async def serialize(self, value: dict | None) -> str:
        return json.dumps(value)

    async def deserialize(self, payload: str) -> dict | None:
        return json.loads(payload)


async def submitter() -> None:
    pass


@durable_execution
async def handler(event: Any) -> str:
    result = await wait_for_callback(submitter, name=event, serdes=JsonObjectSerDes())
    return result["status"]
