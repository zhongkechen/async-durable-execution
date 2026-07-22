"""1-4: Returning complex object."""

from async_durable_execution import durable_callable, durable_execution, step
from typing import Any


@durable_callable
async def build_response(name: str, tags: list[str]) -> dict:
    return {
        "user": {
            "name": name,
            "tags": tags,
        },
        "count": len(tags),
    }


@durable_execution
async def handler(event: Any) -> dict:
    result: dict = await step(build_response(event["name"], event["tags"]))
    return result
