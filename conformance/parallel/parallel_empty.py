"""8-5: Parallel with no branches."""

from typing import Any

from async_durable_execution import durable_execution, parallel


@durable_execution
async def handler(_event: Any) -> list:
    return (await parallel([], name="empty")).get_results()
