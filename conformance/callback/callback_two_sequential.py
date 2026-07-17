from async_durable_execution import create_callback, durable_execution

# 4-17: Two callbacks — sequential create and wait (cbA, wcbA, cbB, wcbB)
from typing import Any


@durable_execution
async def handler(event: Any) -> dict[str, str]:
    name_a: str = event[0]
    name_b: str = event[1]

    callback_a = await create_callback(name=name_a)
    result_a = await callback_a.result()

    callback_b = await create_callback(name=name_b)
    result_b = await callback_b.result()

    return {"a": result_a, "b": result_b}
