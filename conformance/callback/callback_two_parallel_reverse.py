from async_durable_execution import create_callback, durable_execution

# 4-19: Two callbacks — create both then wait in reverse order (cbA, cbB, wcbB, wcbA)
from typing import Any


@durable_execution
async def handler(event: Any) -> dict[str, str]:
    name_a: str = event[0]
    name_b: str = event[1]

    callback_a = await create_callback(name=name_a)
    callback_b = await create_callback(name=name_b)

    result_b = await callback_b.result()
    result_a = await callback_a.result()

    return {"a": result_a, "b": result_b}
