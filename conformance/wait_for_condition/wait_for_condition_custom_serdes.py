"""6-11: Wait-for-condition with custom state serdes."""

from typing import Any

from async_durable_execution import (
    SerDes,
    durable_execution,
    wait_for_condition,
)


class AppendSerDes(SerDes[str]):
    async def serialize(self, value: str) -> str:
        return "ENC:" + value

    async def deserialize(self, data: str) -> str:
        return data.removeprefix("ENC:")


@durable_execution
async def handler(_event: Any) -> str:
    async def check(state: str | None) -> str:
        return (state or "") + "x"

    return await wait_for_condition(
        check,
        initial_state="",
        polling_strategy=lambda state, _attempt: None if len(state) >= 2 else 1,
        serdes=AppendSerDes(),
    )
