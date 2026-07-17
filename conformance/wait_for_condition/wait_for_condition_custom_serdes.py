"""6-11: Wait-for-condition with custom state serdes."""

from typing import Any

from async_durable_execution import (
    SerDes,
    WaitForConditionDecision,
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
    async def check(state: str | None):
        next_state = (state or "") + "x"
        decision = (
            WaitForConditionDecision.stop_polling()
            if len(next_state) >= 2
            else WaitForConditionDecision.continue_waiting()
        )
        return next_state, decision

    return await wait_for_condition(
        check,
        initial_state="",
        wait_strategy=lambda _s, _a: 1,
        serdes=AppendSerDes(),
    )
