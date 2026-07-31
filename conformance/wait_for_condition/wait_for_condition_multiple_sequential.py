"""6-13: Multiple sequential wait-for-condition operations."""

from collections.abc import Awaitable, Callable
from typing import Any

from async_durable_execution import durable_execution, wait_for_condition


def make_check(threshold: int) -> Callable[[int | None], Awaitable[int]]:
    async def check(state: int | None) -> int:
        return (state or 0) + 1

    return check


def make_polling_strategy(threshold: int) -> Callable[[int, int], int | None]:
    def polling_strategy(state: int, _attempt: int) -> int | None:
        return None if state >= threshold else 1

    return polling_strategy


@durable_execution
async def handler(_event: Any) -> int:
    first = await wait_for_condition(
        make_check(2),
        initial_state=0,
        polling_strategy=make_polling_strategy(2),
    )
    return await wait_for_condition(
        make_check(4),
        initial_state=first,
        polling_strategy=make_polling_strategy(4),
    )
