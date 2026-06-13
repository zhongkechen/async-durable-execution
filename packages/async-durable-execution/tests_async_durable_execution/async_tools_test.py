import asyncio

import pytest

from async_durable_execution.async_tools import invoke_callable
from async_durable_execution.exceptions import ValidationError


def test_invoke_callable_runs_async_callable():
    async def async_callable() -> str:
        await asyncio.sleep(0)
        return "async-result"

    assert asyncio.run(invoke_callable(async_callable)) == "async-result"


def test_invoke_callable_runs_async_callable_from_running_loop():
    async def async_callable() -> str:
        await asyncio.sleep(0)
        return "nested-async-result"

    async def main() -> str:
        return await invoke_callable(async_callable)

    assert asyncio.run(main()) == "nested-async-result"


def test_invoke_callable_rejects_sync_callable():
    def sync_callable() -> str:
        return "sync-result"

    with pytest.raises(
        ValidationError,
        match="Non-async callables are no longer supported",
    ):
        asyncio.run(invoke_callable(sync_callable))
