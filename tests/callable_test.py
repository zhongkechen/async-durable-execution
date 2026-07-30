import asyncio
import threading
from contextvars import ContextVar
from unittest.mock import AsyncMock, patch

from async_durable_execution._core.callable import call_user_function


async def test_call_user_function_runs_sync_callable_in_thread_with_context():
    marker = ContextVar("marker", default="missing")
    marker.set("bound")
    event_loop_thread = threading.get_ident()

    def sync_callable() -> tuple[int, str]:
        return threading.get_ident(), marker.get()

    worker_thread, context_value = await call_user_function(sync_callable)

    assert worker_thread != event_loop_thread
    assert context_value == "bound"


async def test_call_user_function_runs_async_callable_on_event_loop():
    async def async_callable() -> int:
        await asyncio.sleep(0)
        return threading.get_ident()

    with patch(
        "async_durable_execution._core.callable.asyncio.to_thread",
        new=AsyncMock(),
    ) as to_thread:
        assert await call_user_function(async_callable) == threading.get_ident()

    to_thread.assert_not_awaited()


async def test_call_user_function_awaits_value_returned_by_sync_callable():
    def sync_callable():
        async def result() -> str:
            await asyncio.sleep(0)
            return "done"

        return result()

    assert await call_user_function(sync_callable) == "done"
