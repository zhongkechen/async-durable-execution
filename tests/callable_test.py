import asyncio
import functools
import threading
from collections.abc import Awaitable
from contextvars import ContextVar
from unittest.mock import AsyncMock, Mock, patch

import pytest
from async_durable_execution import (
    DurableContext,
    InvalidStateError,
    get_current_context,
    get_durable_context,
    step,
)
from async_durable_execution._core.callable import call_user_function
from async_durable_execution._core.context import bind_current_context
from async_durable_execution._core.context import ensure_durable_operations_allowed
from async_durable_execution._core.models import (
    OperationIdentifier,
    OperationSubType,
)
from async_durable_execution._core.state import ExecutionState


def _create_durable_context() -> DurableContext:
    state = Mock(spec=ExecutionState)
    state.durable_execution_arn = "arn:test:execution"
    return DurableContext(
        execution_state=state,
        operation_identifier=OperationIdentifier(
            operation_id=None,
            sub_type=OperationSubType.EXECUTION,
        ),
    )


async def test_call_user_function_runs_sync_callable_in_thread_with_context() -> None:
    marker = ContextVar("marker", default="missing")
    marker.set("bound")
    event_loop_thread = threading.get_ident()

    def sync_callable() -> tuple[int, str]:
        return threading.get_ident(), marker.get()

    worker_thread, context_value = await call_user_function(sync_callable)

    assert worker_thread != event_loop_thread
    assert context_value == "bound"


async def test_call_user_function_drains_sync_worker_before_propagating_cancel() -> (
    None
):
    started = threading.Event()
    release = threading.Event()
    completed = threading.Event()

    def sync_callable() -> str:
        started.set()
        release.wait()
        completed.set()
        return "done"

    task = asyncio.create_task(call_user_function(sync_callable))
    try:
        while not started.is_set():
            await asyncio.sleep(0.001)

        task.cancel()
        await asyncio.sleep(0.01)

        assert not task.done()
        assert not completed.is_set()
    finally:
        release.set()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert completed.is_set()


async def test_call_user_function_runs_async_callable_on_event_loop() -> None:
    async def async_callable() -> int:
        await asyncio.sleep(0)
        return threading.get_ident()

    with patch(
        "async_durable_execution._core.callable.asyncio.to_thread",
        new=AsyncMock(),
    ) as to_thread:
        assert await call_user_function(async_callable) == threading.get_ident()

    to_thread.assert_not_awaited()


async def test_call_user_function_runs_async_callable_instance_on_event_loop() -> None:
    class AsyncCallable:
        async def __call__(self) -> int:
            ensure_durable_operations_allowed("step()")
            return threading.get_ident()

    with patch(
        "async_durable_execution._core.callable.asyncio.to_thread",
        new=AsyncMock(),
    ) as to_thread:
        assert await call_user_function(AsyncCallable()) == threading.get_ident()

    to_thread.assert_not_awaited()


async def test_call_user_function_unwraps_async_target() -> None:
    async def async_callable() -> int:
        ensure_durable_operations_allowed("step()")
        return threading.get_ident()

    @functools.wraps(async_callable)
    def wrapped_callable() -> Awaitable[int]:
        return async_callable()

    with patch(
        "async_durable_execution._core.callable.asyncio.to_thread",
        new=AsyncMock(),
    ) as to_thread:
        assert await call_user_function(wrapped_callable) == threading.get_ident()

    to_thread.assert_not_awaited()


async def test_call_user_function_unwraps_partial_async_callable_instance() -> None:
    class AsyncCallable:
        async def __call__(self, value: str, *, suffix: str) -> str:
            ensure_durable_operations_allowed("step()")
            return f"{value}{suffix}"

    partial_callable = functools.partial(AsyncCallable(), suffix="-done")
    context = _create_durable_context()

    with (
        bind_current_context(context),
        patch(
            "async_durable_execution._core.callable.asyncio.to_thread",
            new=AsyncMock(),
        ) as to_thread,
    ):
        assert await call_user_function(partial_callable, "work") == "work-done"

    to_thread.assert_not_awaited()


async def test_call_user_function_awaits_value_returned_by_sync_callable() -> None:
    def sync_callable() -> Awaitable[str]:
        async def result() -> str:
            await asyncio.sleep(0)
            return "done"

        return result()

    assert await call_user_function(sync_callable) == "done"


async def test_sync_callable_can_access_bound_durable_context() -> None:
    context = _create_durable_context()

    def sync_callable() -> tuple[object, DurableContext]:
        return get_current_context(), get_durable_context()

    with bind_current_context(context):
        current_context, durable_context = await call_user_function(sync_callable)

    assert current_context is context
    assert durable_context is context


async def test_sync_callable_cannot_create_durable_operation() -> None:
    context = _create_durable_context()

    def sync_callable() -> None:
        step(lambda: "nested", name="nested")

    with (
        bind_current_context(context),
        pytest.raises(
            InvalidStateError,
            match=r"step\(\) cannot be created from a synchronous user callable",
        ),
    ):
        await call_user_function(sync_callable)


async def test_awaitable_returned_by_sync_callable_cannot_compose_operations() -> None:
    context = _create_durable_context()

    def sync_callable() -> Awaitable[str]:
        async def compose() -> str:
            return await step(lambda: "nested", name="nested")

        return compose()

    with (
        bind_current_context(context),
        pytest.raises(
            InvalidStateError,
            match=r"step\(\) cannot be created from a synchronous user callable",
        ),
    ):
        await call_user_function(sync_callable)
