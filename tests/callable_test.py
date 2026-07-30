import asyncio
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
