import asyncio
from unittest.mock import Mock

import pytest

from async_durable_execution.async_tools import invoke_user_callable, invoke_callable
from async_durable_execution.context import get_current_context
from async_durable_execution import DurableContext
from async_durable_execution.exceptions import ValidationError
from async_durable_execution.models import OperationIdentifier, OperationSubType
from async_durable_execution.state import ExecutionState


async def test_invoke_callable_runs_async_callable():
    async def async_callable() -> str:
        await asyncio.sleep(0)
        return "async-result"

    assert await invoke_callable(async_callable) == "async-result"


async def test_invoke_callable_runs_async_callable_from_running_loop():
    async def async_callable() -> str:
        await asyncio.sleep(0)
        return "nested-async-result"

    async def main() -> str:
        return await invoke_callable(async_callable)

    assert await main() == "nested-async-result"


async def test_invoke_callable_rejects_sync_callable():
    def sync_callable() -> str:
        return "sync-result"

    with pytest.raises(
        ValidationError,
        match="Non-async callables are no longer supported",
    ):
        await invoke_callable(sync_callable)


async def test_invoke_user_callable_sets_context_for_invocation():
    state = Mock(spec=ExecutionState)
    state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    context = DurableContext(
        execution_state=state,
        operation_identifier=OperationIdentifier(
            operation_id=None,
            sub_type=OperationSubType.EXECUTION,
        ),
    )

    async def async_callable() -> DurableContext:
        return get_current_context()

    assert await invoke_user_callable(context, async_callable) is context
