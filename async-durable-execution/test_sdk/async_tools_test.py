import asyncio
from unittest.mock import Mock

import pytest

from async_durable_execution.async_tools import invoke_user_callable, invoke_callable
from async_durable_execution.context import get_current_context
from async_durable_execution import DurableContext, durable_callable
from async_durable_execution.exceptions import ValidationError
from async_durable_execution.models import OperationIdentifier, OperationSubType
from async_durable_execution.primitive.step import StepContext
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


async def test_invoke_user_callable_accepts_step_context():
    state = Mock(spec=ExecutionState)
    state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    context = StepContext(
        execution_state=state,
        operation_identifier=OperationIdentifier(
            operation_id="step-1",
            sub_type=OperationSubType.STEP,
        ),
        attempt=2,
    )

    async def async_callable() -> StepContext:
        return get_current_context()

    assert await invoke_user_callable(context, async_callable) is context


async def test_durable_callable_supports_instance_methods():
    class Greeter:
        def __init__(self, prefix: str):
            self.prefix = prefix

        @durable_callable
        async def greet(self, name: str) -> str:
            return f"{self.prefix}, {name}"

    greeter = Greeter("hello")

    assert await greeter.greet("Ada")() == "hello, Ada"


async def test_durable_callable_supports_classmethod_inside_order():
    class Greeter:
        prefix = "hello"

        @classmethod
        @durable_callable
        async def greet(cls, name: str) -> str:
            return f"{cls.prefix}, {name}"

    assert await Greeter.greet("Ada")() == "hello, Ada"


async def test_durable_callable_supports_classmethod_outside_order():
    class Greeter:
        prefix = "hello"

        @durable_callable
        @classmethod
        async def greet(cls, name: str) -> str:
            return f"{cls.prefix}, {name}"

    assert await Greeter.greet("Ada")() == "hello, Ada"


async def test_durable_callable_supports_staticmethod_orders():
    class Greeter:
        @staticmethod
        @durable_callable
        async def greet_inside(name: str) -> str:
            return f"hello, {name}"

        @durable_callable
        @staticmethod
        async def greet_outside(name: str) -> str:
            return f"hi, {name}"

    assert await Greeter.greet_inside("Ada")() == "hello, Ada"
    assert await Greeter.greet_outside("Grace")() == "hi, Grace"
