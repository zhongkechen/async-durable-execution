from typing import no_type_check
from unittest.mock import Mock

from async_durable_execution import DurableContext, durable_callable
from async_durable_execution._core.context import (
    bind_current_context,
    get_current_context,
)
from async_durable_execution._core.models import OperationIdentifier, OperationSubType
from async_durable_execution._primitive.step import StepContext
from async_durable_execution._core.state import ExecutionState


@no_type_check
async def test_bind_current_context_sets_context_for_invocation() -> None:
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

    with bind_current_context(context):
        assert await async_callable() is context


@no_type_check
async def test_bind_current_context_accepts_step_context() -> None:
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

    with bind_current_context(context):
        assert await async_callable() is context


async def test_durable_callable_supports_instance_methods() -> None:
    class Greeter:
        def __init__(self, prefix: str) -> None:
            self.prefix = prefix

        @durable_callable
        async def greet(self, name: str) -> str:
            return f"{self.prefix}, {name}"

    greeter = Greeter("hello")

    bound_greet = greeter.greet("Ada")
    assert getattr(bound_greet, "__name__") == "greet"
    assert await bound_greet() == "hello, Ada"


async def test_durable_callable_supports_classmethod_inside_order() -> None:
    class Greeter:
        prefix = "hello"

        @classmethod
        @durable_callable
        async def greet(cls, name: str) -> str:
            return f"{cls.prefix}, {name}"

    bound_greet = Greeter.greet("Ada")
    assert getattr(bound_greet, "__name__") == "greet"
    assert await bound_greet() == "hello, Ada"


async def test_durable_callable_supports_classmethod_outside_order() -> None:
    class Greeter:
        prefix = "hello"

        @durable_callable
        @classmethod
        async def greet(cls, name: str) -> str:
            return f"{cls.prefix}, {name}"

    bound_greet = Greeter.greet("Ada")
    assert getattr(bound_greet, "__name__") == "greet"
    assert await bound_greet() == "hello, Ada"


async def test_durable_callable_supports_staticmethod_orders() -> None:
    class Greeter:
        @staticmethod
        @durable_callable
        async def greet_inside(name: str) -> str:
            return f"hello, {name}"

        @durable_callable
        @staticmethod
        async def greet_outside(name: str) -> str:
            return f"hi, {name}"

    bound_greet_inside = Greeter.greet_inside("Ada")
    bound_greet_outside = Greeter.greet_outside("Grace")
    assert getattr(bound_greet_inside, "__name__") == "greet_inside"
    assert getattr(bound_greet_outside, "__name__") == "greet_outside"
    assert await bound_greet_inside() == "hello, Ada"
    assert await bound_greet_outside() == "hi, Grace"


async def test_durable_callable_supports_synchronous_methods():
    class Greeter:
        prefix = "hello"

        @durable_callable
        def instance(self, name: str) -> str:
            return f"{self.prefix}, {name}"

        @classmethod
        @durable_callable
        def class_inside(cls, name: str) -> str:
            return f"{cls.prefix}, {name}"

        @durable_callable
        @classmethod
        def class_outside(cls, name: str) -> str:
            return f"{cls.prefix}, {name}"

        @staticmethod
        @durable_callable
        def static_inside(name: str) -> str:
            return f"hi, {name}"

        @durable_callable
        @staticmethod
        def static_outside(name: str) -> str:
            return f"hi, {name}"

    greeter = Greeter()

    assert await greeter.instance("Ada")() == "hello, Ada"
    assert await Greeter.class_inside("Grace")() == "hello, Grace"
    assert await Greeter.class_outside("Linus")() == "hello, Linus"
    assert await Greeter.static_inside("Guido")() == "hi, Guido"
    assert await Greeter.static_outside("Margaret")() == "hi, Margaret"
