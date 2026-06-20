"""Tests for the types module."""

from datetime import timedelta
from unittest.mock import ANY, AsyncMock, Mock, patch

from async_durable_execution.operation.child import ChildConfig
from async_durable_execution import (
    create_callback,
    step,
    wait,
    parallel,
    run_in_child_context,
    wait_for_callback,
    map as map_operation,
    DurableContext as RuntimeDurableContext,
)
from async_durable_execution.context import reset_current_context, set_current_context
from async_durable_execution.models import OperationIdentifier, OperationSubType
from async_durable_execution.operation import child
from async_durable_execution.operation.callback import (
    CallbackConfig,
    WaitForCallbackConfig,
)
from async_durable_execution.operation.map import MapConfig
from async_durable_execution.operation.parallel import ParallelConfig
from async_durable_execution.operation.step import StepConfig
from async_durable_execution.types import Callback, DurableContext


async def test_callback_protocol():
    """Test Callback protocol implementation."""
    mock_callback = Mock(spec=Callback)
    mock_callback.callback_id = "test-callback-123"
    mock_callback.result = AsyncMock(return_value="test_result")

    assert mock_callback.callback_id == "test-callback-123"
    assert await mock_callback.result() == "test_result"


async def test_durable_context_protocol_fields():
    """Test DurableContext protocol exposes execution state fields."""
    mock_context = Mock(spec=DurableContext)
    mock_context.execution_state = Mock()
    mock_context.durable_execution_arn = "arn:aws:lambda:region:acct:function:name:1"
    mock_context.parent_id = "parent-op"
    mock_context.operation_id = "operation-op"
    mock_context.operation_name = "operation-name"

    assert mock_context.execution_state is not None
    assert mock_context.durable_execution_arn.endswith(":1")
    assert mock_context.parent_id == "parent-op"
    assert mock_context.operation_id == "operation-op"
    assert mock_context.operation_name == "operation-name"


async def test_durable_context_protocol_optional_fields_can_be_none():
    """Optional DurableContext protocol fields may be unset."""
    mock_context = Mock(spec=DurableContext)
    mock_context.execution_state = None
    mock_context.durable_execution_arn = None
    mock_context.parent_id = None
    mock_context.operation_id = None
    mock_context.operation_name = None

    assert mock_context.execution_state is None
    assert mock_context.durable_execution_arn is None
    assert mock_context.parent_id is None
    assert mock_context.operation_id is None
    assert mock_context.operation_name is None


async def test_module_level_operations_delegate_to_mock_context_methods():
    """Module-level operations use durable-context helper paths."""
    mock_state = Mock()
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    context = child.DurableContext(
        execution_state=mock_state,
        operation_identifier=OperationIdentifier(
            operation_id=None,
            sub_type=OperationSubType.EXECUTION,
        ),
    )

    async def test_callable():
        return "result"

    async def submitter(_callback_id: str):
        return "submitted"

    mock_wait = AsyncMock(return_value=None)
    mock_child = AsyncMock(side_effect=["child_result", "callback_result"])
    mock_child_handler = AsyncMock(side_effect=[["mapped"], ["parallel"]])

    step_executor = AsyncMock()
    step_executor.process.return_value = "step_result"
    callback_executor = AsyncMock()
    callback_executor.process.return_value = "callback-id"

    token = set_current_context(context)
    try:
        with (
            patch(
                "async_durable_execution.operation.step.StepOperationExecutor"
            ) as mock_step_executor,
            patch(
                "async_durable_execution.operation.callback.CallbackOperationExecutor"
            ) as mock_callback_executor,
            patch("async_durable_execution.operation.wait._wait_in_context", mock_wait),
            patch(
                "async_durable_execution.operation.child._run_in_child_context_in_context",
                mock_child,
            ),
            patch(
                "async_durable_execution.operation.callback._run_in_child_context_in_context",
                mock_child,
            ),
            patch(
                "async_durable_execution.operation.map.child_handler",
                mock_child_handler,
            ),
            patch(
                "async_durable_execution.operation.parallel.child_handler",
                mock_child_handler,
            ),
        ):
            mock_step_executor.return_value = step_executor
            mock_callback_executor.return_value = callback_executor

            assert (
                await step(test_callable, name="test_step", config=StepConfig())
                == "step_result"
            )
            assert (
                await run_in_child_context(
                    test_callable,
                    name="test_child",
                    config=ChildConfig(),
                )
                == "child_result"
            )
            assert await map_operation(
                ["a"],
                test_callable,
                name="test_map",
                config=MapConfig(),
            ) == ["mapped"]
            assert await parallel(
                [test_callable],
                name="test_parallel",
                config=ParallelConfig(),
            ) == ["parallel"]
            await wait(timedelta(seconds=5), name="test_wait")
            callback_result = await create_callback(
                name="test_callback",
                config=CallbackConfig(),
            )
            assert callback_result.callback_id == "callback-id"
            assert (
                await wait_for_callback(
                    submitter,
                    name="test_wait_for_callback",
                    config=WaitForCallbackConfig(),
                )
                == "callback_result"
            )
    finally:
        reset_current_context(token)

    mock_step_executor.assert_called_once()
    step_executor.process.assert_awaited_once()
    mock_wait.assert_awaited_once_with(
        context,
        duration=timedelta(seconds=5),
        name="test_wait",
    )
    mock_callback_executor.assert_called_once_with(
        state=mock_state,
        operation_identifier=ANY,
        config=ANY,
    )
    callback_executor.process.assert_awaited_once()
    assert mock_child.await_count == 2
    assert mock_child.await_args_list[0].kwargs["func"] is test_callable
    assert mock_child.await_args_list[0].kwargs["name"] == "test_child"
    assert mock_child.await_args_list[1].kwargs["name"] == "test_wait_for_callback"
    assert mock_child_handler.await_count == 2


async def test_protocol_members_reflect_current_surface():
    """Test that protocols retain the current public surface."""
    assert hasattr(Callback, "result")

    assert DurableContext.__annotations__ == {
        "execution_state": "ExecutionState | None",
        "durable_execution_arn": "str | None",
        "parent_id": "str | None",
        "operation_id": "str | None",
        "operation_name": "str | None",
    }


async def test_concrete_callback_implementation():
    """Test a concrete implementation of Callback protocol."""

    class ConcreteCallback:
        def __init__(self, callback_id: str):
            self.callback_id = callback_id
            self._result = None

        async def result(self):
            return self._result

    callback = ConcreteCallback("test-123")
    assert callback.callback_id == "test-123"
    assert await callback.result() is None
