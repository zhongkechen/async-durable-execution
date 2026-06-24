"""Tests for the types module."""

from datetime import timedelta
from unittest.mock import ANY, AsyncMock, Mock, patch

from async_durable_execution import (
    create_callback,
    step,
    wait,
    parallel,
    run_in_child_context,
    wait_for_callback,
    map as map_operation,
)
from async_durable_execution.context import reset_current_context, set_current_context
from async_durable_execution.models import OperationIdentifier, OperationSubType
from async_durable_execution.operation import child
from async_durable_execution.config import JitterStrategy
from async_durable_execution.plugin import (
    InvocationEndInfo,
    InvocationInfo,
    InvocationStartInfo,
    OperationEndInfo,
    OperationInfo,
    OperationStartInfo,
    UserFunctionEndInfo,
    UserFunctionOutcome,
    UserFunctionStartInfo,
)
from async_durable_execution.serdes import ExtendedTypeSerDes
from async_durable_execution.types import DurableServiceClient, SummaryGenerator


def test_additional_public_types_importable_from_package_root():
    """Supporting public types remain available from the package root."""
    import async_durable_execution as ade

    expected_exports = {
        "DurableServiceClient": DurableServiceClient,
        "ExtendedTypeSerDes": ExtendedTypeSerDes,
        "InvocationEndInfo": InvocationEndInfo,
        "InvocationInfo": InvocationInfo,
        "InvocationStartInfo": InvocationStartInfo,
        "JitterStrategy": JitterStrategy,
        "OperationEndInfo": OperationEndInfo,
        "OperationInfo": OperationInfo,
        "OperationStartInfo": OperationStartInfo,
        "OperationSubType": OperationSubType,
        "SummaryGenerator": SummaryGenerator,
        "UserFunctionEndInfo": UserFunctionEndInfo,
        "UserFunctionOutcome": UserFunctionOutcome,
        "UserFunctionStartInfo": UserFunctionStartInfo,
    }

    for name, public_type in expected_exports.items():
        assert getattr(ade, name) is public_type
        assert name in ade.__all__


def test_internal_model_types_not_exported_from_package_root():
    """Internal construction models stay in async_durable_execution.models."""
    import async_durable_execution as ade

    assert not hasattr(ade, "OperationIdentifier")
    assert "OperationIdentifier" not in ade.__all__


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

    async def submitter():
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

            assert await step(test_callable, name="test_step") == "step_result"
            assert (
                await run_in_child_context(
                    test_callable,
                    name="test_child",
                )
                == "child_result"
            )
            assert await map_operation(
                test_callable,
                ["a"],
                name="test_map",
            ) == ["mapped"]
            assert await parallel(
                [test_callable],
                name="test_parallel",
            ) == ["parallel"]
            await wait(timedelta(seconds=5), name="test_wait")
            callback_result = await create_callback(
                name="test_callback",
            )
            assert callback_result.callback_id == "callback-id"
            assert (
                await wait_for_callback(
                    submitter,
                    name="test_wait_for_callback",
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
