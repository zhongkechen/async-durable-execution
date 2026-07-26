"""Tests for public package exports and module-level operation helpers."""

from collections.abc import Callable
from datetime import timedelta
from typing import get_origin
from unittest.mock import ANY, AsyncMock, MagicMock, Mock, patch

from async_durable_execution import (
    DurableContext,
    DurableFunctionCloudTestRunner,
    DurableFunctionLocalTestRunner,
    DurableFunctionTestResult,
    FlowDefinitionError,
    FlowExecutionError,
    FlowNode,
    FlowNodeContext,
    FlowNodeResult,
    FlowNodeStatus,
    FlowResult,
    WaitForConditionError,
    WithRetryContext,
    create_callback,
    create_cloud_runner,
    create_default_sync_client,
    create_local_runner,
    durable_dag,
    durable_node,
    flow,
    get_node_context,
    get_step_context,
    now,
    node,
    step,
    wait,
    parallel,
    random as durable_random,
    recurse,
    run_in_child_context,
    timestamp,
    uuid as durable_uuid,
    wait_for_callback,
    map as map_operation,
)
from async_durable_execution.context import (
    DurableContext as ModuleDurableContext,
    reset_current_context,
    set_current_context,
)
from async_durable_execution.models import (
    LambdaContext,
    OperationIdentifier,
    OperationSubType,
)
from async_durable_execution.config import JitterStrategy
from async_durable_execution.config import RetryStrategy
from async_durable_execution.extension.parallel import SummaryGenerator
from async_durable_execution.extension.parallel import CompletionDecision
from async_durable_execution.extension.parallel import CompletionStatus
from async_durable_execution.extension.with_retry import (
    WithRetryContext as ModuleWithRetryContext,
)
from async_durable_execution.extension.wait_for_condition import PollingStrategy
from async_durable_execution.extension.recurse import recurse as module_recurse
from async_durable_execution.extension.replay_safe import (
    now as module_now,
    random as module_random,
    timestamp as module_timestamp,
    uuid as module_uuid,
)
from async_durable_execution.serdes import ExtendedTypeSerDes
from async_durable_execution.client import DurableServiceClient


def make_async_executor(result):
    mock_executor = MagicMock()
    mock_executor.process = AsyncMock(return_value=result)
    return mock_executor


def test_additional_public_types_importable_from_package_root():
    """Supporting public types remain available from the package root."""
    import async_durable_execution as ade

    expected_exports = {
        "DurableServiceClient": DurableServiceClient,
        "DurableFunctionCloudTestRunner": DurableFunctionCloudTestRunner,
        "DurableFunctionLocalTestRunner": DurableFunctionLocalTestRunner,
        "DurableFunctionTestResult": DurableFunctionTestResult,
        "DurableContext": ModuleDurableContext,
        "ExtendedTypeSerDes": ExtendedTypeSerDes,
        "FlowDefinitionError": FlowDefinitionError,
        "FlowExecutionError": FlowExecutionError,
        "FlowNode": FlowNode,
        "FlowNodeContext": FlowNodeContext,
        "FlowNodeResult": FlowNodeResult,
        "FlowNodeStatus": FlowNodeStatus,
        "FlowResult": FlowResult,
        "JitterStrategy": JitterStrategy,
        "LambdaContext": LambdaContext,
        "OperationSubType": OperationSubType,
        "RetryStrategy": RetryStrategy,
        "SummaryGenerator": SummaryGenerator,
        "CompletionDecision": CompletionDecision,
        "CompletionStatus": CompletionStatus,
        "WithRetryContext": ModuleWithRetryContext,
        "PollingStrategy": PollingStrategy,
        "WaitForConditionError": WaitForConditionError,
        "create_cloud_runner": create_cloud_runner,
        "create_default_sync_client": create_default_sync_client,
        "create_local_runner": create_local_runner,
        "durable_dag": durable_dag,
        "durable_node": durable_node,
        "flow": flow,
        "get_node_context": get_node_context,
        "get_step_context": get_step_context,
        "now": now,
        "node": node,
        "random": durable_random,
        "recurse": recurse,
        "timestamp": timestamp,
        "uuid": durable_uuid,
    }
    assert DurableContext is ModuleDurableContext
    assert WithRetryContext is ModuleWithRetryContext
    assert recurse is module_recurse
    assert now is module_now
    assert durable_random is module_random
    assert timestamp is module_timestamp
    assert durable_uuid is module_uuid

    for name, public_type in expected_exports.items():
        assert getattr(ade, name) is public_type
        assert name in ade.__all__


def test_summary_generator_is_callable_type_alias():
    """SummaryGenerator is a callable interface, not a protocol class."""
    assert get_origin(SummaryGenerator) is Callable


def test_internal_model_types_not_exported_from_package_root():
    """Internal construction models stay in async_durable_execution.models."""
    import async_durable_execution as ade

    assert not hasattr(ade, "OperationIdentifier")
    assert "OperationIdentifier" not in ade.__all__


def test_merged_retry_presets_not_exported_from_package_root():
    """Retry preset factories live on RetryStrategy."""
    import async_durable_execution as ade

    assert hasattr(ade.RetryStrategy, "default")
    assert hasattr(ade.RetryStrategy, "linear")
    assert not hasattr(ade, "RetryPresets")
    assert "RetryPresets" not in ade.__all__


def test_wait_for_condition_decision_not_exported_from_package_root():
    """wait_for_condition checks return state directly."""
    import async_durable_execution as ade

    assert not hasattr(ade, "WaitForConditionDecision")
    assert "WaitForConditionDecision" not in ade.__all__


def test_polling_strategy_builder_not_exported_from_package_root():
    """PollingStrategy is directly callable without a builder."""
    import async_durable_execution as ade

    assert not hasattr(ade, "WaitDelayStrategy")
    assert "WaitDelayStrategy" not in ade.__all__
    assert not hasattr(ade, "WaitStrategyBuilder")
    assert "WaitStrategyBuilder" not in ade.__all__


def test_get_attempt_not_exported_from_package_root():
    """Step attempts are available from get_step_context().attempt."""
    import async_durable_execution as ade

    assert not hasattr(ade, "get_attempt")
    assert "get_attempt" not in ade.__all__


async def test_module_level_operations_delegate_to_mock_context_methods():
    """Module-level operations use durable-context helper paths."""
    mock_state = Mock()
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    context = DurableContext(
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
    mock_callback_child = AsyncMock(return_value="callback_result")
    mock_child_executor = MagicMock(return_value=make_async_executor("child_result"))
    mock_map_child = AsyncMock(return_value=["mapped"])
    mock_parallel_child = AsyncMock(return_value=["parallel"])

    step_executor = AsyncMock()
    step_executor.process.return_value = "step_result"
    callback_executor = AsyncMock()
    callback_executor.process.return_value = "callback-id"

    token = set_current_context(context)
    try:
        with (
            patch(
                "async_durable_execution.primitive.step.StepOperationExecutor"
            ) as mock_step_executor,
            patch(
                "async_durable_execution.primitive.callback.CallbackOperationExecutor"
            ) as mock_callback_executor,
            patch(
                "async_durable_execution.primitive.wait.WaitOperationExecutor"
            ) as mock_wait_executor,
            patch(
                "async_durable_execution.primitive.child.ChildOperationExecutor",
                mock_child_executor,
            ),
            patch(
                "async_durable_execution.extension.wait_for_callback._create_child_context_task",
                mock_callback_child,
            ),
            patch(
                "async_durable_execution.extension.map._run_in_child_context",
                mock_map_child,
            ),
            patch(
                "async_durable_execution.extension.parallel._run_in_child_context",
                mock_parallel_child,
            ),
        ):
            mock_step_executor.return_value = step_executor
            mock_callback_executor.return_value = callback_executor
            mock_wait_executor.return_value.process = mock_wait

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
    mock_wait_executor.assert_called_once_with(
        seconds=5,
        state=mock_state,
        operation_identifier=ANY,
    )
    mock_wait.assert_awaited_once()
    mock_callback_executor.assert_called_once_with(
        state=mock_state,
        operation_identifier=ANY,
        timeout=None,
        heartbeat_timeout=None,
    )
    callback_executor.process.assert_awaited_once()
    mock_child_executor.assert_called_once()
    assert mock_child_executor.call_args.args[2].name == "test_child"
    mock_callback_child.assert_awaited_once()
    assert (
        mock_callback_child.await_args.kwargs["sub_type"]
        is OperationSubType.WAIT_FOR_CALLBACK
    )
    assert mock_callback_child.await_args.kwargs["name"] == "test_wait_for_callback"
    mock_map_child.assert_awaited_once()
    mock_parallel_child.assert_awaited_once()


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
