"""Tests for public package exports and module-level operation helpers."""

import importlib
import inspect
from typing import no_type_check

from typing import Any, cast

from collections.abc import Callable
from datetime import timedelta
from typing import get_origin
from unittest.mock import ANY, AsyncMock, MagicMock, Mock, patch

from async_durable_execution import (
    DurableContext,
    DurableFunctionCloudTestRunner,
    DurableFunctionLocalTestRunner,
    DurableFunctionTestResult,
    DurableTerminalActions,
    ExtensionContext,
    ExtensionOperation,
    ExtensionStepResult,
    FlowDefinitionError,
    FlowExecutionError,
    FlowNode,
    FlowNodeContext,
    FlowNodeResult,
    FlowNodeStatus,
    FlowResult,
    TerminalFailure,
    TerminalFailurePhase,
    TerminalScopeConfig,
    TerminalScopeError,
    WaitForConditionError,
    WithRetryContext,
    create_callback,
    create_cloud_runner,
    create_default_sync_client,
    create_local_runner,
    durable_dag,
    durable_node,
    flow,
    get_durable_context,
    get_extension_context,
    get_map_item_context,
    get_node_context,
    get_serdes_context,
    get_step_context,
    get_wait_for_callback_context,
    get_wait_for_condition_check_context,
    get_with_retry_context,
    invoke,
    now,
    node,
    step,
    terminal_scope,
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
from async_durable_execution._core.context import (
    DurableContext as ModuleDurableContext,
    reset_current_context,
    set_current_context,
)
from async_durable_execution._core.models import (
    LambdaContext,
    OperationIdentifier,
    OperationSubType,
)
from async_durable_execution._core.config import JitterStrategy
from async_durable_execution._core.config import RetryStrategy
from async_durable_execution._operation.parallel import CompletionDecision
from async_durable_execution._operation.parallel import CompletionStatus
from async_durable_execution._operation.callback import (
    create_callback as module_create_callback,
)
from async_durable_execution._operation.child import (
    SummaryGenerator,
    run_in_child_context as module_run_in_child_context,
)
from async_durable_execution._operation.invoke import invoke as module_invoke
from async_durable_execution._operation.step import step as module_step
from async_durable_execution._operation.terminal_scope import (
    terminal_scope as module_terminal_scope,
)
from async_durable_execution._operation.wait import wait as module_wait
from async_durable_execution._operation.with_retry import (
    WithRetryContext as ModuleWithRetryContext,
)
from async_durable_execution._operation.wait_for_condition import PollingStrategy
from async_durable_execution._operation.recurse import recurse as module_recurse
from async_durable_execution._operation.replay_safe import (
    now as module_now,
    random as module_random,
    timestamp as module_timestamp,
    uuid as module_uuid,
)


def test_legacy_extension_modules_alias_operation_modules() -> None:
    """Former private module paths resolve to the canonical operation modules."""
    for module_name in (
        "flow",
        "map",
        "parallel",
        "recurse",
        "replay_safe",
        "wait_for_callback",
        "wait_for_condition",
        "with_retry",
    ):
        legacy = importlib.import_module(
            f"async_durable_execution._extension.{module_name}"
        )
        canonical = importlib.import_module(
            f"async_durable_execution._operation.{module_name}"
        )

        assert legacy is canonical


def test_user_facing_primitives_are_owned_by_operation_modules() -> None:
    """Package-root primitive helpers resolve to the operation layer."""
    assert create_callback is module_create_callback
    assert invoke is module_invoke
    assert run_in_child_context is module_run_in_child_context
    assert step is module_step
    assert terminal_scope is module_terminal_scope
    assert wait is module_wait


def test_legacy_primitive_helper_imports_remain_compatible() -> None:
    """Former private helper imports retain the public call signatures."""
    canonical_helpers = {
        "callback": ("create_callback", module_create_callback),
        "child": ("run_in_child_context", module_run_in_child_context),
        "invoke": ("invoke", module_invoke),
        "step": ("step", module_step),
        "wait": ("wait", module_wait),
    }

    for module_name, (helper_name, canonical) in canonical_helpers.items():
        legacy_module = importlib.import_module(
            f"async_durable_execution._primitive.{module_name}"
        )
        legacy = getattr(legacy_module, helper_name)

        assert inspect.signature(legacy) == inspect.signature(
            cast("Callable[..., Any]", canonical)
        )


from async_durable_execution._core.serdes import ExtendedTypeSerDes
from async_durable_execution._core.client import DurableServiceClient


def make_async_executor(result) -> Any:
    mock_executor = MagicMock()
    mock_executor.process = AsyncMock(return_value=result)
    return mock_executor


def test_additional_public_types_importable_from_package_root() -> None:
    """Supporting public types remain available from the package root."""
    import async_durable_execution as ade

    expected_exports = {
        "DurableServiceClient": DurableServiceClient,
        "DurableFunctionCloudTestRunner": DurableFunctionCloudTestRunner,
        "DurableFunctionLocalTestRunner": DurableFunctionLocalTestRunner,
        "DurableFunctionTestResult": DurableFunctionTestResult,
        "DurableTerminalActions": DurableTerminalActions,
        "DurableContext": ModuleDurableContext,
        "ExtendedTypeSerDes": ExtendedTypeSerDes,
        "ExtensionContext": ExtensionContext,
        "ExtensionOperation": ExtensionOperation,
        "ExtensionStepResult": ExtensionStepResult,
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
        "TerminalFailure": TerminalFailure,
        "TerminalFailurePhase": TerminalFailurePhase,
        "TerminalScopeConfig": TerminalScopeConfig,
        "TerminalScopeError": TerminalScopeError,
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
        "get_durable_context": get_durable_context,
        "get_extension_context": get_extension_context,
        "get_map_item_context": get_map_item_context,
        "get_node_context": get_node_context,
        "get_serdes_context": get_serdes_context,
        "get_step_context": get_step_context,
        "get_wait_for_callback_context": get_wait_for_callback_context,
        "get_wait_for_condition_check_context": get_wait_for_condition_check_context,
        "get_with_retry_context": get_with_retry_context,
        "now": now,
        "node": node,
        "random": durable_random,
        "recurse": recurse,
        "terminal_scope": terminal_scope,
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


def test_core_public_api_is_reexported_from_core_package() -> None:
    """Core public symbols are available from both supported package facades."""
    import async_durable_execution as ade
    import async_durable_execution._core as core

    expected_exports = {
        "CallableRuntimeError",
        "ComposableSerDes",
        "DurableContext",
        "DurableExecutionsError",
        "DurableServiceClient",
        "ErrorObject",
        "ExecutionError",
        "ExtendedTypeSerDes",
        "InvalidStateError",
        "InvocationError",
        "InvocationStatus",
        "JitterStrategy",
        "JsonSerDes",
        "LambdaContext",
        "OperationStatus",
        "OperationSubType",
        "OperationType",
        "RetryStrategy",
        "RetryableSerDesError",
        "SerDes",
        "SerDesContext",
        "SerDesError",
        "SerDesPipelineError",
        "SerDesStage",
        "UserlandError",
        "ValidationError",
        "create_default_sync_client",
        "create_serdes_pipeline",
        "durable_callable",
        "durable_execution",
        "get_current_context",
        "get_durable_context",
        "get_serdes_context",
        "is_composable_serdes",
    }

    assert set(core.__all__) == expected_exports
    for name in expected_exports:
        assert getattr(core, name) is getattr(ade, name)


def test_summary_generator_is_callable_type_alias() -> None:
    """SummaryGenerator is a callable interface, not a protocol class."""
    assert get_origin(SummaryGenerator) is Callable


def test_internal_model_types_not_exported_from_package_root() -> None:
    """Internal construction models stay in async_durable_execution._core.models."""
    import async_durable_execution as ade

    assert not hasattr(ade, "OperationIdentifier")
    assert "OperationIdentifier" not in ade.__all__


def test_merged_retry_presets_not_exported_from_package_root() -> None:
    """Retry preset factories live on RetryStrategy."""
    import async_durable_execution as ade

    assert hasattr(ade.RetryStrategy, "default")
    assert hasattr(ade.RetryStrategy, "linear")
    assert not hasattr(ade, "RetryPresets")
    assert "RetryPresets" not in ade.__all__


def test_wait_for_condition_decision_not_exported_from_package_root() -> None:
    """wait_for_condition checks return state directly."""
    import async_durable_execution as ade

    assert not hasattr(ade, "WaitForConditionDecision")
    assert "WaitForConditionDecision" not in ade.__all__


def test_polling_strategy_builder_not_exported_from_package_root() -> None:
    """PollingStrategy is directly callable without a builder."""
    import async_durable_execution as ade

    assert not hasattr(ade, "WaitDelayStrategy")
    assert "WaitDelayStrategy" not in ade.__all__
    assert not hasattr(ade, "WaitStrategyBuilder")
    assert "WaitStrategyBuilder" not in ade.__all__


def test_get_attempt_not_exported_from_package_root() -> None:
    """Step attempts are available from get_step_context().attempt."""
    import async_durable_execution as ade

    assert not hasattr(ade, "get_attempt")
    assert "get_attempt" not in ade.__all__


@no_type_check
async def test_module_level_operations_delegate_to_mock_context_methods() -> None:
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

    async def test_callable() -> str:
        return "result"

    async def submitter() -> str:
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
                "async_durable_execution._primitive.step.StepOperationExecutor"
            ) as mock_step_executor,
            patch(
                "async_durable_execution._primitive.callback.CallbackOperationExecutor"
            ) as mock_callback_executor,
            patch(
                "async_durable_execution._primitive.wait.WaitOperationExecutor"
            ) as mock_wait_executor,
            patch(
                "async_durable_execution._primitive.child.ChildOperationExecutor",
                mock_child_executor,
            ),
            patch(
                "async_durable_execution._operation.wait_for_callback._create_child_context_task",
                mock_callback_child,
            ),
            patch(
                "async_durable_execution._operation.map._run_in_child_context",
                mock_map_child,
            ),
            patch(
                "async_durable_execution._operation.parallel._run_in_child_context",
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


async def test_concrete_callback_implementation() -> None:
    """Test a concrete implementation of Callback protocol."""

    class ConcreteCallback:
        def __init__(self, callback_id: str) -> None:
            self.callback_id = callback_id
            self._result = None

        async def result(self) -> Any:
            return self._result

    callback = ConcreteCallback("test-123")
    assert callback.callback_id == "test-123"
    assert await callback.result() is None
