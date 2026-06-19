"""Tests for map operation."""

import asyncio
import importlib
import json
from collections.abc import Mapping
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import pytest

# Mock the executor.execute method
from async_durable_execution.models import (
    BatchItem,
    BatchItemStatus,
    BatchResult,
    CompletionReason,
    ContextDetails,
    Executable,
    Operation,
    OperationStatus,
    OperationType,
)
from async_durable_execution.config import (
    CompletionConfig,
    ItemBatcher,
    MapConfig,
    NestingType,
)
from async_durable_execution.context import (
    get_current_context,
    reset_current_context,
    set_current_context,
)
from async_durable_execution import map as map_operation, DurableContext
from async_durable_execution.models import OperationIdentifier
from async_durable_execution.models import OperationSubType
from async_durable_execution.operation import child  # PLC0415
from async_durable_execution.operation.map import MapExecutor, map_handler
from async_durable_execution.serdes import serialize
from async_durable_execution.state import ExecutionState

from ..serdes_test import CustomStrSerDes


async def _invoke_maybe_async(func, *args, **kwargs):
    return await func(*args, **kwargs)


def _mock_call_kwargs_by_operation_id(
    mock: Mock,
) -> dict[str, Mapping[str, Any]]:
    return {call.kwargs["operation_id"]: call.kwargs for call in mock.call_args_list}


def create_test_context(
    state: ExecutionState | None = None, parent_id: str | None = None
) -> DurableContext:
    """Helper to create DurableContext for tests."""
    if state is None:
        state = Mock(spec=ExecutionState)
        state.durable_execution_arn = (
            "arn:aws:durable:us-east-1:123456789012:execution/test"
        )

    return child.DurableContext(
        execution_state=state,
        operation_identifier=OperationIdentifier(
            operation_id=None,
            sub_type=OperationSubType.EXECUTION,
            parent_id=parent_id,
        ),
    )


def create_mock_execution_state():
    state = Mock(spec=ExecutionState)
    state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    state._create_checkpoint_async = AsyncMock()
    state.wrap_user_function = lambda func, *args, **kwargs: (
        lambda *a, **kw: _invoke_maybe_async(func, *a, **kw)
    )
    return state


def create_mock_child_context(state):
    child_context = Mock()
    child_context.state = state
    child_context.execution_state = state
    return child_context


async def run_with_context(context: DurableContext, awaitable):
    token = set_current_context(context)
    try:
        return await awaitable
    finally:
        reset_current_context(token)


async def test_map_executor_init():
    """Test MapExecutor initialization."""
    executables = [Executable(index=0, func=lambda: None)]
    items = ["item1"]

    executor = MapExecutor(
        executables=executables,
        items=items,
        max_concurrency=2,
        completion_config=CompletionConfig(),
        top_level_sub_type=OperationSubType.MAP,
        iteration_sub_type=OperationSubType.MAP_ITERATION,
        name_prefix="test-",
        serdes=None,
        nesting_type=NestingType.FLAT,
    )

    assert executor.items == items
    assert executor.executables == executables
    assert executor.nesting_type is NestingType.FLAT


async def test_map_executor_from_items():
    """Test MapExecutor.from_items class method."""
    items = ["a", "b", "c"]

    async def callable_func(item, idx, items):
        return item.upper()

    config = MapConfig(max_concurrency=3, nesting_type=NestingType.FLAT)

    executor = MapExecutor.from_items(items, callable_func, config)

    assert len(executor.executables) == 3
    assert executor.items == items
    assert all(exe.func == callable_func for exe in executor.executables)
    assert [exe.index for exe in executor.executables] == [0, 1, 2]
    assert executor.nesting_type is NestingType.FLAT


async def test_map_executor_from_items_default_config():
    """Test MapExecutor.from_items with default config."""
    items = ["x"]

    async def callable_func(item, idx, items):
        return item

    executor = MapExecutor.from_items(
        items,
        callable_func,
        MapConfig(),
    )

    assert len(executor.executables) == 1
    assert executor.items == items
    assert executor.nesting_type is NestingType.NESTED


@patch("async_durable_execution.operation.map.logger")
async def test_map_executor_execute_item(mock_logger):
    """Test MapExecutor.execute_item method with logging."""
    items = ["hello", "world"]

    async def callable_func(item, idx, items):
        return f"{item}_{idx}"

    executor = MapExecutor.from_items(
        items,
        callable_func,
        MapConfig(),
    )
    executable = executor.executables[0]

    result = await executor.execute_item(None, executable)

    assert result == "hello_0"
    assert mock_logger.debug.call_count == 2
    mock_logger.debug.assert_any_call("🗺️ Processing map item: %s", 0)
    mock_logger.debug.assert_any_call("✅ Processed map item: %s", 0)


async def test_map_executor_execute_item_with_context():
    """Test MapExecutor.execute_item with context usage."""
    items = [1, 2, 3]

    async def callable_func(item, idx, items):
        return item * 2 + idx

    executor = MapExecutor.from_items(
        items,
        callable_func,
        MapConfig(),
    )
    executable = executor.executables[1]

    result = await executor.execute_item("mock_context", executable)

    assert result == 5  # 2 * 2 + 1


async def test_map_executor_execute_item_with_async_callable():
    items = ["hello"]

    async def callable_func(item, idx, items):
        await asyncio.sleep(0)
        ctx = get_current_context()
        return f"{ctx}-{item}-{idx}-{len(items)}"

    executor = MapExecutor.from_items(
        items,
        callable_func,
        MapConfig(),
    )
    executable = executor.executables[0]

    result = await executor.execute_item("mock_context", executable)

    assert result == "mock_context-hello-0-1"


async def test_map_handler():
    """Test map_handler function."""
    items = ["a", "b"]

    async def callable_func(item, idx, items):
        return item.upper()

    async def mock_run_in_child_context(func, name, config):
        return await func("mock_context")

    # Create a minimal ExecutionState mock
    class MockExecutionState:
        def __init__(self):
            self.operations = Mock()
            self.operations.get.return_value = Mock(
                is_succeeded=Mock(return_value=False)
            )

    execution_state = MockExecutionState()
    config = MapConfig()
    operation_identifier = OperationIdentifier(
        "test_op", OperationSubType.MAP, "parent", "test_map"
    )

    result = await map_handler(
        items,
        callable_func,
        config,
        execution_state,
        mock_run_in_child_context,
        operation_identifier,
    )

    assert isinstance(result, BatchResult)


async def test_map_handler_with_none_config():
    """Test map_handler with None config creates default MapConfig."""
    items = ["test"]

    async def callable_func(item, idx, items):
        return item

    async def mock_run_in_child_context(func, name, config):
        return await func("mock_context")

    class MockExecutionState:
        def __init__(self):
            self.operations = Mock()
            self.operations.get.return_value = Mock(
                is_succeeded=Mock(return_value=False)
            )

    execution_state = MockExecutionState()
    operation_identifier = OperationIdentifier(
        "test_op", OperationSubType.MAP, "parent", "test_map"
    )

    # Since MapConfig() is called in map_handler when config is None,
    # we need to provide a valid config to avoid the NameError
    # This tests the behavior when config is provided instead
    result = await map_handler(
        items,
        callable_func,
        MapConfig(),
        execution_state,
        mock_run_in_child_context,
        operation_identifier,
    )

    assert isinstance(result, BatchResult)


async def test_map_executor_execute_item_accesses_all_parameters():
    """Test that execute_item passes all parameters correctly."""
    items = ["first", "second", "third"]

    async def callable_func(item, idx, items_list):
        # Verify all parameters are passed correctly
        ctx = get_current_context()
        assert ctx == "test_context"
        assert item in items_list
        assert idx < len(items_list)
        assert items_list == items
        return f"{item}_{idx}_{len(items_list)}"

    executor = MapExecutor.from_items(
        items,
        callable_func,
        MapConfig(),
    )
    executable = executor.executables[2]

    result = await executor.execute_item("test_context", executable)

    assert result == "third_2_3"


async def test_map_executor_from_items_empty_list():
    """Test MapExecutor.from_items with empty items list."""
    items = []

    async def callable_func(item, idx, items):
        return item

    executor = MapExecutor.from_items(
        items,
        callable_func,
        MapConfig(),
    )

    assert len(executor.executables) == 0
    assert executor.items == []


async def test_map_executor_from_items_single_item():
    """Test MapExecutor.from_items with single item."""
    items = ["only"]

    async def callable_func(item, idx, items):
        return f"processed_{item}"

    executor = MapExecutor.from_items(
        items,
        callable_func,
        MapConfig(),
    )

    assert len(executor.executables) == 1
    assert executor.executables[0].index == 0
    assert executor.items == items


async def test_map_executor_inheritance():
    """Test that MapExecutor properly inherits from ConcurrentExecutor."""
    items = ["test"]

    async def callable_func(item, idx, items):
        return item

    executor = MapExecutor.from_items(
        items,
        callable_func,
        MapConfig(),
    )

    # Verify it has inherited attributes from ConcurrentExecutor
    assert hasattr(executor, "executables")
    assert hasattr(executor, "execute")
    assert executor.items == items


async def test_map_handler_calls_executor_execute():
    """Test that map_handler calls executor.execute method."""
    items = ["test_item"]

    async def callable_func(item, idx, items):
        return f"result_{item}"

    mock_batch_result = BatchResult(
        all=[BatchItem(index=0, status=BatchItemStatus.SUCCEEDED, result="test")],
        completion_reason=CompletionReason.ALL_COMPLETED,
    )

    executor_context = Mock()
    executor_context._step_counter._create_step_id_for_logical_step = (  # noqa: SLF001
        lambda *args: "1"
    )
    executor_context.create_child_context = lambda *args, **kwargs: Mock()

    with patch.object(
        MapExecutor, "execute", return_value=mock_batch_result
    ) as mock_execute:

        class MockExecutionState:
            def __init__(self):
                self.operations = Mock()
                self.operations.get.return_value = Mock(
                    is_succeeded=Mock(return_value=False)
                )

        execution_state = MockExecutionState()
        config = MapConfig()
        operation_identifier = OperationIdentifier(
            "test_op", OperationSubType.MAP, "parent", "test_map"
        )

        result = await map_handler(
            items,
            callable_func,
            config,
            execution_state,
            executor_context,
            operation_identifier,
        )

        # Verify execute was called
        mock_execute.assert_called_once_with(
            execution_state, executor_context=executor_context
        )
        assert result == mock_batch_result


async def test_map_handler_with_none_config_creates_default():
    """Test that map_handler creates default MapConfig when config is None."""
    items = ["test"]

    async def callable_func(item, idx, items):
        return item

    # Mock MapExecutor.from_items to verify it's called with default config
    with patch.object(MapExecutor, "from_items") as mock_from_items:
        mock_executor = Mock()
        mock_batch_result = BatchResult(
            all=[BatchItem(index=0, status=BatchItemStatus.SUCCEEDED, result="test")],
            completion_reason=CompletionReason.ALL_COMPLETED,
        )
        mock_executor.execute = AsyncMock(return_value=mock_batch_result)
        mock_from_items.return_value = mock_executor

        executor_context = Mock()
        executor_context._step_counter._create_step_id_for_logical_step = (  # noqa: SLF001
            lambda *args: "1"
        )
        executor_context.create_child_context = lambda *args, **kwargs: Mock()

        class MockExecutionState:
            def __init__(self):
                self.operations = Mock()
                self.operations.get.return_value = Mock(
                    is_succeeded=Mock(return_value=False)
                )

        execution_state = MockExecutionState()
        operation_identifier = OperationIdentifier(
            "test_op", OperationSubType.MAP, "parent", "test_map"
        )

        result = await map_handler(
            items,
            callable_func,
            None,
            execution_state,
            executor_context,
            operation_identifier,
        )

        # Verify from_items was called with a MapConfig instance
        mock_from_items.assert_called_once()
        call_args = mock_from_items.call_args
        # Check that the call was made with keyword arguments
        if call_args.args:
            assert call_args.args[0] == items
            assert call_args.args[1] == callable_func
            assert isinstance(call_args.args[2], MapConfig)
        else:
            # Called with keyword arguments
            assert call_args.kwargs["items"] == items
            assert call_args.kwargs["func"] == callable_func
            assert isinstance(call_args.kwargs["config"], MapConfig)

        assert result == mock_batch_result


async def test_map_handler_with_serdes():
    """Test that map_handler with serdes"""
    items = ["test_item"]

    async def callable_func(item, idx, items):
        return f"RESULT_{item.upper()}"

    executor_context = Mock()
    executor_context.step_counter = Mock()
    executor_context._step_counter._create_step_id_for_logical_step = (  # noqa: SLF001
        lambda *args: "1"
    )
    executor_context.step_counter._create_step_id_for_logical_step = (  # noqa: SLF001
        lambda *args: "1"
    )
    execution_state = create_mock_execution_state()
    execution_state.operations.get.return_value = Mock(
        is_succeeded=Mock(return_value=False),
        is_failed=Mock(return_value=False),
        is_existent=Mock(return_value=False),
        is_replay_children=Mock(return_value=False),
    )
    child_context = create_mock_child_context(execution_state)
    executor_context.create_child_context = lambda *args, **kwargs: child_context
    config = MapConfig(serdes=CustomStrSerDes())
    operation_identifier = OperationIdentifier(
        "test_op", OperationSubType.MAP, "parent", "test_map"
    )

    result = await map_handler(
        items,
        callable_func,
        config,
        execution_state,
        executor_context,
        operation_identifier,
    )

    # Verify execute was called
    assert result.all[0].result == "RESULT_TEST_ITEM"


async def test_map_handler_with_summary_generator():
    """Test that map_handler calls executor_context methods correctly."""
    items = ["item1", "item2"]

    async def callable_func(item, idx, items):
        return f"large_result_{item}" * 1000  # Create a large result

    def mock_summary_generator(result):
        return f"Summary of {len(result)} chars for map item"

    config = MapConfig(summary_generator=mock_summary_generator)

    executor_context = Mock()
    executor_context.step_counter = Mock()
    executor_context._step_counter._create_step_id_for_logical_step = Mock(  # noqa: SLF001
        side_effect=["1", "2"]
    )
    executor_context.step_counter._create_step_id_for_logical_step = Mock(
        side_effect=["1", "2"]
    )
    execution_state = create_mock_execution_state()
    execution_state.operations.get.return_value = Mock(
        is_succeeded=Mock(return_value=False),
        is_failed=Mock(return_value=False),
        is_existent=Mock(return_value=False),
        is_replay_children=Mock(return_value=False),
    )
    executor_context.create_child_context = Mock(
        return_value=create_mock_child_context(execution_state)
    )
    operation_identifier = OperationIdentifier(
        "test_op", OperationSubType.MAP, "parent", "test_map"
    )

    # Call map_handler
    await map_handler(
        items,
        callable_func,
        config,
        execution_state,
        executor_context,
        operation_identifier,
    )

    # Verify that create_child_context was called twice (N=2 items)
    assert executor_context.create_child_context.call_count == 2

    # Verify that _create_step_id_for_logical_step was called twice with unique values
    assert (
        executor_context.step_counter._create_step_id_for_logical_step.call_count == 2
    )
    calls = (
        executor_context.step_counter._create_step_id_for_logical_step.call_args_list
    )
    # Verify unique values were passed
    assert calls[0] != calls[1]


async def test_map_executor_from_items_with_summary_generator():
    """Test MapExecutor.from_items preserves summary_generator."""
    items = ["item1"]

    async def callable_func(item, idx, items):
        return f"result_{item}"

    def mock_summary_generator(result):
        return f"Map summary: {result}"

    config = MapConfig(summary_generator=mock_summary_generator)

    executor = MapExecutor.from_items(items, callable_func, config)

    # Verify that the summary_generator is preserved in the executor
    assert executor.summary_generator is mock_summary_generator


async def test_map_handler_default_summary_generator():
    """Test that map_handler calls executor_context methods correctly with default config."""
    items = ["item1"]

    async def callable_func(item, idx, items):
        return f"result_{item}"

    executor_context = Mock()
    executor_context.step_counter = Mock()
    executor_context._step_counter._create_step_id_for_logical_step = Mock(  # noqa: SLF001
        return_value="1"
    )
    executor_context.step_counter._create_step_id_for_logical_step = Mock(
        return_value="1"
    )
    execution_state = create_mock_execution_state()
    execution_state.operations.get.return_value = Mock(
        is_succeeded=Mock(return_value=False),
        is_failed=Mock(return_value=False),
        is_existent=Mock(return_value=False),
        is_replay_children=Mock(return_value=False),
    )
    executor_context.create_child_context = Mock(
        return_value=create_mock_child_context(execution_state)
    )  # SLF001
    operation_identifier = OperationIdentifier(
        "test_op", OperationSubType.MAP, "parent", "test_map"
    )

    # Call map_handler with None config (should use default)
    await map_handler(
        items,
        callable_func,
        None,
        execution_state,
        executor_context,
        operation_identifier,
    )

    # Verify that create_child_context was called once (N=1 item)
    assert executor_context.create_child_context.call_count == 1

    # Verify that _create_step_id_for_logical_step was called once
    assert (
        executor_context.step_counter._create_step_id_for_logical_step.call_count == 1
    )


async def test_map_executor_init_with_summary_generator():
    """Test MapExecutor initialization with summary_generator."""
    items = ["item1"]
    executables = [Executable(index=0, func=lambda: None)]

    def mock_summary_generator(result):
        return f"Summary: {result}"

    executor = MapExecutor(
        executables=executables,
        items=items,
        max_concurrency=2,
        completion_config=CompletionConfig(),
        top_level_sub_type=OperationSubType.MAP,
        iteration_sub_type=OperationSubType.MAP_ITERATION,
        name_prefix="test-",
        serdes=None,
        summary_generator=mock_summary_generator,
    )

    assert executor.summary_generator is mock_summary_generator
    assert executor.items == items
    assert executor.executables == executables


async def test_map_handler_with_explicit_none_summary_generator():
    """Test that map_handler calls executor_context methods correctly with explicit None summary_generator."""

    async def func(item, index, array):
        return f"processed_{item}"

    items = ["item1", "item2", "item3"]
    # Explicitly set summary_generator to None
    config = MapConfig(summary_generator=None)

    class MockExecutionState:
        def __init__(self):
            self.operations = Mock()
            self.operations.get.return_value = Mock(
                is_succeeded=Mock(return_value=False)
            )

    execution_state = MockExecutionState()
    operation_identifier = OperationIdentifier(
        "test_op", OperationSubType.MAP, "parent", "test_map"
    )

    executor_context = Mock()
    executor_context.step_counter = Mock()
    executor_context._step_counter._create_step_id_for_logical_step = Mock(  # noqa: SLF001
        side_effect=["1", "2", "3"]
    )
    executor_context.step_counter._create_step_id_for_logical_step = Mock(
        side_effect=["1", "2", "3"]
    )
    execution_state = create_mock_execution_state()
    execution_state.operations.get.return_value = Mock(
        is_succeeded=Mock(return_value=False),
        is_failed=Mock(return_value=False),
        is_existent=Mock(return_value=False),
        is_replay_children=Mock(return_value=False),
    )
    executor_context.create_child_context = Mock(
        return_value=create_mock_child_context(execution_state)
    )
    # Call map_handler
    await map_handler(
        items=items,
        func=func,
        config=config,
        execution_state=execution_state,
        map_context=executor_context,
        operation_identifier=operation_identifier,
    )

    # Verify that create_child_context was called 3 times (N=3 items)
    assert executor_context.create_child_context.call_count == 3


async def test_map_handler_replay_mechanism():
    """Test that map_handler uses replay when operation has already succeeded."""
    items = ["item1", "item2"]

    async def callable_func(item, idx, items):
        return f"result_{item}"

    # Mock execution state that indicates operation already succeeded
    class MockExecutionState:
        durable_execution_arn = "arn:aws:durable:us-east-1:123456789012:execution/test"

        def __init__(self):
            self.operations = Mock()

            def _get(operation_id):
                mock_result = Mock()
                mock_result.is_succeeded.return_value = True
                mock_result.is_replay_children.return_value = False
                # Provide properly serialized JSON data
                mock_result.result = f'"cached_result_{operation_id}"'  # JSON string
                return mock_result

            self.operations.get.side_effect = _get

    execution_state = MockExecutionState()
    config = MapConfig()
    operation_identifier = OperationIdentifier(
        "test_op", OperationSubType.MAP, "parent", "test_map"
    )

    # Mock map context
    map_context = Mock()
    map_context._step_counter._create_step_id_for_logical_step = Mock(  # noqa: SLF001
        side_effect=["child_1", "child_2"]
    )

    # Mock the executor's replay method
    with patch.object(MapExecutor, "replay") as mock_replay:
        expected_batch_result = BatchResult(
            all=[
                BatchItem(
                    index=0,
                    status=BatchItemStatus.SUCCEEDED,
                    result="cached_result_child_1",
                ),
                BatchItem(
                    index=1,
                    status=BatchItemStatus.SUCCEEDED,
                    result="cached_result_child_2",
                ),
            ],
            completion_reason=CompletionReason.ALL_COMPLETED,
        )
        mock_replay.return_value = expected_batch_result

        result = await map_handler(
            items,
            callable_func,
            config,
            execution_state,
            map_context,
            operation_identifier,
        )

        # Verify replay was called instead of execute
        mock_replay.assert_called_once_with(execution_state, map_context)
        assert result == expected_batch_result


async def test_map_handler_replay_with_replay_children():
    """Test map_handler replay when children need to be re-executed."""
    items = ["item1"]

    async def callable_func(item, idx, items):
        return f"result_{item}"

    # Mock execution state that indicates operation succeeded but children need replay
    class MockExecutionState:
        def __init__(self):
            self.operations = Mock()

            def _get(operation_id):
                mock_result = Mock()
                if operation_id == "test_op":
                    mock_result.is_succeeded.return_value = True
                else:  # child operations
                    mock_result.is_succeeded.return_value = True
                    mock_result.is_replay_children.return_value = True
                return mock_result

            self.operations.get.side_effect = _get

    execution_state = MockExecutionState()
    config = MapConfig()
    operation_identifier = OperationIdentifier(
        "test_op", OperationSubType.MAP, "parent", "test_map"
    )

    # Mock map context
    map_context = Mock()
    map_context._step_counter._create_step_id_for_logical_step = Mock(  # noqa: SLF001
        return_value="child_1"
    )

    # Mock the executor's replay method and _execute_item_in_child_context
    with (
        patch.object(MapExecutor, "replay") as mock_replay,
        patch.object(
            MapExecutor, "_execute_item_in_child_context"
        ) as mock_execute_item,
    ):
        mock_execute_item.return_value = "re_executed_result"
        expected_batch_result = BatchResult(
            all=[
                BatchItem(
                    index=0,
                    status=BatchItemStatus.SUCCEEDED,
                    result="re_executed_result",
                )
            ],
            completion_reason=CompletionReason.ALL_COMPLETED,
        )
        mock_replay.return_value = expected_batch_result

        result = await map_handler(
            items,
            callable_func,
            config,
            execution_state,
            map_context,
            operation_identifier,
        )

        mock_replay.assert_called_once_with(execution_state, map_context)
        assert result == expected_batch_result


async def test_map_config_with_explicit_none_summary_generator():
    """Test MapConfig with explicitly set None summary_generator."""
    config = MapConfig(summary_generator=None)

    assert config.summary_generator is None
    assert config.max_concurrency is None
    assert isinstance(config.item_batcher, ItemBatcher)
    assert isinstance(config.completion_config, CompletionConfig)
    assert config.serdes is None


async def test_map_config_default_summary_generator_behavior():
    """Test MapConfig() with no summary_generator should result in empty string behavior."""
    # When creating MapConfig() with no summary_generator specified
    config = MapConfig()

    # The summary_generator should be None by default
    assert config.summary_generator is None

    # But when used in the actual child.py logic, it should result in empty string
    # This matches child.py: config.summary_generator(raw_result) if config.summary_generator else ""
    test_result = (
        config.summary_generator("test_data") if config.summary_generator else ""
    )
    assert test_result == ""  # noqa PLC1901


async def test_map_handler_first_execution_then_replay_integration():
    """Test map_handler called twice - first calls execute, second calls replay."""

    async def test_func(item, idx, items):
        return f"processed_{item}"

    items = ["a", "b"]
    config = MapConfig()
    operation_identifier = OperationIdentifier(
        "test_op", OperationSubType.MAP, "parent", "test_map"
    )

    # Track whether we're in first or second execution
    execution_count = 0

    class MockExecutionState:
        durable_execution_arn = "arn:aws:durable:us-east-1:123456789012:execution/test"

        def __init__(self):
            self.operations = Mock()

            def _get(operation_id):
                nonlocal execution_count
                mock_result = Mock()

                if operation_id == "test_op":
                    # Main operation checkpoint
                    if execution_count == 0:
                        # First execution - operation not succeeded yet
                        mock_result.is_succeeded.return_value = False
                    else:
                        # Second execution - operation succeeded, trigger replay
                        mock_result.is_succeeded.return_value = True

                return mock_result

            self.operations.get.side_effect = _get

    execution_state = MockExecutionState()
    map_context = Mock()

    with (
        patch(
            "async_durable_execution.operation.map.MapExecutor.execute"
        ) as mock_execute,
        patch(
            "async_durable_execution.operation.map.MapExecutor.replay"
        ) as mock_replay,
    ):
        mock_execute.return_value = Mock()  # Mock BatchResult
        mock_replay.return_value = Mock()  # Mock BatchResult

        # FIRST EXECUTION - should call execute
        execution_count = 0
        await map_handler(
            items, test_func, config, execution_state, map_context, operation_identifier
        )

        # Verify execute was called, replay was not
        mock_execute.assert_called_once()
        mock_replay.assert_not_called()

        # Reset mocks for second call
        mock_execute.reset_mock()
        mock_replay.reset_mock()

        # SECOND EXECUTION - should call replay
        execution_count = 1
        await map_handler(
            items, test_func, config, execution_state, map_context, operation_identifier
        )

        # Verify replay was called, execute was not
        mock_replay.assert_called_once()
        mock_execute.assert_not_called()


@pytest.mark.parametrize(
    ("item_serdes", "batch_serdes"),
    [
        (Mock(), Mock()),
        (None, Mock()),
        (Mock(), None),
    ],
)
@patch("async_durable_execution.operation.child.serialize")
async def test_map_item_serialize(mock_serialize, item_serdes, batch_serdes):
    """Test map serializes items with item_serdes or fallback."""
    mock_serialize.return_value = '"serialized"'

    parent_checkpoint = child.CheckpointedResult.create_from_operation(
        Operation(
            operation_id="parent",
            operation_type=OperationType.CONTEXT,
            status=OperationStatus.STARTED,
        )
    )

    def child_checkpoint_for(op_id: str):
        return child.CheckpointedResult.create_from_operation(
            Operation(
                operation_id=op_id,
                operation_type=OperationType.CONTEXT,
                status=OperationStatus.STARTED,
            )
        )

    def get_checkpoint(op_id):
        return (
            child_checkpoint_for(op_id)
            if op_id.startswith("child-")
            else parent_checkpoint
        )

    mock_state = Mock()
    mock_state.durable_execution_arn = "arn:test"
    mock_state.operations = Mock()
    mock_state.operations.get = Mock(side_effect=get_checkpoint)
    mock_state._create_checkpoint_async = AsyncMock()
    mock_state.wrap_user_function = lambda func, *args, **kwargs: (
        lambda *a, **kw: _invoke_maybe_async(func, *a, **kw)
    )

    context_map = {}

    def create_id(self, i):
        ctx_id = id(self)
        if ctx_id not in context_map:
            context_map[ctx_id] = []
        context_map[ctx_id].append(i)
        return (
            "parent"
            if len(context_map) == 1 and len(context_map[ctx_id]) == 1
            else f"child-{i}"
        )

    with patch.object(
        child.OperationIdGenerator, "_create_step_id_for_logical_step", create_id
    ):
        context = create_test_context(state=mock_state)

        async def map_item(item, idx, items):
            return item

        await run_with_context(
            context,
            map_operation(
                ["a", "b"],
                map_item,
                config=MapConfig(serdes=batch_serdes, item_serdes=item_serdes),
            ),
        )

    expected = item_serdes or batch_serdes
    call_kwargs = _mock_call_kwargs_by_operation_id(mock_serialize)

    assert call_kwargs["child-0"]["serdes"] is expected
    assert call_kwargs["child-1"]["serdes"] is expected
    assert call_kwargs["parent"]["serdes"] is batch_serdes


@pytest.mark.parametrize(
    ("item_serdes", "batch_serdes"),
    [
        (Mock(), Mock()),
        (None, Mock()),
        (Mock(), None),
    ],
)
@patch("async_durable_execution.operation.child.deserialize")
async def test_map_item_deserialize(mock_deserialize, item_serdes, batch_serdes):
    """Test map deserializes items with item_serdes or fallback."""
    mock_deserialize.return_value = "deserialized"
    if batch_serdes is not None:
        batch_serdes.serialize.return_value = '"serialized"'

    parent_checkpoint = Mock()
    parent_checkpoint.is_succeeded.return_value = False
    parent_checkpoint.is_failed.return_value = False
    parent_checkpoint.is_existent.return_value = False

    def child_checkpoint_for(op_id: str):
        return child.CheckpointedResult.create_from_operation(
            Operation(
                operation_id=op_id,
                operation_type=OperationType.CONTEXT,
                status=OperationStatus.SUCCEEDED,
                context_details=ContextDetails(result='"cached"'),
            )
        )

    def get_checkpoint(op_id):
        return (
            child_checkpoint_for(op_id)
            if op_id.startswith("child-")
            else parent_checkpoint
        )

    mock_state = Mock()
    mock_state.durable_execution_arn = "arn:test"
    mock_state.operations = Mock()
    mock_state.operations.get = Mock(side_effect=get_checkpoint)
    mock_state._create_checkpoint_async = AsyncMock()
    mock_state.wrap_user_function = lambda func, *args, **kwargs: (
        lambda *a, **kw: _invoke_maybe_async(func, *a, **kw)
    )

    context_map = {}

    def create_id(self, i):
        ctx_id = id(self)
        if ctx_id not in context_map:
            context_map[ctx_id] = []
        context_map[ctx_id].append(i)
        return (
            "parent"
            if len(context_map) == 1 and len(context_map[ctx_id]) == 1
            else f"child-{i}"
        )

    with patch.object(
        child.OperationIdGenerator, "_create_step_id_for_logical_step", create_id
    ):
        context = create_test_context(state=mock_state)

        async def map_item(item, idx, items):
            return item

        await run_with_context(
            context,
            map_operation(
                ["a", "b"],
                map_item,
                config=MapConfig(serdes=batch_serdes, item_serdes=item_serdes),
            ),
        )

    expected = item_serdes or batch_serdes
    call_kwargs = _mock_call_kwargs_by_operation_id(mock_deserialize)

    assert call_kwargs["child-0"]["serdes"] is expected
    assert call_kwargs["child-1"]["serdes"] is expected


async def test_map_result_serialization_roundtrip():
    """Test that map operation BatchResult can be serialized and deserialized."""

    items = ["a", "b", "c"]

    async def func(item, idx, items):
        return {"item": item.upper(), "index": idx}

    execution_state = create_mock_execution_state()
    execution_state.durable_execution_arn = "arn:test"
    execution_state.operations.get.return_value = Mock(
        is_succeeded=Mock(return_value=False),
        is_failed=Mock(return_value=False),
        is_existent=Mock(return_value=False),
        is_replay_children=Mock(return_value=False),
    )
    map_context = Mock()
    map_context.step_counter = Mock()
    map_context._step_counter._create_step_id_for_logical_step = Mock(  # noqa: SLF001
        side_effect=["1", "2", "3"]
    )
    map_context.step_counter._create_step_id_for_logical_step = Mock(
        side_effect=["1", "2", "3"]
    )
    child_context = create_mock_child_context(execution_state)
    map_context.create_child_context = Mock(return_value=child_context)
    operation_identifier = OperationIdentifier(
        "test_op", OperationSubType.MAP, "parent", "test_map"
    )

    # Execute map
    result = await map_handler(
        items, func, MapConfig(), execution_state, map_context, operation_identifier
    )

    # Serialize the BatchResult
    serialized = json.dumps(result.to_dict())

    # Deserialize
    deserialized = BatchResult.from_dict(json.loads(serialized))

    # Verify all data preserved
    assert len(deserialized.all) == 3
    assert deserialized.all[0].result == {"item": "A", "index": 0}
    assert deserialized.all[1].result == {"item": "B", "index": 1}
    assert deserialized.all[2].result == {"item": "C", "index": 2}
    assert deserialized.completion_reason == result.completion_reason
    assert all(item.status == BatchItemStatus.SUCCEEDED for item in deserialized.all)


async def test_map_handler_serializes_batch_result():
    """Verify map_handler serializes BatchResult at parent level."""
    try:
        with patch("async_durable_execution.serdes.serialize") as mock_serdes_serialize:
            mock_serdes_serialize.return_value = '"serialized"'
            importlib.reload(child)

            parent_checkpoint = Mock()
            parent_checkpoint.is_succeeded.return_value = False
            parent_checkpoint.is_failed.return_value = False
            parent_checkpoint.is_existent.return_value = False
            parent_checkpoint.is_replay_children.return_value = False

            child_checkpoint = Mock()
            child_checkpoint.is_succeeded.return_value = False
            child_checkpoint.is_failed.return_value = False
            child_checkpoint.is_existent.return_value = False
            child_checkpoint.is_replay_children.return_value = False

            def get_checkpoint(op_id):
                return (
                    child_checkpoint
                    if op_id.startswith("child-")
                    else parent_checkpoint
                )

            mock_state = Mock()
            mock_state.durable_execution_arn = "arn:test"
            mock_state.operations = Mock()
            mock_state.operations.get = Mock(side_effect=get_checkpoint)
            mock_state._create_checkpoint_async = AsyncMock()
            mock_state.wrap_user_function = lambda func, *args, **kwargs: (
                lambda *a, **kw: _invoke_maybe_async(func, *a, **kw)
            )

            context_map = {}

            def create_id(self, i):
                ctx_id = id(self)
                if ctx_id not in context_map:
                    context_map[ctx_id] = []
                context_map[ctx_id].append(i)
                return (
                    "parent"
                    if len(context_map) == 1 and len(context_map[ctx_id]) == 1
                    else f"child-{i}"
                )

            with patch.object(
                child.OperationIdGenerator,
                "_create_step_id_for_logical_step",
                create_id,
            ):
                context = create_test_context(state=mock_state)

                async def map_item(item, idx, items):
                    return item

                result = await run_with_context(
                    context, map_operation(["a", "b"], map_item)
                )

            assert len(mock_serdes_serialize.call_args_list) == 3
            parent_call = mock_serdes_serialize.call_args_list[2]
            assert parent_call[1]["value"] is result
    finally:
        importlib.reload(child)


async def test_map_default_serdes_serializes_batch_result():
    """Verify default serdes automatically serializes BatchResult."""
    try:
        with patch(
            "async_durable_execution.serdes.serialize", wraps=serialize
        ) as mock_serialize:
            importlib.reload(child)

            parent_checkpoint = Mock()
            parent_checkpoint.is_succeeded.return_value = False
            parent_checkpoint.is_failed.return_value = False
            parent_checkpoint.is_existent.return_value = False
            parent_checkpoint.is_replay_children.return_value = False

            child_checkpoint = Mock()
            child_checkpoint.is_succeeded.return_value = False
            child_checkpoint.is_failed.return_value = False
            child_checkpoint.is_existent.return_value = False
            child_checkpoint.is_replay_children.return_value = False

            def get_checkpoint(op_id):
                return (
                    child_checkpoint
                    if op_id.startswith("child-")
                    else parent_checkpoint
                )

            mock_state = Mock()
            mock_state.durable_execution_arn = "arn:test"
            mock_state.operations = Mock()
            mock_state.operations.get = Mock(side_effect=get_checkpoint)
            mock_state._create_checkpoint_async = AsyncMock()
            mock_state.wrap_user_function = lambda func, *args, **kwargs: (
                lambda *a, **kw: _invoke_maybe_async(func, *a, **kw)
            )

            context_map = {}

            def create_id(self, i):
                ctx_id = id(self)
                if ctx_id not in context_map:
                    context_map[ctx_id] = []
                context_map[ctx_id].append(i)
                return (
                    "parent"
                    if len(context_map) == 1 and len(context_map[ctx_id]) == 1
                    else f"child-{i}"
                )

            with patch.object(
                child.OperationIdGenerator,
                "_create_step_id_for_logical_step",
                create_id,
            ):
                context = create_test_context(state=mock_state)

                async def map_item(item, idx, items):
                    return item

                result = await run_with_context(
                    context, map_operation(["a", "b"], map_item)
                )

            assert isinstance(result, BatchResult)
            assert len(mock_serialize.call_args_list) == 3
            parent_call = mock_serialize.call_args_list[2]
            assert parent_call[1]["serdes"] is None
            assert isinstance(parent_call[1]["value"], BatchResult)
            assert parent_call[1]["value"] is result
    finally:
        importlib.reload(child)


async def test_map_custom_serdes_serializes_batch_result():
    """Verify custom serdes is used for BatchResult serialization."""

    custom_serdes = CustomStrSerDes()

    try:
        with patch("async_durable_execution.serdes.serialize") as mock_serialize:
            mock_serialize.return_value = '"serialized"'
            importlib.reload(child)

            parent_checkpoint = Mock()
            parent_checkpoint.is_succeeded.return_value = False
            parent_checkpoint.is_failed.return_value = False
            parent_checkpoint.is_existent.return_value = False
            parent_checkpoint.is_replay_children.return_value = False

            child_checkpoint = Mock()
            child_checkpoint.is_succeeded.return_value = False
            child_checkpoint.is_failed.return_value = False
            child_checkpoint.is_existent.return_value = False
            child_checkpoint.is_replay_children.return_value = False

            def get_checkpoint(op_id):
                return (
                    child_checkpoint
                    if op_id.startswith("child-")
                    else parent_checkpoint
                )

            mock_state = Mock()
            mock_state.durable_execution_arn = "arn:test"
            mock_state.operations = Mock()
            mock_state.operations.get = Mock(side_effect=get_checkpoint)
            mock_state._create_checkpoint_async = AsyncMock()
            mock_state.wrap_user_function = lambda func, *args, **kwargs: (
                lambda *a, **kw: _invoke_maybe_async(func, *a, **kw)
            )

            context_map = {}

            def create_id(self, i):
                ctx_id = id(self)
                if ctx_id not in context_map:
                    context_map[ctx_id] = []
                context_map[ctx_id].append(i)
                return (
                    "parent"
                    if len(context_map) == 1 and len(context_map[ctx_id]) == 1
                    else f"child-{i}"
                )

            with patch.object(
                child.OperationIdGenerator,
                "_create_step_id_for_logical_step",
                create_id,
            ):
                context = create_test_context(state=mock_state)

                async def map_item(item, idx, items):
                    return item

                result = await run_with_context(
                    context,
                    map_operation(
                        ["a", "b"],
                        map_item,
                        config=MapConfig(serdes=custom_serdes),
                    ),
                )

            assert isinstance(result, BatchResult)
            assert len(mock_serialize.call_args_list) == 3
            parent_call = mock_serialize.call_args_list[2]
            assert parent_call[1]["serdes"] is custom_serdes
            assert isinstance(parent_call[1]["value"], BatchResult)
            assert parent_call[1]["value"] is result
    finally:
        importlib.reload(child)


async def test_map_with_empty_list_should_exit_early():
    """Test that map with empty list completes without crashing."""
    items = []

    async def map_func(item, idx, items):
        return f"processed_{item}"

    mock_state = Mock()
    mock_state.durable_execution_arn = "arn:test"

    parent_checkpoint = Mock()
    parent_checkpoint.is_succeeded.return_value = False
    parent_checkpoint.is_failed.return_value = False
    parent_checkpoint.is_existent.return_value = False

    mock_state.operations = Mock()
    mock_state.operations.get = Mock(return_value=parent_checkpoint)
    mock_state._create_checkpoint_async = AsyncMock()
    mock_state.wrap_user_function = lambda func, *args, **kwargs: (
        lambda *a, **kw: _invoke_maybe_async(func, *a, **kw)
    )

    context = create_test_context(state=mock_state)

    # This should complete immediately without crashing
    result = await run_with_context(
        context,
        map_operation(
            items,
            map_func,
            name="EmptyMap",
        ),
    )

    # Should return empty BatchResult
    assert isinstance(result, BatchResult)
    assert len(result.all) == 0
    assert result.total_count == 0
    assert result.success_count == 0
    assert result.failure_count == 0


async def test_map_executor_get_iteration_name_default():
    """Without item_namer, iterations use default 'map-item-{index}' naming."""
    items = ["a", "b", "c"]
    config = MapConfig(max_concurrency=2)

    executor = MapExecutor.from_items(
        items=items,
        func=lambda item, idx, items: item,
        config=config,
    )

    assert executor.get_iteration_name(0) == "map-item-0"
    assert executor.get_iteration_name(1) == "map-item-1"
    assert executor.get_iteration_name(2) == "map-item-2"


async def test_map_executor_get_iteration_name_with_item_namer():
    """With item_namer, iterations use custom names."""
    items = [{"id": "order-1"}, {"id": "order-2"}, {"id": "order-3"}]
    config = MapConfig(
        max_concurrency=2,
        item_namer=lambda item, index: f"process-{item['id']}",
    )

    executor = MapExecutor.from_items(
        items=items,
        func=lambda item, idx, items: item,
        config=config,
    )

    assert executor.get_iteration_name(0) == "process-order-1"
    assert executor.get_iteration_name(1) == "process-order-2"
    assert executor.get_iteration_name(2) == "process-order-3"


async def test_map_executor_item_namer_receives_item_and_index():
    """item_namer receives both the item and its index."""
    items = ["alpha", "beta", "gamma"]
    received_args: list[tuple] = []

    def namer(item, index):
        received_args.append((item, index))
        return f"item-{index}-{item}"

    config = MapConfig(item_namer=namer)

    executor = MapExecutor.from_items(
        items=items,
        func=lambda item, idx, items: item,
        config=config,
    )

    executor.get_iteration_name(0)
    executor.get_iteration_name(2)

    assert received_args == [("alpha", 0), ("gamma", 2)]


async def test_map_executor_item_namer_uses_index():
    """item_namer can use the index to generate names."""
    items = [10, 20, 30]
    config = MapConfig(item_namer=lambda item, index: f"step-{index + 1}")

    executor = MapExecutor.from_items(
        items=items,
        func=lambda item, idx, items: item,
        config=config,
    )

    assert executor.get_iteration_name(0) == "step-1"
    assert executor.get_iteration_name(1) == "step-2"
    assert executor.get_iteration_name(2) == "step-3"


async def test_map_executor_item_namer_none_falls_back_to_default():
    """Explicitly passing item_namer=None uses default naming."""
    items = ["x", "y"]
    config = MapConfig(item_namer=None)

    executor = MapExecutor.from_items(
        items=items,
        func=lambda item, idx, items: item,
        config=config,
    )

    assert executor.get_iteration_name(0) == "map-item-0"
    assert executor.get_iteration_name(1) == "map-item-1"


async def test_map_executor_from_items_passes_item_namer():
    """MapExecutor.from_items correctly passes item_namer from config."""
    namer = lambda item, index: f"custom-{index}"  # noqa: E731
    config = MapConfig(item_namer=namer)

    executor = MapExecutor.from_items(
        items=["a"],
        func=lambda item, idx, items: item,
        config=config,
    )

    assert executor._item_namer is namer


async def test_map_config_generic_with_item_namer():
    """MapConfig can be parameterized with a type and use item_namer."""
    config: MapConfig[dict] = MapConfig(
        item_namer=lambda item, index: f"item-{item['name']}",
    )

    assert config.item_namer is not None
    assert config.item_namer({"name": "test"}, 0) == "item-test"
