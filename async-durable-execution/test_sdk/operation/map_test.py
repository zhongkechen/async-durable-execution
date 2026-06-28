"""Tests for map operation."""

import asyncio
import importlib
import json
from collections.abc import Mapping
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import pytest

# Mock the executor.execute method
from async_durable_execution.composite.concurrency import (
    BatchItem,
    BatchItemStatus,
    BatchResult,
    CompletionReason,
    Executable,
)
from async_durable_execution.models import (
    ContextDetails,
    Operation,
    OperationStatus,
    OperationType,
)
from async_durable_execution.context import (
    get_current_context,
    reset_current_context,
    set_current_context,
)
from async_durable_execution import map as map_operation, DurableContext
from async_durable_execution.models import OperationIdentifier
from async_durable_execution.models import OperationSubType
from async_durable_execution.primitive import child  # PLC0415
from async_durable_execution.composite.concurrency import CompletionConfig, NestingType
from async_durable_execution.composite.map import (
    BatchedInput,
    MapExecutor,
    MapItemContext,
    map_handler,
)
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
    state.create_checkpoint = AsyncMock()
    return state


def create_mock_child_context(state):
    return child.DurableContext(
        execution_state=state,
        operation_identifier=OperationIdentifier(
            operation_id=None,
            sub_type=OperationSubType.EXECUTION,
            parent_id="test_parent",
        ),
        step_id_prefix="test_parent",
    )


def create_map_executor(**kwargs):
    execution_state = kwargs.pop("execution_state", None)
    if execution_state is None:
        execution_state = create_mock_execution_state()
    operation_identifier = kwargs.pop("operation_identifier", None)
    if operation_identifier is None:
        operation_identifier = OperationIdentifier(
            "test_op",
            OperationSubType.MAP,
            "parent",
            "test_map",
        )
    executor_context = kwargs.pop("executor_context", None)
    if executor_context is None:
        executor_context = create_test_context(execution_state)
    return MapExecutor(
        execution_state=execution_state,
        operation_identifier=operation_identifier,
        executor_context=executor_context,
        **kwargs,
    )


async def run_with_context(context: DurableContext, awaitable):
    token = set_current_context(context)
    try:
        return await awaitable
    finally:
        reset_current_context(token)


async def invoke_map_handler(*args, **kwargs):
    return await map_handler(*args, **kwargs)()


async def test_map_executor_init():
    """Test MapExecutor initialization."""
    executables = [Executable(index=0, func=lambda: None)]
    items = ["item1"]

    executor = create_map_executor(
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


def test_batched_input():
    """BatchedInput is owned by the map module."""
    batch_input = BatchedInput("batch", [1, 2, 3])

    assert batch_input.batch_input == "batch"
    assert batch_input.items == [1, 2, 3]


async def test_map_executor_init_from_items():
    """Test MapExecutor initialization with item executables."""
    items = ["a", "b", "c"]

    async def callable_func(item):
        return item.upper()

    executor = create_map_executor(
        executables=[
            Executable(index=i, func=callable_func) for i in range(len(items))
        ],
        items=items,
        max_concurrency=3,
        completion_config=CompletionConfig(),
        top_level_sub_type=OperationSubType.MAP,
        iteration_sub_type=OperationSubType.MAP_ITERATION,
        name_prefix="map-item-",
        serdes=None,
        nesting_type=NestingType.FLAT,
    )

    assert len(executor.executables) == 3
    assert executor.items == items
    assert all(exe.func == callable_func for exe in executor.executables)
    assert [exe.index for exe in executor.executables] == [0, 1, 2]
    assert executor.nesting_type is NestingType.FLAT


async def test_map_executor_init_default_config():
    """Test MapExecutor initialization with default map config."""
    items = ["x"]

    async def callable_func(item):
        return item

    executor = create_map_executor(
        executables=[
            Executable(index=i, func=callable_func) for i in range(len(items))
        ],
        items=items,
        max_concurrency=None,
        completion_config=CompletionConfig(),
        top_level_sub_type=OperationSubType.MAP,
        iteration_sub_type=OperationSubType.MAP_ITERATION,
        name_prefix="map-item-",
        serdes=None,
    )

    assert len(executor.executables) == 1
    assert executor.items == items
    assert executor.nesting_type is NestingType.NESTED


@patch("async_durable_execution.composite.map.logger")
async def test_map_executor_execute_item(mock_logger):
    """Test MapExecutor.execute_item method with logging."""
    items = ["hello", "world"]

    async def callable_func(item):
        ctx = get_current_context()
        return f"{item}_{ctx.index}"

    executor = create_map_executor(
        executables=[
            Executable(index=i, func=callable_func) for i in range(len(items))
        ],
        items=items,
        max_concurrency=None,
        completion_config=CompletionConfig(),
        top_level_sub_type=OperationSubType.MAP,
        iteration_sub_type=OperationSubType.MAP_ITERATION,
        name_prefix="map-item-",
        serdes=None,
    )
    executable = executor.executables[0]

    result = await executor.execute_item(create_test_context(), executable)

    assert result == "hello_0"
    assert mock_logger.debug.call_count == 2
    mock_logger.debug.assert_any_call("🗺️ Processing map item: %s", 0)
    mock_logger.debug.assert_any_call("✅ Processed map item: %s", 0)


async def test_map_executor_execute_item_with_context():
    """Test MapExecutor.execute_item with context usage."""
    items = [1, 2, 3]

    async def callable_func(item):
        ctx = get_current_context()
        return item * 2 + ctx.index

    executor = create_map_executor(
        executables=[
            Executable(index=i, func=callable_func) for i in range(len(items))
        ],
        items=items,
        max_concurrency=None,
        completion_config=CompletionConfig(),
        top_level_sub_type=OperationSubType.MAP,
        iteration_sub_type=OperationSubType.MAP_ITERATION,
        name_prefix="map-item-",
        serdes=None,
    )
    executable = executor.executables[1]

    result = await executor.execute_item(create_test_context(), executable)

    assert result == 5  # 2 * 2 + 1


async def test_map_executor_execute_item_with_async_callable():
    items = ["hello"]

    async def callable_func(item):
        await asyncio.sleep(0)
        ctx = get_current_context()
        return f"{item}-{ctx.index}-{len(ctx.items)}"

    executor = create_map_executor(
        executables=[
            Executable(index=i, func=callable_func) for i in range(len(items))
        ],
        items=items,
        max_concurrency=None,
        completion_config=CompletionConfig(),
        top_level_sub_type=OperationSubType.MAP,
        iteration_sub_type=OperationSubType.MAP_ITERATION,
        name_prefix="map-item-",
        serdes=None,
    )
    executable = executor.executables[0]

    result = await executor.execute_item(create_test_context(), executable)

    assert result == "hello-0-1"


async def test_map_handler():
    """Test map_handler function."""
    items = ["a", "b"]

    async def callable_func(item):
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
    operation_identifier = OperationIdentifier(
        "test_op", OperationSubType.MAP, "parent", "test_map"
    )

    result = await invoke_map_handler(
        items,
        callable_func,
        execution_state,
        mock_run_in_child_context,
        operation_identifier,
    )

    assert isinstance(result, BatchResult)


async def test_map_handler_with_defaults():
    """Test map_handler with default options."""
    items = ["test"]

    async def callable_func(item):
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

    result = await invoke_map_handler(
        items,
        callable_func,
        execution_state,
        mock_run_in_child_context,
        operation_identifier,
    )

    assert isinstance(result, BatchResult)


async def test_map_executor_execute_item_accesses_all_parameters():
    """Test that execute_item passes all parameters correctly."""
    items = ["first", "second", "third"]

    async def callable_func(item):
        # Verify all parameters are passed correctly
        ctx = get_current_context()
        assert isinstance(ctx, MapItemContext)
        assert item in ctx.items
        assert ctx.index < len(ctx.items)
        assert ctx.items == items
        return f"{item}_{ctx.index}_{len(ctx.items)}"

    executor = create_map_executor(
        executables=[
            Executable(index=i, func=callable_func) for i in range(len(items))
        ],
        items=items,
        max_concurrency=None,
        completion_config=CompletionConfig(),
        top_level_sub_type=OperationSubType.MAP,
        iteration_sub_type=OperationSubType.MAP_ITERATION,
        name_prefix="map-item-",
        serdes=None,
    )
    executable = executor.executables[2]

    result = await executor.execute_item(create_test_context(), executable)

    assert result == "third_2_3"


async def test_map_executor_init_empty_list():
    """Test MapExecutor initialization with empty items list."""
    items = []

    async def callable_func(item):
        return item

    executor = create_map_executor(
        executables=[
            Executable(index=i, func=callable_func) for i in range(len(items))
        ],
        items=items,
        max_concurrency=None,
        completion_config=CompletionConfig(),
        top_level_sub_type=OperationSubType.MAP,
        iteration_sub_type=OperationSubType.MAP_ITERATION,
        name_prefix="map-item-",
        serdes=None,
    )

    assert len(executor.executables) == 0
    assert executor.items == []


async def test_map_executor_init_single_item():
    """Test MapExecutor initialization with single item."""
    items = ["only"]

    async def callable_func(item):
        return f"processed_{item}"

    executor = create_map_executor(
        executables=[
            Executable(index=i, func=callable_func) for i in range(len(items))
        ],
        items=items,
        max_concurrency=None,
        completion_config=CompletionConfig(),
        top_level_sub_type=OperationSubType.MAP,
        iteration_sub_type=OperationSubType.MAP_ITERATION,
        name_prefix="map-item-",
        serdes=None,
    )

    assert len(executor.executables) == 1
    assert executor.executables[0].index == 0
    assert executor.items == items


async def test_map_executor_inheritance():
    """Test that MapExecutor properly inherits from ConcurrentExecutor."""
    items = ["test"]

    async def callable_func(item):
        return item

    executor = create_map_executor(
        executables=[
            Executable(index=i, func=callable_func) for i in range(len(items))
        ],
        items=items,
        max_concurrency=None,
        completion_config=CompletionConfig(),
        top_level_sub_type=OperationSubType.MAP,
        iteration_sub_type=OperationSubType.MAP_ITERATION,
        name_prefix="map-item-",
        serdes=None,
    )

    # Verify it has inherited attributes from ConcurrentExecutor
    assert hasattr(executor, "executables")
    assert hasattr(executor, "execute")
    assert executor.items == items


async def test_map_handler_calls_executor_execute():
    """Test that map_handler calls executor.execute method."""
    items = ["test_item"]

    async def callable_func(item):
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
        operation_identifier = OperationIdentifier(
            "test_op", OperationSubType.MAP, "parent", "test_map"
        )

        result = await invoke_map_handler(
            items,
            callable_func,
            execution_state,
            executor_context,
            operation_identifier,
        )

        # Verify execute was called
        mock_execute.assert_called_once_with()
        assert result == mock_batch_result


async def test_map_handler_passes_default_fields():
    """Test that map_handler passes default field values to MapExecutor."""
    items = ["test"]

    async def callable_func(item):
        return item

    # Mock MapExecutor to verify it's called with default fields.
    with patch(
        "async_durable_execution.composite.map.MapExecutor"
    ) as mock_executor_class:
        mock_executor = Mock()
        mock_batch_result = BatchResult(
            all=[BatchItem(index=0, status=BatchItemStatus.SUCCEEDED, result="test")],
            completion_reason=CompletionReason.ALL_COMPLETED,
        )
        mock_executor.process = AsyncMock(return_value=mock_batch_result)
        mock_executor_class.return_value = mock_executor

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

        result = await invoke_map_handler(
            items,
            callable_func,
            execution_state,
            executor_context,
            operation_identifier,
        )

        mock_executor_class.assert_called_once()
        call_args = mock_executor_class.call_args
        assert call_args.kwargs["items"] == items
        assert [exe.index for exe in call_args.kwargs["executables"]] == [0]
        assert all(exe.func == callable_func for exe in call_args.kwargs["executables"])
        assert call_args.kwargs["max_concurrency"] is None
        assert isinstance(call_args.kwargs["completion_config"], CompletionConfig)
        assert call_args.kwargs["serdes"] is None
        assert call_args.kwargs["summary_generator"] is None
        assert call_args.kwargs["item_serdes"] is None
        assert call_args.kwargs["nesting_type"] is NestingType.NESTED
        assert call_args.kwargs["item_namer"] is None
        assert call_args.kwargs["execution_state"] is execution_state
        assert call_args.kwargs["operation_identifier"] is operation_identifier
        assert call_args.kwargs["executor_context"] is executor_context

        assert result == mock_batch_result


async def test_map_handler_with_serdes():
    """Test that map_handler with serdes"""
    items = ["test_item"]

    async def callable_func(item):
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
    serdes = CustomStrSerDes()
    operation_identifier = OperationIdentifier(
        "test_op", OperationSubType.MAP, "parent", "test_map"
    )

    result = await invoke_map_handler(
        items,
        callable_func,
        execution_state,
        executor_context,
        operation_identifier,
        serdes=serdes,
    )

    # Verify execute was called
    assert result.all[0].result == "RESULT_TEST_ITEM"


async def test_map_handler_with_summary_generator():
    """Test that map_handler calls executor_context methods correctly."""
    items = ["item1", "item2"]

    async def callable_func(item):
        return f"large_result_{item}" * 1000  # Create a large result

    def mock_summary_generator(result):
        return f"Summary of {len(result)} chars for map item"

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
    await invoke_map_handler(
        items,
        callable_func,
        execution_state,
        executor_context,
        operation_identifier,
        summary_generator=mock_summary_generator,
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


async def test_map_executor_init_with_summary_generator_preserves_it():
    """Test MapExecutor initialization preserves summary_generator."""
    items = ["item1"]

    async def callable_func(item):
        return f"result_{item}"

    def mock_summary_generator(result):
        return f"Map summary: {result}"

    executor = create_map_executor(
        executables=[
            Executable(index=i, func=callable_func) for i in range(len(items))
        ],
        items=items,
        max_concurrency=None,
        completion_config=CompletionConfig(),
        top_level_sub_type=OperationSubType.MAP,
        iteration_sub_type=OperationSubType.MAP_ITERATION,
        name_prefix="map-item-",
        serdes=None,
        summary_generator=mock_summary_generator,
    )

    # Verify that the summary_generator is preserved in the executor
    assert executor.summary_generator is mock_summary_generator


async def test_map_handler_default_summary_generator():
    """Test that map_handler calls executor_context methods correctly with default config."""
    items = ["item1"]

    async def callable_func(item):
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

    # Call map_handler with default fields.
    await invoke_map_handler(
        items,
        callable_func,
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

    executor = create_map_executor(
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

    async def func(item):
        return f"processed_{item}"

    items = ["item1", "item2", "item3"]

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
    await invoke_map_handler(
        items=items,
        func=func,
        execution_state=execution_state,
        map_context=executor_context,
        operation_identifier=operation_identifier,
        summary_generator=None,
    )

    # Verify that create_child_context was called 3 times (N=3 items)
    assert executor_context.create_child_context.call_count == 3


async def test_map_handler_replay_mechanism():
    """Test that map_handler uses replay when operation has already succeeded."""
    items = ["item1", "item2"]

    async def callable_func(item):
        return f"result_{item}"

    # Mock execution state that indicates operation already succeeded
    class MockExecutionState:
        durable_execution_arn = "arn:aws:durable:us-east-1:123456789012:execution/test"

        def __init__(self):
            self.operations = Mock()

            def _get(operation_id):
                return Operation(
                    operation_id=operation_id,
                    operation_type=OperationType.CONTEXT,
                    status=OperationStatus.SUCCEEDED,
                    context_details=ContextDetails(
                        result=f'"cached_result_{operation_id}"'
                    ),
                )

            self.operations.get.side_effect = _get

    execution_state = MockExecutionState()
    operation_identifier = OperationIdentifier(
        "test_op", OperationSubType.MAP, "parent", "test_map"
    )

    # Mock map context
    map_context = Mock()
    map_context._step_counter._create_step_id_for_logical_step = Mock(  # noqa: SLF001
        side_effect=["child_1", "child_2"]
    )

    # Mock the executor's replay_completed method
    with patch.object(MapExecutor, "replay_completed") as mock_replay:
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

        result = await invoke_map_handler(
            items,
            callable_func,
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

    async def callable_func(item):
        return f"result_{item}"

    # Mock execution state that indicates operation succeeded but children need replay
    class MockExecutionState:
        def __init__(self):
            self.operations = Mock()

            def _get(operation_id):
                return Operation(
                    operation_id=operation_id,
                    operation_type=OperationType.CONTEXT,
                    status=OperationStatus.SUCCEEDED,
                )

            self.operations.get.side_effect = _get

    execution_state = MockExecutionState()
    operation_identifier = OperationIdentifier(
        "test_op", OperationSubType.MAP, "parent", "test_map"
    )

    # Mock map context
    map_context = Mock()
    map_context._step_counter._create_step_id_for_logical_step = Mock(  # noqa: SLF001
        return_value="child_1"
    )

    # Mock the executor's replay_completed method and _execute_item_in_child_context
    with (
        patch.object(MapExecutor, "replay_completed") as mock_replay,
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

        result = await invoke_map_handler(
            items,
            callable_func,
            execution_state,
            map_context,
            operation_identifier,
        )

        mock_replay.assert_called_once_with(execution_state, map_context)
        assert result == expected_batch_result


@patch("async_durable_execution.composite.map.ChildOperationExecutor")
async def test_map_iterates_items_iterable_once(mock_handler):
    """Test map materializes one-shot items iterables exactly once."""
    mock_executor = Mock()
    mock_executor.process = AsyncMock(return_value="map_result")
    mock_handler.return_value = mock_executor
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    class OneShotItems:
        def __init__(self):
            self.iterations = 0

        def __iter__(self):
            self.iterations += 1
            if self.iterations > 1:
                raise AssertionError("items iterated more than once")
            return iter([1, 2, 3])

    async def test_function(item):
        return item

    items = OneShotItems()
    context = create_test_context(state=mock_state)

    result = await run_with_context(context, map_operation(test_function, items))

    assert result == "map_result"
    assert items.iterations == 1


async def test_map_name_is_keyword_only():
    """Test map rejects name as a positional argument."""

    async def test_function(item):
        return item

    with pytest.raises(TypeError):
        map_operation(test_function, [1], "map-name")


async def test_map_handler_first_execution_then_replay_integration():
    """Test map_handler called twice - first calls execute, second calls replay."""

    async def test_func(item):
        return f"processed_{item}"

    items = ["a", "b"]
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

                if operation_id == "test_op":
                    # Main operation checkpoint
                    if execution_count == 0:
                        # First execution - operation not succeeded yet
                        return None
                    # Second execution - operation succeeded, trigger replay
                    return Operation(
                        operation_id=operation_id,
                        operation_type=OperationType.CONTEXT,
                        status=OperationStatus.SUCCEEDED,
                    )

                return None

            self.operations.get.side_effect = _get

    execution_state = MockExecutionState()
    map_context = Mock()

    with (
        patch(
            "async_durable_execution.composite.map.MapExecutor.execute"
        ) as mock_execute,
        patch(
            "async_durable_execution.composite.map.MapExecutor.replay_completed"
        ) as mock_replay,
    ):
        mock_execute.return_value = Mock()  # Mock BatchResult
        mock_replay.return_value = Mock()  # Mock BatchResult

        # FIRST EXECUTION - should call execute
        execution_count = 0
        await invoke_map_handler(
            items, test_func, execution_state, map_context, operation_identifier
        )

        # Verify execute was called, replay was not
        mock_execute.assert_called_once()
        mock_replay.assert_not_called()

        # Reset mocks for second call
        mock_execute.reset_mock()
        mock_replay.reset_mock()

        # SECOND EXECUTION - should call replay
        execution_count = 1
        await invoke_map_handler(
            items, test_func, execution_state, map_context, operation_identifier
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
@patch("async_durable_execution.primitive.child.serialize")
async def test_map_item_serialize(mock_serialize, item_serdes, batch_serdes):
    """Test map serializes items with item_serdes or fallback."""
    mock_serialize.return_value = '"serialized"'

    parent_checkpoint = Operation(
        operation_id="parent",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.STARTED,
    )

    def child_checkpoint_for(op_id: str):
        return Operation(
            operation_id=op_id,
            operation_type=OperationType.CONTEXT,
            status=OperationStatus.STARTED,
        )

    def get_checkpoint(op_id):
        return child_checkpoint_for(op_id) if op_id.startswith("child-") else None

    mock_state = Mock()
    mock_state.durable_execution_arn = "arn:test"
    mock_state.operations = Mock()
    mock_state.operations.get = Mock(side_effect=get_checkpoint)
    mock_state.create_checkpoint = AsyncMock()

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

        async def map_item(item):
            return item

        await run_with_context(
            context,
            map_operation(
                map_item,
                ["a", "b"],
                serdes=batch_serdes,
                item_serdes=item_serdes,
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
@patch("async_durable_execution.primitive.child.deserialize")
async def test_map_item_deserialize(mock_deserialize, item_serdes, batch_serdes):
    """Test map deserializes items with item_serdes or fallback."""
    mock_deserialize.return_value = "deserialized"
    if batch_serdes is not None:
        batch_serdes.serialize = AsyncMock(return_value='"serialized"')

    parent_checkpoint = Mock()
    parent_checkpoint.is_succeeded.return_value = False
    parent_checkpoint.is_failed.return_value = False
    parent_checkpoint.is_existent.return_value = False

    def child_checkpoint_for(op_id: str):
        return Operation(
            operation_id=op_id,
            operation_type=OperationType.CONTEXT,
            status=OperationStatus.SUCCEEDED,
            context_details=ContextDetails(result='"cached"'),
        )

    def get_checkpoint(op_id):
        return child_checkpoint_for(op_id) if op_id.startswith("child-") else None

    mock_state = Mock()
    mock_state.durable_execution_arn = "arn:test"
    mock_state.operations = Mock()
    mock_state.operations.get = Mock(side_effect=get_checkpoint)
    mock_state.create_checkpoint = AsyncMock()

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

        async def map_item(item):
            return item

        await run_with_context(
            context,
            map_operation(
                map_item,
                ["a", "b"],
                serdes=batch_serdes,
                item_serdes=item_serdes,
            ),
        )

    expected = item_serdes or batch_serdes
    call_kwargs = _mock_call_kwargs_by_operation_id(mock_deserialize)

    assert call_kwargs["child-0"]["serdes"] is expected
    assert call_kwargs["child-1"]["serdes"] is expected


async def test_map_result_serialization_roundtrip():
    """Test that map operation BatchResult can be serialized and deserialized."""

    items = ["a", "b", "c"]

    async def func(item):
        ctx = get_current_context()
        return {"item": item.upper(), "index": ctx.index}

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
    result = await invoke_map_handler(
        items, func, execution_state, map_context, operation_identifier
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
                return None

            mock_state = Mock()
            mock_state.durable_execution_arn = "arn:test"
            mock_state.operations = Mock()
            mock_state.operations.get = Mock(side_effect=get_checkpoint)
            mock_state.create_checkpoint = AsyncMock()

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

                async def map_item(item):
                    return item

                result = await run_with_context(
                    context, map_operation(map_item, ["a", "b"])
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
                return None

            mock_state = Mock()
            mock_state.durable_execution_arn = "arn:test"
            mock_state.operations = Mock()
            mock_state.operations.get = Mock(side_effect=get_checkpoint)
            mock_state.create_checkpoint = AsyncMock()

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

                async def map_item(item):
                    return item

                result = await run_with_context(
                    context, map_operation(map_item, ["a", "b"])
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
                return None

            mock_state = Mock()
            mock_state.durable_execution_arn = "arn:test"
            mock_state.operations = Mock()
            mock_state.operations.get = Mock(side_effect=get_checkpoint)
            mock_state.create_checkpoint = AsyncMock()

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

                async def map_item(item):
                    return item

                result = await run_with_context(
                    context,
                    map_operation(
                        map_item,
                        ["a", "b"],
                        serdes=custom_serdes,
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

    async def map_func(item):
        return f"processed_{item}"

    mock_state = Mock()
    mock_state.durable_execution_arn = "arn:test"

    parent_checkpoint = Mock()
    parent_checkpoint.is_succeeded.return_value = False
    parent_checkpoint.is_failed.return_value = False
    parent_checkpoint.is_existent.return_value = False

    mock_state.operations = Mock()
    mock_state.operations.get = Mock(return_value=None)
    mock_state.create_checkpoint = AsyncMock()

    context = create_test_context(state=mock_state)

    # This should complete immediately without crashing
    result = await run_with_context(
        context,
        map_operation(
            map_func,
            items,
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
    func = lambda item: item  # noqa: E731

    executor = create_map_executor(
        executables=[Executable(index=i, func=func) for i in range(len(items))],
        items=items,
        max_concurrency=2,
        completion_config=CompletionConfig(),
        top_level_sub_type=OperationSubType.MAP,
        iteration_sub_type=OperationSubType.MAP_ITERATION,
        name_prefix="map-item-",
        serdes=None,
    )

    assert executor.get_iteration_name(0) == "map-item-0"
    assert executor.get_iteration_name(1) == "map-item-1"
    assert executor.get_iteration_name(2) == "map-item-2"


async def test_map_executor_get_iteration_name_with_item_namer():
    """With item_namer, iterations use custom names."""
    items = [{"id": "order-1"}, {"id": "order-2"}, {"id": "order-3"}]
    func = lambda item: item  # noqa: E731

    executor = create_map_executor(
        executables=[Executable(index=i, func=func) for i in range(len(items))],
        items=items,
        max_concurrency=2,
        completion_config=CompletionConfig(),
        top_level_sub_type=OperationSubType.MAP,
        iteration_sub_type=OperationSubType.MAP_ITERATION,
        name_prefix="map-item-",
        serdes=None,
        item_namer=lambda item, index: f"process-{item['id']}",
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

    func = lambda item: item  # noqa: E731
    executor = create_map_executor(
        executables=[Executable(index=i, func=func) for i in range(len(items))],
        items=items,
        max_concurrency=None,
        completion_config=CompletionConfig(),
        top_level_sub_type=OperationSubType.MAP,
        iteration_sub_type=OperationSubType.MAP_ITERATION,
        name_prefix="map-item-",
        serdes=None,
        item_namer=namer,
    )

    executor.get_iteration_name(0)
    executor.get_iteration_name(2)

    assert received_args == [("alpha", 0), ("gamma", 2)]


async def test_map_executor_item_namer_uses_index():
    """item_namer can use the index to generate names."""
    items = [10, 20, 30]
    func = lambda item: item  # noqa: E731

    executor = create_map_executor(
        executables=[Executable(index=i, func=func) for i in range(len(items))],
        items=items,
        max_concurrency=None,
        completion_config=CompletionConfig(),
        top_level_sub_type=OperationSubType.MAP,
        iteration_sub_type=OperationSubType.MAP_ITERATION,
        name_prefix="map-item-",
        serdes=None,
        item_namer=lambda item, index: f"step-{index + 1}",
    )

    assert executor.get_iteration_name(0) == "step-1"
    assert executor.get_iteration_name(1) == "step-2"
    assert executor.get_iteration_name(2) == "step-3"


async def test_map_executor_item_namer_none_falls_back_to_default():
    """Explicitly passing item_namer=None uses default naming."""
    items = ["x", "y"]
    func = lambda item: item  # noqa: E731

    executor = create_map_executor(
        executables=[Executable(index=i, func=func) for i in range(len(items))],
        items=items,
        max_concurrency=None,
        completion_config=CompletionConfig(),
        top_level_sub_type=OperationSubType.MAP,
        iteration_sub_type=OperationSubType.MAP_ITERATION,
        name_prefix="map-item-",
        serdes=None,
        item_namer=None,
    )

    assert executor.get_iteration_name(0) == "map-item-0"
    assert executor.get_iteration_name(1) == "map-item-1"


async def test_map_executor_init_passes_item_namer():
    """MapExecutor initialization correctly passes item_namer."""
    namer = lambda item, index: f"custom-{index}"  # noqa: E731
    func = lambda item: item  # noqa: E731

    executor = create_map_executor(
        executables=[Executable(index=0, func=func)],
        items=["a"],
        max_concurrency=None,
        completion_config=CompletionConfig(),
        top_level_sub_type=OperationSubType.MAP,
        iteration_sub_type=OperationSubType.MAP_ITERATION,
        name_prefix="map-item-",
        serdes=None,
        item_namer=namer,
    )

    assert executor._item_namer is namer


async def test_map_executor_item_namer_accepts_typed_items():
    """item_namer can be typed to match map items."""
    item_namer = lambda item, index: f"item-{item['name']}"  # noqa: E731
    items = [{"name": "test"}]
    func = lambda item: item  # noqa: E731

    executor = create_map_executor(
        executables=[Executable(index=i, func=func) for i in range(len(items))],
        items=items,
        max_concurrency=None,
        completion_config=CompletionConfig(),
        top_level_sub_type=OperationSubType.MAP,
        iteration_sub_type=OperationSubType.MAP_ITERATION,
        name_prefix="map-item-",
        serdes=None,
        item_namer=item_namer,
    )

    assert executor.get_iteration_name(0) == "item-test"
