"""Tests for the parallel operation module."""

from typing import no_type_check

import asyncio
import inspect
import importlib
import json
from collections.abc import Awaitable, Callable, Iterator, Mapping
from typing import Any, NoReturn
from unittest.mock import ANY, AsyncMock, Mock, patch

import pytest
import async_durable_execution._core.context as context_module
import async_durable_execution._primitive.child as child
from async_durable_execution._extension.parallel import (
    _BATCH_RESULT_SERDES,
    _BatchResultSerDes,
    BatchItem,
    BatchItemStatus,
    BatchResult,
    CompletionReason,
    Executable,
)

# Mock the executor.execute method to return a BatchResult
from async_durable_execution._core.models import (
    ContextDetails,
    Operation,
    OperationStatus,
    OperationType,
)
from async_durable_execution._core.context import (
    reset_current_context,
    set_current_context,
)
from async_durable_execution import durable_callable, parallel, DurableContext
from async_durable_execution._core.models import OperationIdentifier
from async_durable_execution._core.models import OperationSubType
from async_durable_execution._extension.parallel import CompletionConfig, NestingType
from async_durable_execution._extension.parallel import (
    ParallelExecutor,
    ParallelSummaryGenerator,
    parallel_handler,
)
from async_durable_execution._core.exceptions import SerDesError
from async_durable_execution._core.serdes import ExtendedTypeSerDes, serialize
from async_durable_execution._core.state import ExecutionState

from ..serdes_test import CustomStrSerDes


async def _invoke_maybe_async(func, *args, **kwargs) -> Any:
    return await func(*args, **kwargs)


def create_test_context(
    state: ExecutionState | None = None, parent_id: str | None = None
) -> DurableContext:
    """Helper to create DurableContext for tests."""
    if state is None:
        state = Mock(spec=ExecutionState)
        state.durable_execution_arn = (
            "arn:aws:durable:us-east-1:123456789012:execution/test"
        )

    return DurableContext(
        execution_state=state,
        operation_identifier=OperationIdentifier(
            operation_id=None,
            sub_type=OperationSubType.EXECUTION,
            parent_id=parent_id,
        ),
    )


def create_mock_execution_state() -> Any:
    state = Mock(spec=ExecutionState)
    state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    state.operations.get.return_value = None
    state.create_checkpoint = AsyncMock()
    return state


def create_mock_child_context(state) -> Any:
    child_context = Mock()
    child_context.state = state
    child_context.execution_state = state
    return child_context


def create_parallel_executor(**kwargs) -> Any:
    execution_state = kwargs.pop("execution_state", None)
    if execution_state is None:
        execution_state = create_mock_execution_state()
    operation_identifier = kwargs.pop("operation_identifier", None)
    if operation_identifier is None:
        operation_identifier = OperationIdentifier(
            "test_op",
            OperationSubType.PARALLEL,
            "parent",
            "test_parallel",
        )
    executor_context = kwargs.pop("executor_context", None)
    if executor_context is None:
        executor_context = create_test_context(execution_state)
    return ParallelExecutor(
        execution_state=execution_state,
        operation_identifier=operation_identifier,
        executor_context=executor_context,
        **kwargs,
    )


async def run_with_context(context: DurableContext, awaitable) -> Any:
    token = set_current_context(context)
    try:
        if callable(awaitable):
            awaitable = awaitable()
        if hasattr(awaitable, "__await__"):
            return await awaitable
        return awaitable
    finally:
        reset_current_context(token)


def configure_mock_child_serdes_roundtrip(mock_serialize, mock_deserialize) -> None:
    parent_value = None

    async def serialize_side_effect(*, value, operation_id, **kwargs) -> str:
        nonlocal parent_value
        if operation_id == "parent":
            parent_value = value
        return '"serialized"'

    async def deserialize_side_effect(*, operation_id, **kwargs) -> Any:
        if operation_id == "parent" and parent_value is not None:
            return parent_value
        return "deserialized"

    mock_serialize.side_effect = serialize_side_effect
    mock_deserialize.side_effect = deserialize_side_effect


def _mock_call_kwargs_by_operation_id(mock: Mock) -> dict[str, Mapping[str, Any]]:
    """Return mock call keyword arguments keyed by operation_id."""
    return {call.kwargs["operation_id"]: call.kwargs for call in mock.call_args_list}


async def test_parallel_executor_init() -> None:
    """Test ParallelExecutor initialization."""
    executables = [Executable(index=0, func=lambda x: x)]
    completion_config = CompletionConfig.all_successful()

    executor = create_parallel_executor(
        executables=executables,
        max_concurrency=2,
        completion_config=completion_config,
        top_level_sub_type=OperationSubType.PARALLEL,
        iteration_sub_type=OperationSubType.PARALLEL_BRANCH,
        name_prefix="test-",
        serdes=None,
        nesting_type=NestingType.FLAT,
    )

    assert executor.executables == executables
    assert executor.max_concurrency == 2
    assert executor.completion_config == completion_config
    assert executor.sub_type_top == OperationSubType.PARALLEL
    assert executor.sub_type_iteration == OperationSubType.PARALLEL_BRANCH
    assert executor.name_prefix == "test-"
    assert executor.nesting_type is NestingType.FLAT


def test_parallel_signature_accepts_config_fields_directly() -> None:
    """The public parallel operation exposes config fields directly."""
    parameters = inspect.signature(parallel).parameters

    assert "config" not in parameters
    assert "max_concurrency" in parameters
    assert "completion_config" in parameters
    assert "serdes" in parameters
    assert "item_serdes" in parameters
    assert "summary_generator" in parameters
    assert "nesting_type" in parameters
    assert isinstance(
        parameters["summary_generator"].default,
        ParallelSummaryGenerator,
    )


def test_parallel_signature_requires_keyword_only_options() -> None:
    """All parallel configuration options are keyword-only."""
    parameters = inspect.signature(parallel).parameters

    assert parameters["branches"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert parameters["name"].kind is inspect.Parameter.KEYWORD_ONLY
    assert parameters["max_concurrency"].kind is inspect.Parameter.KEYWORD_ONLY
    assert parameters["completion_config"].kind is inspect.Parameter.KEYWORD_ONLY
    assert parameters["serdes"].kind is inspect.Parameter.KEYWORD_ONLY
    assert parameters["item_serdes"].kind is inspect.Parameter.KEYWORD_ONLY
    assert parameters["summary_generator"].kind is inspect.Parameter.KEYWORD_ONLY
    assert parameters["nesting_type"].kind is inspect.Parameter.KEYWORD_ONLY


@patch("async_durable_execution._extension.parallel.parallel_handler")
async def test_parallel_passes_config_fields_to_handler(
    mock_parallel_handler,
) -> None:
    """Direct config fields are passed to the handler."""

    async def branch_a() -> str:
        return "a"

    async def handler_result() -> str:
        return "parallel_result"

    completion_config = CompletionConfig.first_successful()
    serdes = Mock()
    serdes.serialize = AsyncMock(return_value='"parallel_result"')
    serdes.deserialize = AsyncMock(return_value="parallel_result")
    item_serdes = Mock()
    summary_generator = Mock()
    context = create_test_context(state=create_mock_execution_state())

    mock_parallel_handler.return_value = handler_result

    result = await run_with_context(
        context,
        lambda: parallel(
            [branch_a],
            max_concurrency=7,
            completion_config=completion_config,
            serdes=serdes,
            item_serdes=item_serdes,
            summary_generator=summary_generator,
            nesting_type=NestingType.FLAT,
        ),
    )

    assert result == "parallel_result"
    mock_parallel_handler.assert_called_once()
    kwargs = mock_parallel_handler.call_args.kwargs
    assert kwargs["max_concurrency"] == 7
    assert kwargs["completion_config"] is completion_config
    assert kwargs["serdes"] is serdes
    assert kwargs["item_serdes"] is item_serdes
    assert kwargs["summary_generator"] is summary_generator
    assert kwargs["nesting_type"] is NestingType.FLAT


@patch("async_durable_execution._extension.parallel.parallel_handler")
async def test_parallel_passes_default_summary_generator(
    mock_parallel_handler,
) -> None:
    """summary_generator defaults to ParallelSummaryGenerator in the public wrapper."""

    async def branch_a() -> str:
        return "a"

    async def handler_result() -> str:
        return "parallel_result"

    context = create_test_context(state=create_mock_execution_state())

    mock_parallel_handler.return_value = handler_result

    result = await run_with_context(
        context,
        lambda: parallel([branch_a]),
    )

    assert result == "parallel_result"
    mock_parallel_handler.assert_called_once()
    assert isinstance(
        mock_parallel_handler.call_args.kwargs["summary_generator"],
        ParallelSummaryGenerator,
    )


def test_parallel_summary_generator_returns_compact_json_payload() -> None:
    """ParallelSummaryGenerator summarizes counts without embedding branch payloads."""
    result = BatchResult(
        all=[
            BatchItem(index=0, status=BatchItemStatus.SUCCEEDED, result="ok"),
            BatchItem(index=1, status=BatchItemStatus.FAILED, error=Mock()),
            BatchItem(index=2, status=BatchItemStatus.STARTED),
        ],
        completion_reason=CompletionReason.FAILURE_TOLERANCE_EXCEEDED,
    )

    summary = json.loads(ParallelSummaryGenerator()(result))

    assert summary == {
        "totalCount": 3,
        "successCount": 1,
        "failureCount": 1,
        "completionReason": "FAILURE_TOLERANCE_EXCEEDED",
        "status": "FAILED",
        "startedCount": 1,
        "type": "ParallelResult",
    }


@patch("async_durable_execution._extension.parallel._run_in_child_context")
async def test_parallel_raises_when_child_operation_id_is_missing(
    mock_run_in_child_context,
) -> None:
    """The public wrapper fails clearly if no child operation id is available."""

    async def branch_a() -> str:
        return "a"

    async def run_child_func(func, *_args, **_kwargs) -> Any:
        return await func()

    context = create_test_context(state=create_mock_execution_state())
    mock_run_in_child_context.side_effect = run_child_func

    with pytest.raises(
        RuntimeError,
        match="parallel operation id is not available in the current context",
    ):
        await run_with_context(context, lambda: parallel([branch_a]))


@patch("async_durable_execution._extension.parallel.parallel_handler")
async def test_parallel_accepts_one_shot_branch_iterable(
    mock_parallel_handler,
) -> None:
    """The public wrapper consumes branch iterables only once."""

    async def branch_a() -> str:
        return "a"

    async def branch_b() -> str:
        return "b"

    async def handler_result() -> str:
        return "parallel_result"

    class OneShotBranches:
        def __init__(self) -> None:
            self.iteration_count = 0

        def __iter__(self) -> Iterator[Callable[[], Awaitable[str]]]:
            self.iteration_count += 1
            if self.iteration_count > 1:
                msg = "branches were iterated more than once"
                raise AssertionError(msg)
            yield branch_a
            yield branch_b

    branches = OneShotBranches()
    context = create_test_context(state=create_mock_execution_state())

    mock_parallel_handler.return_value = handler_result

    result = await run_with_context(context, lambda: parallel(branches))

    assert result == "parallel_result"
    assert branches.iteration_count == 1
    mock_parallel_handler.assert_called_once()
    assert mock_parallel_handler.call_args.kwargs["callables"] == [branch_a, branch_b]


async def test_parallel_executor_init_with_callables() -> None:
    """ParallelExecutor accepts callables through constructor executables."""

    async def func1() -> str:
        return "result1"

    async def func2() -> str:
        return "result2"

    executables = [
        Executable(index=0, func=func1),
        Executable(index=1, func=func2),
    ]
    executor = create_parallel_executor(
        executables=executables,
        max_concurrency=3,
        completion_config=CompletionConfig.all_successful(),
        top_level_sub_type=OperationSubType.PARALLEL,
        iteration_sub_type=OperationSubType.PARALLEL_BRANCH,
        name_prefix="parallel-branch-",
        serdes=None,
        nesting_type=NestingType.FLAT,
    )

    assert len(executor.executables) == 2
    assert executor.executables[0].index == 0
    assert executor.executables[0].func == func1
    assert executor.executables[1].index == 1
    assert executor.executables[1].func == func2
    assert executor.max_concurrency == 3
    assert executor.sub_type_top == OperationSubType.PARALLEL
    assert executor.sub_type_iteration == OperationSubType.PARALLEL_BRANCH
    assert executor.name_prefix == "parallel-branch-"
    assert executor.nesting_type is NestingType.FLAT


async def test_parallel_executor_execute_item() -> None:
    """Test ParallelExecutor.execute_item method."""

    def test_func() -> str:
        return "processed"

    executable = Executable(index=0, func=test_func)
    executor = create_parallel_executor(
        executables=[executable],
        max_concurrency=None,
        completion_config=CompletionConfig.all_successful(),
        top_level_sub_type=OperationSubType.PARALLEL,
        iteration_sub_type=OperationSubType.PARALLEL_BRANCH,
        name_prefix="test-",
        serdes=None,
    )

    child_context = "test-context"
    result = await executor.execute_item(child_context, executable)

    assert result == "processed"


async def test_parallel_executor_execute_item_with_async_callable() -> None:
    async def test_func() -> str:
        await asyncio.sleep(0)
        return "processed"

    executable = Executable(index=0, func=test_func)
    executor = create_parallel_executor(
        executables=[executable],
        max_concurrency=None,
        completion_config=CompletionConfig.all_successful(),
        top_level_sub_type=OperationSubType.PARALLEL,
        iteration_sub_type=OperationSubType.PARALLEL_BRANCH,
        name_prefix="test-",
        serdes=None,
    )

    result = await executor.execute_item("test-context", executable)

    assert result == "processed"


async def test_parallel_executor_execute_item_with_exception() -> None:
    """Test ParallelExecutor.execute_item with callable that raises exception."""

    async def failing_func() -> NoReturn:
        msg = "Test error"
        raise ValueError(msg)

    executable = Executable(index=0, func=failing_func)
    executor = create_parallel_executor(
        executables=[executable],
        max_concurrency=None,
        completion_config=CompletionConfig.all_successful(),
        top_level_sub_type=OperationSubType.PARALLEL,
        iteration_sub_type=OperationSubType.PARALLEL_BRANCH,
        name_prefix="test-",
        serdes=None,
    )

    child_context = "test-context"

    with pytest.raises(ValueError, match="Test error"):
        await executor.execute_item(child_context, executable)


@no_type_check
async def test_parallel_handler() -> None:
    """Test parallel_handler function."""

    async def func1() -> str:
        return "result1"

    async def func2() -> str:
        return "result2"

    callables = [func1, func2]

    class MockExecutionState:
        def __init__(self) -> None:
            self.operations = Mock()
            self.operations.get.return_value = Mock(
                is_succeeded=Mock(return_value=False)
            )

    execution_state = MockExecutionState()
    operation_identifier = OperationIdentifier(
        "test_op", OperationSubType.PARALLEL, "parent", "test_parallel"
    )

    # Mock the run_in_child_context function
    def mock_run_in_child_context(callable_func, name, child_config) -> Any:
        return callable_func("mock-context")

    mock_batch_result = BatchResult(
        all=[BatchItem(index=0, status=BatchItemStatus.SUCCEEDED, result="test")],
        completion_reason=CompletionReason.ALL_COMPLETED,
    )

    with patch.object(ParallelExecutor, "execute", return_value=mock_batch_result):
        result = await parallel_handler(
            callables,
            execution_state,
            mock_run_in_child_context,
            operation_identifier,
            max_concurrency=2,
        )()

        assert result == mock_batch_result


@no_type_check
async def test_parallel_handler_with_default_fields() -> None:
    """Test parallel_handler function with default config fields."""

    async def func1() -> str:
        return "result1"

    callables = [func1]

    class MockExecutionState:
        def __init__(self) -> None:
            self.operations = Mock()
            self.operations.get.return_value = Mock(
                is_succeeded=Mock(return_value=False)
            )

    execution_state = MockExecutionState()
    operation_identifier = OperationIdentifier(
        "test_op", OperationSubType.PARALLEL, "parent", "test_parallel"
    )

    def mock_run_in_child_context(callable_func, name, child_config) -> Any:
        return callable_func("mock-context")

    mock_batch_result = BatchResult(
        all=[BatchItem(index=0, status=BatchItemStatus.SUCCEEDED, result="test")],
        completion_reason=CompletionReason.ALL_COMPLETED,
    )

    with patch.object(ParallelExecutor, "execute", return_value=mock_batch_result):
        result = await parallel_handler(
            callables,
            execution_state,
            mock_run_in_child_context,
            operation_identifier,
        )()

        assert result == mock_batch_result


@no_type_check
async def test_parallel_handler_creates_executor_with_correct_config() -> None:
    """Test that parallel_handler creates ParallelExecutor with correct configuration."""

    async def func1() -> str:
        return "result1"

    callables = [func1]

    class MockExecutionState:
        def __init__(self) -> None:
            self.operations = Mock()
            self.operations.get.return_value = Mock(
                is_succeeded=Mock(return_value=False)
            )

    execution_state = MockExecutionState()
    operation_identifier = OperationIdentifier(
        "test_op", OperationSubType.PARALLEL, "parent", "test_parallel"
    )

    executor_context = Mock()
    executor_context._step_counter._create_step_id_for_logical_step = (  # noqa: SLF001
        lambda *args: "1"
    )
    executor_context.create_child_context = lambda *args, **kwargs: Mock()

    with patch(
        "async_durable_execution._extension.parallel.ParallelExecutor"
    ) as mock_executor_class:
        mock_batch_result = Mock(spec=BatchResult)
        mock_executor = Mock()
        mock_executor.process = AsyncMock(return_value=mock_batch_result)
        mock_executor_class.return_value = mock_executor

        result = await parallel_handler(
            callables,
            execution_state,
            executor_context,
            operation_identifier,
            max_concurrency=5,
        )()

        mock_executor_class.assert_called_once_with(
            executables=[Executable(index=0, func=func1)],
            max_concurrency=5,
            completion_config=CompletionConfig.all_successful(),
            top_level_sub_type=OperationSubType.PARALLEL,
            iteration_sub_type=OperationSubType.PARALLEL_BRANCH,
            name_prefix="parallel-branch-",
            serdes=None,
            summary_generator=ANY,
            item_serdes=None,
            nesting_type=NestingType.NESTED,
            branch_namer=None,
            execution_state=execution_state,
            operation_identifier=operation_identifier,
            executor_context=executor_context,
        )
        mock_executor.process.assert_called_once_with()
        assert result == mock_batch_result


@no_type_check
async def test_parallel_handler_creates_executor_with_default_fields() -> None:
    """Test that parallel_handler creates ParallelExecutor with default fields."""

    async def func1() -> str:
        return "result1"

    callables = [func1]

    class MockExecutionState:
        def __init__(self) -> None:
            self.operations = Mock()
            self.operations.get.return_value = Mock(
                is_succeeded=Mock(return_value=False)
            )

    execution_state = MockExecutionState()
    operation_identifier = OperationIdentifier(
        "test_op", OperationSubType.PARALLEL, "parent", "test_parallel"
    )

    executor_context = Mock()
    executor_context._step_counter._create_step_id_for_logical_step = (  # noqa: SLF001
        lambda *args: "1"
    )
    executor_context.create_child_context = lambda *args, **kwargs: Mock()

    with patch(
        "async_durable_execution._extension.parallel.ParallelExecutor"
    ) as mock_executor_class:
        mock_batch_result = Mock(spec=BatchResult)
        mock_executor = Mock()
        mock_executor.process = AsyncMock(return_value=mock_batch_result)
        mock_executor_class.return_value = mock_executor

        result = await parallel_handler(
            callables, execution_state, executor_context, operation_identifier
        )()

        assert result == mock_batch_result
        mock_executor_class.assert_called_once_with(
            executables=[Executable(index=0, func=func1)],
            max_concurrency=None,
            completion_config=CompletionConfig.all_successful(),
            top_level_sub_type=OperationSubType.PARALLEL,
            iteration_sub_type=OperationSubType.PARALLEL_BRANCH,
            name_prefix="parallel-branch-",
            serdes=None,
            summary_generator=ANY,
            item_serdes=None,
            nesting_type=NestingType.NESTED,
            branch_namer=None,
            execution_state=execution_state,
            operation_identifier=operation_identifier,
            executor_context=executor_context,
        )
        mock_executor.process.assert_called_once_with()


async def test_parallel_executor_inheritance() -> None:
    """Test that ParallelExecutor properly inherits from ParallelExecutor."""
    executables = [Executable(index=0, func=lambda x: x)]
    executor = create_parallel_executor(
        executables=executables,
        max_concurrency=None,
        completion_config=CompletionConfig.all_successful(),
        top_level_sub_type=OperationSubType.PARALLEL,
        iteration_sub_type=OperationSubType.PARALLEL_BRANCH,
        name_prefix="test-",
        serdes=None,
    )

    assert isinstance(executor, ParallelExecutor)


async def test_parallel_executor_init_empty_list() -> None:
    """Test ParallelExecutor with empty executables list."""
    executor = create_parallel_executor(
        executables=[],
        max_concurrency=None,
        completion_config=CompletionConfig.all_successful(),
        top_level_sub_type=OperationSubType.PARALLEL,
        iteration_sub_type=OperationSubType.PARALLEL_BRANCH,
        name_prefix="parallel-branch-",
        serdes=None,
    )

    assert len(executor.executables) == 0
    assert executor.max_concurrency is None


async def test_parallel_executor_execute_item_return_type() -> None:
    """Test that ParallelExecutor.execute_item returns the correct type."""

    async def int_func() -> int:
        return 42

    async def str_func() -> str:
        return "hello"

    async def dict_func() -> Any:
        return {"key": "value"}

    executor = create_parallel_executor(
        executables=[],
        max_concurrency=None,
        completion_config=CompletionConfig.all_successful(),
        top_level_sub_type=OperationSubType.PARALLEL,
        iteration_sub_type=OperationSubType.PARALLEL_BRANCH,
        name_prefix="test-",
        serdes=None,
    )

    # Test different return types
    int_executable = Executable(index=0, func=int_func)
    str_executable = Executable(index=1, func=str_func)
    dict_executable = Executable(index=2, func=dict_func)

    assert await executor.execute_item("ctx", int_executable) == 42
    assert await executor.execute_item("ctx", str_executable) == "hello"
    assert await executor.execute_item("ctx", dict_executable) == {"key": "value"}


async def test_parallel_handler_with_serdes() -> None:
    """Test that parallel_handler with serdes"""

    async def func1() -> str:
        return "RESULT1"

    callables = [func1]

    execution_state = create_mock_execution_state()
    execution_state.operations.get.return_value = None
    operation_identifier = OperationIdentifier(
        "test_op", OperationSubType.PARALLEL, "parent", "test_parallel"
    )

    executor_context = Mock()
    executor_context.step_counter = Mock()
    executor_context._step_counter._create_step_id_for_logical_step = (  # noqa: SLF001
        lambda *args: "1"
    )
    executor_context.step_counter._create_step_id_for_logical_step = lambda *args: "1"
    child_context = create_mock_child_context(execution_state)
    executor_context.create_child_context = lambda *args, **kwargs: child_context

    result = await parallel_handler(
        callables,
        execution_state,
        executor_context,
        operation_identifier,
        serdes=CustomStrSerDes(),
    )()

    assert result.all[0].result == "result1"


async def test_parallel_handler_with_summary_generator() -> None:
    """Test that parallel_handler calls executor_context methods correctly."""

    async def func1() -> str:
        return "large_result" * 1000  # Create a large result

    def mock_summary_generator(result) -> str:
        return f"Summary of {len(result)} chars"

    callables = [func1]

    execution_state = create_mock_execution_state()
    execution_state.operations.get.return_value = Mock(
        is_succeeded=Mock(return_value=False),
        is_failed=Mock(return_value=False),
        is_existent=Mock(return_value=False),
        is_replay_children=Mock(return_value=False),
    )
    operation_identifier = OperationIdentifier(
        "test_op", OperationSubType.PARALLEL, "parent", "test_parallel"
    )

    executor_context = Mock()
    executor_context.step_counter = Mock()
    executor_context._step_counter._create_step_id_for_logical_step = Mock(  # noqa: SLF001
        return_value="1"
    )
    executor_context.step_counter._create_step_id_for_logical_step = Mock(
        return_value="1"
    )
    executor_context.create_child_context = Mock(
        return_value=create_mock_child_context(execution_state)
    )

    # Call parallel_handler
    await parallel_handler(
        callables,
        execution_state,
        executor_context,
        operation_identifier,
        summary_generator=mock_summary_generator,
    )()

    # Verify that create_child_context was called once (N=1 job)
    assert executor_context.create_child_context.call_count == 1

    # Verify that _create_step_id_for_logical_step was called once with unique value
    assert (
        executor_context.step_counter._create_step_id_for_logical_step.call_count == 1
    )


async def test_parallel_executor_init_with_summary_generator() -> None:
    """Test ParallelExecutor preserves summary_generator."""

    async def func1() -> str:
        return "result1"

    def mock_summary_generator(result) -> str:
        return f"Summary: {result}"

    executor = create_parallel_executor(
        executables=[Executable(index=0, func=func1)],
        max_concurrency=None,
        completion_config=CompletionConfig.all_successful(),
        top_level_sub_type=OperationSubType.PARALLEL,
        iteration_sub_type=OperationSubType.PARALLEL_BRANCH,
        name_prefix="parallel-branch-",
        serdes=None,
        summary_generator=mock_summary_generator,
    )

    # Verify that the summary_generator is preserved in the executor
    assert executor.summary_generator is mock_summary_generator


async def test_parallel_handler_default_summary_generator() -> None:
    """Test that parallel_handler calls executor_context methods correctly with default config."""

    async def func1() -> str:
        return "result1"

    async def func2() -> str:
        return "result2"

    callables = [func1, func2]

    execution_state = create_mock_execution_state()
    execution_state.operations.get.return_value = Mock(
        is_succeeded=Mock(return_value=False),
        is_failed=Mock(return_value=False),
        is_existent=Mock(return_value=False),
        is_replay_children=Mock(return_value=False),
    )
    operation_identifier = OperationIdentifier(
        "test_op", OperationSubType.PARALLEL, "parent", "test_parallel"
    )

    executor_context = Mock()
    executor_context.step_counter = Mock()
    executor_context._step_counter._create_step_id_for_logical_step = Mock(  # noqa: SLF001
        side_effect=["1", "2"]
    )
    executor_context.step_counter._create_step_id_for_logical_step = Mock(
        side_effect=["1", "2"]
    )
    executor_context.create_child_context = Mock(
        return_value=create_mock_child_context(execution_state)
    )

    # Call parallel_handler with defaults.
    await parallel_handler(
        callables, execution_state, executor_context, operation_identifier
    )()

    # Verify that create_child_context was called twice (N=2 jobs)
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


async def test_parallel_handler_with_explicit_none_summary_generator() -> None:
    """Test that parallel_handler calls executor_context methods correctly with explicit None summary_generator."""

    async def func1() -> str:
        return "result1"

    async def func2() -> str:
        return "result2"

    async def func3() -> str:
        return "result3"

    callables = [func1, func2, func3]

    execution_state = create_mock_execution_state()
    execution_state.operations.get.return_value = Mock(
        is_succeeded=Mock(return_value=False),
        is_failed=Mock(return_value=False),
        is_existent=Mock(return_value=False),
        is_replay_children=Mock(return_value=False),
    )
    operation_identifier = OperationIdentifier(
        "test_op", OperationSubType.PARALLEL, "parent", "test_parallel"
    )

    executor_context = Mock()
    executor_context.step_counter = Mock()
    executor_context._step_counter._create_step_id_for_logical_step = Mock(  # noqa: SLF001
        side_effect=["1", "2", "3"]
    )
    executor_context.step_counter._create_step_id_for_logical_step = Mock(
        side_effect=["1", "2", "3"]
    )
    executor_context.create_child_context = Mock(
        return_value=create_mock_child_context(execution_state)
    )

    # Call parallel_handler
    await parallel_handler(
        callables=callables,
        execution_state=execution_state,
        parallel_context=executor_context,
        operation_identifier=operation_identifier,
        summary_generator=None,
    )()

    # Verify that create_child_context was called 3 times (N=3 jobs)
    assert executor_context.create_child_context.call_count == 3


@no_type_check
async def test_parallel_handler_replay_mechanism() -> None:
    """Test that parallel_handler uses replay when operation has already succeeded."""

    async def func1() -> str:
        return "result1"

    async def func2() -> str:
        return "result2"

    callables = [func1, func2]

    # Mock execution state that indicates operation already succeeded
    class MockExecutionState:
        durable_execution_arn = "arn:aws:durable:us-east-1:123456789012:execution/test"

        def __init__(self) -> None:
            self.operations = Mock()

            def _get(operation_id) -> Any:
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
        "test_op", OperationSubType.PARALLEL, "parent", "test_parallel"
    )

    # Mock parallel context
    parallel_context = Mock()
    parallel_context._step_counter._create_step_id_for_logical_step = Mock(  # noqa: SLF001
        side_effect=["child_1", "child_2"]
    )

    # Mock the executor's replay_completed method
    with patch.object(ParallelExecutor, "replay_completed") as mock_replay:
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

        result = await parallel_handler(
            callables, execution_state, parallel_context, operation_identifier
        )()

        # Verify replay was called instead of execute
        mock_replay.assert_called_once_with(execution_state, parallel_context)
        assert result == expected_batch_result


@no_type_check
async def test_parallel_handler_replay_with_replay_children() -> None:
    """Test parallel_handler replay when children need to be re-executed."""

    async def func1() -> str:
        return "result1"

    callables = [func1]

    # Mock execution state that indicates operation succeeded but children need replay
    class MockExecutionState:
        def __init__(self) -> None:
            self.operations = Mock()

            def _get(operation_id) -> Any:
                return Operation(
                    operation_id=operation_id,
                    operation_type=OperationType.CONTEXT,
                    status=OperationStatus.SUCCEEDED,
                )

            self.operations.get.side_effect = _get

    execution_state = MockExecutionState()
    operation_identifier = OperationIdentifier(
        "test_op", OperationSubType.PARALLEL, "parent", "test_parallel"
    )

    # Mock parallel context
    parallel_context = Mock()
    parallel_context._step_counter._create_step_id_for_logical_step = Mock(  # noqa: SLF001
        return_value="child_1"
    )

    # Mock the executor's replay_completed method and _execute_item_in_child_context
    with (
        patch.object(ParallelExecutor, "replay_completed") as mock_replay,
        patch.object(
            ParallelExecutor, "_execute_item_in_child_context"
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

        result = await parallel_handler(
            callables, execution_state, parallel_context, operation_identifier
        )()

        mock_replay.assert_called_once_with(execution_state, parallel_context)
        assert result == expected_batch_result


@no_type_check
async def test_parallel_handler_first_execution_then_replay() -> None:
    """Test parallel_handler called twice - first calls execute, second calls replay."""

    async def task1() -> str:
        return "result1"

    async def task2() -> str:
        return "result2"

    callables = [task1, task2]
    operation_identifier = OperationIdentifier(
        "test_op", OperationSubType.PARALLEL, "parent", "test_parallel"
    )

    # Track whether we're in first or second execution
    execution_count = 0

    class MockExecutionState:
        durable_execution_arn = "arn:aws:durable:us-east-1:123456789012:execution/test"

        def __init__(self) -> None:
            self.operations = Mock()

            def _get(operation_id) -> Any:
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
    parallel_context = Mock()

    with (
        patch(
            "async_durable_execution._extension.parallel.ParallelExecutor.execute"
        ) as mock_execute,
        patch(
            "async_durable_execution._extension.parallel.ParallelExecutor.replay_completed"
        ) as mock_replay,
    ):
        mock_execute.return_value = Mock()  # Mock BatchResult
        mock_replay.return_value = Mock()  # Mock BatchResult

        # FIRST EXECUTION - should call execute
        execution_count = 0
        await parallel_handler(
            callables, execution_state, parallel_context, operation_identifier
        )()

        # Verify execute was called, replay was not
        mock_execute.assert_called_once()
        mock_replay.assert_not_called()

        # Reset mocks for second call
        mock_execute.reset_mock()
        mock_replay.reset_mock()

        # SECOND EXECUTION - should call replay
        execution_count = 1
        await parallel_handler(
            callables, execution_state, parallel_context, operation_identifier
        )()

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
@patch("async_durable_execution._primitive.child.deserialize")
@patch("async_durable_execution._primitive.child.serialize")
@no_type_check
async def test_parallel_item_serialize(
    mock_serialize, mock_deserialize, item_serdes, batch_serdes
) -> None:
    """Test parallel serializes branches with item_serdes or fallback."""
    mock_serialize.return_value = '"serialized"'
    mock_deserialize.return_value = "deserialized"

    parent_checkpoint = Operation(
        operation_id="parent",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.STARTED,
    )

    def child_checkpoint_for(op_id: str) -> Any:
        return Operation(
            operation_id=op_id,
            operation_type=OperationType.CONTEXT,
            status=OperationStatus.STARTED,
        )

    def get_checkpoint(op_id) -> Any:
        return child_checkpoint_for(op_id) if op_id.startswith("child-") else None

    mock_state = Mock()
    mock_state.durable_execution_arn = "arn:test"
    mock_state.operations = Mock()
    mock_state.operations.get = Mock(side_effect=get_checkpoint)
    mock_state.create_checkpoint = AsyncMock()

    context_map = {}

    def create_id(self, i) -> str:
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
        context_module.OperationIdGenerator,
        "_create_step_id_for_logical_step",
        create_id,
    ):
        context = create_test_context(state=mock_state)

        async def branch_a() -> str:
            return "a"

        async def branch_b() -> str:
            return "b"

        await run_with_context(
            context,
            lambda: parallel(
                [branch_a, branch_b],
                serdes=batch_serdes,
                item_serdes=item_serdes,
            ),
        )

    expected = item_serdes or batch_serdes
    calls_by_operation_id = _mock_call_kwargs_by_operation_id(mock_serialize)

    assert set(calls_by_operation_id) == {"child-0", "child-1", "parent"}
    assert calls_by_operation_id["child-0"]["serdes"] is expected
    assert calls_by_operation_id["child-1"]["serdes"] is expected
    expected_parent_serdes = (
        batch_serdes if batch_serdes is not None else _BATCH_RESULT_SERDES
    )
    assert calls_by_operation_id["parent"]["serdes"] is expected_parent_serdes


@pytest.mark.parametrize(
    ("item_serdes", "batch_serdes"),
    [
        (Mock(), Mock()),
        (None, Mock()),
        (Mock(), None),
    ],
)
@patch("async_durable_execution._primitive.child.deserialize")
@no_type_check
async def test_parallel_item_deserialize(
    mock_deserialize, item_serdes, batch_serdes
) -> None:
    """Test parallel deserializes branches with item_serdes or fallback."""
    mock_deserialize.return_value = "deserialized"
    if batch_serdes is not None:
        batch_serdes.serialize = AsyncMock(return_value='"serialized"')

    parent_checkpoint = Mock()
    parent_checkpoint.is_succeeded.return_value = False
    parent_checkpoint.is_failed.return_value = False
    parent_checkpoint.is_existent.return_value = False

    def child_checkpoint_for(op_id: str) -> Any:
        return Operation(
            operation_id=op_id,
            operation_type=OperationType.CONTEXT,
            status=OperationStatus.SUCCEEDED,
            context_details=ContextDetails(result='"cached"'),
        )

    def get_checkpoint(op_id) -> Any:
        return child_checkpoint_for(op_id) if op_id.startswith("child-") else None

    mock_state = Mock()
    mock_state.durable_execution_arn = "arn:test"
    mock_state.operations = Mock()
    mock_state.operations.get = Mock(side_effect=get_checkpoint)
    mock_state.create_checkpoint = AsyncMock()

    context_map = {}

    def create_id(self, i) -> str:
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
        context_module.OperationIdGenerator,
        "_create_step_id_for_logical_step",
        create_id,
    ):
        context = create_test_context(state=mock_state)

        async def branch_a() -> str:
            return "a"

        async def branch_b() -> str:
            return "b"

        await run_with_context(
            context,
            lambda: parallel(
                [branch_a, branch_b],
                serdes=batch_serdes,
                item_serdes=item_serdes,
            ),
        )

    expected = item_serdes or batch_serdes
    calls_by_operation_id = {
        operation_id: kwargs
        for operation_id, kwargs in _mock_call_kwargs_by_operation_id(
            mock_deserialize
        ).items()
        if operation_id.startswith("child-")
    }

    assert set(calls_by_operation_id) == {"child-0", "child-1"}
    assert calls_by_operation_id["child-0"]["serdes"] is expected
    assert calls_by_operation_id["child-1"]["serdes"] is expected


@no_type_check
async def test_parallel_result_serialization_roundtrip() -> None:
    """Test that parallel operation BatchResult can be serialized and deserialized."""

    async def func1() -> Any:
        return [1, 2, 3]

    async def func2() -> Any:
        return {"status": "complete", "count": 42}

    async def func3() -> str:
        return "simple string"

    callables = [func1, func2, func3]

    execution_state = create_mock_execution_state()
    execution_state.durable_execution_arn = "arn:test"
    execution_state.operations.get.return_value = Mock(
        is_succeeded=Mock(return_value=False),
        is_failed=Mock(return_value=False),
        is_existent=Mock(return_value=False),
        is_replay_children=Mock(return_value=False),
    )
    parallel_context = Mock()
    parallel_context.step_counter = Mock()
    parallel_context._step_counter._create_step_id_for_logical_step = Mock(  # noqa: SLF001
        side_effect=["1", "2", "3"]
    )
    parallel_context.step_counter._create_step_id_for_logical_step = Mock(
        side_effect=["1", "2", "3"]
    )
    child_context = create_mock_child_context(execution_state)
    parallel_context.create_child_context = Mock(return_value=child_context)
    operation_identifier = OperationIdentifier(
        "test_op", OperationSubType.PARALLEL, "parent", "test_parallel"
    )

    # Execute parallel
    result = await parallel_handler(
        callables,
        execution_state,
        parallel_context,
        operation_identifier,
    )()

    # Serialize the BatchResult
    serialized = json.dumps(result.to_dict())

    # Deserialize
    deserialized = BatchResult.from_dict(json.loads(serialized))

    # Verify all data preserved
    assert len(deserialized.all) == 3
    assert deserialized.all[0].result == [1, 2, 3]
    assert deserialized.all[1].result == {"status": "complete", "count": 42}
    assert deserialized.all[2].result == "simple string"
    assert deserialized.completion_reason == result.completion_reason
    assert all(item.status == BatchItemStatus.SUCCEEDED for item in deserialized.all)


async def test_batch_result_serdes_owns_type_roundtrip() -> None:
    """BatchResult serialization is implemented by the parallel operation."""
    nested = BatchResult(
        all=[BatchItem(0, BatchItemStatus.SUCCEEDED, result=b"nested")],
        completion_reason=CompletionReason.ALL_COMPLETED,
    )
    reserved_key_dict = {
        "__async_durable_execution_batch_result__": "user value",
        "nested": nested,
    }
    result = BatchResult(
        all=[BatchItem(0, BatchItemStatus.SUCCEEDED, result=reserved_key_dict)],
        completion_reason=CompletionReason.ALL_COMPLETED,
    )

    serdes = _BatchResultSerDes()
    serialized = await serdes.serialize(result)
    restored = await serdes.deserialize(serialized)

    assert json.loads(serialized)["t"] == "br"
    assert isinstance(restored, BatchResult)
    assert restored == result
    restored_value = restored.all[0].result
    assert isinstance(restored_value, dict)
    restored_nested = restored_value["nested"]
    assert isinstance(restored_nested, BatchResult)


@no_type_check
async def test_core_serdes_does_not_know_batch_result() -> None:
    """Generic core serialization does not depend on operation-specific types."""
    result = BatchResult(
        all=[],
        completion_reason=CompletionReason.ALL_COMPLETED,
    )

    with pytest.raises(SerDesError, match="Unsupported type"):
        await ExtendedTypeSerDes().serialize(result)


@no_type_check
async def test_batch_result_serdes_preserves_legacy_wire_format() -> None:
    """Existing BatchResult checkpoints remain readable and stable."""
    legacy_payload = (
        '{"t":"br","v":{"all":{"t":"l","v":[]},'
        '"completionReason":{"t":"s","v":"ALL_COMPLETED"}}}'
    )
    expected = BatchResult(
        all=[],
        completion_reason=CompletionReason.ALL_COMPLETED,
    )

    serdes = _BatchResultSerDes()

    assert await serdes.deserialize(legacy_payload) == expected
    assert await serdes.serialize(expected) == legacy_payload


@no_type_check
async def test_parallel_handler_serializes_batch_result() -> None:
    """Verify parallel_handler serializes BatchResult at parent level."""
    try:
        with (
            patch("async_durable_execution._core.serialize") as mock_serdes_serialize,
            patch("async_durable_execution._core.deserialize") as mock_deserialize,
        ):
            configure_mock_child_serdes_roundtrip(
                mock_serdes_serialize, mock_deserialize
            )
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

            def get_checkpoint(op_id) -> None:
                return None

            mock_state = Mock()
            mock_state.durable_execution_arn = "arn:test"
            mock_state.operations = Mock()
            mock_state.operations.get = Mock(side_effect=get_checkpoint)
            mock_state.create_checkpoint = AsyncMock()

            context_map = {}

            def create_id(self, i) -> str:
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
                context_module.OperationIdGenerator,
                "_create_step_id_for_logical_step",
                create_id,
            ):
                context = create_test_context(state=mock_state)

                async def branch_a() -> str:
                    return "a"

                async def branch_b() -> str:
                    return "b"

                result = await run_with_context(
                    context, lambda: parallel([branch_a, branch_b])
                )

            assert len(mock_serdes_serialize.call_args_list) == 3
            parent_call = mock_serdes_serialize.call_args_list[2]
            assert parent_call[1]["value"] is result
    finally:
        importlib.reload(child)


@no_type_check
async def test_parallel_default_serdes_serializes_batch_result() -> None:
    """Verify default serdes automatically serializes BatchResult."""
    try:
        with patch(
            "async_durable_execution._core.serialize", wraps=serialize
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

            def get_checkpoint(op_id) -> None:
                return None

            mock_state = Mock()
            mock_state.durable_execution_arn = "arn:test"
            mock_state.operations = Mock()
            mock_state.operations.get = Mock(side_effect=get_checkpoint)
            mock_state.create_checkpoint = AsyncMock()

            context_map = {}

            def create_id(self, i) -> str:
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
                context_module.OperationIdGenerator,
                "_create_step_id_for_logical_step",
                create_id,
            ):
                context = create_test_context(state=mock_state)

                async def branch_a() -> str:
                    return "a"

                async def branch_b() -> str:
                    return "b"

                result = await run_with_context(
                    context, lambda: parallel([branch_a, branch_b])
                )

            assert isinstance(result, BatchResult)
            assert len(mock_serialize.call_args_list) == 3
            parent_call = mock_serialize.call_args_list[2]
            assert parent_call[1]["serdes"] is _BATCH_RESULT_SERDES
            assert isinstance(parent_call[1]["value"], BatchResult)
            assert parent_call[1]["value"] == result
    finally:
        importlib.reload(child)


@no_type_check
async def test_parallel_custom_serdes_serializes_batch_result() -> None:
    """Verify custom serdes is used for BatchResult serialization."""

    custom_serdes = CustomStrSerDes()

    try:
        with (
            patch("async_durable_execution._core.serialize") as mock_serialize,
            patch("async_durable_execution._core.deserialize") as mock_deserialize,
        ):
            configure_mock_child_serdes_roundtrip(mock_serialize, mock_deserialize)
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

            def get_checkpoint(op_id) -> None:
                return None

            mock_state = Mock()
            mock_state.durable_execution_arn = "arn:test"
            mock_state.operations = Mock()
            mock_state.operations.get = Mock(side_effect=get_checkpoint)
            mock_state.create_checkpoint = AsyncMock()

            context_map = {}

            def create_id(self, i) -> str:
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
                context_module.OperationIdGenerator,
                "_create_step_id_for_logical_step",
                create_id,
            ):
                context = create_test_context(state=mock_state)

                async def branch_a() -> str:
                    return "a"

                async def branch_b() -> str:
                    return "b"

                result = await run_with_context(
                    context,
                    lambda: parallel(
                        [branch_a, branch_b],
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


async def test_parallel_executor_get_iteration_name_default() -> None:
    """Branches use default 'parallel-branch-{index}' naming."""

    async def branch_a() -> str:
        return "a"

    async def branch_b() -> str:
        return "b"

    async def branch_c() -> str:
        return "c"

    executor = create_parallel_executor(
        executables=[
            Executable(index=0, func=branch_a),
            Executable(index=1, func=branch_b),
            Executable(index=2, func=branch_c),
        ],
        max_concurrency=None,
        completion_config=CompletionConfig.all_successful(),
        top_level_sub_type=OperationSubType.PARALLEL,
        iteration_sub_type=OperationSubType.PARALLEL_BRANCH,
        name_prefix="parallel-branch-",
        serdes=None,
    )

    assert executor.get_iteration_name(0) == "parallel-branch-0"
    assert executor.get_iteration_name(1) == "parallel-branch-1"
    assert executor.get_iteration_name(2) == "parallel-branch-2"


async def test_parallel_executor_execute_item_with_bound_durable_callable() -> None:
    """Bound durable_callables work correctly in execute_item."""

    @durable_callable
    async def branch_func(value: str) -> str:
        return f"result-{value}"

    executable = Executable(index=0, func=branch_func("bound"))

    executor = create_parallel_executor(
        executables=[executable],
        max_concurrency=None,
        completion_config=CompletionConfig.all_successful(),
        top_level_sub_type=OperationSubType.PARALLEL,
        iteration_sub_type=OperationSubType.PARALLEL_BRANCH,
        name_prefix="parallel-branch-",
        serdes=None,
    )

    result = await executor.execute_item("test-ctx", executable)
    assert result == "result-bound"
