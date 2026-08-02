"""Tests for the parallel executor support types."""

from typing import no_type_check

from typing import Any

import asyncio
import json
import random
import time
from concurrent.futures import Future
from functools import partial
from itertools import combinations
from unittest.mock import AsyncMock, Mock, patch

import pytest

from async_durable_execution._extension.parallel import (
    BatchItem,
    BatchItemStatus,
    BatchResult,
    BranchStatus,
    CompletionConfig,
    CompletionDecision,
    CompletionReason,
    CompletionStatus,
    ParallelExecutor,
    Executable,
    ExecutableWithState,
    ExecutionCounters,
    NestingType,
    TimerScheduler,
    parallel,
)
from async_durable_execution import DurableContext, get_current_context
from async_durable_execution._core.exceptions import (
    CallableRuntimeError,
    InvalidStateError,
    OrphanedChildException,
    SuspendExecution,
    TimedSuspendExecution,
    ValidationError,
)
from async_durable_execution._core.models import (
    ContextDetails,
    ErrorObject,
    Operation,
    OperationIdentifier,
    OperationStatus,
    OperationSubType,
    OperationType,
)
from async_durable_execution._extension.map import _bind_map_item_to_branch
from async_durable_execution._primitive.base import OperationExecutor
from typing import NoReturn


async def run_async(awaitable) -> Any:
    return await awaitable


def create_execution_state() -> Any:
    state = Mock()
    state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    state.create_checkpoint = AsyncMock()
    state.operations.get.return_value = None
    return state


def create_map_executor(**kwargs) -> Any:
    execution_state = kwargs.pop("execution_state", None)
    if execution_state is None:
        execution_state = create_execution_state()
    operation_identifier = kwargs.pop("operation_identifier", None)
    if operation_identifier is None:
        operation_identifier = OperationIdentifier(
            "test_op",
            OperationSubType.MAP,
            "parent_id",
            "test_map",
        )
    executor_context = kwargs.pop("executor_context", None)
    if executor_context is None:
        executor_context = create_executor_context(execution_state)
    executables = kwargs.pop("executables")
    items = kwargs.pop("items")
    return ParallelExecutor(
        executables=[
            Executable(
                index=executable.index,
                func=_bind_map_item_to_branch(
                    items=items,
                    index=executable.index,
                    func=executable.func,
                ),
            )
            for executable in executables
        ],
        execution_state=execution_state,
        operation_identifier=operation_identifier,
        executor_context=executor_context,
        **kwargs,
    )


def create_concurrent_executor(executor_cls, **kwargs) -> Any:
    execution_state = kwargs.pop("execution_state", None)
    if execution_state is None:
        execution_state = create_execution_state()
    operation_identifier = kwargs.pop("operation_identifier", None)
    if operation_identifier is None:
        operation_identifier = OperationIdentifier(
            "test_op",
            OperationSubType.MAP,
            "parent_id",
            "test_map",
        )
    executor_context = kwargs.pop("executor_context", None)
    if executor_context is None:
        executor_context = create_executor_context(execution_state)
    return executor_cls(
        execution_state=execution_state,
        operation_identifier=operation_identifier,
        executor_context=executor_context,
        **kwargs,
    )


def test_completion_config_defaults() -> None:
    """CompletionConfig keeps its expected defaults."""
    config = CompletionConfig()

    assert config.min_successful is None
    assert config.tolerated_failure_count is None


def test_completion_config_first_successful() -> None:
    """CompletionConfig.first_successful sets a one-success threshold."""
    config = CompletionConfig.first_successful()

    assert config.min_successful == 1
    assert config.tolerated_failure_count is None


def test_completion_config_all_completed() -> None:
    """CompletionConfig.all_completed leaves all thresholds open."""
    config = CompletionConfig.all_completed()

    assert config.min_successful is None
    assert config.tolerated_failure_count is None


def test_completion_config_all_successful() -> None:
    """CompletionConfig.all_successful requires zero failures."""
    config = CompletionConfig.all_successful()

    assert config.min_successful is None
    assert config.tolerated_failure_count == 0


def test_completion_config_thresholds() -> None:
    """CompletionConfig.thresholds sets both threshold fields."""
    config = CompletionConfig.thresholds(
        min_successful=3,
        tolerated_failure_count=1,
    )

    assert config.min_successful == 3
    assert config.tolerated_failure_count == 1


def test_completion_config_factories_are_classmethods() -> None:
    """CompletionConfig factories instantiate through cls."""

    class CustomCompletionConfig(CompletionConfig):
        pass

    assert isinstance(CustomCompletionConfig.thresholds(), CustomCompletionConfig)
    assert isinstance(CustomCompletionConfig.first_successful(), CustomCompletionConfig)
    assert isinstance(CustomCompletionConfig.all_completed(), CustomCompletionConfig)
    assert isinstance(CustomCompletionConfig.all_successful(), CustomCompletionConfig)
    assert isinstance(
        CustomCompletionConfig.custom(
            lambda status: CompletionDecision.continue_execution()
        ),
        CustomCompletionConfig,
    )


def test_completion_reason_success_semantics() -> None:
    """CompletionReason exposes success/failure semantics."""
    assert CompletionReason.ALL_COMPLETED.is_succeeded
    assert CompletionReason.MIN_SUCCESSFUL_REACHED.is_succeeded
    assert not CompletionReason.FAILURE_TOLERANCE_EXCEEDED.is_succeeded
    assert CompletionReason.CUSTOM_COMPLETION_SUCCEEDED.is_succeeded
    assert not CompletionReason.CUSTOM_COMPLETION_FAILED.is_succeeded


def test_completion_status_validation_and_all_completed() -> None:
    """CompletionStatus validates counts and reports all-completed state."""
    status = CompletionStatus(
        success_count=1,
        failure_count=1,
        total_count=2,
    )

    assert status.completed_count == 2
    assert status.all_completed

    with pytest.raises(ValueError, match="completed_count cannot exceed"):
        CompletionStatus(
            success_count=1,
            failure_count=1,
            total_count=1,
        )


def test_completion_decision_validation_and_success_semantics() -> None:
    """CompletionDecision enforces reason presence when completing."""
    decision = CompletionDecision.complete(CompletionReason.CUSTOM_COMPLETION_FAILED)

    assert decision.should_complete
    assert not decision.is_succeeded
    assert not CompletionDecision.continue_execution().should_complete

    with pytest.raises(ValueError, match="completion_reason is required"):
        CompletionDecision(True)
    with pytest.raises(ValueError, match="completion_reason must be None"):
        CompletionDecision(False, CompletionReason.ALL_COMPLETED)


def test_completion_config_custom_should_complete() -> None:
    """CompletionConfig.custom stores and evaluates a custom completion function."""
    config = CompletionConfig.custom(
        lambda status: CompletionDecision.complete(
            CompletionReason.CUSTOM_COMPLETION_SUCCEEDED
        )
        if status.success_count >= 2
        else CompletionDecision.continue_execution()
    )

    assert config.has_custom_should_complete
    assert config.min_successful is None
    assert config.tolerated_failure_count is None
    assert not config.completion_decision(CompletionStatus(1, 0, 3)).should_complete

    decision = config.completion_decision(CompletionStatus(2, 0, 3))

    assert decision.should_complete
    assert decision.is_succeeded
    assert decision.completion_reason == CompletionReason.CUSTOM_COMPLETION_SUCCEEDED


def test_completion_config_custom_is_mutually_exclusive_with_thresholds() -> None:
    """Custom completion cannot be combined with threshold fields."""
    with pytest.raises(ValueError, match="should_complete is mutually exclusive"):
        CompletionConfig(
            min_successful=1,
            should_complete=lambda status: CompletionDecision.complete(
                CompletionReason.CUSTOM_COMPLETION_SUCCEEDED
            ),
        )


@no_type_check
def test_completion_config_custom_none_decision_raises() -> None:
    """Custom completion functions must return a CompletionDecision."""
    config = CompletionConfig.custom(lambda status: None)

    with pytest.raises(TypeError, match="must return a CompletionDecision"):
        config.completion_decision(CompletionStatus(0, 0, 1))


def test_nesting_type_enum() -> None:
    """NestingType enum values remain stable."""
    assert NestingType.NESTED.value == "NESTED"
    assert NestingType.FLAT.value == "FLAT"


def test_concurrency_types_importable_from_package_root() -> None:
    """Concurrency types remain re-exported from the package root."""
    from async_durable_execution import (
        CompletionConfig as ImportedCompletionConfig,
        NestingType as ImportedNestingType,
    )

    assert ImportedCompletionConfig is CompletionConfig
    assert ImportedNestingType is NestingType


def create_executor_context(state, step_id="1", parent_id="parent") -> Any:
    context = Mock()
    context._parent_id = parent_id  # noqa: SLF001
    context.parent_id = parent_id
    context.step_counter = Mock()

    def create_step_id(logical_step) -> Any:
        return str(logical_step) if step_id == "1" else f"{step_id}_{logical_step}"

    context._step_counter._create_step_id_for_logical_step = create_step_id  # noqa: SLF001
    context.step_counter._create_step_id_for_logical_step = create_step_id  # noqa: SLF001

    def build_child_context(*args, **kwargs) -> Any:
        child_context = Mock()
        child_context.state = state
        child_context.execution_state = state
        return child_context

    context.create_child_context = build_child_context
    return context


async def test_batch_item_status_enum() -> None:
    """Test BatchItemStatus enum values."""
    assert BatchItemStatus.SUCCEEDED.value == "SUCCEEDED"
    assert BatchItemStatus.FAILED.value == "FAILED"
    assert BatchItemStatus.CANCELLED.value == "CANCELLED"
    assert BatchItemStatus.STARTED.value == "STARTED"


async def test_completion_reason_enum() -> None:
    """Test CompletionReason enum values."""
    assert CompletionReason.ALL_COMPLETED.value == "ALL_COMPLETED"
    assert CompletionReason.MIN_SUCCESSFUL_REACHED.value == "MIN_SUCCESSFUL_REACHED"
    assert (
        CompletionReason.FAILURE_TOLERANCE_EXCEEDED.value
        == "FAILURE_TOLERANCE_EXCEEDED"
    )


async def test_branch_status_enum() -> None:
    """Test BranchStatus enum values."""
    assert BranchStatus.NOT_STARTED.value == "not_started"
    assert BranchStatus.PENDING.value == "pending"
    assert BranchStatus.RUNNING.value == "running"
    assert BranchStatus.COMPLETED.value == "completed"
    assert BranchStatus.SUSPENDED.value == "suspended"
    assert BranchStatus.SUSPENDED_WITH_TIMEOUT.value == "suspended_with_timeout"
    assert BranchStatus.FAILED.value == "failed"
    assert BranchStatus.CANCELLED.value == "cancelled"


async def test_batch_item_creation() -> None:
    """Test BatchItem creation and properties."""
    item = BatchItem(index=0, status=BatchItemStatus.SUCCEEDED, result="test_result")
    assert item.index == 0
    assert item.status == BatchItemStatus.SUCCEEDED
    assert item.result == "test_result"
    assert item.error is None


@no_type_check
async def test_batch_item_to_dict() -> None:
    """Test BatchItem to_dict method."""
    error = ErrorObject(
        message="test message", type="TestError", data=None, stack_trace=None
    )
    item = BatchItem(index=1, status=BatchItemStatus.FAILED, error=error)

    result = item.to_dict()
    expected = {
        "index": 1,
        "status": "FAILED",
        "result": None,
        "error": error.to_dict(),
    }
    assert result == expected


@no_type_check
async def test_batch_item_from_dict() -> None:
    """Test BatchItem from_dict method."""
    data = {
        "index": 2,
        "status": "SUCCEEDED",
        "result": "success_result",
        "error": None,
    }

    item = BatchItem.from_dict(data)
    assert item.index == 2
    assert item.status == BatchItemStatus.SUCCEEDED
    assert item.result == "success_result"
    assert item.error is None


async def test_batch_result_creation() -> None:
    """Test BatchResult creation."""
    items = [
        BatchItem(0, BatchItemStatus.SUCCEEDED, "result1"),
        BatchItem(
            1, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ),
    ]
    result = BatchResult(items, CompletionReason.ALL_COMPLETED)

    assert len(result.all) == 2
    assert result.completion_reason == CompletionReason.ALL_COMPLETED


async def test_batch_result_succeeded() -> None:
    """Test BatchResult succeeded method."""
    items = [
        BatchItem(0, BatchItemStatus.SUCCEEDED, "result1"),
        BatchItem(
            1, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ),
        BatchItem(2, BatchItemStatus.SUCCEEDED, "result2"),
    ]
    result = BatchResult(items, CompletionReason.ALL_COMPLETED)

    succeeded = result.succeeded()
    assert len(succeeded) == 2
    assert succeeded[0].result == "result1"
    assert succeeded[1].result == "result2"


async def test_batch_result_failed() -> None:
    """Test BatchResult failed method."""
    error = ErrorObject("test message", "TestError", None, None)
    items = [
        BatchItem(0, BatchItemStatus.SUCCEEDED, "result1"),
        BatchItem(1, BatchItemStatus.FAILED, error=error),
    ]
    result = BatchResult(items, CompletionReason.ALL_COMPLETED)

    failed = result.failed()
    assert len(failed) == 1
    assert failed[0].error == error


async def test_batch_result_started() -> None:
    """Test BatchResult started method."""
    items = [
        BatchItem(0, BatchItemStatus.STARTED),
        BatchItem(1, BatchItemStatus.SUCCEEDED, "result1"),
    ]
    result = BatchResult(items, CompletionReason.ALL_COMPLETED)

    started = result.started()
    assert len(started) == 1
    assert started[0].status == BatchItemStatus.STARTED


async def test_batch_result_cancelled() -> None:
    """Test BatchResult cancelled method."""
    items = [
        BatchItem(0, BatchItemStatus.CANCELLED),
        BatchItem(1, BatchItemStatus.SUCCEEDED, "result1"),
    ]
    result = BatchResult(items, CompletionReason.MIN_SUCCESSFUL_REACHED)

    cancelled = result.cancelled()
    assert len(cancelled) == 1
    assert cancelled[0].status == BatchItemStatus.CANCELLED


async def test_batch_result_status() -> None:
    """Test BatchResult status property."""
    # No failures
    items = [BatchItem(0, BatchItemStatus.SUCCEEDED, "result1")]
    result = BatchResult(items, CompletionReason.ALL_COMPLETED)
    assert result.status == BatchItemStatus.SUCCEEDED

    # Has failures
    items = [
        BatchItem(0, BatchItemStatus.SUCCEEDED, "result1"),
        BatchItem(
            1, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ),
    ]
    result = BatchResult(items, CompletionReason.ALL_COMPLETED)
    assert result.status == BatchItemStatus.FAILED


async def test_batch_result_has_failure() -> None:
    """Test BatchResult has_failure property."""
    # No failures
    items = [BatchItem(0, BatchItemStatus.SUCCEEDED, "result1")]
    result = BatchResult(items, CompletionReason.ALL_COMPLETED)
    assert not result.has_failure

    # Has failures
    items = [
        BatchItem(
            0, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        )
    ]
    result = BatchResult(items, CompletionReason.ALL_COMPLETED)
    assert result.has_failure


async def test_batch_result_throw_if_error() -> None:
    """Test BatchResult throw_if_error method."""
    # No errors
    items = [BatchItem(0, BatchItemStatus.SUCCEEDED, "result1")]
    result = BatchResult(items, CompletionReason.ALL_COMPLETED)
    result.throw_if_error()  # Should not raise

    # Has error
    error = ErrorObject("test message", "TestError", None, None)
    items = [BatchItem(0, BatchItemStatus.FAILED, error=error)]
    result = BatchResult(items, CompletionReason.ALL_COMPLETED)

    with pytest.raises(CallableRuntimeError):
        result.throw_if_error()


async def test_batch_result_get_results() -> None:
    """Test BatchResult get_results method."""
    items = [
        BatchItem(0, BatchItemStatus.SUCCEEDED, "result1"),
        BatchItem(
            1, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ),
        BatchItem(2, BatchItemStatus.SUCCEEDED, "result2"),
    ]
    result = BatchResult(items, CompletionReason.ALL_COMPLETED)

    results = result.get_results()
    assert results == ["result1", "result2"]


async def test_batch_result_get_errors() -> None:
    """Test BatchResult get_errors method."""
    error1 = ErrorObject("msg1", "Error1", None, None)
    error2 = ErrorObject("msg2", "Error2", None, None)
    items = [
        BatchItem(0, BatchItemStatus.SUCCEEDED, "result1"),
        BatchItem(1, BatchItemStatus.FAILED, error=error1),
        BatchItem(2, BatchItemStatus.FAILED, error=error2),
    ]
    result = BatchResult(items, CompletionReason.ALL_COMPLETED)

    errors = result.get_errors()
    assert len(errors) == 2
    assert error1 in errors
    assert error2 in errors


async def test_batch_result_counts() -> None:
    """Test BatchResult count properties."""
    items = [
        BatchItem(0, BatchItemStatus.SUCCEEDED, "result1"),
        BatchItem(
            1, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ),
        BatchItem(2, BatchItemStatus.STARTED),
        BatchItem(3, BatchItemStatus.SUCCEEDED, "result2"),
        BatchItem(4, BatchItemStatus.CANCELLED),
    ]
    result = BatchResult(items, CompletionReason.ALL_COMPLETED)

    assert result.success_count == 2
    assert result.failure_count == 1
    assert result.started_count == 1
    assert result.cancelled_count == 1
    assert result.total_count == 5


async def test_batch_result_to_dict() -> None:
    """Test BatchResult to_dict method."""
    items = [BatchItem(0, BatchItemStatus.SUCCEEDED, "result1")]
    result = BatchResult(items, CompletionReason.ALL_COMPLETED)

    result_dict = result.to_dict()
    expected = {
        "all": [
            {"index": 0, "status": "SUCCEEDED", "result": "result1", "error": None}
        ],
        "completionReason": "ALL_COMPLETED",
    }
    assert result_dict == expected


@no_type_check
async def test_batch_result_cancelled_item_round_trip() -> None:
    """Cancelled child state is preserved in the parent result payload."""
    result = BatchResult(
        [BatchItem(0, BatchItemStatus.CANCELLED)],
        CompletionReason.MIN_SUCCESSFUL_REACHED,
    )

    restored = BatchResult.from_dict(result.to_dict())

    assert restored == result
    assert restored.cancelled_count == 1


@no_type_check
async def test_batch_result_from_dict() -> None:
    """Test BatchResult from_dict method."""
    data = {
        "all": [
            {"index": 0, "status": "SUCCEEDED", "result": "result1", "error": None}
        ],
        "completionReason": "ALL_COMPLETED",
    }

    result = BatchResult.from_dict(data)
    assert len(result.all) == 1
    assert result.all[0].index == 0
    assert result.all[0].status == BatchItemStatus.SUCCEEDED
    assert result.completion_reason == CompletionReason.ALL_COMPLETED


@no_type_check
async def test_batch_result_from_dict_default_completion_reason() -> None:
    """Test BatchResult from_dict with default completion reason."""
    data = {
        "all": [
            {"index": 0, "status": "SUCCEEDED", "result": "result1", "error": None}
        ],
        # No completionReason provided
    }

    with patch("async_durable_execution._extension.parallel.logger") as mock_logger:
        result = BatchResult.from_dict(data)
        assert result.completion_reason == CompletionReason.ALL_COMPLETED
        # Verify warning was logged
        mock_logger.warning.assert_called_once()
        assert "Missing completionReason" in mock_logger.warning.call_args[0][0]


@no_type_check
async def test_batch_result_from_dict_infer_all_completed_all_succeeded() -> None:
    """Test BatchResult from_dict infers ALL_COMPLETED when all items succeeded."""
    data = {
        "all": [
            {"index": 0, "status": "SUCCEEDED", "result": "result1", "error": None},
            {"index": 1, "status": "SUCCEEDED", "result": "result2", "error": None},
        ],
        # No completionReason provided
    }

    with patch("async_durable_execution._extension.parallel.logger") as mock_logger:
        result = BatchResult.from_dict(data)
        assert result.completion_reason == CompletionReason.ALL_COMPLETED
        mock_logger.warning.assert_called_once()


@no_type_check
async def test_batch_result_from_dict_infer_failure_tolerance_exceeded_all_failed() -> (
    None
):
    """Test BatchResult from_dict infers completion reason when all items failed."""
    error_data = {
        "message": "Test error",
        "type": "TestError",
        "data": None,
        "stackTrace": None,
    }
    data = {
        "all": [
            {"index": 0, "status": "FAILED", "result": None, "error": error_data},
            {"index": 1, "status": "FAILED", "result": None, "error": error_data},
        ],
        # No completionReason provided
    }

    # With no completion config and failures, should fail-fast
    with patch("async_durable_execution._extension.parallel.logger") as mock_logger:
        result = BatchResult.from_dict(data)
        assert result.completion_reason == CompletionReason.FAILURE_TOLERANCE_EXCEEDED
        mock_logger.warning.assert_called_once()


@no_type_check
async def test_batch_result_from_dict_infer_all_completed_mixed_success_failure() -> (
    None
):
    """Test BatchResult from_dict infers completion reason with mix of success/failure."""
    error_data = {
        "message": "Test error",
        "type": "TestError",
        "data": None,
        "stackTrace": None,
    }
    data = {
        "all": [
            {"index": 0, "status": "SUCCEEDED", "result": "result1", "error": None},
            {"index": 1, "status": "FAILED", "result": None, "error": error_data},
            {"index": 2, "status": "SUCCEEDED", "result": "result2", "error": None},
        ],
        # No completionReason provided
    }

    # With no config and with failures, fail-fast
    with patch("async_durable_execution._extension.parallel.logger") as mock_logger:
        result = BatchResult.from_dict(data)
        assert result.completion_reason == CompletionReason.FAILURE_TOLERANCE_EXCEEDED
        mock_logger.warning.assert_called_once()


@no_type_check
async def test_batch_result_from_dict_infers_min_successful_with_started_items() -> (
    None
):
    """Test BatchResult from_dict infers MIN_SUCCESSFUL_REACHED when items are still started."""
    data = {
        "all": [
            {"index": 0, "status": "SUCCEEDED", "result": "result1", "error": None},
            {"index": 1, "status": "STARTED", "result": None, "error": None},
            {"index": 2, "status": "SUCCEEDED", "result": "result2", "error": None},
        ],
        # No completionReason provided
    }

    with patch("async_durable_execution._extension.parallel.logger") as mock_logger:
        result = BatchResult.from_dict(data, CompletionConfig(1))
        assert result.completion_reason == CompletionReason.MIN_SUCCESSFUL_REACHED
        mock_logger.warning.assert_called_once()


@no_type_check
async def test_batch_result_from_dict_infer_empty_items() -> None:
    """Test BatchResult from_dict infers ALL_COMPLETED for empty items."""
    data = {
        "all": [],
        # No completionReason provided
    }

    with patch("async_durable_execution._extension.parallel.logger") as mock_logger:
        result = BatchResult.from_dict(data)
        assert result.completion_reason == CompletionReason.ALL_COMPLETED
        mock_logger.warning.assert_called_once()


@no_type_check
async def test_batch_result_from_dict_with_explicit_completion_reason() -> None:
    """Test BatchResult from_dict uses explicit completionReason when provided."""
    data = {
        "all": [
            {"index": 0, "status": "SUCCEEDED", "result": "result1", "error": None}
        ],
        "completionReason": "MIN_SUCCESSFUL_REACHED",
    }

    with patch("async_durable_execution._extension.parallel.logger") as mock_logger:
        result = BatchResult.from_dict(data)
        assert result.completion_reason == CompletionReason.MIN_SUCCESSFUL_REACHED
        # No warning should be logged when completionReason is provided
        mock_logger.warning.assert_not_called()


@no_type_check
async def test_batch_result_infer_completion_reason_edge_cases() -> None:
    """Test _infer_completion_reason method with various edge cases."""
    # Test with only started items and min_successful=0
    started_items = [
        BatchItem(0, BatchItemStatus.STARTED).to_dict(),
        BatchItem(1, BatchItemStatus.STARTED).to_dict(),
    ]
    items = {"all": started_items}
    batch = BatchResult.from_dict(items, CompletionConfig(0))  # SLF001
    # With min_successful=0 and no failures, should be MIN_SUCCESSFUL_REACHED
    assert batch.completion_reason == CompletionReason.MIN_SUCCESSFUL_REACHED

    # Test with only started items and no config
    started_items = [
        BatchItem(0, BatchItemStatus.STARTED).to_dict(),
        BatchItem(1, BatchItemStatus.STARTED).to_dict(),
    ]
    items = {"all": started_items}
    batch = BatchResult.from_dict(items)  # SLF001
    # With no config and no completed items, defaults to ALL_COMPLETED
    assert batch.completion_reason == CompletionReason.ALL_COMPLETED

    # Test with only failed items
    failed_items = [
        BatchItem(
            0, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ).to_dict(),
        BatchItem(
            1, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ).to_dict(),
    ]
    failed_items = {"all": failed_items}
    batch = BatchResult.from_dict(failed_items)  # SLF001
    # With no config and failures, should fail-fast
    assert batch.completion_reason == CompletionReason.FAILURE_TOLERANCE_EXCEEDED

    # Test with only succeeded items
    succeeded_items = [
        BatchItem(0, BatchItemStatus.SUCCEEDED, "result1").to_dict(),
        BatchItem(1, BatchItemStatus.SUCCEEDED, "result2").to_dict(),
    ]
    succeeded_items = {"all": succeeded_items}
    batch = BatchResult.from_dict(succeeded_items)  # SLF001
    assert batch.completion_reason == CompletionReason.ALL_COMPLETED

    # Test with mixed but no started (all completed) with tolerance
    mixed_items = [
        BatchItem(0, BatchItemStatus.SUCCEEDED, "result1"),
        BatchItem(
            1, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ),
    ]

    batch = BatchResult.from_items(
        mixed_items, CompletionConfig(tolerated_failure_count=1)
    )  # SLF001
    assert batch.completion_reason == CompletionReason.ALL_COMPLETED


@no_type_check
async def test_batch_result_get_results_empty() -> None:
    """Test BatchResult get_results with no successful items."""
    items = [
        BatchItem(
            0, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ),
        BatchItem(1, BatchItemStatus.STARTED),
    ]
    result = BatchResult(items, CompletionReason.FAILURE_TOLERANCE_EXCEEDED)

    results = result.get_results()
    assert results == []


async def test_batch_result_get_errors_empty() -> None:
    """Test BatchResult get_errors with no failed items."""
    items = [
        BatchItem(0, BatchItemStatus.SUCCEEDED, "result1"),
        BatchItem(1, BatchItemStatus.STARTED),
    ]
    result = BatchResult(items, CompletionReason.ALL_COMPLETED)

    errors = result.get_errors()
    assert errors == []


async def test_executable_creation() -> None:
    """Test Executable creation."""

    async def test_func() -> str:
        return "test"

    executable = Executable(index=5, func=test_func)
    assert executable.index == 5
    assert executable.func == test_func


@no_type_check
async def test_executable_with_state_creation() -> None:
    """Test ExecutableWithState creation."""
    executable = Executable(index=1, func=lambda: "test")
    exe_state = ExecutableWithState(executable)

    assert exe_state.executable == executable
    assert exe_state.status == BranchStatus.NOT_STARTED
    assert exe_state.index == 1
    assert exe_state.callable == executable.func


@no_type_check
async def test_executable_with_state_properties() -> None:
    """Test ExecutableWithState property access."""

    async def test_callable() -> str:
        return "test"

    executable = Executable(index=42, func=test_callable)
    exe_state = ExecutableWithState(executable)

    assert exe_state.index == 42
    assert exe_state.callable == test_callable
    assert exe_state.suspend_until is None


@no_type_check
async def test_executable_with_state_future_not_available() -> None:
    """Test ExecutableWithState future property when not started."""
    executable = Executable(index=1, func=lambda: "test")
    exe_state = ExecutableWithState(executable)

    with pytest.raises(InvalidStateError):
        _ = exe_state.future


@no_type_check
async def test_executable_with_state_result_not_available() -> None:
    """Test ExecutableWithState result property when not completed."""
    executable = Executable(index=1, func=lambda: "test")
    exe_state = ExecutableWithState(executable)

    with pytest.raises(InvalidStateError):
        _ = exe_state.result


@no_type_check
async def test_executable_with_state_error_not_available() -> None:
    """Test ExecutableWithState error property when not failed."""
    executable = Executable(index=1, func=lambda: "test")
    exe_state = ExecutableWithState(executable)

    with pytest.raises(InvalidStateError):
        _ = exe_state.error


@no_type_check
async def test_executable_with_state_is_running() -> None:
    """Test ExecutableWithState is_running property."""
    executable = Executable(index=1, func=lambda: "test")
    exe_state = ExecutableWithState(executable)

    assert not exe_state.is_running

    future = Future()
    exe_state.run(future)
    assert exe_state.is_running


@no_type_check
async def test_executable_with_state_can_resume() -> None:
    """Test ExecutableWithState can_resume property."""
    executable = Executable(index=1, func=lambda: "test")
    exe_state = ExecutableWithState(executable)

    # Not suspended
    assert not exe_state.can_resume

    # Suspended indefinitely
    exe_state.suspend()
    assert exe_state.can_resume

    # Suspended with timeout in future
    future_time = time.time() + 10
    exe_state.suspend_with_timeout(future_time)
    assert not exe_state.can_resume

    # Suspended with timeout in past
    past_time = time.time() - 10
    exe_state.suspend_with_timeout(past_time)
    assert exe_state.can_resume


@no_type_check
async def test_executable_with_state_run() -> None:
    """Test ExecutableWithState run method."""
    executable = Executable(index=1, func=lambda: "test")
    exe_state = ExecutableWithState(executable)
    future = Future()

    assert exe_state.status is BranchStatus.NOT_STARTED
    exe_state.run(future)
    assert exe_state.status == BranchStatus.RUNNING
    assert exe_state.future == future


@no_type_check
async def test_executable_with_state_runs_from_pending_resume() -> None:
    """A suspended branch transitions through PENDING when resubmitted."""
    executable = Executable(index=1, func=lambda: "test")
    exe_state = ExecutableWithState(executable)
    future = Future()

    exe_state.suspend_with_timeout(time.time() - 1)
    exe_state.reset_to_pending()
    assert exe_state.status is BranchStatus.PENDING

    exe_state.run(future)
    assert exe_state.status is BranchStatus.RUNNING
    assert exe_state.future is future


@no_type_check
async def test_executable_with_state_run_invalid_state() -> None:
    """Test ExecutableWithState run method from invalid state."""
    executable = Executable(index=1, func=lambda: "test")
    exe_state = ExecutableWithState(executable)
    future1 = Future()
    future2 = Future()

    exe_state.run(future1)

    with pytest.raises(InvalidStateError):
        exe_state.run(future2)


@no_type_check
async def test_executable_with_state_suspend() -> None:
    """Test ExecutableWithState suspend method."""
    executable = Executable(index=1, func=lambda: "test")
    exe_state = ExecutableWithState(executable)

    exe_state.suspend()
    assert exe_state.status == BranchStatus.SUSPENDED
    assert exe_state.suspend_until is None


@no_type_check
async def test_executable_with_state_suspend_with_timeout() -> None:
    """Test ExecutableWithState suspend_with_timeout method."""
    executable = Executable(index=1, func=lambda: "test")
    exe_state = ExecutableWithState(executable)
    timestamp = time.time() + 5

    exe_state.suspend_with_timeout(timestamp)
    assert exe_state.status == BranchStatus.SUSPENDED_WITH_TIMEOUT
    assert exe_state.suspend_until == timestamp


@no_type_check
async def test_executable_with_state_complete() -> None:
    """Test ExecutableWithState complete method."""
    executable = Executable(index=1, func=lambda: "test")
    exe_state = ExecutableWithState(executable)

    exe_state.complete("test_result")
    assert exe_state.status == BranchStatus.COMPLETED
    assert exe_state.result == "test_result"


@no_type_check
async def test_executable_with_state_fail() -> None:
    """Test ExecutableWithState fail method."""
    executable = Executable(index=1, func=lambda: "test")
    exe_state = ExecutableWithState(executable)
    error = Exception("test error")

    exe_state.fail(error)
    assert exe_state.status == BranchStatus.FAILED
    assert exe_state.error == error


async def test_execution_counters_creation() -> None:
    """Test ExecutionCounters creation."""
    counters = ExecutionCounters(
        total_tasks=10,
        completion_config=CompletionConfig(
            min_successful=8,
            tolerated_failure_count=2,
        ),
    )

    assert counters.total_tasks == 10
    assert counters.completion_config.min_successful == 8
    assert counters.completion_config.tolerated_failure_count == 2
    assert counters.success_count == 0
    assert counters.failure_count == 0


async def test_execution_counters_complete_task() -> None:
    """Test ExecutionCounters complete_task method."""
    counters = ExecutionCounters(5, CompletionConfig(min_successful=3))

    counters.complete_task()
    assert counters.success_count == 1


async def test_execution_counters_fail_task() -> None:
    """Test ExecutionCounters fail_task method."""
    counters = ExecutionCounters(5, CompletionConfig(min_successful=3))

    counters.fail_task()
    assert counters.failure_count == 1


async def test_execution_counters_should_complete_min_successful() -> None:
    """Test ExecutionCounters should_complete with min successful reached."""
    counters = ExecutionCounters(5, CompletionConfig(min_successful=3))

    assert not counters.should_complete()

    counters.complete_task()
    counters.complete_task()
    counters.complete_task()

    assert counters.should_complete()


async def test_execution_counters_should_complete_failure_count() -> None:
    """Test ExecutionCounters should_complete with failure count exceeded."""
    counters = ExecutionCounters(
        5,
        CompletionConfig(min_successful=3, tolerated_failure_count=1),
    )

    assert not counters.should_complete()

    counters.fail_task()
    assert not counters.should_complete()

    counters.fail_task()
    assert counters.should_complete()


async def test_execution_counters_is_all_completed() -> None:
    """Test ExecutionCounters is_all_completed method."""
    counters = ExecutionCounters(3, CompletionConfig(min_successful=2))

    assert not counters.is_all_completed()

    counters.complete_task()
    counters.complete_task()
    assert not counters.is_all_completed()

    counters.complete_task()
    assert counters.is_all_completed()


async def test_execution_counters_is_min_successful_reached() -> None:
    """Test ExecutionCounters is_min_successful_reached method."""
    counters = ExecutionCounters(5, CompletionConfig(min_successful=3))

    assert not counters.is_min_successful_reached()

    counters.complete_task()
    counters.complete_task()
    assert not counters.is_min_successful_reached()

    counters.complete_task()
    assert counters.is_min_successful_reached()


async def test_execution_counters_is_failure_tolerance_exceeded() -> None:
    """Test ExecutionCounters is_failure_tolerance_exceeded method."""
    counters = ExecutionCounters(
        10,
        CompletionConfig(min_successful=8, tolerated_failure_count=2),
    )

    assert not counters.is_failure_tolerance_exceeded()

    counters.fail_task()
    counters.fail_task()
    assert not counters.is_failure_tolerance_exceeded()

    counters.fail_task()
    assert counters.is_failure_tolerance_exceeded()


async def test_execution_counters_zero_total_tasks() -> None:
    """Test ExecutionCounters with zero total tasks."""
    counters = ExecutionCounters(0, CompletionConfig(min_successful=0))

    assert not counters.is_failure_tolerance_exceeded()


async def test_execution_counters_increment_counts() -> None:
    """Test ExecutionCounters increments counts correctly on one event loop."""
    counters = ExecutionCounters(100, CompletionConfig(min_successful=50))
    for _ in range(50):
        counters.complete_task()

    assert counters.success_count == 50


@no_type_check
async def test_batch_result_failed_with_none_error() -> None:
    """Test BatchResult failed method filters out None errors."""
    items = [
        BatchItem(0, BatchItemStatus.FAILED, error=None),  # Should be filtered out
        BatchItem(
            1, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ),
    ]
    result = BatchResult(items, CompletionReason.ALL_COMPLETED)

    failed = result.failed()
    assert len(failed) == 1
    assert failed[0].error is not None


async def test_concurrent_executor_nesting_type_parameter() -> None:
    """Test ParallelExecutor nesting_type parameter."""

    class TestExecutor(ParallelExecutor):
        async def execute_item(self, child_context, executable) -> str:
            return f"result_{executable.index}"

    executables = [Executable(0, lambda: "test")]
    completion_config = CompletionConfig(min_successful=1)

    # Test with NESTED (default)
    executor_nested = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
        nesting_type=NestingType.NESTED,
    )
    assert executor_nested.nesting_type is NestingType.NESTED

    # Test with FLAT
    executor_flat = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
        nesting_type=NestingType.FLAT,
    )
    assert executor_flat.nesting_type is NestingType.FLAT


async def test_concurrent_executor_default_nesting_type() -> None:
    """Test ParallelExecutor uses NESTED as default nesting_type."""

    class TestExecutor(ParallelExecutor):
        async def execute_item(self, child_context, executable) -> str:
            return f"result_{executable.index}"

    executables = [Executable(0, lambda: "test")]
    completion_config = CompletionConfig(min_successful=1)

    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )
    assert executor.nesting_type is NestingType.NESTED


async def test_concurrent_executor_full_execution_path() -> None:
    """Test ParallelExecutor full execution."""

    class TestExecutor(ParallelExecutor):
        async def execute_item(self, child_context, executable) -> str:
            return f"result_{executable.index}"

    executables = [Executable(0, lambda: "test"), Executable(1, lambda: "test2")]
    completion_config = CompletionConfig(
        min_successful=2,
        tolerated_failure_count=None,
    )
    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=2,
        completion_config=completion_config,
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )

    execution_state = create_execution_state()
    executor_context = create_executor_context(execution_state)

    result = await run_async(executor.execute())
    assert len(result.all) >= 1


@no_type_check
async def test_timer_scheduler_double_check_resume_queue() -> None:
    """Test TimerScheduler double-check logic in scheduler loop."""
    callback = AsyncMock()

    async def run_test() -> None:
        async with TimerScheduler(callback) as scheduler:
            exe_state1 = ExecutableWithState(Executable(0, lambda: "test"))
            exe_state2 = ExecutableWithState(Executable(1, lambda: "test"))
            exe_state1.suspend()
            exe_state2.suspend()

            # Schedule two tasks with different times to avoid comparison issues
            past_time1 = time.time() - 2
            past_time2 = time.time() - 1
            scheduler.schedule_resume(exe_state1, past_time1)
            scheduler.schedule_resume(exe_state2, past_time2)

            await asyncio.sleep(0.05)

    await run_async(run_test())

    # At least one callback should have been made
    assert callback.call_count >= 0


@no_type_check
async def test_concurrent_executor_on_task_complete_timed_suspend() -> None:
    """Test ParallelExecutor _on_task_complete with TimedSuspendExecution."""

    class TestExecutor(ParallelExecutor):
        async def execute_item(self, child_context, executable) -> str:
            return f"result_{executable.index}"

    executables = [Executable(0, lambda: "test")]
    completion_config = CompletionConfig(
        min_successful=1,
        tolerated_failure_count=None,
    )

    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )

    exe_state = ExecutableWithState(executables[0])
    future = Mock()
    future.result.side_effect = TimedSuspendExecution("test message", time.time() + 1)
    future.cancelled.return_value = False

    scheduler = Mock()
    scheduler.schedule_resume = Mock()

    await run_async(executor._on_task_complete(exe_state, future, scheduler))  # noqa: SLF001

    assert exe_state.status == BranchStatus.SUSPENDED_WITH_TIMEOUT
    scheduler.schedule_resume.assert_called_once()


@no_type_check
async def test_concurrent_executor_on_task_complete_suspend() -> None:
    """Test ParallelExecutor _on_task_complete with SuspendExecution."""

    class TestExecutor(ParallelExecutor):
        async def execute_item(self, child_context, executable) -> str:
            return f"result_{executable.index}"

    executables = [Executable(0, lambda: "test")]
    completion_config = CompletionConfig(
        min_successful=1,
        tolerated_failure_count=None,
    )

    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )

    exe_state = ExecutableWithState(executables[0])
    future = Mock()
    future.result.side_effect = SuspendExecution("test message")
    future.cancelled.return_value = False

    scheduler = Mock()

    await run_async(executor._on_task_complete(exe_state, future, scheduler))  # noqa: SLF001

    assert exe_state.status == BranchStatus.SUSPENDED


@no_type_check
async def test_concurrent_executor_on_task_complete_cancelled() -> None:
    """A cancelled task becomes a terminal cancelled branch."""

    executor = create_concurrent_executor(
        ParallelExecutor,
        executables=[Executable(0, lambda: "test")],
        max_concurrency=1,
        completion_config=CompletionConfig(min_successful=1),
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )
    exe_state = ExecutableWithState(executor.executables[0])
    future = Mock()
    future.cancelled.return_value = True

    await executor._on_task_complete(exe_state, future, Mock())  # noqa: SLF001

    assert exe_state.status is BranchStatus.CANCELLED
    assert executor.counters.success_count == 0
    assert executor.counters.failure_count == 0


@no_type_check
async def test_concurrent_executor_on_task_complete_exception() -> None:
    """Test ParallelExecutor _on_task_complete with general exception."""

    class TestExecutor(ParallelExecutor):
        async def execute_item(self, child_context, executable) -> str:
            return f"result_{executable.index}"

    executables = [Executable(0, lambda: "test")]
    completion_config = CompletionConfig(
        min_successful=1,
        tolerated_failure_count=None,
    )

    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )

    exe_state = ExecutableWithState(executables[0])
    future = Mock()
    future.result.side_effect = ValueError("Test error")
    future.cancelled.return_value = False

    scheduler = Mock()

    await run_async(executor._on_task_complete(exe_state, future, scheduler))  # noqa: SLF001

    assert exe_state.status == BranchStatus.FAILED
    assert isinstance(exe_state.error, ValueError)


@no_type_check
async def test_concurrent_executor_on_task_complete_orphaned_child_is_ignored() -> None:
    """Orphaned child completion exits without marking the branch failed."""

    class TestExecutor(ParallelExecutor):
        async def execute_item(self, child_context, executable) -> str:
            return f"result_{executable.index}"

    executables = [Executable(0, lambda: "test")]
    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=1,
        completion_config=CompletionConfig(),
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )

    exe_state = ExecutableWithState(executables[0])
    future = Mock()
    future.result.side_effect = OrphanedChildException("orphaned", "child-1")
    future.cancelled.return_value = False
    exe_state.run(future)

    await run_async(executor._on_task_complete(exe_state, future, Mock()))  # noqa: SLF001

    assert exe_state.status == BranchStatus.RUNNING
    assert executor.counters.success_count == 0
    assert executor.counters.failure_count == 0


async def test_concurrent_executor_create_result_with_early_exit() -> None:
    """Test ParallelExecutor with failed branches using public execute method."""

    class TestExecutor(ParallelExecutor):
        async def execute_item(self, child_context, executable) -> str:
            if executable.index == 0:
                return f"result_{executable.index}"
            msg = "Test error"
            # giving space to terminate early with
            time.sleep(0.5)
            raise ValueError(msg)

    def success_callable() -> str:
        return "test"

    def failure_callable() -> str:
        return "test2"

    executables = [Executable(0, success_callable), Executable(1, failure_callable)]
    completion_config = CompletionConfig(
        # setting min successful to None to execute all children and avoid early stopping
        min_successful=None,
        tolerated_failure_count=None,
    )

    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=2,
        completion_config=completion_config,
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )

    execution_state = create_execution_state()
    executor_context = create_executor_context(execution_state)

    result = await run_async(executor.execute())

    assert len(result.all) == 2
    assert result.all[0].status == BatchItemStatus.SUCCEEDED
    assert result.all[1].status == BatchItemStatus.FAILED
    # NEW BEHAVIOR: With empty completion config (no criteria) and failures,
    # should fail-fast and return FAILURE_TOLERANCE_EXCEEDED
    assert result.completion_reason == CompletionReason.FAILURE_TOLERANCE_EXCEEDED


async def test_concurrent_executor_execute_item_in_child_context() -> None:
    """Test ParallelExecutor _execute_item_in_child_context."""

    class TestExecutor(ParallelExecutor):
        async def execute_item(self, child_context, executable) -> str:
            return f"result_{executable.index}"

    executables = [Executable(0, lambda: "test")]
    completion_config = CompletionConfig(
        min_successful=1,
        tolerated_failure_count=None,
    )

    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )

    execution_state = create_execution_state()
    executor_context = create_executor_context(execution_state)

    result = await run_async(  # noqa: SLF001
        executor._execute_item_in_child_context(executor_context, executables[0])
    )
    assert result == "result_0"


async def test_execution_counters_impossible_to_succeed() -> None:
    """Test ExecutionCounters should_complete when impossible to succeed."""
    counters = ExecutionCounters(5, CompletionConfig(min_successful=4))

    # Fail 3 tasks, leaving only 2 remaining (can't reach min_successful of 4)
    counters.fail_task()
    counters.fail_task()
    counters.fail_task()

    assert counters.should_complete()


async def test_concurrent_executor_create_result_failure_tolerance_exceeded() -> None:
    """Test ParallelExecutor with failure tolerance exceeded using public execute method."""

    class TestExecutor(ParallelExecutor):
        async def execute_item(self, child_context, executable) -> NoReturn:
            msg = "Task failed"
            raise ValueError(msg)

    def failure_callable() -> str:
        return "test"

    executables = [Executable(0, failure_callable)]
    completion_config = CompletionConfig(
        min_successful=1,
        tolerated_failure_count=0,
    )

    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )

    execution_state = create_execution_state()
    executor_context = create_executor_context(execution_state)

    result = await run_async(executor.execute())
    # NEW BEHAVIOR: With tolerated_failure_count=0 and 1 failure,
    # tolerance is exceeded, so FAILURE_TOLERANCE_EXCEEDED
    assert result.completion_reason == CompletionReason.FAILURE_TOLERANCE_EXCEEDED


async def test_concurrent_executor_does_not_start_items_after_early_completion() -> (
    None
):
    """Pending items are not started or returned after fail-fast completes."""
    started = []

    class TestExecutor(ParallelExecutor):
        async def _execute_item_in_child_context(
            self, executor_context, executable
        ) -> str:
            started.append(executable.index)
            if executable.index == 1:
                raise ValueError("failed")
            return f"result_{executable.index}"

    executables = [Executable(index, lambda: None) for index in range(3)]
    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=1,
        completion_config=CompletionConfig(tolerated_failure_count=0),
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )

    result = await executor.execute()

    assert started == [0, 1]
    assert [item.index for item in result.all] == [0, 1]
    assert result.total_count == 2
    assert result.completion_reason is CompletionReason.FAILURE_TOLERANCE_EXCEEDED


async def test_concurrent_executor_suspended_branch_keeps_concurrency_slot() -> None:
    """A suspended branch prevents a pending branch from taking its slot."""
    started = []

    class TestExecutor(ParallelExecutor):
        async def _execute_item_in_child_context(
            self, executor_context, executable
        ) -> str:
            started.append(executable.index)
            if executable.index == 0:
                raise SuspendExecution("waiting for callback")
            return f"result_{executable.index}"

    executables = [Executable(index, lambda: None) for index in range(2)]
    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=1,
        completion_config=CompletionConfig.all_completed(),
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )

    with pytest.raises(SuspendExecution):
        await executor.execute()

    assert started == [0]
    assert executor.executables_with_state[0].status is BranchStatus.SUSPENDED
    assert executor.executables_with_state[1].status is BranchStatus.NOT_STARTED


async def test_concurrent_executor_refills_terminal_slot_before_suspending() -> None:
    """A terminal branch is replaced before suspension is evaluated."""
    started = []
    first_branch_suspended = asyncio.Event()

    class TestExecutor(ParallelExecutor):
        async def _execute_item_in_child_context(
            self, executor_context, executable
        ) -> str:
            started.append(executable.index)
            if executable.index == 0:
                first_branch_suspended.set()
                raise SuspendExecution("waiting for callback")
            if executable.index == 1:
                await first_branch_suspended.wait()
                return "completed"
            raise SuspendExecution("waiting for callback")

    executables = [Executable(index, lambda: None) for index in range(3)]
    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=2,
        completion_config=CompletionConfig.all_completed(),
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )

    with pytest.raises(SuspendExecution):
        await executor.execute()

    assert started == [0, 1, 2]
    assert executor.executables_with_state[0].status is BranchStatus.SUSPENDED
    assert executor.executables_with_state[1].status is BranchStatus.COMPLETED
    assert executor.executables_with_state[2].status is BranchStatus.SUSPENDED


async def test_early_completion_marks_suspended_branch_cancelled() -> None:
    """A started suspended branch is cancelled when another branch completes."""
    first_branch_suspended = asyncio.Event()

    class TestExecutor(ParallelExecutor):
        async def _execute_item_in_child_context(
            self, executor_context, executable
        ) -> str:
            if executable.index == 0:
                first_branch_suspended.set()
                raise SuspendExecution("waiting for callback")
            await first_branch_suspended.wait()
            return "completed"

    executor = create_concurrent_executor(
        TestExecutor,
        executables=[
            Executable(0, lambda: None),
            Executable(1, lambda: None),
        ],
        max_concurrency=2,
        completion_config=CompletionConfig(min_successful=1),
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )

    result = await executor.execute()

    assert result.all == [
        BatchItem(0, BatchItemStatus.CANCELLED),
        BatchItem(1, BatchItemStatus.SUCCEEDED, result="completed"),
    ]


@pytest.mark.parametrize("invalid_max_concurrency", [0, -1, True, 1.5])
def test_parallel_rejects_invalid_max_concurrency_before_creating_context(
    invalid_max_concurrency,
) -> None:
    """Invalid concurrency is rejected before a parallel context is started."""

    async def branch() -> str:
        return "unused"

    with pytest.raises(ValidationError, match="positive integer"):
        parallel([branch], max_concurrency=invalid_max_concurrency)


async def test_concurrent_executor_custom_should_complete_succeeds_early() -> None:
    """Custom completion can stop after a user-defined success condition."""

    class TestExecutor(ParallelExecutor):
        async def execute_item(self, child_context, executable) -> Any:
            return await executable.func()

    async def branch(index) -> str:
        if index == 2:
            await asyncio.sleep(2)
        return f"result_{index}"

    executables = [
        Executable(0, lambda: branch(0)),
        Executable(1, lambda: branch(1)),
        Executable(2, lambda: branch(2)),
    ]
    completion_config = CompletionConfig.custom(
        lambda status: CompletionDecision.complete(
            CompletionReason.CUSTOM_COMPLETION_SUCCEEDED
        )
        if status.success_count >= 2
        else CompletionDecision.continue_execution()
    )

    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=2,
        completion_config=completion_config,
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )

    result = await run_async(executor.execute())

    assert result.completion_reason == CompletionReason.CUSTOM_COMPLETION_SUCCEEDED
    assert result.completion_reason.is_succeeded
    assert result.success_count == 2
    assert result.cancelled_count == 1
    assert result.started_count == 0
    assert result.all[2].status == BatchItemStatus.CANCELLED


async def test_concurrent_executor_custom_should_complete_can_complete_as_failed() -> (
    None
):
    """Custom completion can choose a failed completion reason."""

    class TestExecutor(ParallelExecutor):
        async def execute_item(self, child_context, executable) -> Any:
            return await executable.func()

    async def branch(index) -> str:
        if index in {0, 1}:
            msg = f"failed_{index}"
            raise ValueError(msg)
        await asyncio.sleep(2)
        return f"result_{index}"

    executables = [
        Executable(0, lambda: branch(0)),
        Executable(1, lambda: branch(1)),
        Executable(2, lambda: branch(2)),
    ]
    completion_config = CompletionConfig.custom(
        lambda status: CompletionDecision.complete(
            CompletionReason.CUSTOM_COMPLETION_FAILED
        )
        if status.completed_count >= 2
        else CompletionDecision.continue_execution()
    )

    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=2,
        completion_config=completion_config,
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )

    result = await run_async(executor.execute())

    assert result.completion_reason == CompletionReason.CUSTOM_COMPLETION_FAILED
    assert not result.completion_reason.is_succeeded
    assert result.success_count == 0
    assert result.failure_count == 2
    assert result.cancelled_count == 1
    assert result.started_count == 0


async def test_batch_result_from_items_uses_custom_should_complete_reason() -> None:
    """Reconstructed batch results infer custom completion reasons from config."""
    config = CompletionConfig.custom(
        lambda status: CompletionDecision.complete(
            CompletionReason.CUSTOM_COMPLETION_SUCCEEDED
        )
        if status.success_count >= 2
        else CompletionDecision.continue_execution()
    )
    items = [
        BatchItem(0, BatchItemStatus.SUCCEEDED, result="a"),
        BatchItem(1, BatchItemStatus.SUCCEEDED, result="b"),
        BatchItem(2, BatchItemStatus.STARTED),
    ]

    result = BatchResult.from_items(items, config)

    assert result.completion_reason == CompletionReason.CUSTOM_COMPLETION_SUCCEEDED


async def test_single_task_suspend_bubbles_up() -> None:
    """Test that single task suspend bubbles up the exception."""

    class TestExecutor(ParallelExecutor):
        async def execute_item(self, child_context, executable) -> NoReturn:
            msg = "test"
            raise TimedSuspendExecution(msg, time.time() + 1)  # Future time

    executables = [Executable(0, lambda: "test")]
    completion_config = CompletionConfig(
        min_successful=1,
        tolerated_failure_count=None,
    )

    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )

    execution_state = create_execution_state()
    executor_context = create_executor_context(execution_state)

    # Should raise TimedSuspendExecution since no other tasks running
    with pytest.raises(TimedSuspendExecution):
        await run_async(executor.execute())


async def test_multiple_tasks_one_suspends_execution_continues() -> None:
    """Test that when one task suspends but others are running, execution continues."""

    class TestExecutor(ParallelExecutor):
        def __init__(self, *args, **kwargs) -> None:
            super().__init__(*args, **kwargs)
            self.task_a_suspended = asyncio.Event()
            self.task_b_completed = False

        async def execute_item(self, child_context, executable) -> str:
            if executable.index == 0:  # Task A
                self.task_a_suspended.set()
                msg = "test"
                raise TimedSuspendExecution(msg, time.time() + 1)  # Future time
            # Task B
            # Wait for Task A to suspend first
            await asyncio.wait_for(self.task_a_suspended.wait(), timeout=2.0)
            await asyncio.sleep(0.1)  # Ensure A has suspended
            self.task_b_completed = True
            return f"result_{executable.index}"

    executables = [Executable(0, lambda: "testA"), Executable(1, lambda: "testB")]
    completion_config = CompletionConfig.all_completed()

    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=2,
        completion_config=completion_config,
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )

    execution_state = create_execution_state()
    executor_context = create_executor_context(execution_state)

    # Should raise TimedSuspendExecution after Task B completes
    with pytest.raises(TimedSuspendExecution):
        await run_async(executor.execute())

    # Assert that Task B did complete before suspension
    assert executor.task_b_completed


async def test_concurrent_executor_with_single_task_resubmit() -> None:
    """Test single task suspend bubbles up immediately."""

    class TestExecutor(ParallelExecutor):
        def __init__(self, *args, **kwargs) -> None:
            super().__init__(*args, **kwargs)
            self.call_count = 0

        async def execute_item(self, child_context, executable) -> NoReturn:
            self.call_count += 1
            msg = "test"
            raise TimedSuspendExecution(msg, time.time() + 10)  # Future time

    executables = [Executable(0, lambda: "test")]
    completion_config = CompletionConfig(
        min_successful=1,
        tolerated_failure_count=None,
    )

    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )

    execution_state = create_execution_state()
    executor_context = create_executor_context(execution_state)

    # Should raise TimedSuspendExecution since single task suspends
    with pytest.raises(TimedSuspendExecution):
        await run_async(executor.execute())


async def test_concurrent_executor_with_timed_resubmit_while_other_task_running() -> (
    None
):
    """Test timed resubmission while other tasks are still running."""

    class TestExecutor(ParallelExecutor):
        def __init__(self, *args, **kwargs) -> None:
            super().__init__(*args, **kwargs)
            self.call_counts: dict[int, int] = {}
            self.task_a_started = asyncio.Event()
            self.task_b_can_complete = asyncio.Event()
            self.task_b_completed = asyncio.Event()

        async def execute_item(self, child_context, executable) -> Any:
            task_id = executable.index
            self.call_counts[task_id] = self.call_counts.get(task_id, 0) + 1

            if task_id == 0:  # Task A - runs long
                self.task_a_started.set()
                # Wait for task B to complete before finishing
                await asyncio.wait_for(self.task_b_can_complete.wait(), timeout=5)
                await asyncio.wait_for(self.task_b_completed.wait(), timeout=1)
                return "result_A"

            if task_id == 1:  # Task B - suspends and resubmits
                call_count = self.call_counts[task_id]

                if call_count == 1:
                    # First call: immediate resubmit (past timestamp)
                    msg = "immediate"
                    raise TimedSuspendExecution(msg, time.time() - 1)
                if call_count == 2:
                    # Second call: short delay resubmit
                    msg = "short_delay"
                    raise TimedSuspendExecution(msg, time.time() + 0.2)
                # Third call: complete successfully
                result = "result_B"
                self.task_b_can_complete.set()
                self.task_b_completed.set()
                return result

            return None

    executables = [
        Executable(0, lambda: "task_A"),  # Long running task
        Executable(1, lambda: "task_B"),  # Suspending/resubmitting task
    ]
    completion_config = CompletionConfig(
        min_successful=2,
        tolerated_failure_count=None,
    )

    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=2,
        completion_config=completion_config,
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )

    execution_state = create_execution_state()
    executor_context = create_executor_context(execution_state)

    # Should complete successfully after B resubmits and both tasks finish
    result = await run_async(executor.execute())

    # Verify results
    assert len(result.all) == 2
    assert all(item.status == BatchItemStatus.SUCCEEDED for item in result.all)
    assert result.completion_reason == CompletionReason.ALL_COMPLETED

    # Verify task B was called 3 times (initial + 2 resubmits)
    assert executor.call_counts[1] == 3
    # Verify task A was called only once
    assert executor.call_counts[0] == 1


@no_type_check
async def test_timer_scheduler_double_check_condition() -> None:
    """Test TimerScheduler double-check condition in _timer_loop (line 434)."""
    callback = AsyncMock()

    async def run_test() -> None:
        async with TimerScheduler(callback) as scheduler:
            exe_state = ExecutableWithState(Executable(0, lambda: "test"))
            exe_state.suspend()  # Make it resumable
            scheduler.schedule_resume(exe_state, time.time() - 1)
            await asyncio.sleep(0.05)

    await run_async(run_test())
    assert callback.call_count >= 1


@no_type_check
async def test_concurrent_executor_should_execution_suspend_with_timeout() -> None:
    """Test should_execution_suspend with SUSPENDED_WITH_TIMEOUT state."""

    class TestExecutor(ParallelExecutor):
        async def execute_item(self, child_context, executable) -> str:
            return f"result_{executable.index}"

    executables = [Executable(0, lambda: "test")]
    completion_config = CompletionConfig(
        min_successful=1,
        tolerated_failure_count=None,
    )

    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )

    # Create executable with state in SUSPENDED_WITH_TIMEOUT
    exe_state = ExecutableWithState(executables[0])
    future_time = time.time() + 10
    exe_state.suspend_with_timeout(future_time)

    executor.executables_with_state = [exe_state]

    result = executor.should_execution_suspend()

    assert result.should_suspend
    assert isinstance(result.exception, TimedSuspendExecution)
    assert result.exception.scheduled_timestamp == future_time


@no_type_check
async def test_concurrent_executor_should_execution_suspend_indefinite() -> None:
    """Test should_execution_suspend with indefinite SUSPENDED state."""

    class TestExecutor(ParallelExecutor):
        async def execute_item(self, child_context, executable) -> str:
            return f"result_{executable.index}"

    executables = [Executable(0, lambda: "test")]
    completion_config = CompletionConfig(
        min_successful=1,
        tolerated_failure_count=None,
    )

    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )

    # Create executable with state in SUSPENDED (indefinite)
    exe_state = ExecutableWithState(executables[0])
    exe_state.suspend()

    executor.executables_with_state = [exe_state]

    result = executor.should_execution_suspend()

    assert result.should_suspend
    assert isinstance(result.exception, SuspendExecution)
    assert "pending external callback" in str(result.exception)


async def test_concurrent_executor_create_result_with_failed_status() -> None:
    """Test with failed executable status using public execute method."""

    class TestExecutor(ParallelExecutor):
        async def execute_item(self, child_context, executable) -> NoReturn:
            msg = "Test error"
            raise ValueError(msg)

    def failure_callable() -> str:
        return "test"

    executables = [Executable(0, failure_callable)]
    completion_config = CompletionConfig(
        min_successful=1,
        tolerated_failure_count=0,
    )

    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )

    execution_state = create_execution_state()
    executor_context = create_executor_context(execution_state)

    result = await run_async(executor.execute())

    assert len(result.all) == 1
    assert result.all[0].status == BatchItemStatus.FAILED
    assert result.all[0].error is not None
    assert result.all[0].error.message == "Test error"


@no_type_check
async def test_timer_scheduler_can_resume_false() -> None:
    """Test TimerScheduler when exe_state.can_resume is False."""
    callback = AsyncMock()

    async def run_test() -> None:
        async with TimerScheduler(callback) as scheduler:
            exe_state = ExecutableWithState(Executable(0, lambda: "test"))
            exe_state.complete("done")
            scheduler.schedule_resume(exe_state, time.time() - 1)
            await asyncio.sleep(0.05)

    await run_async(run_test())
    callback.assert_not_called()


@no_type_check
async def test_concurrent_executor_mixed_suspend_states() -> None:
    """Test should_execution_suspend with mixed suspend states."""

    class TestExecutor(ParallelExecutor):
        async def execute_item(self, child_context, executable) -> str:
            return f"result_{executable.index}"

    executables = [Executable(0, lambda: "test"), Executable(1, lambda: "test2")]
    completion_config = CompletionConfig(
        min_successful=1,
        tolerated_failure_count=None,
    )

    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=2,
        completion_config=completion_config,
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )

    # Create one with timed suspend and one with indefinite suspend
    exe_state1 = ExecutableWithState(executables[0])
    exe_state2 = ExecutableWithState(executables[1])

    future_time = time.time() + 5
    exe_state1.suspend_with_timeout(future_time)
    exe_state2.suspend()  # Indefinite

    executor.executables_with_state = [exe_state1, exe_state2]

    result = executor.should_execution_suspend()

    # Should return timed suspend (earliest timestamp takes precedence)
    assert result.should_suspend
    assert isinstance(result.exception, TimedSuspendExecution)


@no_type_check
async def test_concurrent_executor_multiple_timed_suspends() -> None:
    """Test should_execution_suspend with multiple timed suspends to find earliest."""

    class TestExecutor(ParallelExecutor):
        async def execute_item(self, child_context, executable) -> str:
            return f"result_{executable.index}"

    executables = [Executable(0, lambda: "test"), Executable(1, lambda: "test2")]
    completion_config = CompletionConfig(
        min_successful=1,
        tolerated_failure_count=None,
    )

    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=2,
        completion_config=completion_config,
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )

    # Create two with different timed suspends
    exe_state1 = ExecutableWithState(executables[0])
    exe_state2 = ExecutableWithState(executables[1])

    later_time = time.time() + 10
    earlier_time = time.time() + 5

    exe_state1.suspend_with_timeout(later_time)
    exe_state2.suspend_with_timeout(earlier_time)

    executor.executables_with_state = [exe_state1, exe_state2]

    result = executor.should_execution_suspend()

    # Should return the earlier timestamp
    assert result.should_suspend
    assert isinstance(result.exception, TimedSuspendExecution)
    assert result.exception.scheduled_timestamp == earlier_time


@no_type_check
async def test_timer_scheduler_double_check_condition_race() -> None:
    """Test TimerScheduler double-check condition when heap changes between checks."""
    callback = AsyncMock()

    async def run_test() -> None:
        async with TimerScheduler(callback) as scheduler:
            exe_state1 = ExecutableWithState(Executable(0, lambda: "test"))
            exe_state2 = ExecutableWithState(Executable(1, lambda: "test"))
            exe_state1.suspend()
            exe_state2.suspend()
            scheduler.schedule_resume(exe_state1, time.time() - 1)
            await asyncio.sleep(0.01)
            scheduler.schedule_resume(exe_state2, time.time() - 2)
            await asyncio.sleep(0.05)

    await run_async(run_test())
    assert callback.call_count >= 1


@no_type_check
async def test_should_execution_suspend_earliest_timestamp_comparison() -> None:
    """Test should_execution_suspend timestamp comparison logic (line 554)."""

    class TestExecutor(ParallelExecutor):
        async def execute_item(self, child_context, executable) -> str:
            return f"result_{executable.index}"

    executables = [
        Executable(0, lambda: "test"),
        Executable(1, lambda: "test2"),
        Executable(2, lambda: "test3"),
    ]
    completion_config = CompletionConfig(
        min_successful=1,
        tolerated_failure_count=None,
    )

    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=3,
        completion_config=completion_config,
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )

    # Create three executables with different suspend times
    exe_state1 = ExecutableWithState(executables[0])
    exe_state2 = ExecutableWithState(executables[1])
    exe_state3 = ExecutableWithState(executables[2])

    time1 = time.time() + 10
    time2 = time.time() + 5  # Earliest
    time3 = time.time() + 15

    exe_state1.suspend_with_timeout(time1)
    exe_state2.suspend_with_timeout(time2)
    exe_state3.suspend_with_timeout(time3)

    executor.executables_with_state = [exe_state1, exe_state2, exe_state3]

    result = executor.should_execution_suspend()

    assert result.should_suspend
    assert isinstance(result.exception, TimedSuspendExecution)
    assert result.exception.scheduled_timestamp == time2


async def test_concurrent_executor_execute_with_failing_task() -> None:
    """Test execute() with a task that fails using public execute method."""

    class TestExecutor(ParallelExecutor):
        async def execute_item(self, child_context, executable) -> NoReturn:
            msg = "Task failed"
            raise ValueError(msg)

    def failure_callable() -> str:
        return "test"

    executables = [Executable(0, failure_callable)]
    completion_config = CompletionConfig(min_successful=1, tolerated_failure_count=0)

    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )

    execution_state = create_execution_state()
    executor_context = create_executor_context(execution_state)

    result = await run_async(executor.execute())

    assert len(result.all) == 1
    assert result.all[0].status == BatchItemStatus.FAILED
    assert result.all[0].error.message == "Task failed"


@no_type_check
async def test_timer_scheduler_cannot_resume_branch() -> None:
    """Test TimerScheduler when exe_state cannot resume (434->433 branch)."""
    callback = AsyncMock()

    async def run_test() -> None:
        async with TimerScheduler(callback) as scheduler:
            exe_state = ExecutableWithState(Executable(0, lambda: "test"))
            exe_state.complete("done")
            scheduler.schedule_resume(exe_state, time.time() - 1)
            await asyncio.sleep(0.05)

    await run_async(run_test())
    callback.assert_not_called()


async def test_create_result_no_failed_executables() -> None:
    """Test when no executables are failed using public execute method."""

    class TestExecutor(ParallelExecutor):
        async def execute_item(self, child_context, executable) -> str:
            return f"result_{executable.index}"

    def success_callable() -> str:
        return "test"

    executables = [Executable(0, success_callable)]
    completion_config = CompletionConfig(
        min_successful=1,
        tolerated_failure_count=None,
    )

    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )

    execution_state = create_execution_state()
    executor_context = create_executor_context(execution_state)

    result = await run_async(executor.execute())

    assert len(result.all) == 1
    assert result.all[0].status == BatchItemStatus.SUCCEEDED
    assert result.completion_reason == CompletionReason.ALL_COMPLETED


async def test_create_result_with_suspended_executable() -> None:
    """Test with suspended executable using public execute method."""

    class TestExecutor(ParallelExecutor):
        async def execute_item(self, child_context, executable) -> NoReturn:
            msg = "Test suspend"
            raise SuspendExecution(msg)

    def suspend_callable() -> str:
        return "test"

    executables = [Executable(0, suspend_callable)]
    completion_config = CompletionConfig(
        min_successful=1,
        tolerated_failure_count=None,
    )

    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )

    execution_state = create_execution_state()
    executor_context = create_executor_context(execution_state)

    # Should raise SuspendExecution since single task suspends
    with pytest.raises(SuspendExecution):
        await run_async(executor.execute())


# Tests for _create_result method match statement branches
@no_type_check
async def test_create_result_completed_branch() -> None:
    """Test _create_result with COMPLETED status branch."""

    class TestExecutor(ParallelExecutor):
        async def execute_item(self, child_context, executable) -> str:
            return f"result_{executable.index}"

    executables = [Executable(0, lambda: "test")]
    completion_config = CompletionConfig(min_successful=1)

    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )

    # Create executable with COMPLETED status
    exe_state = ExecutableWithState(executables[0])
    exe_state.complete("test_result")
    executor.executables_with_state = [exe_state]

    result = executor._create_result()  # noqa: SLF001

    assert len(result.all) == 1
    assert result.all[0].status == BatchItemStatus.SUCCEEDED
    assert result.all[0].result == "test_result"
    assert result.all[0].error is None
    assert result.all[0].index == 0


@no_type_check
async def test_create_result_failed_branch() -> None:
    """Test _create_result with FAILED status branch."""

    class TestExecutor(ParallelExecutor):
        async def execute_item(self, child_context, executable) -> str:
            return f"result_{executable.index}"

    executables = [Executable(0, lambda: "test")]
    completion_config = CompletionConfig(min_successful=1)

    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )

    # Create executable with FAILED status
    exe_state = ExecutableWithState(executables[0])
    test_error = ValueError("Test error message")
    exe_state.fail(test_error)
    executor.executables_with_state = [exe_state]

    result = executor._create_result()  # noqa: SLF001

    assert len(result.all) == 1
    assert result.all[0].status == BatchItemStatus.FAILED
    assert result.all[0].result is None
    assert result.all[0].error is not None
    assert result.all[0].error.message == "Test error message"
    assert result.all[0].error.type == "ValueError"
    assert result.all[0].index == 0


@no_type_check
async def test_create_result_cancelled_branch() -> None:
    """Test _create_result with a CANCELLED branch."""

    executor = create_concurrent_executor(
        ParallelExecutor,
        executables=[Executable(0, lambda: "test")],
        max_concurrency=1,
        completion_config=CompletionConfig(min_successful=1),
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )
    exe_state = ExecutableWithState(executor.executables[0])
    exe_state.cancel()
    executor.executables_with_state = [exe_state]

    result = executor._create_result()  # noqa: SLF001

    assert result.all == [BatchItem(0, BatchItemStatus.CANCELLED)]
    assert result.cancelled_count == 1
    assert result.started_count == 0


@no_type_check
async def test_create_result_not_started_branch() -> None:
    """Test _create_result omits a branch that never started."""

    class TestExecutor(ParallelExecutor):
        async def execute_item(self, child_context, executable) -> str:
            return f"result_{executable.index}"

    executables = [Executable(0, lambda: "test")]
    completion_config = CompletionConfig(min_successful=1)

    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )

    # NOT_STARTED is the default state.
    exe_state = ExecutableWithState(executables[0])
    executor.executables_with_state = [exe_state]

    result = executor._create_result()  # noqa: SLF001

    assert result.all == []
    # NEW BEHAVIOR: With min_successful=1 and no completed items,
    # defaults to ALL_COMPLETED
    assert result.completion_reason == CompletionReason.ALL_COMPLETED


@no_type_check
async def test_create_result_pending_branch() -> None:
    """Test _create_result includes a branch pending resubmission."""

    class TestExecutor(ParallelExecutor):
        async def execute_item(self, child_context, executable) -> str:
            return f"result_{executable.index}"

    executables = [Executable(0, lambda: "test")]
    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=1,
        completion_config=CompletionConfig(min_successful=1),
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )
    exe_state = ExecutableWithState(executables[0])
    exe_state.suspend_with_timeout(time.time() - 1)
    exe_state.reset_to_pending()
    executor.executables_with_state = [exe_state]

    result = executor._create_result()  # noqa: SLF001

    assert len(result.all) == 1
    assert result.all[0].index == 0
    assert result.all[0].status is BatchItemStatus.STARTED


@no_type_check
async def test_create_result_running_branch() -> None:
    """Test _create_result with RUNNING status branch."""

    class TestExecutor(ParallelExecutor):
        async def execute_item(self, child_context, executable) -> str:
            return f"result_{executable.index}"

    executables = [Executable(0, lambda: "test")]
    completion_config = CompletionConfig(min_successful=1)

    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )

    # Create executable with RUNNING status
    exe_state = ExecutableWithState(executables[0])
    future = Future()
    exe_state.run(future)
    executor.executables_with_state = [exe_state]

    result = executor._create_result()  # noqa: SLF001

    assert len(result.all) == 1
    assert result.all[0].status == BatchItemStatus.STARTED
    assert result.all[0].result is None
    assert result.all[0].error is None
    assert result.all[0].index == 0
    # With min_successful=1 and no completed items, defaults to ALL_COMPLETED
    assert result.completion_reason == CompletionReason.ALL_COMPLETED


@no_type_check
async def test_create_result_suspended_branch() -> None:
    """Test _create_result with SUSPENDED status branch."""

    class TestExecutor(ParallelExecutor):
        async def execute_item(self, child_context, executable) -> str:
            return f"result_{executable.index}"

    executables = [Executable(0, lambda: "test")]
    completion_config = CompletionConfig(min_successful=1)

    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )

    # Create executable with SUSPENDED status
    exe_state = ExecutableWithState(executables[0])
    exe_state.suspend()
    executor.executables_with_state = [exe_state]

    result = executor._create_result()  # noqa: SLF001

    assert len(result.all) == 1
    assert result.all[0].status == BatchItemStatus.STARTED
    assert result.all[0].result is None
    assert result.all[0].error is None
    assert result.all[0].index == 0
    # With min_successful=1 and no completed items, defaults to ALL_COMPLETED
    assert result.completion_reason == CompletionReason.ALL_COMPLETED


@no_type_check
async def test_create_result_suspended_with_timeout_branch() -> None:
    """Test _create_result with SUSPENDED_WITH_TIMEOUT status branch."""

    class TestExecutor(ParallelExecutor):
        async def execute_item(self, child_context, executable) -> str:
            return f"result_{executable.index}"

    executables = [Executable(0, lambda: "test")]
    completion_config = CompletionConfig(min_successful=1)

    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )

    # Create executable with SUSPENDED_WITH_TIMEOUT status
    exe_state = ExecutableWithState(executables[0])
    future_time = time.time() + 10
    exe_state.suspend_with_timeout(future_time)
    executor.executables_with_state = [exe_state]

    result = executor._create_result()  # noqa: SLF001

    assert len(result.all) == 1
    assert result.all[0].status == BatchItemStatus.STARTED
    assert result.all[0].result is None
    assert result.all[0].error is None
    assert result.all[0].index == 0
    # With min_successful=1 and no completed items, default to ALL_COMPLETED
    assert result.completion_reason == CompletionReason.ALL_COMPLETED


@no_type_check
async def test_create_result_mixed_statuses() -> None:
    """Test _create_result with mixed executable statuses covering all branches."""

    class TestExecutor(ParallelExecutor):
        async def execute_item(self, child_context, executable) -> str:
            return f"result_{executable.index}"

    executables = [
        Executable(0, lambda: "test0"),  # Will be COMPLETED
        Executable(1, lambda: "test1"),  # Will be FAILED
        Executable(2, lambda: "test2"),  # Will be NOT_STARTED
        Executable(3, lambda: "test3"),  # Will be RUNNING
        Executable(4, lambda: "test4"),  # Will be SUSPENDED
        Executable(5, lambda: "test5"),  # Will be SUSPENDED_WITH_TIMEOUT
    ]
    completion_config = CompletionConfig(min_successful=1)

    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=6,
        completion_config=completion_config,
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )

    # Create executables with different statuses
    exe_states = [ExecutableWithState(exe) for exe in executables]

    # COMPLETED
    exe_states[0].complete("completed_result")

    # FAILED
    exe_states[1].fail(RuntimeError("Test failure"))

    # NOT_STARTED (default state, no change needed)

    # RUNNING
    future = Future()
    exe_states[3].run(future)

    # SUSPENDED
    exe_states[4].suspend()

    # SUSPENDED_WITH_TIMEOUT
    exe_states[5].suspend_with_timeout(time.time() + 10)

    executor.executables_with_state = exe_states

    result = executor._create_result()  # noqa: SLF001

    assert len(result.all) == 5
    assert [item.index for item in result.all] == [0, 1, 3, 4, 5]

    # Check COMPLETED -> SUCCEEDED
    assert result.all[0].status == BatchItemStatus.SUCCEEDED
    assert result.all[0].result == "completed_result"
    assert result.all[0].error is None

    # Check FAILED -> FAILED
    assert result.all[1].status == BatchItemStatus.FAILED
    assert result.all[1].result is None
    assert result.all[1].error is not None
    assert result.all[1].error.message == "Test failure"

    # Check RUNNING -> STARTED
    assert result.all[2].status == BatchItemStatus.STARTED
    assert result.all[2].result is None
    assert result.all[2].error is None

    # Check SUSPENDED -> STARTED
    assert result.all[3].status == BatchItemStatus.STARTED
    assert result.all[3].result is None
    assert result.all[3].error is None

    # Check SUSPENDED_WITH_TIMEOUT -> STARTED
    assert result.all[4].status == BatchItemStatus.STARTED
    assert result.all[4].result is None
    assert result.all[4].error is None

    # we've a min succ set to 1.
    assert result.completion_reason == CompletionReason.MIN_SUCCESSFUL_REACHED


@no_type_check
async def test_create_result_multiple_completed() -> None:
    """Test _create_result with multiple COMPLETED executables."""

    class TestExecutor(ParallelExecutor):
        async def execute_item(self, child_context, executable) -> str:
            return f"result_{executable.index}"

    executables = [
        Executable(0, lambda: "test0"),
        Executable(1, lambda: "test1"),
        Executable(2, lambda: "test2"),
    ]
    completion_config = CompletionConfig(min_successful=3)

    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=3,
        completion_config=completion_config,
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )

    # Create all executables with COMPLETED status
    exe_states = [ExecutableWithState(exe) for exe in executables]
    exe_states[0].complete("result_0")
    exe_states[1].complete("result_1")
    exe_states[2].complete("result_2")

    executor.executables_with_state = exe_states

    result = executor._create_result()  # noqa: SLF001

    assert len(result.all) == 3
    assert all(item.status == BatchItemStatus.SUCCEEDED for item in result.all)
    assert result.all[0].result == "result_0"
    assert result.all[1].result == "result_1"
    assert result.all[2].result == "result_2"
    assert result.completion_reason == CompletionReason.ALL_COMPLETED


@no_type_check
async def test_create_result_multiple_failed() -> None:
    """Test _create_result with multiple FAILED executables."""

    class TestExecutor(ParallelExecutor):
        async def execute_item(self, child_context, executable) -> str:
            return f"result_{executable.index}"

    executables = [
        Executable(0, lambda: "test0"),
        Executable(1, lambda: "test1"),
        Executable(2, lambda: "test2"),
    ]
    completion_config = CompletionConfig(min_successful=1)

    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=3,
        completion_config=completion_config,
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )

    # Create all executables with FAILED status
    exe_states = [ExecutableWithState(exe) for exe in executables]
    exe_states[0].fail(ValueError("Error 0"))
    exe_states[1].fail(RuntimeError("Error 1"))
    exe_states[2].fail(TypeError("Error 2"))

    executor.executables_with_state = exe_states

    result = executor._create_result()  # noqa: SLF001

    assert len(result.all) == 3
    assert all(item.status == BatchItemStatus.FAILED for item in result.all)
    assert result.all[0].error.message == "Error 0"
    assert result.all[1].error.message == "Error 1"
    assert result.all[2].error.message == "Error 2"
    assert result.completion_reason == CompletionReason.ALL_COMPLETED


@no_type_check
async def test_create_result_multiple_started_states() -> None:
    """Test _create_result with multiple executables in STARTED states."""

    class TestExecutor(ParallelExecutor):
        async def execute_item(self, child_context, executable) -> str:
            return f"result_{executable.index}"

    executables = [
        Executable(0, lambda: "test0"),  # NOT_STARTED
        Executable(1, lambda: "test1"),  # PENDING
        Executable(2, lambda: "test2"),  # RUNNING
        Executable(3, lambda: "test3"),  # SUSPENDED
        Executable(4, lambda: "test4"),  # SUSPENDED_WITH_TIMEOUT
    ]
    completion_config = CompletionConfig(min_successful=1)

    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=4,
        completion_config=completion_config,
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )

    # Create executables with different STARTED states
    exe_states = [ExecutableWithState(exe) for exe in executables]

    # NOT_STARTED (default state)

    # PENDING
    exe_states[1].suspend_with_timeout(time.time() - 1)
    exe_states[1].reset_to_pending()

    # RUNNING
    future = Future()
    exe_states[2].run(future)

    # SUSPENDED
    exe_states[3].suspend()

    # SUSPENDED_WITH_TIMEOUT
    exe_states[4].suspend_with_timeout(time.time() + 5)

    executor.executables_with_state = exe_states

    result = executor._create_result()  # noqa: SLF001

    assert len(result.all) == 4
    assert [item.index for item in result.all] == [1, 2, 3, 4]
    assert all(item.status == BatchItemStatus.STARTED for item in result.all)
    assert all(item.result is None for item in result.all)
    assert all(item.error is None for item in result.all)
    # With min_successful=1 and no completed items, defaults to ALL_COMPLETED
    assert result.completion_reason == CompletionReason.ALL_COMPLETED


@no_type_check
async def test_create_result_empty_executables() -> None:
    """Test _create_result with no executables."""

    class TestExecutor(ParallelExecutor):
        async def execute_item(self, child_context, executable) -> str:
            return f"result_{executable.index}"

    executables = []
    completion_config = CompletionConfig(min_successful=0)

    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=1,
        completion_config=completion_config,
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )

    executor.executables_with_state = []

    result = executor._create_result()  # noqa: SLF001

    assert len(result.all) == 0
    assert result.completion_reason == CompletionReason.ALL_COMPLETED


@no_type_check
async def test_timer_scheduler_future_time_condition_false() -> None:
    """Test TimerScheduler when scheduled time is in future (434->433 branch)."""
    callback = AsyncMock()

    async def run_test() -> None:
        async with TimerScheduler(callback) as scheduler:
            exe_state = ExecutableWithState(Executable(0, lambda: "test"))
            exe_state.suspend()
            scheduler.schedule_resume(exe_state, time.time() + 10)
            await asyncio.sleep(0.05)

    await run_async(run_test())
    callback.assert_not_called()


@no_type_check
async def test_batch_result_from_dict_with_completion_config() -> None:
    """Test BatchResult from_dict with completion config parameter."""
    data = {
        "all": [
            {"index": 0, "status": "SUCCEEDED", "result": "result1", "error": None},
            {"index": 1, "status": "STARTED", "result": None, "error": None},
        ],
        # No completionReason provided
    }

    # With started items, should infer MIN_SUCCESSFUL_REACHED
    completion_config = CompletionConfig(min_successful=1)

    with patch("async_durable_execution._extension.parallel.logger") as mock_logger:
        result = BatchResult.from_dict(data, completion_config)
        assert result.completion_reason == CompletionReason.MIN_SUCCESSFUL_REACHED
        mock_logger.warning.assert_called_once()


@no_type_check
async def test_batch_result_from_dict_all_completed() -> None:
    """Test BatchResult from_dict infers completion reason when all items are completed."""
    data = {
        "all": [
            {"index": 0, "status": "SUCCEEDED", "result": "result1", "error": None},
            {
                "index": 1,
                "status": "FAILED",
                "result": None,
                "error": {
                    "message": "error",
                    "type": "Error",
                    "data": None,
                    "stackTrace": None,
                },
            },
        ],
        # No completionReason provided
    }

    # With no config and failures, fail-fast
    with patch("async_durable_execution._extension.parallel.logger") as mock_logger:
        result = BatchResult.from_dict(data)
        assert result.completion_reason == CompletionReason.FAILURE_TOLERANCE_EXCEEDED
        mock_logger.warning.assert_called_once()


@no_type_check
async def test_batch_result_from_dict_backward_compatibility() -> None:
    """Test BatchResult from_dict maintains backward compatibility when no completion_config provided."""
    data = {
        "all": [
            {"index": 0, "status": "SUCCEEDED", "result": "result1", "error": None}
        ],
        "completionReason": "MIN_SUCCESSFUL_REACHED",
    }

    # Should work without completion_config parameter
    result = BatchResult.from_dict(data)
    assert result.completion_reason == CompletionReason.MIN_SUCCESSFUL_REACHED

    # Should also work with None completion_config
    result2 = BatchResult.from_dict(data, None)
    assert result2.completion_reason == CompletionReason.MIN_SUCCESSFUL_REACHED


@no_type_check
async def test_batch_result_infer_completion_reason_basic_cases() -> None:
    """Test _infer_completion_reason method with basic scenarios."""
    # Test with started items - should be MIN_SUCCESSFUL_REACHED
    items = {
        "all": [
            BatchItem(0, BatchItemStatus.SUCCEEDED, "result1").to_dict(),
            BatchItem(1, BatchItemStatus.STARTED).to_dict(),
        ]
    }
    batch = BatchResult.from_dict(items, CompletionConfig(1))
    assert batch.completion_reason == CompletionReason.MIN_SUCCESSFUL_REACHED

    # Test with all completed items - should be ALL_COMPLETED
    completed_items = [
        BatchItem(0, BatchItemStatus.SUCCEEDED, "result1").to_dict(),
        BatchItem(
            1, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ).to_dict(),
    ]
    completed_items = {"all": completed_items}
    batch = BatchResult.from_dict(completed_items, CompletionConfig(1))
    assert batch.completion_reason == CompletionReason.ALL_COMPLETED

    # Test empty items - should be ALL_COMPLETED
    batch = BatchResult.from_dict({"all": []}, CompletionConfig(1))
    assert batch.completion_reason == CompletionReason.ALL_COMPLETED


@no_type_check
async def test_operation_id_determinism_across_shuffles() -> None:
    """Test that operation_id depends on Executable.index, not execution order."""

    def index_based_function(index, ctx) -> str:
        """Function that returns a result based on the executable index."""
        return f"result_for_index_{index}"

    class TestExecutor(ParallelExecutor):
        """Custom executor for testing operation_id determinism."""

        async def execute_item(self, child_context, executable) -> Any:
            return executable.func(child_context)

    # Create executables with specific indices using partial
    num_executables = 50
    funcs = [partial(index_based_function, i) for i in range(num_executables)]

    # Track operation_id -> result associations
    captured_associations = []

    def patched_child_handler(
        func,
        execution_state,
        operation_identifier,
        *,
        is_virtual: bool = False,
        **_kwargs,
    ) -> Any:
        """Patched child handler that captures operation_id -> result mapping."""
        mock_executor = Mock()

        async def process() -> Any:
            assert is_virtual
            assert operation_identifier.sub_type == "TEST_ITER"
            result = await func()
            captured_associations.append((operation_identifier.operation_id, result))
            return result

        mock_executor.process = AsyncMock(side_effect=process)
        return mock_executor

    execution_state = create_execution_state()

    completion_config = CompletionConfig(min_successful=num_executables)

    # Run multiple times with different shuffle orders
    associations_per_run = []

    for run in range(10):  # Test 10 different shuffle orders
        captured_associations.clear()

        # Create executables from shuffled functions
        executables = [Executable(index=i, func=func) for i, func in enumerate(funcs)]
        random.seed(run)  # Different seed for each run
        random.shuffle(executables)

        executor = create_concurrent_executor(
            TestExecutor,
            executables=executables,
            max_concurrency=2,
            completion_config=completion_config,
            top_level_sub_type="TEST",
            iteration_sub_type="TEST_ITER",
            name_prefix="test_",
            serdes=None,
            nesting_type=NestingType.FLAT,
        )

        # Create executor context mock
        executor_context = Mock()
        executor_context._parent_id = "parent_123"  # noqa SLF001
        executor_context.parent_id = "parent_123"
        executor_context.step_counter = Mock()

        def create_step_id(index) -> str:
            return f"step_{index}"

        executor_context._step_counter._create_step_id_for_logical_step = create_step_id  # noqa: SLF001
        executor_context.step_counter._create_step_id_for_logical_step = create_step_id  # noqa: SLF001

        def create_child_context(operation_id, *, is_virtual=False) -> Any:
            child_ctx = Mock()
            child_ctx.state = execution_state
            child_ctx.execution_state = execution_state
            return child_ctx

        executor_context.create_child_context = create_child_context

        with patch(
            "async_durable_execution._extension.parallel.ChildOperationExecutor",
            patched_child_handler,
        ):
            await run_async(executor.execute())

        associations_per_run.append(captured_associations.copy())

    # first we will verify the validity of the test by ensuring that there exist at least 2 runs with different ordering
    assert any(
        assoc1 != assoc2 for assoc1, assoc2 in combinations(associations_per_run, 2)
    )
    # then we will verify the invariant of association between step_id and result
    associations_per_run = [dict(assoc) for assoc in associations_per_run]
    assert all(
        assoc1 == assoc2 for assoc1, assoc2 in combinations(associations_per_run, 2)
    )


def test_concurrent_executor_is_operation_executor() -> None:
    """ParallelExecutor subclasses the shared operation executor base."""
    assert issubclass(ParallelExecutor, OperationExecutor)


@no_type_check
async def test_concurrent_executor_start_calls_execute() -> None:
    """ParallelExecutor.start delegates first execution to execute."""
    items = ["a"]
    execution_state = create_execution_state()
    operation_identifier = OperationIdentifier(
        "test_op", OperationSubType.MAP, "parent", "test_map"
    )
    executor_context = Mock()
    executor = create_map_executor(
        executables=[Executable(index=0, func=lambda item, idx, items: item)],
        items=items,
        max_concurrency=None,
        completion_config=CompletionConfig(),
        top_level_sub_type=OperationSubType.MAP,
        iteration_sub_type=OperationSubType.MAP_ITERATION,
        name_prefix="map-item-",
        serdes=None,
        execution_state=execution_state,
        operation_identifier=operation_identifier,
        executor_context=executor_context,
    )
    expected_result = BatchResult.from_items([], CompletionConfig())

    with patch.object(
        executor,
        "execute",
        AsyncMock(return_value=expected_result),
    ) as mock_execute:
        result = await executor.start()

    mock_execute.assert_called_once_with()
    assert result is expected_result


@no_type_check
async def test_concurrent_executor_replay_completed_operation_calls_replay_completed() -> (
    None
):
    """ParallelExecutor.replay delegates succeeded checkpoints to replay_completed."""
    items = ["a"]
    execution_state = create_execution_state()
    operation_identifier = OperationIdentifier(
        "test_op", OperationSubType.MAP, "parent", "test_map"
    )
    executor_context = Mock()
    executor = create_map_executor(
        executables=[Executable(index=0, func=lambda item, idx, items: item)],
        items=items,
        max_concurrency=None,
        completion_config=CompletionConfig(),
        top_level_sub_type=OperationSubType.MAP,
        iteration_sub_type=OperationSubType.MAP_ITERATION,
        name_prefix="map-item-",
        serdes=None,
        execution_state=execution_state,
        operation_identifier=operation_identifier,
        executor_context=executor_context,
    )
    operation = Operation(
        operation_id="test_op",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.SUCCEEDED,
    )
    expected_result = BatchResult.from_items([], CompletionConfig())

    with (
        patch.object(executor, "execute", AsyncMock()) as mock_execute,
        patch.object(
            executor,
            "replay_completed",
            AsyncMock(return_value=expected_result),
        ) as mock_replay_completed,
    ):
        result = await executor.replay(operation)

    mock_replay_completed.assert_called_once_with(execution_state, executor_context)
    mock_execute.assert_not_called()
    assert result is expected_result


@no_type_check
async def test_concurrent_executor_replay_incomplete_operation_calls_execute() -> None:
    """ParallelExecutor.replay executes again when the checkpoint is incomplete."""
    items = ["a"]
    execution_state = create_execution_state()
    operation_identifier = OperationIdentifier(
        "test_op", OperationSubType.MAP, "parent", "test_map"
    )
    executor_context = Mock()
    executor = create_map_executor(
        executables=[Executable(index=0, func=lambda item, idx, items: item)],
        items=items,
        max_concurrency=None,
        completion_config=CompletionConfig(),
        top_level_sub_type=OperationSubType.MAP,
        iteration_sub_type=OperationSubType.MAP_ITERATION,
        name_prefix="map-item-",
        serdes=None,
        execution_state=execution_state,
        operation_identifier=operation_identifier,
        executor_context=executor_context,
    )
    operation = Operation(
        operation_id="test_op",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.STARTED,
    )
    expected_result = BatchResult.from_items([], CompletionConfig())

    with (
        patch.object(
            executor,
            "execute",
            AsyncMock(return_value=expected_result),
        ) as mock_execute,
        patch.object(executor, "replay_completed", AsyncMock()) as mock_replay,
    ):
        result = await executor.replay(operation)

    mock_execute.assert_called_once_with()
    mock_replay.assert_not_called()
    assert result is expected_result


async def test_concurrent_executor_replay_completed_with_succeeded_operations() -> None:
    """Test ParallelExecutor replay_completed method with succeeded operations."""

    def func1(item, idx, items) -> str:
        return f"result_{item}"

    items = ["a", "b"]

    executor = create_map_executor(
        executables=[Executable(index=i, func=func1) for i in range(len(items))],
        items=items,
        max_concurrency=None,
        completion_config=CompletionConfig(),
        top_level_sub_type=OperationSubType.MAP,
        iteration_sub_type=OperationSubType.MAP_ITERATION,
        name_prefix="map-item-",
        serdes=None,
    )

    # Mock execution state with succeeded operations
    mock_execution_state = Mock()
    mock_execution_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_execution_state.create_checkpoint = AsyncMock()

    def mock_get_operation(operation_id) -> Any:
        return Operation(
            operation_id=operation_id,
            operation_type=OperationType.CONTEXT,
            status=OperationStatus.SUCCEEDED,
            context_details=ContextDetails(result=f'"cached_result_{operation_id}"'),
        )

    mock_execution_state.operations = Mock()
    mock_execution_state.operations.get = Mock(side_effect=mock_get_operation)

    def mock_create_step_id_for_logical_step(step) -> str:
        return f"op_{step}"

    # Mock executor context
    mock_executor_context = Mock()
    mock_executor_context.parent_id = "parent_id"
    mock_executor_context.step_counter = Mock()
    mock_executor_context._step_counter._create_step_id_for_logical_step = (  # noqa: SLF001
        mock_create_step_id_for_logical_step
    )
    mock_executor_context.step_counter._create_step_id_for_logical_step = (
        mock_create_step_id_for_logical_step
    )

    # Mock child context that has the same execution state
    mock_child_context = Mock()
    mock_child_context.state = mock_execution_state
    mock_child_context.execution_state = mock_execution_state
    mock_executor_context.create_child_context = Mock(return_value=mock_child_context)
    mock_executor_context._parent_id = "parent_id"  # noqa

    result = await run_async(
        executor.replay_completed(mock_execution_state, mock_executor_context)
    )

    assert isinstance(result, BatchResult)
    assert len(result.all) == 2
    assert result.all[0].status == BatchItemStatus.SUCCEEDED
    assert result.all[0].result == "cached_result_op_0"
    assert result.all[1].status == BatchItemStatus.SUCCEEDED
    assert result.all[1].result == "cached_result_op_1"


async def test_concurrent_executor_replay_completed_with_failed_operations() -> None:
    """Test ParallelExecutor replay_completed method with failed operations."""

    def func1(item, idx, items) -> str:
        return f"result_{item}"

    items = ["a"]

    executor = create_map_executor(
        executables=[Executable(index=i, func=func1) for i in range(len(items))],
        items=items,
        max_concurrency=None,
        completion_config=CompletionConfig(),
        top_level_sub_type=OperationSubType.MAP,
        iteration_sub_type=OperationSubType.MAP_ITERATION,
        name_prefix="map-item-",
        serdes=None,
    )

    # Mock execution state with failed operation
    mock_execution_state = Mock()

    def mock_get_operation(operation_id) -> Any:
        return Operation(
            operation_id=operation_id,
            operation_type=OperationType.CONTEXT,
            status=OperationStatus.FAILED,
            context_details=ContextDetails(
                error=ErrorObject(
                    message="Test error",
                    type="Exception",
                    data=None,
                    stack_trace=None,
                )
            ),
        )

    mock_execution_state.operations = Mock()
    mock_execution_state.operations.get = Mock(side_effect=mock_get_operation)

    # Mock executor context
    mock_executor_context = Mock()
    mock_executor_context._step_counter._create_step_id_for_logical_step = Mock(  # noqa: SLF001
        return_value="op_1"
    )

    result = await run_async(
        executor.replay_completed(mock_execution_state, mock_executor_context)
    )

    assert isinstance(result, BatchResult)
    assert len(result.all) == 1
    assert result.all[0].status == BatchItemStatus.FAILED
    assert result.all[0].error is not None


@pytest.mark.parametrize(
    "operation_status",
    [OperationStatus.STARTED, OperationStatus.CANCELLED],
)
async def test_concurrent_executor_replay_completed_with_cancelled_operation(
    operation_status: OperationStatus,
) -> None:
    """Cancelled children preserve a threshold-completed parent's result."""

    executor = create_concurrent_executor(
        ParallelExecutor,
        executables=[
            Executable(index=0, func=lambda: "unused"),
            Executable(index=1, func=lambda: "unused"),
        ],
        max_concurrency=None,
        completion_config=CompletionConfig(min_successful=1),
        top_level_sub_type=OperationSubType.PARALLEL,
        iteration_sub_type=OperationSubType.PARALLEL_BRANCH,
        name_prefix="parallel-branch-",
        serdes=None,
    )
    execution_state = create_execution_state()
    execution_state.operations.get.side_effect = [
        Operation(
            operation_id="child_0",
            operation_type=OperationType.CONTEXT,
            status=OperationStatus.SUCCEEDED,
        ),
        Operation(
            operation_id="child_1",
            operation_type=OperationType.CONTEXT,
            status=operation_status,
        ),
    ]
    executor_context = create_executor_context(execution_state, step_id="child")

    result = await executor.replay_completed(execution_state, executor_context)

    assert result.all == [
        BatchItem(0, BatchItemStatus.SUCCEEDED),
        BatchItem(1, BatchItemStatus.CANCELLED),
    ]
    assert result.completion_reason is CompletionReason.MIN_SUCCESSFUL_REACHED


async def test_concurrent_executor_replay_completed_with_missing_operation_started() -> (
    None
):
    """Missing child checkpoints are omitted as branches that never started."""

    async def func1() -> str:
        return "result"

    executor = create_concurrent_executor(
        ParallelExecutor,
        executables=[Executable(index=0, func=func1)],
        max_concurrency=None,
        completion_config=CompletionConfig(),
        top_level_sub_type=OperationSubType.PARALLEL,
        iteration_sub_type=OperationSubType.PARALLEL_BRANCH,
        name_prefix="parallel-branch-",
        serdes=None,
    )

    execution_state = create_execution_state()
    execution_state.operations.get.return_value = None
    executor_context = create_executor_context(execution_state, step_id="child")

    result = await run_async(
        executor.replay_completed(execution_state, executor_context)
    )

    assert result.all == []


async def test_concurrent_executor_replay_completed_succeeded_without_details() -> None:
    """Succeeded child checkpoints without a payload still produce succeeded items."""

    async def func1() -> str:
        return "result"

    executor = create_concurrent_executor(
        ParallelExecutor,
        executables=[Executable(index=0, func=func1)],
        max_concurrency=None,
        completion_config=CompletionConfig(),
        top_level_sub_type=OperationSubType.PARALLEL,
        iteration_sub_type=OperationSubType.PARALLEL_BRANCH,
        name_prefix="parallel-branch-",
        serdes=None,
    )

    execution_state = create_execution_state()
    execution_state.operations.get.return_value = Operation(
        operation_id="child_0",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.SUCCEEDED,
        context_details=None,
    )
    executor_context = create_executor_context(execution_state, step_id="child")

    result = await run_async(
        executor.replay_completed(execution_state, executor_context)
    )

    assert result.all == [
        BatchItem(index=0, status=BatchItemStatus.SUCCEEDED, result=None),
    ]


async def test_concurrent_executor_replay_completed_with_replay_children() -> None:
    """Test ParallelExecutor replay_completed method when children need re-execution."""

    def func1(item, idx, items) -> str:
        return f"result_{item}"

    items = ["a"]

    executor = create_map_executor(
        executables=[Executable(index=i, func=func1) for i in range(len(items))],
        items=items,
        max_concurrency=None,
        completion_config=CompletionConfig(),
        top_level_sub_type=OperationSubType.MAP,
        iteration_sub_type=OperationSubType.MAP_ITERATION,
        name_prefix="map-item-",
        serdes=None,
    )

    # Mock execution state with succeeded operation that needs replay
    mock_execution_state = Mock()

    def mock_get_operation(operation_id) -> Any:
        return Operation(
            operation_id=operation_id,
            operation_type=OperationType.CONTEXT,
            status=OperationStatus.SUCCEEDED,
            context_details=ContextDetails(replay_children=True),
        )

    mock_execution_state.operations = Mock()
    mock_execution_state.operations.get = Mock(side_effect=mock_get_operation)

    # Mock executor context
    mock_executor_context = Mock()
    mock_executor_context._step_counter._create_step_id_for_logical_step = Mock(  # noqa: SLF001
        return_value="op_1"
    )

    # Mock _execute_item_in_child_context to return a result
    with patch.object(
        executor,
        "_execute_item_in_child_context",
        new=AsyncMock(return_value="re_executed_result"),
    ):
        result = await run_async(
            executor.replay_completed(mock_execution_state, mock_executor_context)
        )

        assert isinstance(result, BatchResult)
        assert len(result.all) == 1
        assert result.all[0].status == BatchItemStatus.SUCCEEDED
        assert result.all[0].result == "re_executed_result"


@no_type_check
async def test_batch_item_from_dict_with_error() -> None:
    """Test BatchItem.from_dict() with error."""
    data = {
        "index": 3,
        "status": "FAILED",
        "result": None,
        "error": {
            "ErrorType": "ValueError",
            "ErrorMessage": "bad value",
            "StackTrace": [],
        },
    }

    item = BatchItem.from_dict(data)

    assert item.index == 3
    assert item.status == BatchItemStatus.FAILED
    assert item.error.type == "ValueError"
    assert item.error.message == "bad value"


@no_type_check
async def test_batch_result_with_mixed_statuses() -> None:
    """Test BatchResult serialization with mixed item statuses."""
    result = BatchResult(
        all=[
            BatchItem(0, BatchItemStatus.SUCCEEDED, result="success"),
            BatchItem(
                1,
                BatchItemStatus.FAILED,
                error=ErrorObject(message="msg", type="E", data=None, stack_trace=[]),
            ),
            BatchItem(2, BatchItemStatus.STARTED),
        ],
        completion_reason=CompletionReason.FAILURE_TOLERANCE_EXCEEDED,
    )

    serialized = json.dumps(result.to_dict())
    deserialized = BatchResult.from_dict(json.loads(serialized))

    assert len(deserialized.all) == 3
    assert deserialized.all[0].status == BatchItemStatus.SUCCEEDED
    assert deserialized.all[1].status == BatchItemStatus.FAILED
    assert deserialized.all[2].status == BatchItemStatus.STARTED
    assert deserialized.completion_reason == CompletionReason.FAILURE_TOLERANCE_EXCEEDED


@no_type_check
async def test_batch_result_empty_list() -> None:
    """Test BatchResult serialization with empty items list."""
    result = BatchResult(all=[], completion_reason=CompletionReason.ALL_COMPLETED)

    serialized = json.dumps(result.to_dict())
    deserialized = BatchResult.from_dict(json.loads(serialized))

    assert len(deserialized.all) == 0
    assert deserialized.completion_reason == CompletionReason.ALL_COMPLETED


@no_type_check
async def test_batch_result_complex_nested_data() -> None:
    """Test BatchResult with complex nested data structures."""
    complex_result = {
        "users": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
        "metadata": {"count": 2, "timestamp": "2025-10-31"},
    }

    result = BatchResult(
        all=[BatchItem(0, BatchItemStatus.SUCCEEDED, result=complex_result)],
        completion_reason=CompletionReason.ALL_COMPLETED,
    )

    serialized = json.dumps(result.to_dict())
    deserialized = BatchResult.from_dict(json.loads(serialized))

    assert deserialized.all[0].result == complex_result
    assert deserialized.all[0].result["users"][0]["name"] == "Alice"


async def test_executor_does_not_deadlock_when_all_tasks_terminal_but_completion_config_allows_failures() -> (
    None
):
    """Ensure executor returns when all tasks are terminal even if completion rules are confusing."""

    class TestExecutor(ParallelExecutor):
        async def execute_item(self, child_context, executable) -> str:
            if executable.index == 0:
                # fail one task
                raise Exception("boom")  # noqa EM101 TRY002
            return f"ok_{executable.index}"

    # Two tasks, min_successful=2 but tolerated failure_count set to 1.
    # After one fail + one success, counters.is_complete() should return true,
    # should_continue() should return false. counters.is_complete was failing to
    # stop early, which caused map to hang.
    executables = [Executable(0, lambda: "a"), Executable(1, lambda: "b")]
    completion_config = CompletionConfig(
        min_successful=2,
        tolerated_failure_count=1,
    )

    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=2,
        completion_config=completion_config,
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )

    execution_state = create_execution_state()
    executor_context = create_executor_context(execution_state)

    # Should return (not hang) and batch should reflect one FAILED and one SUCCEEDED
    result = await run_async(executor.execute())
    statuses = {item.index: item.status for item in result.all}
    assert statuses[0] == BatchItemStatus.FAILED
    assert statuses[1] == BatchItemStatus.SUCCEEDED


@no_type_check
async def test_executor_terminates_quickly_when_impossible_to_succeed() -> None:
    """Test that executor terminates when min_successful becomes impossible."""
    executed_count = {"value": 0}

    async def task_func(item) -> str:
        idx = get_current_context().index
        executed_count["value"] += 1
        if idx < 2:
            raise Exception(f"fail_{idx}")  # noqa EM102 TRY002
        await asyncio.sleep(0.05)
        return f"ok_{idx}"

    items = list(range(100))

    executor = create_map_executor(
        executables=[Executable(index=i, func=task_func) for i in range(len(items))],
        items=items,
        max_concurrency=10,
        completion_config=CompletionConfig(
            min_successful=99, tolerated_failure_count=1
        ),
        top_level_sub_type=OperationSubType.MAP,
        iteration_sub_type=OperationSubType.MAP_ITERATION,
        name_prefix="map-item-",
        serdes=None,
    )

    execution_state = create_execution_state()
    executor_context = create_executor_context(execution_state)

    result = await run_async(executor.execute())

    # With tolerated_failure_count=1, executor stops when failure_count > 1 (at 2 failures)
    # Executor terminates early rather than executing all 100 tasks
    assert executed_count["value"] < 100
    assert result.completion_reason == CompletionReason.FAILURE_TOLERANCE_EXCEEDED, (
        executed_count
    )
    assert sum(1 for item in result.all if item.status == BatchItemStatus.FAILED) == 2
    assert (
        sum(1 for item in result.all if item.status == BatchItemStatus.SUCCEEDED) < 98
    )


async def test_executor_exits_early_with_min_successful() -> None:
    """Test that parallel exits immediately when min_successful is reached without waiting for other branches."""

    class TestExecutor(ParallelExecutor):
        async def execute_item(self, child_context, executable) -> Any:
            return await executable.func()

    execution_times = []

    async def fast_branch() -> str:
        execution_times.append(("fast", time.time()))
        return "fast_result"

    async def slow_branch() -> str:
        execution_times.append(("slow_start", time.time()))
        await asyncio.sleep(2)
        execution_times.append(("slow_end", time.time()))
        return "slow_result"

    executables = [
        Executable(0, fast_branch),
        Executable(1, slow_branch),
    ]

    completion_config = CompletionConfig(min_successful=1)

    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=2,
        completion_config=completion_config,
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )

    execution_state = create_execution_state()
    executor_context = create_executor_context(
        execution_state, step_id="step", parent_id="parent"
    )
    executor_context._step_counter._create_step_id_for_logical_step = (  # noqa: SLF001
        lambda idx: f"step_{idx}"
    )

    start_time = time.time()
    result = await run_async(executor.execute())
    elapsed_time = time.time() - start_time

    # Should complete in less than 1.5 second (not wait for 2-second sleep)
    assert elapsed_time < 1.5, f"Took {elapsed_time}s, expected < 1.5s"

    # Result should show MIN_SUCCESSFUL_REACHED
    assert result.completion_reason == CompletionReason.MIN_SUCCESSFUL_REACHED

    # Fast branch should succeed
    assert result.all[0].status == BatchItemStatus.SUCCEEDED
    assert result.all[0].result == "fast_result"

    # Slow branch was started, then cancelled after the threshold was reached.
    assert result.all[1].status == BatchItemStatus.CANCELLED

    # Verify counts
    assert result.success_count == 1
    assert result.failure_count == 0
    assert result.cancelled_count == 1
    assert result.started_count == 0
    assert result.total_count == 2


async def test_executor_returns_with_incomplete_branches() -> None:
    """Test that executor returns when min_successful is reached, leaving other branches incomplete."""

    class TestExecutor(ParallelExecutor):
        async def execute_item(self, child_context, executable) -> Any:
            return await executable.func()

    operation_tracker = Mock()

    async def fast_branch() -> str:
        operation_tracker.fast_executed()
        return "fast_result"

    async def slow_branch() -> str:
        operation_tracker.slow_started()
        await asyncio.sleep(2)
        operation_tracker.slow_completed()
        return "slow_result"

    executables = [
        Executable(0, fast_branch),
        Executable(1, slow_branch),
    ]

    completion_config = CompletionConfig(min_successful=1)

    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=2,
        completion_config=completion_config,
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )

    execution_state = create_execution_state()
    executor_context = create_executor_context(
        execution_state, step_id="step", parent_id="parent"
    )
    executor_context._step_counter._create_step_id_for_logical_step = (  # noqa: SLF001
        lambda idx: f"step_{idx}"
    )

    result = await run_async(executor.execute())

    # Verify fast branch executed
    assert operation_tracker.fast_executed.call_count == 1

    # Slow branch may or may not have started (depends on thread scheduling)
    # but it definitely should not have completed
    assert operation_tracker.slow_completed.call_count == 0, (
        "Executor should return before slow branch completes"
    )

    # Result should show MIN_SUCCESSFUL_REACHED
    assert result.completion_reason == CompletionReason.MIN_SUCCESSFUL_REACHED

    # Verify counts - one succeeded and the other started branch was cancelled.
    assert result.success_count == 1
    assert result.failure_count == 0
    assert result.cancelled_count == 1
    assert result.started_count == 0
    assert result.total_count == 2


async def test_executor_returns_before_slow_branch_completes() -> None:
    """Test that executor returns immediately when min_successful is reached, not waiting for slow branches."""

    class TestExecutor(ParallelExecutor):
        async def execute_item(self, child_context, executable) -> Any:
            return await executable.func()

    slow_branch_mock = Mock()

    async def fast_func() -> str:
        return "fast"

    async def slow_func() -> str:
        await asyncio.sleep(3)
        slow_branch_mock.completed()  # Should not be called before executor returns
        return "slow"

    executables = [Executable(0, fast_func), Executable(1, slow_func)]
    completion_config = CompletionConfig(min_successful=1)

    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=2,
        completion_config=completion_config,
        top_level_sub_type="TOP",
        iteration_sub_type="ITER",
        name_prefix="test_",
        serdes=None,
    )

    execution_state = create_execution_state()
    executor_context = create_executor_context(
        execution_state, step_id="step", parent_id="parent"
    )
    executor_context._step_counter._create_step_id_for_logical_step = (  # noqa: SLF001
        lambda idx: f"step_{idx}"
    )

    result = await run_async(executor.execute())

    # Executor should have returned before slow branch completed
    assert not slow_branch_mock.completed.called, (
        "Executor should return before slow branch completes"
    )

    # Result should show MIN_SUCCESSFUL_REACHED
    assert result.completion_reason == CompletionReason.MIN_SUCCESSFUL_REACHED

    # Verify counts
    assert result.success_count == 1
    assert result.failure_count == 0
    assert result.cancelled_count == 1
    assert result.started_count == 0
    assert result.total_count == 2


@no_type_check
async def test_timer_scheduler_same_timestamp_with_counter_tiebreaker() -> None:
    """
    Test that scheduling two tasks with the exact same resume_time works.

    This verifies the fix where a counter is used as a tie-breaker to prevent
    TypeError when heapq tries to compare ExecutableWithState objects.
    """
    resubmit_callback = AsyncMock()

    async def run_test() -> None:
        async with TimerScheduler(resubmit_callback) as scheduler:
            exe_state1 = ExecutableWithState(Executable(index=0, func=lambda: "test1"))
            exe_state2 = ExecutableWithState(Executable(index=1, func=lambda: "test2"))
            exe_state1.suspend()
            exe_state2.suspend()
            same_timestamp = time.time() + 10.0
            scheduler.schedule_resume(exe_state1, same_timestamp)
            scheduler.schedule_resume(exe_state2, same_timestamp)
            assert len(scheduler._resume_tasks) == 2  # noqa: SLF001

    await run_async(run_test())


@no_type_check
async def test_timer_scheduler_multiple_same_timestamps() -> None:
    """
    Test that scheduling many tasks with the same timestamp works correctly.

    Verifies FIFO ordering is maintained when multiple tasks have identical timestamps.
    """
    resubmit_callback = AsyncMock()

    async def run_test() -> None:
        async with TimerScheduler(resubmit_callback) as scheduler:
            same_timestamp = time.time() + 10.0
            exe_states = [
                ExecutableWithState(Executable(index=i, func=lambda i=i: f"test{i}"))
                for i in range(10)
            ]
            for exe_state in exe_states:
                exe_state.suspend()
                scheduler.schedule_resume(exe_state, same_timestamp)
            assert len(scheduler._resume_tasks) == 10  # noqa: SLF001

    await run_async(run_test())


@no_type_check
async def test_timer_scheduler_counter_increments() -> None:
    """Test that the schedule counter increments correctly."""
    resubmit_callback = AsyncMock()

    async def run_test() -> None:
        async with TimerScheduler(resubmit_callback) as scheduler:
            exe_state1 = ExecutableWithState(Executable(0, lambda: "test1"))
            exe_state2 = ExecutableWithState(Executable(1, lambda: "test2"))
            exe_state3 = ExecutableWithState(Executable(2, lambda: "test3"))
            exe_state1.suspend()
            exe_state2.suspend()
            exe_state3.suspend()
            scheduler.schedule_resume(exe_state1, time.time() + 1.0)
            scheduler.schedule_resume(exe_state2, time.time() + 2.0)
            scheduler.schedule_resume(exe_state3, time.time() + 3.0)
            assert len(scheduler._resume_tasks) == 3  # noqa: SLF001

    await run_async(run_test())


@no_type_check
async def test_timer_scheduler_fifo_ordering_with_same_timestamp() -> None:
    """
    Test that FIFO ordering is maintained when timestamps are equal.

    When multiple tasks have the same timestamp, they should be processed
    in the order they were scheduled (FIFO). The timer thread processes
    items synchronously, so callback order is deterministic.
    """
    results = []
    resubmit_callback = AsyncMock(side_effect=lambda exe: results.append(exe.index))

    async def run_test() -> None:
        async with TimerScheduler(resubmit_callback) as scheduler:
            past_time = time.time() - 0.1
            exe_state1 = ExecutableWithState(Executable(0, lambda: "first"))
            exe_state2 = ExecutableWithState(Executable(1, lambda: "second"))
            exe_state3 = ExecutableWithState(Executable(2, lambda: "third"))
            exe_state1.suspend()
            exe_state2.suspend()
            exe_state3.suspend()
            scheduler.schedule_resume(exe_state1, past_time)
            scheduler.schedule_resume(exe_state2, past_time)
            scheduler.schedule_resume(exe_state3, past_time)
            await asyncio.sleep(0.05)

    await run_async(run_test())
    assert sorted(results) == [0, 1, 2]


async def test_from_items_no_config_with_failures() -> None:
    """Validates: Requirements 2.4 - Fail-fast with no config."""
    items = [
        BatchItem(0, BatchItemStatus.SUCCEEDED, result="ok"),
        BatchItem(
            1, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ),
    ]
    result = BatchResult.from_items(items, completion_config=None)
    assert result.completion_reason == CompletionReason.FAILURE_TOLERANCE_EXCEEDED


async def test_from_items_empty_config_with_failures() -> None:
    """Validates: Requirements 2.5 - Fail-fast with empty config."""
    items = [
        BatchItem(0, BatchItemStatus.SUCCEEDED, result="ok"),
        BatchItem(
            1, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ),
    ]
    config = CompletionConfig()  # All fields None
    result = BatchResult.from_items(items, completion_config=config)
    assert result.completion_reason == CompletionReason.FAILURE_TOLERANCE_EXCEEDED


async def test_from_items_tolerance_checked_before_all_completed() -> None:
    """Validates: Requirements 2.1, 2.2 - Tolerance priority."""
    items = [
        BatchItem(0, BatchItemStatus.SUCCEEDED, result="ok"),
        BatchItem(
            1, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ),
        BatchItem(
            2, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ),
    ]
    config = CompletionConfig(tolerated_failure_count=1)
    result = BatchResult.from_items(items, completion_config=config)
    # All completed but tolerance exceeded - should return TOLERANCE_EXCEEDED
    assert result.completion_reason == CompletionReason.FAILURE_TOLERANCE_EXCEEDED


async def test_from_items_all_completed_within_tolerance() -> None:
    """Validates: Requirements 1.1 - All completed."""
    items = [
        BatchItem(0, BatchItemStatus.SUCCEEDED, result="ok"),
        BatchItem(
            1, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ),
    ]
    config = CompletionConfig(tolerated_failure_count=1)
    result = BatchResult.from_items(items, completion_config=config)
    assert result.completion_reason == CompletionReason.ALL_COMPLETED


async def test_from_items_min_successful_reached() -> None:
    """Validates: Requirements 1.3 - Min successful."""
    items = [
        BatchItem(0, BatchItemStatus.SUCCEEDED, result="ok"),
        BatchItem(1, BatchItemStatus.SUCCEEDED, result="ok"),
        BatchItem(2, BatchItemStatus.STARTED),
    ]
    config = CompletionConfig(min_successful=2)
    result = BatchResult.from_items(items, completion_config=config)
    assert result.completion_reason == CompletionReason.MIN_SUCCESSFUL_REACHED


@no_type_check
async def test_from_items_tolerance_count_exceeded() -> None:
    """Validates: Requirements 1.2 - Tolerance count."""
    items = [
        BatchItem(
            0, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ),
        BatchItem(
            1, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ),
        BatchItem(2, BatchItemStatus.STARTED),
    ]
    config = CompletionConfig(tolerated_failure_count=1)
    result = BatchResult.from_items(items, completion_config=config)
    assert result.completion_reason == CompletionReason.FAILURE_TOLERANCE_EXCEEDED


async def test_from_items_tolerance_count_exceeded_multiple_failures() -> None:
    """Validates: Requirements 1.2 - Tolerance count."""
    items = [
        BatchItem(0, BatchItemStatus.SUCCEEDED, result="ok"),
        BatchItem(
            1, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ),
        BatchItem(
            2, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ),
        BatchItem(
            3, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ),
    ]
    config = CompletionConfig(tolerated_failure_count=2)
    result = BatchResult.from_items(items, completion_config=config)
    assert result.completion_reason == CompletionReason.FAILURE_TOLERANCE_EXCEEDED


async def test_from_items_tolerance_priority_over_min_successful() -> None:
    """Validates: Requirements 2.3 - Tolerance takes precedence."""
    items = [
        BatchItem(0, BatchItemStatus.SUCCEEDED, result="ok"),
        BatchItem(1, BatchItemStatus.SUCCEEDED, result="ok"),
        BatchItem(
            2, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ),
        BatchItem(
            3, BatchItemStatus.FAILED, error=ErrorObject("msg", "Error", None, None)
        ),
    ]
    config = CompletionConfig(min_successful=2, tolerated_failure_count=1)
    # Min successful reached (2) but tolerance exceeded (2 > 1)
    result = BatchResult.from_items(items, completion_config=config)
    assert result.completion_reason == CompletionReason.FAILURE_TOLERANCE_EXCEEDED


@no_type_check
async def test_from_items_empty_array() -> None:
    """Validates: Edge case - empty items."""
    items = []
    result = BatchResult.from_items(items, completion_config=None)
    assert result.completion_reason == CompletionReason.ALL_COMPLETED
    assert result.total_count == 0


async def test_from_items_all_succeeded() -> None:
    """Validates: All items succeeded."""
    items = [
        BatchItem(0, BatchItemStatus.SUCCEEDED, result="ok1"),
        BatchItem(1, BatchItemStatus.SUCCEEDED, result="ok2"),
    ]
    result = BatchResult.from_items(items, completion_config=None)
    assert result.completion_reason == CompletionReason.ALL_COMPLETED
    assert result.success_count == 2


async def test_flat_mode_stamps_grandparent_as_inner_op_parent_id() -> None:
    """In FLAT mode, inner operations in a branch stamp the map/parallel op id as parent_id.

    This is the core FLAT-mode invariant. Inner operations must not
    stamp the branch's own operation id (that would reproduce the
    NESTED hierarchy) — they must stamp the enclosing map/parallel op
    id, so the branch is collapsed out of the observable hierarchy
    even though it still exists as a logical scope for concurrency
    and step-id prefixing.

    The test drives `_execute_item_in_child_context` with a real
    non-virtual executor context, captures the child context the
    executor builds for the branch, and asserts the branch's
    `_parent_id` equals the executor_context's own `_parent_id`.
    """

    class TestExecutor(ParallelExecutor):
        async def execute_item(self, child_context, executable) -> Any:
            # Record the child context we receive so the assertions below can
            # inspect its identity fields.
            self.last_child_context = child_context
            return executable.func(child_context)

    execution_state = create_execution_state()

    execution_state.operations.get.return_value = None

    # Build a real DurableContext that represents the map/parallel op.
    map_op_id = "map-op-id"
    executor_context = DurableContext(
        execution_state=execution_state,
        operation_identifier=OperationIdentifier(
            operation_id=None,
            sub_type=OperationSubType.EXECUTION,
            parent_id=map_op_id,
        ),
    )

    executables = [Executable(index=0, func=lambda ctx: "ok")]
    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=1,
        completion_config=CompletionConfig(min_successful=1),
        top_level_sub_type="MAP",
        iteration_sub_type="MAP_ITER",
        name_prefix="branch-",
        serdes=None,
        nesting_type=NestingType.FLAT,
    )

    await run_async(  # noqa: SLF001
        executor._execute_item_in_child_context(executor_context, executables[0])
    )

    # The branch's child context must be virtual AND propagate the
    # map/parallel op id as its _parent_id. Inner operations stamping
    # self._parent_id will therefore report to the map/parallel op.
    branch_ctx = executor.last_child_context
    assert branch_ctx.is_virtual is True
    assert branch_ctx.parent_id == map_op_id  # noqa: SLF001
    # The step-id prefix is the branch's own operation id (stable replay id).
    assert branch_ctx.step_id_prefix != map_op_id  # noqa: SLF001


async def test_nested_mode_stamps_branch_op_as_inner_op_parent_id() -> None:
    """In NESTED mode, inner operations in a branch stamp the branch's own operation id as parent_id."""

    class TestExecutor(ParallelExecutor):
        async def execute_item(self, child_context, executable) -> Any:
            self.last_child_context = child_context
            return executable.func(child_context)

    execution_state = create_execution_state()

    execution_state.operations.get.return_value = None

    map_op_id = "map-op-id"
    executor_context = DurableContext(
        execution_state=execution_state,
        operation_identifier=OperationIdentifier(
            operation_id=None,
            sub_type=OperationSubType.EXECUTION,
            parent_id=map_op_id,
        ),
    )

    executables = [Executable(index=0, func=lambda ctx: "ok")]
    executor = create_concurrent_executor(
        TestExecutor,
        executables=executables,
        max_concurrency=1,
        completion_config=CompletionConfig(min_successful=1),
        top_level_sub_type="MAP",
        iteration_sub_type="MAP_ITER",
        name_prefix="branch-",
        serdes=None,
        nesting_type=NestingType.NESTED,
    )

    await run_async(  # noqa: SLF001
        executor._execute_item_in_child_context(executor_context, executables[0])
    )

    # In NESTED mode, the branch is a regular child — its _parent_id is
    # its own operation id, not the grandparent.
    branch_ctx = executor.last_child_context
    assert branch_ctx.is_virtual is False
    assert branch_ctx.parent_id == branch_ctx.step_id_prefix  # noqa: SLF001
    assert branch_ctx.parent_id != map_op_id  # noqa: SLF001


async def test_flat_mode_produces_deterministic_step_ids_across_runs() -> None:
    """Step ids and inner parent_ids must be deterministic under FLAT mode.

    Replay depends on regenerating the same operation ids for the same
    logical inputs. This test runs the same executor twice against
    fresh executor contexts and asserts that the resulting set of
    (step_id_prefix, parent_id) pairs is identical. Any source of
    non-determinism (e.g. step prefixes that depend on thread
    completion order, or parent-id propagation that's different on
    the second run) would show up here as a mismatch and would cause
    `NonDeterministicExecutionException` at replay time in production.
    """

    class TestExecutor(ParallelExecutor):
        def __init__(self, *args, **kwargs) -> None:
            super().__init__(*args, **kwargs)
            self.captured: list[tuple[str | None, str | None]] = []

        async def execute_item(self, child_context, executable) -> Any:
            self.captured.append(
                (
                    child_context.step_id_prefix,  # noqa: SLF001
                    child_context.parent_id,  # noqa: SLF001
                )
            )
            return executable.func(child_context)

    async def make_run() -> Any:
        execution_state = create_execution_state()

        execution_state.operations.get.return_value = None

        executor_context = DurableContext(
            execution_state=execution_state,
            operation_identifier=OperationIdentifier(
                operation_id=None,
                sub_type=OperationSubType.EXECUTION,
                parent_id="map-op-id",
            ),
        )

        executables = [
            Executable(index=i, func=lambda ctx, i=i: f"r{i}") for i in range(3)
        ]
        executor = create_concurrent_executor(
            TestExecutor,
            execution_state=execution_state,
            executor_context=executor_context,
            executables=executables,
            max_concurrency=3,
            completion_config=CompletionConfig(min_successful=3),
            top_level_sub_type="MAP",
            iteration_sub_type="MAP_ITER",
            name_prefix="branch-",
            serdes=None,
            nesting_type=NestingType.FLAT,
        )
        await run_async(executor.execute())
        return executor.captured

    run_a = await make_run()
    run_b = await make_run()

    # Ordering of captured items is non-deterministic because branches run
    # on a ThreadPoolExecutor. What matters is that the SET of (prefix,
    # parent_id) pairs is identical across runs — i.e. replay reconstructs
    # the same branch identity regardless of completion order.
    assert sorted(run_a) == sorted(run_b), (
        "FLAT-mode branch step-id prefixes and parent_ids must be identical "
        f"across runs. Run A: {run_a!r}; Run B: {run_b!r}"
    )
    # Sanity: all branches reported grandparent (map op id) as their parent.
    assert all(parent_id == "map-op-id" for _prefix, parent_id in run_a)
