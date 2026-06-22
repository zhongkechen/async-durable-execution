"""Unit tests for wait_for_condition operation."""

import asyncio
import datetime
import inspect
import json
from datetime import timedelta
from unittest.mock import Mock, patch

import pytest
from async_durable_execution.context import (
    get_current_context,
)
from async_durable_execution.exceptions import (
    CallableRuntimeError,
    InvocationError,
    SuspendExecution,
)
from async_durable_execution.models import OperationIdentifier
from async_durable_execution.models import (
    ErrorObject,
    Operation,
    OperationStatus,
    OperationSubType,
    OperationType,
    StepDetails,
)
import logging
from async_durable_execution.operation.wait_for_condition import (
    WaitForConditionConfig,
    WaitForConditionOperationExecutor,
)
from async_durable_execution.state import ExecutionState
from async_durable_execution import WaitForConditionCheckContext
from async_durable_execution.models import WaitForConditionDecision
from async_durable_execution.config import JitterStrategy
from async_durable_execution.operation.base import CheckpointedResult
from async_durable_execution.operation.wait_for_condition import WaitStrategyBuilder

from ..serdes_test import CustomDictSerDes


def test_wait_for_condition_config_defaults():
    """WaitForConditionConfig can be omitted or partially specified."""

    config = WaitForConditionConfig()

    assert config.wait_strategy is None
    assert config.initial_state is None
    assert config.serdes is None


def test_wait_for_condition_config_stores_values():
    """WaitForConditionConfig stores custom values."""

    def wait_strategy(state, attempt):
        return timedelta(seconds=1)

    config = WaitForConditionConfig(
        wait_strategy=wait_strategy, initial_state={"count": 0}
    )

    assert config.wait_strategy is wait_strategy
    assert config.initial_state == {"count": 0}
    assert config.serdes is None


def test_wait_for_condition_config_with_serdes():
    """WaitForConditionConfig stores custom serdes."""
    serdes = CustomDictSerDes()

    def wait_strategy(state, attempt):
        return WaitForConditionDecision.stop_polling()

    config = WaitForConditionConfig(
        wait_strategy=wait_strategy,
        initial_state={"count": 0},
        serdes=serdes,
    )

    assert config.serdes is serdes


def test_wait_for_condition_config_importable_from_package_root():
    """WaitForConditionConfig remains re-exported from the package root."""
    from async_durable_execution import WaitForConditionConfig as ImportedConfig

    assert ImportedConfig is WaitForConditionConfig


async def _invoke_maybe_async(result):
    return await result


def _asyncify(func):
    if inspect.iscoroutinefunction(func):
        return func

    async def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


# Test helper - maintains old handler signature for backward compatibility in tests
async def wait_for_condition_handler(check, config, state, operation_identifier):
    """Test helper that wraps WaitForConditionOperationExecutor with old handler signature."""
    if hasattr(state, "wrap_user_function") and hasattr(
        state.wrap_user_function, "return_value"
    ):
        state.wrap_user_function.return_value = _asyncify(
            state.wrap_user_function.return_value
        )
    executor = WaitForConditionOperationExecutor(
        check=_asyncify(check),
        config=config,
        state=state,
        operation_identifier=operation_identifier,
    )
    return await _invoke_maybe_async(executor.process())


async def test_wait_for_condition_first_execution_condition_met():
    """Test wait_for_condition on first execution when condition is met."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    mock_state.operations.get.return_value = CheckpointedResult.create_not_found()

    mock_logger = Mock(spec=logging.Logger)

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        return state + 1

    mock_state.wrap_user_function.return_value = check_func

    def wait_strategy(state, attempt):
        return WaitForConditionDecision.stop_polling()

    config = WaitForConditionConfig(initial_state=5, wait_strategy=wait_strategy)

    result = await wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        config=config,
    )

    assert result == 6
    assert mock_state.create_checkpoint.call_count == 2  # START and SUCCESS


async def test_wait_for_condition_new_condition_result_with_optional_config():
    """Condition returns the next state and polling decision without config."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    mock_state.operations.get.return_value = CheckpointedResult.create_not_found()

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def condition(state):
        assert state is None
        return {"status": "done"}, WaitForConditionDecision.stop_polling()

    mock_state.wrap_user_function.return_value = condition

    result = await wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=condition,
        config=WaitForConditionConfig(),
    )

    assert result == {"status": "done"}
    assert mock_state.create_checkpoint.call_count == 2


async def test_wait_for_condition_new_condition_uses_delay_only_strategy():
    """Condition decides to continue and wait_strategy supplies only delay."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    mock_state.operations.get.return_value = CheckpointedResult.create_not_found()

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def condition(state):
        return state + 1, WaitForConditionDecision.continue_waiting()

    mock_state.wrap_user_function.return_value = condition

    config = WaitForConditionConfig(
        initial_state=5,
        wait_strategy=lambda state, attempt: timedelta(seconds=7),
    )

    with pytest.raises(SuspendExecution, match="will retry in 7 seconds"):
        await wait_for_condition_handler(
            state=mock_state,
            operation_identifier=op_id,
            check=condition,
            config=config,
        )

    assert mock_state.create_checkpoint.call_count == 2


async def test_wait_for_condition_first_execution_condition_not_met():
    """Test wait_for_condition on first execution when condition is not met."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    mock_state.operations.get.return_value = CheckpointedResult.create_not_found()

    mock_logger = Mock(spec=logging.Logger)

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        return state + 1

    mock_state.wrap_user_function.return_value = check_func

    def wait_strategy(state, attempt):
        return WaitForConditionDecision.continue_waiting(timedelta(seconds=30))

    config = WaitForConditionConfig(initial_state=5, wait_strategy=wait_strategy)

    with pytest.raises(SuspendExecution, match="will retry in 30 seconds"):
        await wait_for_condition_handler(
            state=mock_state,
            operation_identifier=op_id,
            check=check_func,
            config=config,
        )

    assert mock_state.create_checkpoint.call_count == 2  # START and RETRY


async def test_wait_for_condition_already_succeeded():
    """Test wait_for_condition when already completed successfully."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        step_details=StepDetails(result=json.dumps(42)),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.operations.get.return_value = mock_result

    mock_logger = Mock(spec=logging.Logger)
    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        return state + 1

    mock_state.wrap_user_function.return_value = check_func

    config = WaitForConditionConfig(
        initial_state=5,
        wait_strategy=lambda s, a: WaitForConditionDecision.stop_polling(),
    )

    result = await wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        config=config,
    )

    assert result == 42
    assert mock_state.create_checkpoint.call_count == 0  # No new checkpoints


async def test_wait_for_condition_already_succeeded_none_result():
    """Test wait_for_condition when already completed with None result."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        step_details=StepDetails(result=None),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.operations.get.return_value = mock_result

    mock_logger = Mock(spec=logging.Logger)
    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        return state + 1

    mock_state.wrap_user_function.return_value = check_func

    config = WaitForConditionConfig(
        initial_state=5,
        wait_strategy=lambda s, a: WaitForConditionDecision.stop_polling(),
    )

    result = await wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        config=config,
    )

    assert result is None


async def test_wait_for_condition_already_failed():
    """Test wait_for_condition when already failed."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.FAILED,
        step_details=StepDetails(
            error=ErrorObject("Test error", "TestError", None, None)
        ),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.operations.get.return_value = mock_result

    mock_logger = Mock(spec=logging.Logger)
    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        return state + 1

    mock_state.wrap_user_function.return_value = check_func

    config = WaitForConditionConfig(
        initial_state=5,
        wait_strategy=lambda s, a: WaitForConditionDecision.stop_polling(),
    )

    with pytest.raises(CallableRuntimeError):
        await wait_for_condition_handler(
            state=mock_state,
            operation_identifier=op_id,
            check=check_func,
            config=config,
        )


async def test_wait_for_condition_retry_with_state():
    """Test wait_for_condition on retry with previous state."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
        step_details=StepDetails(result=json.dumps(10), attempt=2),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.operations.get.return_value = mock_result

    mock_logger = Mock(spec=logging.Logger)

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        return state + 1

    mock_state.wrap_user_function.return_value = check_func

    config = WaitForConditionConfig(
        initial_state=5,
        wait_strategy=lambda s, a: WaitForConditionDecision.stop_polling(),
    )

    result = await wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        config=config,
    )

    assert result == 11  # 10 (from checkpoint) + 1
    assert mock_state.create_checkpoint.call_count == 1  # Only SUCCESS


async def test_wait_for_condition_retry_without_state():
    """Test wait_for_condition on retry without previous state."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
        step_details=StepDetails(result=None, attempt=2),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.operations.get.return_value = mock_result

    mock_logger = Mock(spec=logging.Logger)

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        return state + 1

    mock_state.wrap_user_function.return_value = check_func

    config = WaitForConditionConfig(
        initial_state=5,
        wait_strategy=lambda s, a: WaitForConditionDecision.stop_polling(),
    )

    result = await wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        config=config,
    )

    assert result == 6  # 5 (initial) + 1


async def test_wait_for_condition_retry_invalid_json_state():
    """Test wait_for_condition on retry with invalid JSON state."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
        step_details=StepDetails(result="invalid json", attempt=2),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.operations.get.return_value = mock_result

    mock_logger = Mock(spec=logging.Logger)

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        return state + 1

    mock_state.wrap_user_function.return_value = check_func

    config = WaitForConditionConfig(
        initial_state=5,
        wait_strategy=lambda s, a: WaitForConditionDecision.stop_polling(),
    )

    result = await wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        config=config,
    )

    assert result == 6  # Falls back to initial state


async def test_wait_for_condition_check_function_exception():
    """Test wait_for_condition when check function raises exception."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    mock_state.operations.get.return_value = CheckpointedResult.create_not_found()

    mock_logger = Mock(spec=logging.Logger)

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        msg = "Test error"
        raise ValueError(msg)

    mock_state.wrap_user_function.return_value = check_func

    config = WaitForConditionConfig(
        initial_state=5,
        wait_strategy=lambda s, a: WaitForConditionDecision.stop_polling(),
    )

    with pytest.raises(ValueError, match="Test error"):
        await wait_for_condition_handler(
            state=mock_state,
            operation_identifier=op_id,
            check=check_func,
            config=config,
        )

    assert mock_state.create_checkpoint.call_count == 2  # START and FAIL


async def test_wait_for_condition_check_context():
    """Test that check context is available via contextvars."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    mock_state.operations.get.return_value = CheckpointedResult.create_not_found()

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    captured_context = None

    def check_func(state):
        nonlocal captured_context
        captured_context = get_current_context()
        return state + 1

    mock_state.wrap_user_function.return_value = check_func

    config = WaitForConditionConfig(
        initial_state=5,
        wait_strategy=lambda s, a: WaitForConditionDecision.stop_polling(),
    )

    await wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        config=config,
    )

    assert isinstance(captured_context, WaitForConditionCheckContext)
    assert captured_context.attempt == 1


async def test_wait_for_condition_delay_seconds_none():
    """Test wait_for_condition with None delay_seconds."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    mock_state.operations.get.return_value = CheckpointedResult.create_not_found()

    mock_logger = Mock(spec=logging.Logger)

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        return state + 1

    mock_state.wrap_user_function.return_value = check_func

    def wait_strategy(state, attempt):
        return WaitForConditionDecision(should_continue=True, delay=timedelta())

    config = WaitForConditionConfig(initial_state=5, wait_strategy=wait_strategy)

    with pytest.raises(SuspendExecution, match="will retry in 0 seconds"):
        await wait_for_condition_handler(
            state=mock_state,
            operation_identifier=op_id,
            check=check_func,
            config=config,
        )


async def test_wait_for_condition_no_operation_in_checkpoint():
    """Test wait_for_condition when checkpoint has no operation."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"

    # Create a mock result that is started but has no operation
    mock_result = Mock()
    mock_result.is_succeeded.return_value = False
    mock_result.is_failed.return_value = False
    mock_result.is_pending.return_value = False
    mock_result.is_started_or_ready.return_value = True
    mock_result.is_existent.return_value = True
    mock_result.result = json.dumps(10)
    mock_result.operation = None

    mock_state.operations.get.return_value = mock_result

    mock_logger = Mock(spec=logging.Logger)

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        return state + 1

    mock_state.wrap_user_function.return_value = check_func

    config = WaitForConditionConfig(
        initial_state=5,
        wait_strategy=lambda s, a: WaitForConditionDecision.stop_polling(),
    )

    with pytest.raises(CallableRuntimeError, match="Missing checkpoint operation"):
        await wait_for_condition_handler(
            state=mock_state,
            operation_identifier=op_id,
            check=check_func,
            config=config,
        )


async def test_wait_for_condition_operation_no_step_details():
    """Test wait_for_condition when operation has no step_details."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"

    # Create operation without step_details
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
        step_details=None,
    )
    mock_result = CheckpointedResult.create_from_operation(operation)

    mock_state.operations.get.return_value = mock_result

    mock_logger = Mock(spec=logging.Logger)

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        return state + 1

    mock_state.wrap_user_function.return_value = check_func

    config = WaitForConditionConfig(
        initial_state=5,
        wait_strategy=lambda s, a: WaitForConditionDecision.stop_polling(),
    )

    result = await wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        config=config,
    )

    assert result == 6  # Falls back to initial_state and uses attempt=1


async def test_wait_for_condition_custom_delay_seconds():
    """Test wait_for_condition with custom delay_seconds."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    mock_state.operations.get.return_value = CheckpointedResult.create_not_found()

    mock_logger = Mock(spec=logging.Logger)

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        return state + 1

    mock_state.wrap_user_function.return_value = check_func

    def wait_strategy(state, attempt):
        return WaitForConditionDecision(
            should_continue=True, delay=timedelta(minutes=1)
        )

    config = WaitForConditionConfig(initial_state=5, wait_strategy=wait_strategy)

    with pytest.raises(SuspendExecution, match="will retry in 60 seconds"):
        await wait_for_condition_handler(
            state=mock_state,
            operation_identifier=op_id,
            check=check_func,
            config=config,
        )


async def test_wait_for_condition_attempt_number_passed_to_strategy():
    """Test that attempt number is correctly passed to wait strategy."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
        step_details=StepDetails(result=json.dumps(10), attempt=3),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.operations.get.return_value = mock_result

    mock_logger = Mock(spec=logging.Logger)

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        return state + 1

    mock_state.wrap_user_function.return_value = check_func

    captured_attempt = None

    def wait_strategy(state, attempt):
        nonlocal captured_attempt
        captured_attempt = attempt
        return WaitForConditionDecision.stop_polling()

    config = WaitForConditionConfig(initial_state=5, wait_strategy=wait_strategy)

    await wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        config=config,
    )

    assert captured_attempt == 4


async def test_wait_for_condition_attempt_sequence_is_monotonic():
    """Test that attempt numbers form a monotonically increasing sequence: 1, 2, 3, 4...

    This test validates the fix for the attempt counting bug where:
    - First execution (no checkpoint): attempt = 1
    - After first retry (checkpoint.attempt = 1): attempt = 2
    - After second retry (checkpoint.attempt = 2): attempt = 3
    - After third retry (checkpoint.attempt = 3): attempt = 4

    The current attempt should always be: checkpointed_attempts + 1
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"

    mock_logger = Mock(spec=logging.Logger)

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        return state + 1

    mock_state.wrap_user_function.return_value = check_func

    captured_attempts = []

    def wait_strategy(state, attempt):
        captured_attempts.append(attempt)
        return WaitForConditionDecision.stop_polling()

    config = WaitForConditionConfig(initial_state=5, wait_strategy=wait_strategy)

    # Test 1: First execution (no checkpoint exists)
    mock_state.operations.get.return_value = CheckpointedResult.create_not_found()

    await wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        config=config,
    )

    assert captured_attempts[-1] == 1, "First execution should have attempt=1"

    # Test 2: After first retry (checkpoint has attempt=1)
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
        step_details=StepDetails(result=json.dumps(10), attempt=1),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.operations.get.return_value = mock_result

    await wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        config=config,
    )

    assert captured_attempts[-1] == 2, (
        "After first retry (checkpoint.attempt=1), current attempt should be 2"
    )

    # Test 3: After second retry (checkpoint has attempt=2)
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
        step_details=StepDetails(result=json.dumps(10), attempt=2),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.operations.get.return_value = mock_result

    await wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        config=config,
    )

    assert captured_attempts[-1] == 3, (
        "After second retry (checkpoint.attempt=2), current attempt should be 3"
    )

    # Test 4: After third retry (checkpoint has attempt=3)
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
        step_details=StepDetails(result=json.dumps(10), attempt=3),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.operations.get.return_value = mock_result

    await wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        config=config,
    )

    assert captured_attempts[-1] == 4, (
        "After third retry (checkpoint.attempt=3), current attempt should be 4"
    )

    # Verify the complete sequence is monotonically increasing
    assert captured_attempts == [
        1,
        2,
        3,
        4,
    ], f"Expected [1, 2, 3, 4] but got {captured_attempts}"


async def test_wait_for_condition_state_passed_to_strategy():
    """Test that new state is correctly passed to wait strategy."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    mock_state.operations.get.return_value = CheckpointedResult.create_not_found()

    mock_logger = Mock(spec=logging.Logger)

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        return state * 2

    mock_state.wrap_user_function.return_value = check_func

    captured_state = None

    def wait_strategy(state, attempt):
        nonlocal captured_state
        captured_state = state
        return WaitForConditionDecision.stop_polling()

    config = WaitForConditionConfig(initial_state=5, wait_strategy=wait_strategy)

    await wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        config=config,
    )

    assert captured_state == 10  # 5 * 2


async def test_wait_for_condition_logger_with_log_info():
    """Test that the active context carries durable log metadata."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test:execution:123"
    mock_state.operations.get.return_value = CheckpointedResult.create_not_found()

    mock_logger = Mock(spec=logging.Logger)

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    captured_context = None

    def check_func(state):
        nonlocal captured_context
        captured_context = get_current_context()
        assert isinstance(captured_context, WaitForConditionCheckContext)
        assert captured_context.attempt == 1
        return state + 1

    mock_state.wrap_user_function.return_value = check_func

    config = WaitForConditionConfig(
        initial_state=5,
        wait_strategy=lambda s, a: WaitForConditionDecision.stop_polling(),
    )

    await wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        config=config,
    )

    assert captured_context is not None
    assert isinstance(captured_context, WaitForConditionCheckContext)
    assert captured_context.durable_execution_arn == "arn:aws:test:execution:123"
    assert captured_context.operation_id == "op1"
    assert captured_context.operation_name == "test_wait"
    assert captured_context.attempt == 1


async def test_wait_for_condition_zero_delay_seconds():
    """Test wait_for_condition with zero delay_seconds."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    mock_state.operations.get.return_value = CheckpointedResult.create_not_found()

    mock_logger = Mock(spec=logging.Logger)

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        return state + 1

    mock_state.wrap_user_function.return_value = check_func

    def wait_strategy(state, attempt):
        return WaitForConditionDecision(
            should_continue=True, delay=timedelta(seconds=0)
        )

    config = WaitForConditionConfig(initial_state=5, wait_strategy=wait_strategy)

    with pytest.raises(SuspendExecution, match="will retry in 0 seconds"):
        await wait_for_condition_handler(
            state=mock_state,
            operation_identifier=op_id,
            check=check_func,
            config=config,
        )


async def test_wait_for_condition_custom_serdes_first_execution_condition_met():
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    mock_state.operations.get.return_value = CheckpointedResult.create_not_found()

    mock_logger = Mock(spec=logging.Logger)

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )
    complex_result = {"key": "value", "number": 42, "list": [1, 2, 3]}

    def check_func(state):
        return complex_result

    mock_state.wrap_user_function.return_value = check_func

    def wait_strategy(state, attempt):
        return WaitForConditionDecision.stop_polling()

    config = WaitForConditionConfig(
        initial_state=5, wait_strategy=wait_strategy, serdes=CustomDictSerDes()
    )

    await wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        config=config,
    )
    expected_checkpoointed_result = (
        '{"key": "VALUE", "number": "84", "list": [1, 2, 3]}'
    )

    success_call = mock_state.create_checkpoint.call_args_list[1]
    success_operation = success_call[1]["operation_update"]
    assert success_operation.payload == expected_checkpoointed_result


async def test_wait_for_condition_custom_serdes_already_succeeded():
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        step_details=StepDetails(
            result='{"key": "VALUE", "number": "84", "list": [1, 2, 3]}'
        ),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.operations.get.return_value = mock_result

    mock_logger = Mock(spec=logging.Logger)
    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        return state + 1

    config = WaitForConditionConfig(
        initial_state=5,
        wait_strategy=lambda s, a: WaitForConditionDecision.stop_polling(),
        serdes=CustomDictSerDes(),
    )

    result = await wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        config=config,
    )

    assert result == {"key": "value", "number": 42, "list": [1, 2, 3]}


async def test_wait_for_condition_pending():
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    operation = Operation(
        operation_id="XXX",
        operation_type=OperationType.STEP,
        status=OperationStatus.PENDING,
        step_details=StepDetails(
            result='{"key": "VALUE", "number": "84", "list": [1, 2, 3]}',
            next_attempt_timestamp=datetime.datetime.fromtimestamp(
                1764547200, tz=datetime.timezone.utc
            ),
        ),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.operations.get.return_value = mock_result

    mock_logger = Mock(spec=logging.Logger)

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        msg = "Should not be called"
        raise InvocationError(msg)

    config = WaitForConditionConfig(
        initial_state=5,
        wait_strategy=lambda s, a: WaitForConditionDecision.stop_polling(),
        serdes=CustomDictSerDes(),
    )

    with pytest.raises(
        SuspendExecution, match="wait_for_condition test_wait will retry at timestamp"
    ):
        await wait_for_condition_handler(
            state=mock_state,
            operation_identifier=op_id,
            check=check_func,
            config=config,
        )


async def test_wait_for_condition_pending_without_next_attempt():
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    operation = Operation(
        operation_id="XXX",
        operation_type=OperationType.STEP,
        status=OperationStatus.PENDING,
        step_details=StepDetails(
            result='{"key": "VALUE", "number": "84", "list": [1, 2, 3]}',
        ),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.operations.get.return_value = mock_result

    mock_logger = Mock(spec=logging.Logger)

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        msg = "Should not be called"
        raise InvocationError(msg)

    config = WaitForConditionConfig(
        initial_state=5,
        wait_strategy=lambda s, a: WaitForConditionDecision.stop_polling(),
        serdes=CustomDictSerDes(),
    )

    with pytest.raises(
        SuspendExecution,
        match="No timestamp provided. Suspending without retry timestamp.",
    ):
        await wait_for_condition_handler(
            state=mock_state,
            operation_identifier=op_id,
            check=check_func,
            config=config,
        )


# Immediate Response Handling Tests


async def test_wait_for_condition_checkpoint_called_once_with_is_sync_false():
    """Test that get_checkpoint_result is called once when checkpoint is created (is_sync=False)."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    mock_state.operations.get.return_value = CheckpointedResult.create_not_found()

    mock_logger = Mock(spec=logging.Logger)

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        return state + 1

    mock_state.wrap_user_function.return_value = check_func

    config = WaitForConditionConfig(
        initial_state=5,
        wait_strategy=lambda s, a: WaitForConditionDecision.stop_polling(),
    )

    await wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        config=config,
    )

    # Verify get_checkpoint_result called only once (no second check for async checkpoint)
    assert mock_state.operations.get.call_count == 1

    # Verify create_checkpoint called with is_sync=False
    assert mock_state.create_checkpoint.call_count == 2  # START and SUCCESS
    start_call = mock_state.create_checkpoint.call_args_list[0]
    assert start_call[1]["is_sync"] is False


async def test_wait_for_condition_immediate_success_without_executing_check():
    """Test immediate success: checkpoint returns SUCCEEDED on first check, returns result without executing check."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        step_details=StepDetails(result=json.dumps(42)),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.operations.get.return_value = mock_result

    mock_logger = Mock(spec=logging.Logger)
    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    # Check function should NOT be called
    def check_func(state):
        msg = "Check function should not be called for immediate success"
        raise AssertionError(msg)

    config = WaitForConditionConfig(
        initial_state=5,
        wait_strategy=lambda s, a: WaitForConditionDecision.stop_polling(),
    )

    result = await wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        config=config,
    )

    # Verify result returned without executing check function
    assert result == 42
    # Verify no new checkpoints created
    assert mock_state.create_checkpoint.call_count == 0


async def test_wait_for_condition_immediate_failure_without_executing_check():
    """Test immediate failure: checkpoint returns FAILED on first check, raises error without executing check."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.FAILED,
        step_details=StepDetails(
            error=ErrorObject("Test error", "TestError", None, None)
        ),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.operations.get.return_value = mock_result

    mock_logger = Mock(spec=logging.Logger)
    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    # Check function should NOT be called
    def check_func(state):
        msg = "Check function should not be called for immediate failure"
        raise AssertionError(msg)

    config = WaitForConditionConfig(
        initial_state=5,
        wait_strategy=lambda s, a: WaitForConditionDecision.stop_polling(),
    )

    # Verify error raised without executing check function
    with pytest.raises(CallableRuntimeError):
        await wait_for_condition_handler(
            state=mock_state,
            operation_identifier=op_id,
            check=check_func,
            config=config,
        )

    # Verify no new checkpoints created
    assert mock_state.create_checkpoint.call_count == 0


async def test_wait_for_condition_pending_suspends_without_executing_check():
    """Test pending handling: checkpoint returns PENDING on first check, suspends without executing check."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.PENDING,
        step_details=StepDetails(
            result=json.dumps(10),
            next_attempt_timestamp=datetime.datetime.fromtimestamp(
                1764547200, tz=datetime.timezone.utc
            ),
        ),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.operations.get.return_value = mock_result

    mock_logger = Mock(spec=logging.Logger)

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    # Check function should NOT be called
    def check_func(state):
        msg = "Check function should not be called for pending status"
        raise AssertionError(msg)

    config = WaitForConditionConfig(
        initial_state=5,
        wait_strategy=lambda s, a: WaitForConditionDecision.stop_polling(),
    )

    # Verify suspend occurs without executing check function
    with pytest.raises(
        SuspendExecution, match="wait_for_condition test_wait will retry at timestamp"
    ):
        await wait_for_condition_handler(
            state=mock_state,
            operation_identifier=op_id,
            check=check_func,
            config=config,
        )

    # Verify no new checkpoints created
    assert mock_state.create_checkpoint.call_count == 0


async def test_wait_for_condition_no_checkpoint_executes_check_function():
    """Test no immediate response: when checkpoint doesn't exist, operation executes check function."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    mock_state.operations.get.return_value = CheckpointedResult.create_not_found()

    mock_logger = Mock(spec=logging.Logger)

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    check_called = False

    def check_func(state):
        nonlocal check_called
        check_called = True
        return state + 1

    mock_state.wrap_user_function.return_value = check_func

    config = WaitForConditionConfig(
        initial_state=5,
        wait_strategy=lambda s, a: WaitForConditionDecision.stop_polling(),
    )

    result = await wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        config=config,
    )

    # Verify check function was executed
    assert check_called is True
    assert result == 6

    # Verify checkpoints created (START and SUCCESS)
    assert mock_state.create_checkpoint.call_count == 2


async def test_wait_for_condition_already_completed_no_checkpoint_created():
    """Test already completed: when checkpoint is SUCCEEDED on first check, no checkpoint created."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        step_details=StepDetails(result=json.dumps(42)),
    )
    mock_result = CheckpointedResult.create_from_operation(operation)
    mock_state.operations.get.return_value = mock_result

    mock_logger = Mock(spec=logging.Logger)
    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        return state + 1

    config = WaitForConditionConfig(
        initial_state=5,
        wait_strategy=lambda s, a: WaitForConditionDecision.stop_polling(),
    )

    result = await wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        config=config,
    )

    # Verify result returned
    assert result == 42

    # Verify NO checkpoints created (already completed)
    assert mock_state.create_checkpoint.call_count == 0


async def test_wait_for_condition_executes_check_when_checkpoint_not_terminal():
    """Test backward compatibility: when checkpoint is not terminal (STARTED),
    the wait_for_condition operation executes the check function normally.

    Note: wait_for_condition uses async checkpoints (is_sync=False), so there's
    only one check, not two.
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    # Single call: checkpoint doesn't exist (async checkpoint, no second check)
    mock_state.operations.get.return_value = CheckpointedResult.create_not_found()

    mock_check_function = Mock(return_value="final_state")
    mock_logger = Mock(spec=logging.Logger)
    mock_state.wrap_user_function.return_value = _asyncify(mock_check_function)

    def mock_wait_strategy(state, attempt):
        return WaitForConditionDecision(
            should_continue=False, delay=timedelta(seconds=0)
        )

    executor = WaitForConditionOperationExecutor(
        check=mock_check_function,
        config=WaitForConditionConfig(
            initial_state="initial",
            wait_strategy=mock_wait_strategy,
        ),
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "wfc-1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wfc"
        ),
    )
    result = await executor.process()

    # Assert - behaves like "old way"
    mock_check_function.assert_called_once()  # Check function executed
    assert result == "final_state"
    assert mock_state.operations.get.call_count == 1  # Single check (async)
    assert mock_state.create_checkpoint.call_count == 2  # START + SUCCESS checkpoints


async def test_wait_for_condition_executes_check_when_checkpoint_not_terminal_duplicate():
    """Test backward compatibility: when checkpoint is not terminal (STARTED),
    the wait_for_condition operation executes the check function normally.

    Note: wait_for_condition uses async checkpoints (is_sync=False), so there's
    only one check, not two.
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    # Single call: checkpoint doesn't exist (async checkpoint, no second check)
    mock_state.operations.get.return_value = CheckpointedResult.create_not_found()

    mock_check_function = Mock(return_value="final_state")
    mock_state.wrap_user_function.return_value = _asyncify(mock_check_function)
    mock_logger = Mock(spec=logging.Logger)

    def mock_wait_strategy(state, attempt):
        return WaitForConditionDecision.stop_polling()

    executor = WaitForConditionOperationExecutor(
        check=mock_check_function,
        config=WaitForConditionConfig(
            initial_state="initial",
            wait_strategy=mock_wait_strategy,
        ),
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "wfc-1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wfc"
        ),
    )
    result = await executor.process()

    # Assert - behaves like "old way"
    mock_check_function.assert_called_once()  # Check function executed
    assert result == "final_state"
    assert mock_state.operations.get.call_count == 1  # Single check (async)
    assert mock_state.create_checkpoint.call_count == 2  # START + SUCCESS checkpoints


def test_wait_strategy_builder_defaults():
    """WaitStrategyBuilder keeps its expected defaults."""
    config = WaitStrategyBuilder(should_continue_polling=lambda x: True)

    assert config.max_attempts == 60
    assert config.initial_delay_seconds == 5
    assert config.max_delay_seconds == 300
    assert config.backoff_rate == 1.5
    assert config.jitter_strategy == JitterStrategy.FULL
    assert config.timeout_seconds is None


def test_wait_strategy_builder_importable_from_package_root():
    """WaitStrategyBuilder remains re-exported from the package root."""
    from async_durable_execution import WaitStrategyBuilder as ImportedBuilder

    assert ImportedBuilder is WaitStrategyBuilder


def test_condition_met_returns_no_wait():
    """Strategy returns no_wait when condition is met."""
    config = WaitStrategyBuilder(should_continue_polling=lambda x: False)
    strategy = config.build()

    decision = strategy("completed", 1)
    assert decision.should_wait is False


def test_max_attempts_exceeded():
    """Strategy returns no_wait when max attempts are exhausted."""
    config = WaitStrategyBuilder(should_continue_polling=lambda x: True, max_attempts=5)
    strategy = config.build()

    decision = strategy("pending", 5)
    assert decision.should_wait is False


def test_should_continue_polling():
    """Strategy continues when the condition is still pending."""
    config = WaitStrategyBuilder(should_continue_polling=lambda x: x == "pending")
    strategy = config.build()

    decision = strategy("pending", 1)
    assert decision.should_wait is True


@patch("random.random")
def test_exponential_backoff_calculation(mock_random):
    """Backoff calculation uses the configured jitter strategy."""
    mock_random.return_value = 0.5
    config = WaitStrategyBuilder(
        should_continue_polling=lambda x: True,
        initial_delay=timedelta(seconds=2),
        backoff_rate=2.0,
        jitter_strategy=JitterStrategy.FULL,
    )
    strategy = config.build()

    assert strategy("pending", 1).delay_seconds == 1
    assert strategy("pending", 2).delay_seconds == 2


def test_max_delay_cap():
    """Delay is capped by max_delay."""
    config = WaitStrategyBuilder(
        should_continue_polling=lambda x: True,
        initial_delay=timedelta(seconds=100),
        max_delay=timedelta(seconds=50),
        backoff_rate=2.0,
        jitter_strategy=JitterStrategy.NONE,
    )
    strategy = config.build()

    assert strategy("pending", 2).delay_seconds == 50


def test_minimum_delay_one_second():
    """Delay is clamped to at least one second."""
    config = WaitStrategyBuilder(
        should_continue_polling=lambda x: True,
        initial_delay=timedelta(seconds=0),
        jitter_strategy=JitterStrategy.NONE,
    )
    strategy = config.build()

    assert strategy("pending", 1).delay_seconds == 1


@patch("random.random")
def test_full_jitter_integration(mock_random):
    """Full jitter is applied during wait strategy execution."""
    mock_random.return_value = 0.8
    config = WaitStrategyBuilder(
        should_continue_polling=lambda x: True,
        initial_delay=timedelta(seconds=10),
        jitter_strategy=JitterStrategy.FULL,
    )

    assert config.build()("pending", 1).delay_seconds == 8


@patch("random.random")
def test_half_jitter_integration(mock_random):
    """Half jitter is applied during wait strategy execution."""
    mock_random.return_value = 0.0
    config = WaitStrategyBuilder(
        should_continue_polling=lambda x: True,
        initial_delay=timedelta(seconds=10),
        jitter_strategy=JitterStrategy.HALF,
    )

    assert config.build()("pending", 1).delay_seconds == 5


def test_none_jitter_integration():
    """No jitter preserves the computed delay."""
    config = WaitStrategyBuilder(
        should_continue_polling=lambda x: True,
        initial_delay=timedelta(seconds=10),
        jitter_strategy=JitterStrategy.NONE,
    )

    assert config.build()("pending", 1).delay_seconds == 10


def test_stateful_condition_check():
    """Stateful objects can drive the continue-polling predicate."""

    class State:
        def __init__(self, count):
            self.count = count

    config = WaitStrategyBuilder(
        should_continue_polling=lambda s: s.count < 3,
        max_attempts=10,
    )
    strategy = config.build()

    assert strategy(State(1), 1).should_wait is True
    assert strategy(State(3), 1).should_wait is False


def test_complex_condition_logic():
    """Complex callables can be used for continue-polling checks."""

    def complex_condition(result):
        return result.get("status") == "pending" and result.get("retries", 0) < 5

    strategy = WaitStrategyBuilder(should_continue_polling=complex_condition).build()

    assert strategy({"status": "pending", "retries": 2}, 1).should_wait is True
    assert strategy({"status": "completed", "retries": 2}, 1).should_wait is False
    assert strategy({"status": "pending", "retries": 5}, 1).should_wait is False


def test_zero_backoff_rate():
    """A zero backoff rate preserves the initial delay."""
    config = WaitStrategyBuilder(
        should_continue_polling=lambda x: True,
        initial_delay=timedelta(seconds=5),
        backoff_rate=0,
        jitter_strategy=JitterStrategy.NONE,
    )

    assert config.build()("pending", 1).delay_seconds == 5


def test_fractional_backoff_rate():
    """Fractional backoff rates are supported."""
    config = WaitStrategyBuilder(
        should_continue_polling=lambda x: True,
        initial_delay=timedelta(seconds=8),
        backoff_rate=0.5,
        jitter_strategy=JitterStrategy.NONE,
    )

    assert config.build()("pending", 2).delay_seconds == 4


def test_large_backoff_rate():
    """Large backoff rates still honor max_delay."""
    config = WaitStrategyBuilder(
        should_continue_polling=lambda x: True,
        initial_delay=timedelta(seconds=10),
        max_delay=timedelta(seconds=100),
        backoff_rate=10.0,
        jitter_strategy=JitterStrategy.NONE,
    )

    assert config.build()("pending", 3).delay_seconds == 100


def test_attempt_at_boundary():
    """The max-attempt boundary stops polling exactly when reached."""
    config = WaitStrategyBuilder(should_continue_polling=lambda x: True, max_attempts=3)
    strategy = config.build()

    assert strategy("pending", 3).should_wait is False
    assert strategy("pending", 2).should_wait is True


def test_negative_delay_clamped_to_one():
    """Very small computed delays are clamped to one second."""
    config = WaitStrategyBuilder(
        should_continue_polling=lambda x: True,
        initial_delay=timedelta(seconds=0),
        backoff_rate=0,
        jitter_strategy=JitterStrategy.NONE,
    )

    assert config.build()("pending", 1).delay_seconds == 1


@patch("random.random")
def test_rounding_behavior(mock_random):
    """Computed delays round up to whole seconds."""
    mock_random.return_value = 0.3
    config = WaitStrategyBuilder(
        should_continue_polling=lambda x: True,
        initial_delay=timedelta(seconds=3),
        jitter_strategy=JitterStrategy.FULL,
    )

    assert config.build()("pending", 1).delay_seconds == 1


def test_lambda_condition():
    """Lambdas can be used for continue-polling checks."""
    strategy = WaitStrategyBuilder(should_continue_polling=lambda x: x < 10).build()

    assert strategy(5, 1).should_wait is True
    assert strategy(10, 1).should_wait is False


def test_function_condition():
    """Functions can be used for continue-polling checks."""

    def is_pending(status):
        return status == "pending"

    strategy = WaitStrategyBuilder(should_continue_polling=is_pending).build()

    assert strategy("pending", 1).should_wait is True
    assert strategy("completed", 1).should_wait is False


def test_method_condition():
    """Bound methods can be used for continue-polling checks."""

    class Checker:
        def __init__(self, threshold):
            self.threshold = threshold

        def should_continue(self, value):
            return value < self.threshold

    checker = Checker(100)
    strategy = WaitStrategyBuilder(
        should_continue_polling=checker.should_continue
    ).build()

    assert strategy(50, 1).should_wait is True
    assert strategy(100, 1).should_wait is False
