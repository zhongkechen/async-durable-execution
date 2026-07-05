"""Unit tests for wait_for_condition operation."""

import asyncio
import datetime
import inspect
import json
from contextlib import nullcontext
from datetime import timedelta
from typing import Any, cast
from unittest.mock import Mock, patch

import pytest
from async_durable_execution.context import (
    get_current_context,
)
from async_durable_execution.exceptions import (
    CallableRuntimeError,
    InvocationError,
    SuspendExecution,
    ValidationError,
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
from async_durable_execution.composite.wait_for_condition import (
    WaitForConditionOperationExecutor,
    wait_for_condition,
)
from async_durable_execution.state import ExecutionState
from async_durable_execution import WaitForConditionCheckContext
from async_durable_execution.config import JitterStrategy
from async_durable_execution.composite.wait_for_condition import WaitDelayStrategy
from async_durable_execution.serdes import SerDes

from ..serdes_test import CustomDictSerDes


class UppercaseSerDes(SerDes[str]):
    async def serialize(self, value: str) -> str:
        return value.upper()

    async def deserialize(self, data: str) -> str:
        return data


def test_wait_for_condition_signature_accepts_config_fields_directly():
    """The public wait_for_condition API exposes config fields directly."""
    parameters = inspect.signature(wait_for_condition).parameters

    assert "config" not in parameters
    assert "wait_strategy" in parameters
    assert "serdes" in parameters


def test_wait_for_condition_signature_requires_keyword_only_options():
    """Only the check callable is positional."""
    parameters = inspect.signature(wait_for_condition).parameters

    assert parameters["check"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert parameters["check"].default is inspect.Parameter.empty
    assert parameters["initial_state"].kind is inspect.Parameter.KEYWORD_ONLY
    assert parameters["name"].kind is inspect.Parameter.KEYWORD_ONLY
    assert parameters["wait_strategy"].kind is inspect.Parameter.KEYWORD_ONLY
    assert parameters["serdes"].kind is inspect.Parameter.KEYWORD_ONLY


async def test_wait_for_condition_requires_check_callable():
    """The public wrapper requires a check callable."""
    with pytest.raises(TypeError, match="required positional argument: 'check'"):
        cast("Any", wait_for_condition)()


async def test_wait_for_condition_public_wrapper_builds_executor_from_context():
    """The public wrapper derives operation identity from the durable context."""

    async def check(state):
        return state

    context = Mock()
    context._replay_aware.return_value = nullcontext()
    context.step_counter.create_step_id.return_value = "wait-op"
    context.parent_id = "parent-op"
    context.execution_state = Mock(spec=ExecutionState)
    wait_strategy = Mock()
    serdes = Mock()
    captured_executor = None

    async def fake_process(self):
        nonlocal captured_executor
        captured_executor = self
        return "done"

    with (
        patch(
            "async_durable_execution.composite.wait_for_condition.get_durable_context",
            return_value=context,
        ),
        patch.object(
            WaitForConditionOperationExecutor,
            "process",
            fake_process,
        ),
    ):
        result = await wait_for_condition(
            check,
            initial_state={"status": "pending"},
            name="poll-job",
            wait_strategy=wait_strategy,
            serdes=serdes,
        )

    assert result == "done"
    assert captured_executor is not None
    executor = captured_executor
    assert executor.initial_state == {"status": "pending"}
    assert executor.wait_strategy is wait_strategy
    assert executor.serdes is serdes
    assert executor.operation_identifier == OperationIdentifier(
        operation_id="wait-op",
        sub_type=OperationSubType.WAIT_FOR_CONDITION,
        parent_id="parent-op",
        name="poll-job",
    )


async def _invoke_maybe_async(result):
    return await result


def _asyncify(func):
    if inspect.iscoroutinefunction(func):
        return func

    async def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


async def wait_for_condition_handler(
    check,
    state,
    operation_identifier,
    initial_state=5,
    wait_strategy=None,
    serdes=None,
):
    """Test helper that wraps WaitForConditionOperationExecutor."""
    executor = WaitForConditionOperationExecutor(
        check=_asyncify(check),
        initial_state=initial_state,
        state=state,
        operation_identifier=operation_identifier,
        wait_strategy=wait_strategy,
        serdes=serdes,
    )
    return await _invoke_maybe_async(executor.process())


async def test_wait_for_condition_first_execution_condition_met():
    """Test wait_for_condition on first execution when condition is met."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    mock_state.operations.get.return_value = None

    mock_logger = Mock(spec=logging.Logger)

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        return state + 1

    result = await wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
    )

    assert result == 6
    assert mock_state.create_checkpoint.call_count == 2  # START and SUCCESS


async def test_wait_for_condition_new_condition_result_with_optional_config():
    """Condition returns the next state without a wait strategy."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    mock_state.operations.get.return_value = None

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def condition(state):
        assert state is None
        return {"status": "done"}

    result = await wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=condition,
        initial_state=None,
    )

    assert result == {"status": "done"}
    assert mock_state.create_checkpoint.call_count == 2


async def test_wait_for_condition_returns_deserialized_serialized_custom_serdes_result():
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    mock_state.operations.get.return_value = None

    op_id = OperationIdentifier(
        "op_uppercase", OperationSubType.WAIT_FOR_CONDITION, None, "uppercase"
    )

    def condition(state):
        return "hello"

    result = await wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=condition,
        initial_state=None,
        serdes=UppercaseSerDes(),
    )

    success_call = mock_state.create_checkpoint.call_args_list[1]
    success_operation = success_call[1]["operation_update"]
    assert success_operation.payload == "HELLO"
    assert result == "HELLO"


async def test_wait_for_condition_new_condition_uses_delay_only_strategy():
    """Condition decides to continue and wait_strategy supplies only delay."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    mock_state.operations.get.return_value = None

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def condition(state):
        return None

    wait_strategy = lambda state, attempt: timedelta(seconds=7)

    with pytest.raises(SuspendExecution, match="will retry in 7 seconds"):
        await wait_for_condition_handler(
            state=mock_state,
            operation_identifier=op_id,
            check=condition,
            wait_strategy=wait_strategy,
        )

    assert mock_state.create_checkpoint.call_count == 2


async def test_wait_for_condition_first_execution_condition_not_met():
    """Test wait_for_condition on first execution when condition is not met."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    mock_state.operations.get.return_value = None

    mock_logger = Mock(spec=logging.Logger)

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        return None

    def wait_strategy(state, attempt):
        return timedelta(seconds=30)

    with pytest.raises(SuspendExecution, match="will retry in 30 seconds"):
        await wait_for_condition_handler(
            state=mock_state,
            operation_identifier=op_id,
            check=check_func,
            wait_strategy=wait_strategy,
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
    mock_result = operation
    mock_state.operations.get.return_value = mock_result

    mock_logger = Mock(spec=logging.Logger)
    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        return None

    wait_strategy = lambda s, a: timedelta(seconds=1)

    result = await wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        wait_strategy=wait_strategy,
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
    mock_result = operation
    mock_state.operations.get.return_value = mock_result

    mock_logger = Mock(spec=logging.Logger)
    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        return None

    wait_strategy = lambda s, a: None

    result = await wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        wait_strategy=wait_strategy,
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
    mock_result = operation
    mock_state.operations.get.return_value = mock_result

    mock_logger = Mock(spec=logging.Logger)
    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        return None

    wait_strategy = lambda s, a: None

    with pytest.raises(CallableRuntimeError):
        await wait_for_condition_handler(
            state=mock_state,
            operation_identifier=op_id,
            check=check_func,
            wait_strategy=wait_strategy,
        )


async def test_wait_for_condition_already_failed_without_error_object():
    """Failed checkpoints without error details raise an unknown CallableRuntimeError."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_state.operations.get.return_value = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.FAILED,
        step_details=None,
    )

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        return None

    with pytest.raises(
        CallableRuntimeError,
        match="Unknown error. No ErrorObject exists",
    ):
        await wait_for_condition_handler(
            state=mock_state,
            operation_identifier=op_id,
            check=check_func,
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
    mock_result = operation
    mock_state.operations.get.return_value = mock_result

    mock_logger = Mock(spec=logging.Logger)

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        return state + 1

    wait_strategy = lambda s, a: None

    result = await wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        wait_strategy=wait_strategy,
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
    mock_result = operation
    mock_state.operations.get.return_value = mock_result

    mock_logger = Mock(spec=logging.Logger)

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        return state + 1

    wait_strategy = lambda s, a: None

    result = await wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        wait_strategy=wait_strategy,
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
    mock_result = operation
    mock_state.operations.get.return_value = mock_result

    mock_logger = Mock(spec=logging.Logger)

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        return state + 1

    wait_strategy = lambda s, a: None

    result = await wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        wait_strategy=wait_strategy,
    )

    assert result == 6  # Falls back to initial state


async def test_wait_for_condition_check_function_exception():
    """Test wait_for_condition when check function raises exception."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    mock_state.operations.get.return_value = None

    mock_logger = Mock(spec=logging.Logger)

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        msg = "Test error"
        raise ValueError(msg)

    wait_strategy = lambda s, a: None

    with pytest.raises(ValueError, match="Test error"):
        await wait_for_condition_handler(
            state=mock_state,
            operation_identifier=op_id,
            check=check_func,
            wait_strategy=wait_strategy,
        )

    assert mock_state.create_checkpoint.call_count == 2  # START and FAIL


async def test_wait_for_condition_check_context():
    """Test that check context is available via contextvars."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    mock_state.operations.get.return_value = None

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    captured_context = None

    def check_func(state):
        nonlocal captured_context
        captured_context = get_current_context()
        return state + 1

    wait_strategy = lambda s, a: None

    await wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        wait_strategy=wait_strategy,
    )

    assert isinstance(captured_context, WaitForConditionCheckContext)
    assert captured_context.attempt == 1


async def test_wait_for_condition_delay_seconds_none():
    """Test wait_for_condition with None delay_seconds."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    mock_state.operations.get.return_value = None

    mock_logger = Mock(spec=logging.Logger)

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        return None

    def wait_strategy(state, attempt):
        return timedelta()

    with pytest.raises(SuspendExecution, match="will retry in 0 seconds"):
        await wait_for_condition_handler(
            state=mock_state,
            operation_identifier=op_id,
            check=check_func,
            wait_strategy=wait_strategy,
        )


async def test_wait_for_condition_no_operation_in_state_executes_check():
    """Test wait_for_condition treats absent raw operation as a new check."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    mock_state.operations.get.return_value = None

    mock_logger = Mock(spec=logging.Logger)

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        return None

    wait_strategy = lambda s, a: timedelta(seconds=1)

    with pytest.raises(SuspendExecution, match="will retry"):
        await wait_for_condition_handler(
            state=mock_state,
            operation_identifier=op_id,
            check=check_func,
            wait_strategy=wait_strategy,
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
    mock_result = operation

    mock_state.operations.get.return_value = mock_result

    mock_logger = Mock(spec=logging.Logger)

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        return state + 1

    wait_strategy = lambda s, a: None

    result = await wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        wait_strategy=wait_strategy,
    )

    assert result == 6  # Falls back to initial_state and uses attempt=1


async def test_wait_for_condition_ready_checkpoint_restarts_before_check():
    """READY checkpoints are restarted before the condition is evaluated."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    mock_state.operations.get.return_value = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.READY,
        step_details=None,
    )

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        return state + 1

    result = await wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
    )

    assert result == 6
    assert mock_state.create_checkpoint.call_count == 2
    assert mock_state.create_checkpoint.call_args_list[0].kwargs["is_sync"] is False


async def test_wait_for_condition_custom_delay_seconds():
    """Test wait_for_condition with custom delay_seconds."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    mock_state.operations.get.return_value = None

    mock_logger = Mock(spec=logging.Logger)

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        return None

    def wait_strategy(state, attempt):
        return timedelta(minutes=1)

    with pytest.raises(SuspendExecution, match="will retry in 60 seconds"):
        await wait_for_condition_handler(
            state=mock_state,
            operation_identifier=op_id,
            check=check_func,
            wait_strategy=wait_strategy,
        )


async def test_wait_for_condition_custom_delay_accepts_int_seconds():
    """Test wait_for_condition custom strategy can return integer seconds."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    mock_state.operations.get.return_value = None

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        return None

    def wait_strategy(state, attempt):
        return 60

    with pytest.raises(SuspendExecution, match="will retry in 60 seconds"):
        await wait_for_condition_handler(
            state=mock_state,
            operation_identifier=op_id,
            check=check_func,
            wait_strategy=wait_strategy,
        )


async def test_wait_for_condition_custom_delay_rejects_invalid_return_type():
    """Custom wait strategies must return int seconds or timedelta."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    mock_state.operations.get.return_value = None

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        return None

    def wait_strategy(state, attempt):
        return "later"

    with pytest.raises(
        ValidationError,
        match="wait_strategy must return int seconds, timedelta, or None",
    ):
        await wait_for_condition_handler(
            state=mock_state,
            operation_identifier=op_id,
            check=check_func,
            wait_strategy=wait_strategy,
        )

    assert mock_state.create_checkpoint.call_count == 2


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
    mock_result = operation
    mock_state.operations.get.return_value = mock_result

    mock_logger = Mock(spec=logging.Logger)

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        return None

    captured_attempt = None

    def wait_strategy(state, attempt):
        nonlocal captured_attempt
        captured_attempt = attempt
        return timedelta(seconds=1)

    with pytest.raises(SuspendExecution):
        await wait_for_condition_handler(
            state=mock_state,
            operation_identifier=op_id,
            check=check_func,
            wait_strategy=wait_strategy,
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
        return None

    captured_attempts = []

    def wait_strategy(state, attempt):
        captured_attempts.append(attempt)
        return timedelta(seconds=1)

    # Test 1: First execution (no checkpoint exists)
    mock_state.operations.get.return_value = None

    with pytest.raises(SuspendExecution):
        await wait_for_condition_handler(
            state=mock_state,
            operation_identifier=op_id,
            check=check_func,
            wait_strategy=wait_strategy,
        )

    assert captured_attempts[-1] == 1, "First execution should have attempt=1"

    # Test 2: After first retry (checkpoint has attempt=1)
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
        step_details=StepDetails(result=json.dumps(10), attempt=1),
    )
    mock_result = operation
    mock_state.operations.get.return_value = mock_result

    with pytest.raises(SuspendExecution):
        await wait_for_condition_handler(
            state=mock_state,
            operation_identifier=op_id,
            check=check_func,
            wait_strategy=wait_strategy,
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
    mock_result = operation
    mock_state.operations.get.return_value = mock_result

    with pytest.raises(SuspendExecution):
        await wait_for_condition_handler(
            state=mock_state,
            operation_identifier=op_id,
            check=check_func,
            wait_strategy=wait_strategy,
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
    mock_result = operation
    mock_state.operations.get.return_value = mock_result

    with pytest.raises(SuspendExecution):
        await wait_for_condition_handler(
            state=mock_state,
            operation_identifier=op_id,
            check=check_func,
            wait_strategy=wait_strategy,
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
    mock_state.operations.get.return_value = None

    mock_logger = Mock(spec=logging.Logger)

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        return 0

    captured_state = None

    def wait_strategy(state, attempt):
        nonlocal captured_state
        captured_state = state
        return timedelta(seconds=1)

    with pytest.raises(SuspendExecution):
        await wait_for_condition_handler(
            state=mock_state,
            operation_identifier=op_id,
            check=check_func,
            wait_strategy=wait_strategy,
        )

    assert captured_state == 0


async def test_wait_for_condition_logger_with_log_info():
    """Test that the active context carries durable log metadata."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test:execution:123"
    mock_state.operations.get.return_value = None

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

    wait_strategy = lambda s, a: None

    await wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        wait_strategy=wait_strategy,
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
    mock_state.operations.get.return_value = None

    mock_logger = Mock(spec=logging.Logger)

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        return None

    def wait_strategy(state, attempt):
        return timedelta(seconds=0)

    with pytest.raises(SuspendExecution, match="will retry in 0 seconds"):
        await wait_for_condition_handler(
            state=mock_state,
            operation_identifier=op_id,
            check=check_func,
            wait_strategy=wait_strategy,
        )


async def test_wait_for_condition_custom_serdes_first_execution_condition_met():
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    mock_state.operations.get.return_value = None

    mock_logger = Mock(spec=logging.Logger)

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )
    complex_result = {"key": "value", "number": 42, "list": [1, 2, 3]}

    def check_func(state):
        return complex_result

    def wait_strategy(state, attempt):
        return None

    serdes = CustomDictSerDes()

    await wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        wait_strategy=wait_strategy,
        serdes=serdes,
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
    mock_result = operation
    mock_state.operations.get.return_value = mock_result

    mock_logger = Mock(spec=logging.Logger)
    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        return state + 1

    wait_strategy = lambda s, a: None
    serdes = CustomDictSerDes()

    result = await wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        wait_strategy=wait_strategy,
        serdes=serdes,
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
    mock_result = operation
    mock_state.operations.get.return_value = mock_result

    mock_logger = Mock(spec=logging.Logger)

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        msg = "Should not be called"
        raise InvocationError(msg)

    wait_strategy = lambda s, a: None
    serdes = CustomDictSerDes()

    with pytest.raises(
        SuspendExecution, match="wait_for_condition test_wait will retry at timestamp"
    ):
        await wait_for_condition_handler(
            state=mock_state,
            operation_identifier=op_id,
            check=check_func,
            wait_strategy=wait_strategy,
            serdes=serdes,
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
    mock_result = operation
    mock_state.operations.get.return_value = mock_result

    mock_logger = Mock(spec=logging.Logger)

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        msg = "Should not be called"
        raise InvocationError(msg)

    wait_strategy = lambda s, a: None
    serdes = CustomDictSerDes()

    with pytest.raises(
        SuspendExecution,
        match="No timestamp provided. Suspending without retry timestamp.",
    ):
        await wait_for_condition_handler(
            state=mock_state,
            operation_identifier=op_id,
            check=check_func,
            wait_strategy=wait_strategy,
            serdes=serdes,
        )


# Immediate Response Handling Tests


async def test_wait_for_condition_checkpoint_called_once_with_is_sync_false():
    """Test that direct state lookup is called once when checkpoint is created (is_sync=False)."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    mock_state.operations.get.return_value = None

    mock_logger = Mock(spec=logging.Logger)

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        return state + 1

    wait_strategy = lambda s, a: None

    await wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        wait_strategy=wait_strategy,
    )

    # Verify direct state lookup called only once (no second check for async checkpoint)
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
    mock_result = operation
    mock_state.operations.get.return_value = mock_result

    mock_logger = Mock(spec=logging.Logger)
    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    # Check function should NOT be called
    def check_func(state):
        msg = "Check function should not be called for immediate success"
        raise AssertionError(msg)

    wait_strategy = lambda s, a: None

    result = await wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        wait_strategy=wait_strategy,
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
    mock_result = operation
    mock_state.operations.get.return_value = mock_result

    mock_logger = Mock(spec=logging.Logger)
    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    # Check function should NOT be called
    def check_func(state):
        msg = "Check function should not be called for immediate failure"
        raise AssertionError(msg)

    wait_strategy = lambda s, a: timedelta(seconds=1)

    # Verify error raised without executing check function
    with pytest.raises(CallableRuntimeError):
        await wait_for_condition_handler(
            state=mock_state,
            operation_identifier=op_id,
            check=check_func,
            wait_strategy=wait_strategy,
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
    mock_result = operation
    mock_state.operations.get.return_value = mock_result

    mock_logger = Mock(spec=logging.Logger)

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    # Check function should NOT be called
    def check_func(state):
        msg = "Check function should not be called for pending status"
        raise AssertionError(msg)

    wait_strategy = lambda s, a: timedelta(seconds=1)

    # Verify suspend occurs without executing check function
    with pytest.raises(
        SuspendExecution, match="wait_for_condition test_wait will retry at timestamp"
    ):
        await wait_for_condition_handler(
            state=mock_state,
            operation_identifier=op_id,
            check=check_func,
            wait_strategy=wait_strategy,
        )

    # Verify no new checkpoints created
    assert mock_state.create_checkpoint.call_count == 0


async def test_wait_for_condition_no_checkpoint_executes_check_function():
    """Test no immediate response: when checkpoint doesn't exist, operation executes check function."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:aws:test"
    mock_state.operations.get.return_value = None

    mock_logger = Mock(spec=logging.Logger)

    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    check_called = False

    def check_func(state):
        nonlocal check_called
        check_called = True
        return state + 1

    wait_strategy = lambda s, a: None

    result = await wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        wait_strategy=wait_strategy,
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
    mock_result = operation
    mock_state.operations.get.return_value = mock_result

    mock_logger = Mock(spec=logging.Logger)
    op_id = OperationIdentifier(
        "op1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wait"
    )

    def check_func(state):
        return state + 1

    wait_strategy = lambda s, a: timedelta(seconds=1)

    result = await wait_for_condition_handler(
        state=mock_state,
        operation_identifier=op_id,
        check=check_func,
        wait_strategy=wait_strategy,
    )

    # Verify result returned
    assert result == 42

    # Verify NO checkpoints created (already completed)
    assert mock_state.create_checkpoint.call_count == 0


async def test_wait_for_condition_executes_check_when_checkpoint_not_terminal():
    """When checkpoint is not terminal, wait_for_condition executes check normally.

    Note: wait_for_condition uses async checkpoints (is_sync=False), so there's
    only one check, not two.
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    # Single call: checkpoint doesn't exist (async checkpoint, no second check)
    mock_state.operations.get.return_value = None

    mock_check_function = Mock(return_value=("final_state"))
    mock_logger = Mock(spec=logging.Logger)

    def mock_wait_strategy(state, attempt):
        return None

    executor = WaitForConditionOperationExecutor(
        check=_asyncify(mock_check_function),
        initial_state="initial",
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "wfc-1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wfc"
        ),
        wait_strategy=mock_wait_strategy,
    )
    result = await executor.process()

    mock_check_function.assert_called_once()  # Check function executed
    assert result == "final_state"
    assert mock_state.operations.get.call_count == 1  # Single check (async)
    assert mock_state.create_checkpoint.call_count == 2  # START + SUCCESS checkpoints


async def test_wait_for_condition_executes_check_when_checkpoint_not_terminal_duplicate():
    """When checkpoint is not terminal, wait_for_condition executes check normally.

    Note: wait_for_condition uses async checkpoints (is_sync=False), so there's
    only one check, not two.
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    # Single call: checkpoint doesn't exist (async checkpoint, no second check)
    mock_state.operations.get.return_value = None

    mock_check_function = Mock(return_value=("final_state"))
    mock_logger = Mock(spec=logging.Logger)

    def mock_wait_strategy(state, attempt):
        return None

    executor = WaitForConditionOperationExecutor(
        check=_asyncify(mock_check_function),
        initial_state="initial",
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "wfc-1", OperationSubType.WAIT_FOR_CONDITION, None, "test_wfc"
        ),
        wait_strategy=mock_wait_strategy,
    )
    result = await executor.process()

    mock_check_function.assert_called_once()  # Check function executed
    assert result == "final_state"
    assert mock_state.operations.get.call_count == 1  # Single check (async)
    assert mock_state.create_checkpoint.call_count == 2  # START + SUCCESS checkpoints


def test_wait_delay_strategy_defaults():
    """WaitDelayStrategy keeps its expected defaults."""
    config = WaitDelayStrategy()

    assert config.max_attempts == 60
    assert config.initial_delay == 5
    assert config.max_delay == 300
    assert config.initial_delay_seconds == 5
    assert config.max_delay_seconds == 300
    assert config.backoff_rate == 1.5
    assert config.jitter_strategy == JitterStrategy.FULL
    assert config.timeout_seconds is None


def test_wait_delay_strategy_accepts_int_seconds():
    """WaitDelayStrategy duration fields accept integer seconds."""
    config = WaitDelayStrategy(
        initial_delay=2,
        max_delay=50,
        timeout=120,
    )

    assert config.initial_delay == 2
    assert config.max_delay == 50
    assert config.timeout == 120
    assert config.initial_delay_seconds == 2
    assert config.max_delay_seconds == 50
    assert config.timeout_seconds == 120


def test_wait_delay_strategy_importable_from_package_root():
    """WaitDelayStrategy remains re-exported from the package root."""
    from async_durable_execution import WaitDelayStrategy as ImportedStrategy

    assert ImportedStrategy is WaitDelayStrategy


def test_wait_delay_strategy_is_callable_without_build():
    """WaitDelayStrategy is directly callable, not a builder."""
    config = WaitDelayStrategy()

    assert callable(config)
    assert not hasattr(config, "build")


def test_max_attempts_exceeded():
    """Strategy returns no_wait when max attempts are exhausted."""
    strategy = WaitDelayStrategy(max_attempts=5)

    delay = strategy(None, 5)
    assert delay is None


def test_wait_strategy_returns_delay_before_max_attempts():
    """Strategy returns a delay before max attempts are exhausted."""
    strategy = WaitDelayStrategy(max_attempts=5)

    delay = strategy(None, 1)
    assert delay > 0


@patch("random.random")
def test_exponential_backoff_calculation(mock_random):
    """Backoff calculation uses the configured jitter strategy."""
    mock_random.return_value = 0.5
    config = WaitDelayStrategy(
        initial_delay=timedelta(seconds=2),
        backoff_rate=2.0,
        jitter_strategy=JitterStrategy.FULL,
    )

    assert config("pending", 1) == 1
    assert config("pending", 2) == 2


def test_max_delay_cap():
    """Delay is capped by max_delay."""
    config = WaitDelayStrategy(
        initial_delay=timedelta(seconds=100),
        max_delay=timedelta(seconds=50),
        backoff_rate=2.0,
        jitter_strategy=JitterStrategy.NONE,
    )

    assert config("pending", 2) == 50


def test_minimum_delay_one_second():
    """Delay is clamped to at least one second."""
    config = WaitDelayStrategy(
        initial_delay=timedelta(seconds=0),
        jitter_strategy=JitterStrategy.NONE,
    )

    assert config("pending", 1) == 1


@patch("random.random")
def test_full_jitter_integration(mock_random):
    """Full jitter is applied during wait strategy execution."""
    mock_random.return_value = 0.8
    config = WaitDelayStrategy(
        initial_delay=timedelta(seconds=10),
        jitter_strategy=JitterStrategy.FULL,
    )

    assert config("pending", 1) == 8


@patch("random.random")
def test_half_jitter_integration(mock_random):
    """Half jitter is applied during wait strategy execution."""
    mock_random.return_value = 0.0
    config = WaitDelayStrategy(
        initial_delay=timedelta(seconds=10),
        jitter_strategy=JitterStrategy.HALF,
    )

    assert config("pending", 1) == 5


def test_none_jitter_integration():
    """No jitter preserves the computed delay."""
    config = WaitDelayStrategy(
        initial_delay=timedelta(seconds=10),
        jitter_strategy=JitterStrategy.NONE,
    )

    assert config("pending", 1) == 10


def test_zero_backoff_rate():
    """A zero backoff rate preserves the initial delay."""
    config = WaitDelayStrategy(
        initial_delay=timedelta(seconds=5),
        backoff_rate=0,
        jitter_strategy=JitterStrategy.NONE,
    )

    assert config("pending", 1) == 5


def test_fractional_backoff_rate():
    """Fractional backoff rates are supported."""
    config = WaitDelayStrategy(
        initial_delay=timedelta(seconds=8),
        backoff_rate=0.5,
        jitter_strategy=JitterStrategy.NONE,
    )

    assert config("pending", 2) == 4


def test_large_backoff_rate():
    """Large backoff rates still honor max_delay."""
    config = WaitDelayStrategy(
        initial_delay=timedelta(seconds=10),
        max_delay=timedelta(seconds=100),
        backoff_rate=10.0,
        jitter_strategy=JitterStrategy.NONE,
    )

    assert config("pending", 3) == 100


def test_attempt_at_boundary():
    """The max-attempt boundary stops polling exactly when reached."""
    strategy = WaitDelayStrategy(max_attempts=3)

    assert strategy("pending", 3) is None
    assert strategy("pending", 2) > 0


def test_negative_delay_clamped_to_one():
    """Very small computed delays are clamped to one second."""
    config = WaitDelayStrategy(
        initial_delay=timedelta(seconds=0),
        backoff_rate=0,
        jitter_strategy=JitterStrategy.NONE,
    )

    assert config("pending", 1) == 1


@patch("random.random")
def test_rounding_behavior(mock_random):
    """Computed delays round up to whole seconds."""
    mock_random.return_value = 0.3
    config = WaitDelayStrategy(
        initial_delay=timedelta(seconds=3),
        jitter_strategy=JitterStrategy.FULL,
    )

    assert config("pending", 1) == 1
