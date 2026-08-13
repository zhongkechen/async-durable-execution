"""Unit tests for step handler."""

from typing import no_type_check

from typing import Any

import asyncio
import datetime
import inspect
import json
from datetime import timedelta
from unittest.mock import Mock, patch

import pytest
from async_durable_execution._core.exceptions import (
    CallableRuntimeError,
    ExecutionError,
    InvocationError,
    SerDesError,
    SuspendExecution,
    TerminationReason,
    UnrecoverableError,
    _decode_sdk_error_data,
    _restore_sdk_control_error,
)
from async_durable_execution._core.models import OperationIdentifier
from async_durable_execution._core.models import (
    ErrorObject,
    Operation,
    OperationAction,
    OperationStatus,
    OperationSubType,
    OperationType,
    StepDetails,
)
import logging
from async_durable_execution._core.context import (
    get_current_context,
)
from async_durable_execution._primitive.step import (
    StatefulStepOperationExecutor,
    StepOperationExecutor,
)
from async_durable_execution._operation.step import (
    StepInterruptedError,
    StepSemantics,
    get_step_context,
    step,
)
from async_durable_execution._core.serdes import SerDes
from async_durable_execution._core.state import ExecutionState
from async_durable_execution import ExtensionStepResult, StepContext

from ..serdes_test import CustomDictSerDes


async def _invoke_maybe_async(result) -> Any:
    return await result


def _asyncify(func) -> Any:
    if inspect.iscoroutinefunction(func):
        return func

    async def wrapper(*args, **kwargs) -> Any:
        return func(*args, **kwargs)

    return wrapper


class UppercaseSerDes(SerDes[str]):
    async def serialize(self, value: str) -> str:
        return value.upper()

    async def deserialize(self, data: str) -> str:
        return data


class FailingSerDes(SerDes[str]):
    async def serialize(self, value: str) -> str:
        raise ValueError("cannot serialize")

    async def deserialize(self, data: str) -> str:
        return data


# Test helper for StepOperationExecutor.
async def step_handler(
    func,
    state,
    operation_identifier,
    retry_strategy=None,
    step_semantics=StepSemantics.AT_LEAST_ONCE_PER_RETRY,
    serdes=None,
) -> Any:
    """Test helper that wraps StepOperationExecutor."""
    executor = StepOperationExecutor(
        func=_asyncify(func),
        state=state,
        operation_identifier=operation_identifier,
        retry_strategy=retry_strategy,
        step_semantics=step_semantics,
        serdes=serdes,
    )
    return await _invoke_maybe_async(executor.process())


def test_step_semantics_enum() -> None:
    """StepSemantics enum values remain stable."""
    assert StepSemantics.AT_MOST_ONCE_PER_RETRY.value == "AT_MOST_ONCE_PER_RETRY"
    assert StepSemantics.AT_LEAST_ONCE_PER_RETRY.value == "AT_LEAST_ONCE_PER_RETRY"


def test_step_interrupted_error() -> None:
    """StepInterruptedError is owned by the step operation module."""
    error = StepInterruptedError("step interrupted", "step_123")

    assert str(error) == "step interrupted"
    assert isinstance(error, InvocationError)
    assert isinstance(error, UnrecoverableError)
    assert error.termination_reason == TerminationReason.STEP_INTERRUPTED
    assert error.step_id == "step_123"


def test_step_operation_executor_accepts_config_fields_directly() -> None:
    """StepOperationExecutor stores step options directly."""
    retry_strategy = Mock()
    serdes = Mock()

    executor = StepOperationExecutor(
        func=Mock(),
        state=Mock(spec=ExecutionState),
        operation_identifier=OperationIdentifier(
            "step", OperationSubType.STEP, None, "test_step"
        ),
        retry_strategy=retry_strategy,
        step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY,
        serdes=serdes,
    )

    assert executor.retry_strategy is retry_strategy
    assert executor.step_semantics == StepSemantics.AT_MOST_ONCE_PER_RETRY
    assert executor.serdes is serdes


@pytest.mark.parametrize("retry_outcome", [False, True])
async def test_stateful_step_serialization_failure_is_checkpointed(
    retry_outcome,
) -> None:
    state = Mock(spec=ExecutionState)
    state.durable_execution_arn = "arn:test"
    state.operations.get.return_value = None

    async def work(_state):
        if retry_outcome:
            return ExtensionStepResult.retry("result", 1)
        return ExtensionStepResult.succeed("result")

    executor = StatefulStepOperationExecutor(
        func=work,
        state=state,
        operation_identifier=OperationIdentifier(
            "op-1",
            "AcmeStep",
            None,
            "custom",
            operation_type=OperationType.STEP,
        ),
        initial_state=None,
        retry_strategy=None,
        step_semantics=StepSemantics.AT_LEAST_ONCE_PER_RETRY,
        serdes=FailingSerDes(),
    )

    with pytest.raises(ExecutionError, match="Serialization failed"):
        await executor.process()

    actions = [
        call.kwargs["operation_update"].action
        for call in state.create_checkpoint.await_args_list
    ]
    assert actions == [OperationAction.START, OperationAction.FAIL]


def test_step_signature_accepts_config_fields_directly() -> None:
    """The public step API exposes step option fields directly."""
    parameters = inspect.signature(step).parameters

    assert "config" not in parameters
    assert "retry_strategy" in parameters
    assert "step_semantics" in parameters
    assert "serdes" in parameters


def test_step_signature_requires_keyword_only_options() -> None:
    """Only the step callable is positional."""
    parameters = inspect.signature(step).parameters

    assert parameters["func"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert parameters["name"].kind is inspect.Parameter.KEYWORD_ONLY
    assert parameters["retry_strategy"].kind is inspect.Parameter.KEYWORD_ONLY
    assert parameters["step_semantics"].kind is inspect.Parameter.KEYWORD_ONLY
    assert parameters["serdes"].kind is inspect.Parameter.KEYWORD_ONLY


def test_step_types_importable_from_package_root() -> None:
    """Step types remain re-exported from the package root."""
    from async_durable_execution import (
        StepSemantics as ImportedStepSemantics,
    )

    assert ImportedStepSemantics is StepSemantics


async def test_step_handler_already_succeeded() -> None:
    """Test step_handler when operation already succeeded."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="step1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        step_details=StepDetails(result=json.dumps("test_result")),
    )
    mock_result = operation
    mock_state.operations.get.return_value = mock_result

    mock_callable = Mock(return_value="should_not_call")
    mock_logger = Mock(spec=logging.Logger)

    result = await step_handler(
        mock_callable,
        mock_state,
        OperationIdentifier("step1", OperationSubType.STEP, None, "test_step"),
        None,
    )

    assert result == "test_result"
    mock_callable.assert_not_called()
    mock_state.create_checkpoint.assert_not_called()


async def test_step_handler_already_succeeded_none_result() -> None:
    """Test step_handler when operation succeeded with None result."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="step2",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        step_details=StepDetails(result=None),
    )
    mock_result = operation
    mock_state.operations.get.return_value = mock_result

    mock_callable = Mock()
    mock_logger = Mock(spec=logging.Logger)

    result = await step_handler(
        mock_callable,
        mock_state,
        OperationIdentifier("step2", OperationSubType.STEP, None, "test_step"),
        None,
    )

    assert result is None
    mock_callable.assert_not_called()


async def test_step_handler_already_failed() -> None:
    """Test step_handler when operation already failed."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    error = ErrorObject(
        message="Test error", type="TestError", data=None, stack_trace=None
    )
    operation = Operation(
        operation_id="step3",
        operation_type=OperationType.STEP,
        status=OperationStatus.FAILED,
        step_details=StepDetails(error=error),
    )
    mock_result = operation
    mock_state.operations.get.return_value = mock_result

    mock_callable = Mock()
    mock_logger = Mock(spec=logging.Logger)

    with pytest.raises(CallableRuntimeError):
        await step_handler(
            mock_callable,
            mock_state,
            OperationIdentifier("step3", OperationSubType.STEP, None, "test_step"),
            None,
        )

    mock_callable.assert_not_called()


@patch("async_durable_execution._primitive.step.StepOperationExecutor.retry_handler")
async def test_step_handler_at_most_once_interruption_without_retry_raise(
    mock_retry_handler,
) -> None:
    """A broken retry handler fallthrough still raises the checkpointed step error."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="step_interrupted",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
        step_details=StepDetails(attempt=0),
    )
    mock_state.operations.get.return_value = operation

    with pytest.raises(CallableRuntimeError, match="Unknown error"):
        await step_handler(
            Mock(),
            mock_state,
            OperationIdentifier(
                "step_interrupted", OperationSubType.STEP, None, "test_step"
            ),
            step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY,
        )

    mock_retry_handler.assert_called_once()


async def test_step_handler_started_at_most_once() -> None:
    """Test step_handler when operation started with AT_MOST_ONCE semantics."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="step4",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
        step_details=StepDetails(attempt=0),
    )
    mock_result = operation
    mock_state.operations.get.return_value = mock_result

    mock_callable = Mock()
    mock_logger = Mock(spec=logging.Logger)

    with pytest.raises(SuspendExecution):
        await step_handler(
            mock_callable,
            mock_state,
            OperationIdentifier("step4", OperationSubType.STEP, None, "test_step"),
            step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY,
        )


async def test_step_handler_started_at_least_once() -> None:
    """Test step_handler when operation started with AT_LEAST_ONCE semantics."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    error = ErrorObject(
        message="Test error", type="TestError", data=None, stack_trace=None
    )
    operation = Operation(
        operation_id="step5",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
        step_details=StepDetails(error=error),
    )
    mock_result = operation
    mock_state.operations.get.return_value = mock_result

    mock_callable = Mock(return_value="success_result")
    mock_logger = Mock(spec=logging.Logger)

    await step_handler(
        mock_callable,
        mock_state,
        OperationIdentifier("step5", OperationSubType.STEP, None, "test_step"),
        step_semantics=StepSemantics.AT_LEAST_ONCE_PER_RETRY,
    )


async def test_step_handler_success_at_least_once() -> None:
    """Test step_handler successful execution with AT_LEAST_ONCE semantics."""
    mock_state = Mock(spec=ExecutionState)
    mock_result = None
    mock_state.operations.get.return_value = mock_result
    mock_state.durable_execution_arn = "test_arn"

    mock_callable = Mock(return_value="success_result")
    mock_logger = Mock(spec=logging.Logger)

    result = await step_handler(
        mock_callable,
        mock_state,
        OperationIdentifier("step6", OperationSubType.STEP, None, "test_step"),
        step_semantics=StepSemantics.AT_LEAST_ONCE_PER_RETRY,
    )

    assert result == "success_result"

    assert mock_state.create_checkpoint.call_count == 2

    # Verify start checkpoint
    start_call = mock_state.create_checkpoint.call_args_list[0]
    start_operation = start_call[1]["operation_update"]
    assert start_operation.operation_id == "step6"
    assert start_operation.operation_type is OperationType.STEP
    assert start_operation.sub_type is OperationSubType.STEP
    assert start_operation.action is OperationAction.START

    # Verify success checkpoint
    success_call = mock_state.create_checkpoint.call_args_list[1]
    success_operation = success_call[1]["operation_update"]
    assert success_operation.operation_id == "step6"
    assert success_operation.payload == json.dumps("success_result")
    assert success_operation.operation_type is OperationType.STEP
    assert success_operation.sub_type is OperationSubType.STEP
    assert success_operation.action is OperationAction.SUCCEED


async def test_step_handler_passes_attempt_to_step_context() -> None:
    """Test step execution exposes the current attempt on StepContext."""
    mock_state = Mock(spec=ExecutionState)
    mock_result = None
    mock_state.operations.get.return_value = mock_result
    mock_state.durable_execution_arn = "test_arn"
    mock_logger = Mock(spec=logging.Logger)

    async def step_callable() -> Any:
        current_context = get_current_context()
        assert isinstance(current_context, StepContext)
        return current_context.attempt

    result = await step_handler(
        step_callable,
        mock_state,
        OperationIdentifier("step_attempt", OperationSubType.STEP, None, "test_step"),
        step_semantics=StepSemantics.AT_LEAST_ONCE_PER_RETRY,
    )

    assert result == 1


@no_type_check
async def test_step_handler_passes_lambda_context_to_step_context() -> None:
    """Step execution exposes the Lambda context on StepContext."""
    mock_state = Mock(spec=ExecutionState)
    mock_result = None
    mock_state.operations.get.return_value = mock_result
    mock_state.durable_execution_arn = "test_arn"
    lambda_context = Mock()
    lambda_context.aws_request_id = "request-123"
    mock_state.lambda_context = lambda_context

    async def step_callable() -> Any:
        current_context = get_current_context()
        assert isinstance(current_context, StepContext)
        return current_context.lambda_context.aws_request_id

    executor = StepOperationExecutor(
        func=step_callable,
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "step_lambda_context",
            OperationSubType.STEP,
            None,
            "test_step",
        ),
        step_semantics=StepSemantics.AT_LEAST_ONCE_PER_RETRY,
    )

    result = await executor.process()

    assert result == "request-123"


@no_type_check
async def test_step_handler_get_current_context_returns_step_context() -> None:
    """get_current_context() should expose StepContext while a step is executing."""
    mock_state = Mock(spec=ExecutionState)
    mock_result = None
    mock_state.operations.get.return_value = mock_result
    mock_state.durable_execution_arn = "test_arn"
    mock_logger = Mock(spec=logging.Logger)

    async def step_callable() -> Any:
        return get_current_context().attempt

    result = await step_handler(
        step_callable,
        mock_state,
        OperationIdentifier("step_context", OperationSubType.STEP, None, "test_step"),
        step_semantics=StepSemantics.AT_LEAST_ONCE_PER_RETRY,
    )

    assert result == 1


async def test_step_handler_get_step_context_returns_step_context() -> None:
    """get_step_context() should expose StepContext while a step is executing."""
    mock_state = Mock(spec=ExecutionState)
    mock_result = None
    mock_state.operations.get.return_value = mock_result
    mock_state.durable_execution_arn = "test_arn"

    async def step_callable() -> Any:
        return get_step_context().attempt

    result = await step_handler(
        step_callable,
        mock_state,
        OperationIdentifier(
            "step_specific_context", OperationSubType.STEP, None, "test_step"
        ),
        step_semantics=StepSemantics.AT_LEAST_ONCE_PER_RETRY,
    )

    assert result == 1


def test_get_current_context_raises_outside_execution() -> None:
    with pytest.raises(
        RuntimeError,
        match="get_current_context\\(\\) can only be used while a durable function, step function, flow node, wait_for_callback submitter, or wait_for_condition check, or SerDes operation is executing\\.",
    ):
        get_current_context()


async def test_step_handler_success_at_most_once() -> None:
    """Test step_handler successful execution with AT_MOST_ONCE semantics."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    # First call: not found, second call: started (after sync checkpoint)
    not_found = None
    started_op = Operation(
        operation_id="step7",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
        step_details=StepDetails(attempt=0),
    )
    started = started_op
    mock_state.operations.get.side_effect = [not_found, started]

    mock_callable = Mock(return_value="success_result")
    mock_logger = Mock(spec=logging.Logger)

    result = await step_handler(
        mock_callable,
        mock_state,
        OperationIdentifier("step7", OperationSubType.STEP, None, "test_step"),
        step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY,
    )

    assert result == "success_result"

    assert mock_state.create_checkpoint.call_count == 2

    # Verify start checkpoint
    start_call = mock_state.create_checkpoint.call_args_list[0]
    start_operation = start_call[1]["operation_update"]
    assert start_operation.operation_id == "step7"
    assert start_operation.name == "test_step"
    assert start_operation.operation_type is OperationType.STEP
    assert start_operation.sub_type is OperationSubType.STEP
    assert start_operation.action is OperationAction.START

    # Verify success checkpoint
    success_call = mock_state.create_checkpoint.call_args_list[1]
    success_operation = success_call[1]["operation_update"]
    assert success_operation.payload == json.dumps("success_result")
    assert success_operation.operation_type is OperationType.STEP
    assert success_operation.sub_type is OperationSubType.STEP
    assert success_operation.action is OperationAction.SUCCEED


async def test_step_handler_non_retriable_execution_error() -> None:
    """Test step_handler with ExecutionError exception."""
    mock_state = Mock(spec=ExecutionState)
    mock_result = None
    mock_state.operations.get.return_value = mock_result
    mock_state.durable_execution_arn = "test_arn"

    mock_callable = Mock(side_effect=ExecutionError("Do Not Retry"))
    mock_logger = Mock(spec=logging.Logger)

    with pytest.raises(ExecutionError, match="Do Not Retry"):
        await step_handler(
            mock_callable,
            mock_state,
            OperationIdentifier("step8", OperationSubType.STEP, None, "test_step"),
            None,
        )


async def test_step_handler_retry_success() -> None:
    """Test step_handler with retry that succeeds."""
    mock_state = Mock(spec=ExecutionState)
    mock_result = None
    mock_state.operations.get.return_value = mock_result
    mock_state.durable_execution_arn = "test_arn"

    mock_retry_strategy = Mock(return_value=timedelta(seconds=5))
    mock_callable = Mock(side_effect=RuntimeError("Test error"))
    mock_logger = Mock(spec=logging.Logger)

    with pytest.raises(SuspendExecution, match="Retry scheduled"):
        await step_handler(
            mock_callable,
            mock_state,
            OperationIdentifier("step9", OperationSubType.STEP, None, "test_step"),
            retry_strategy=mock_retry_strategy,
        )

    assert mock_state.create_checkpoint.call_count == 2

    # Verify start checkpoint
    start_call = mock_state.create_checkpoint.call_args_list[0]
    start_operation = start_call[1]["operation_update"]
    assert start_operation.operation_id == "step9"
    assert start_operation.operation_type is OperationType.STEP
    assert start_operation.sub_type is OperationSubType.STEP
    assert start_operation.action is OperationAction.START

    # Verify retry checkpoint
    retry_call = mock_state.create_checkpoint.call_args_list[1]
    retry_operation = retry_call[1]["operation_update"]
    assert retry_operation.operation_id == "step9"
    assert retry_operation.operation_type is OperationType.STEP
    assert retry_operation.sub_type is OperationSubType.STEP
    assert retry_operation.action is OperationAction.RETRY


async def test_step_handler_retry_delay_is_clamped_to_minimum() -> None:
    """Retry delays below one second are checkpointed with the minimum delay."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.operations.get.return_value = None
    mock_state.durable_execution_arn = "test_arn"

    retry_strategy = Mock(return_value=0)

    with pytest.raises(SuspendExecution, match="Retry scheduled"):
        await step_handler(
            Mock(side_effect=RuntimeError("temporary failure")),
            mock_state,
            OperationIdentifier("step_delay", OperationSubType.STEP, None, "test_step"),
            retry_strategy=retry_strategy,
        )

    retry_call = mock_state.create_checkpoint.call_args_list[1]
    retry_operation = retry_call.kwargs["operation_update"]
    assert retry_operation.step_options.next_attempt_delay_seconds == 1


async def test_step_handler_retry_exhausted() -> None:
    """Test step_handler checkpoints retry strategy errors as failures."""
    mock_state = Mock(spec=ExecutionState)
    mock_result = None
    mock_state.operations.get.return_value = mock_result
    mock_state.durable_execution_arn = "test_arn"

    error = RuntimeError("Test error")
    mock_retry_strategy = Mock(side_effect=error)
    mock_callable = Mock(side_effect=error)
    mock_logger = Mock(spec=logging.Logger)

    with pytest.raises(CallableRuntimeError):
        await step_handler(
            mock_callable,
            mock_state,
            OperationIdentifier("step10", OperationSubType.STEP, None, "test_step"),
            retry_strategy=mock_retry_strategy,
        )

    assert mock_state.create_checkpoint.call_count == 2

    # Verify start checkpoint
    start_call = mock_state.create_checkpoint.call_args_list[0]
    start_operation = start_call[1]["operation_update"]
    assert start_operation.operation_id == "step10"
    assert start_operation.operation_type is OperationType.STEP
    assert start_operation.sub_type is OperationSubType.STEP
    assert start_operation.action is OperationAction.START

    # Verify fail checkpoint
    fail_call = mock_state.create_checkpoint.call_args_list[1]
    fail_operation = fail_call[1]["operation_update"]
    assert fail_operation.operation_id == "step10"
    assert fail_operation.operation_type is OperationType.STEP
    assert fail_operation.sub_type is OperationSubType.STEP
    assert fail_operation.action is OperationAction.FAIL


async def test_step_handler_retry_strategy_none_stops_retrying() -> None:
    """A retry strategy returns None to stop retrying and fail with the step error."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.operations.get.return_value = None
    mock_state.durable_execution_arn = "test_arn"

    error = RuntimeError("Step execution error")
    mock_retry_strategy = Mock(return_value=None)
    mock_callable = Mock(side_effect=error)

    with pytest.raises(CallableRuntimeError, match="Step execution error"):
        await step_handler(
            mock_callable,
            mock_state,
            OperationIdentifier(
                "step10_none", OperationSubType.STEP, None, "test_step"
            ),
            retry_strategy=mock_retry_strategy,
        )

    mock_retry_strategy.assert_called_once_with(error, 1)
    assert mock_state.create_checkpoint.call_count == 2

    fail_call = mock_state.create_checkpoint.call_args_list[1]
    fail_operation = fail_call[1]["operation_update"]
    assert fail_operation.operation_id == "step10_none"
    assert fail_operation.operation_type is OperationType.STEP
    assert fail_operation.sub_type is OperationSubType.STEP
    assert fail_operation.action is OperationAction.FAIL
    assert fail_operation.error.message == "Step execution error"
    assert fail_operation.error.type == "RuntimeError"


async def test_step_handler_checkpoints_sdk_error_metadata() -> None:
    mock_state = Mock(spec=ExecutionState)
    mock_state.operations.get.return_value = None
    mock_state.durable_execution_arn = "test_arn"
    error = SerDesError("Serialization failed")

    with pytest.raises(CallableRuntimeError, match="Serialization failed"):
        await step_handler(
            Mock(side_effect=error),
            mock_state,
            OperationIdentifier("sdk-error", OperationSubType.STEP, None, "test_step"),
            retry_strategy=Mock(return_value=None),
        )

    fail_operation = mock_state.create_checkpoint.call_args_list[1].kwargs[
        "operation_update"
    ]
    is_sdk_error, _ = _decode_sdk_error_data(
        fail_operation.error.data,
        SerDesError,
    )
    assert fail_operation.error.type == "SerDesError"
    assert is_sdk_error


@pytest.mark.parametrize("failure_source", ["step", "retry-strategy"])
@no_type_check
async def test_terminal_step_invocation_error_is_not_retryable(failure_source) -> None:
    mock_state = Mock(spec=ExecutionState)
    mock_state.operations.get.return_value = None
    mock_state.durable_execution_arn = "test_arn"
    invocation_error = InvocationError(f"{failure_source} invocation failure")
    if failure_source == "step":
        callable_error = invocation_error
        retry_strategy = Mock(return_value=None)
    else:
        callable_error = RuntimeError("step failure")
        retry_strategy = Mock(side_effect=invocation_error)

    with pytest.raises(CallableRuntimeError, match=str(invocation_error)):
        await step_handler(
            Mock(side_effect=callable_error),
            mock_state,
            OperationIdentifier(
                f"terminal-{failure_source}",
                OperationSubType.STEP,
                None,
                "test_step",
            ),
            retry_strategy=retry_strategy,
        )

    fail_operation = mock_state.create_checkpoint.call_args_list[1].kwargs[
        "operation_update"
    ]
    restored = _restore_sdk_control_error(
        fail_operation.error.message,
        fail_operation.error.type,
        fail_operation.error.data,
    )
    assert isinstance(restored, InvocationError)
    assert not restored.is_retryable()


async def test_step_retry_preserves_retryable_invocation_error_metadata() -> None:
    mock_state = Mock(spec=ExecutionState)
    mock_state.operations.get.return_value = None
    mock_state.durable_execution_arn = "test_arn"
    invocation_error = InvocationError("transient invocation failure")

    with pytest.raises(SuspendExecution, match="Retry scheduled"):
        await step_handler(
            Mock(side_effect=invocation_error),
            mock_state,
            OperationIdentifier(
                "retryable-invocation",
                OperationSubType.STEP,
                None,
                "test_step",
            ),
            retry_strategy=Mock(return_value=timedelta(seconds=1)),
        )

    retry_operation = mock_state.create_checkpoint.call_args_list[1].kwargs[
        "operation_update"
    ]
    restored = _restore_sdk_control_error(
        retry_operation.error.message,
        retry_operation.error.type,
        retry_operation.error.data,
    )
    assert isinstance(restored, InvocationError)
    assert restored.is_retryable()


async def test_step_handler_retry_interrupted_error() -> None:
    """Test step_handler with StepInterruptedError in retry."""
    mock_state = Mock(spec=ExecutionState)
    mock_result = None
    mock_state.operations.get.return_value = mock_result
    mock_state.durable_execution_arn = "test_arn"

    interrupted_error = StepInterruptedError("Step interrupted")
    mock_retry_strategy = Mock(side_effect=interrupted_error)
    mock_callable = Mock(side_effect=interrupted_error)
    mock_logger = Mock(spec=logging.Logger)

    with pytest.raises(StepInterruptedError, match="Step interrupted"):
        await step_handler(
            mock_callable,
            mock_state,
            OperationIdentifier("step11", OperationSubType.STEP, None, "test_step"),
            retry_strategy=mock_retry_strategy,
        )


async def test_step_handler_retry_with_existing_attempts() -> None:
    """Test step_handler retry logic with existing attempt count."""
    mock_state = Mock(spec=ExecutionState)

    # Simulate a retry operation that was previously checkpointed
    operation = Operation(
        operation_id="step12",
        operation_type=OperationType.STEP,
        status=OperationStatus.PENDING,
        step_details=StepDetails(
            attempt=2,
            next_attempt_timestamp=datetime.datetime.fromtimestamp(
                1764547200, tz=datetime.timezone.utc
            ),
        ),
    )
    mock_result = operation
    mock_state.operations.get.return_value = mock_result
    mock_state.durable_execution_arn = "test_arn"

    mock_retry_strategy = Mock(return_value=timedelta(seconds=10))
    mock_callable = Mock(side_effect=RuntimeError("Test error"))
    mock_logger = Mock(spec=logging.Logger)

    with pytest.raises(SuspendExecution, match="Retry scheduled"):
        await step_handler(
            mock_callable,
            mock_state,
            OperationIdentifier("step12", OperationSubType.STEP, None, "test_step"),
            retry_strategy=mock_retry_strategy,
        )

    # Verify retry strategy was not called because we already have attempt timestamp in the checkpointed location
    mock_retry_strategy.assert_not_called()


async def test_step_handler_pending_without_existing_attempts() -> None:
    """Test step_handler retry logic with existing attempt count."""
    mock_state = Mock(spec=ExecutionState)

    # Simulate a retry operation that was previously checkpointed
    operation = Operation(
        operation_id="step12",
        operation_type=OperationType.STEP,
        status=OperationStatus.PENDING,
        step_details=StepDetails(attempt=2),
    )
    mock_result = operation
    mock_state.operations.get.return_value = mock_result
    mock_state.durable_execution_arn = "test_arn"

    mock_retry_strategy = Mock(return_value=timedelta(seconds=10))
    mock_callable = Mock(side_effect=RuntimeError("Test error"))
    mock_logger = Mock(spec=logging.Logger)

    with pytest.raises(SuspendExecution, match="No timestamp provided"):
        await step_handler(
            mock_callable,
            mock_state,
            OperationIdentifier("step12", OperationSubType.STEP, None, "test_step"),
            retry_strategy=mock_retry_strategy,
        )

    # Verify retry strategy was not called because we already have attempt timestamp in the checkpointed location
    mock_retry_strategy.assert_not_called()


@patch("async_durable_execution._primitive.step.StepOperationExecutor.retry_handler")
async def test_step_handler_retry_handler_no_exception(mock_retry_handler) -> None:
    """Test step_handler when retry_handler doesn't raise an exception."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    # First call: not found, second call: started (AT_LEAST_ONCE default)
    not_found = None
    started_op = Operation(
        operation_id="step13",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
        step_details=StepDetails(attempt=0),
    )
    started = started_op
    mock_state.operations.get.side_effect = [not_found, started]

    # Mock retry_handler to not raise an exception (which it should always do)
    mock_retry_handler.return_value = None

    mock_callable = Mock(side_effect=RuntimeError("Test error"))
    mock_logger = Mock(spec=logging.Logger)

    with pytest.raises(
        ExecutionError,
        match="retry handler should have raised an exception, but did not.",
    ):
        await step_handler(
            mock_callable,
            mock_state,
            OperationIdentifier("step13", OperationSubType.STEP, None, "test_step"),
            None,
        )

    mock_retry_handler.assert_called_once()


async def test_step_handler_custom_serdes_success() -> None:
    mock_state = Mock(spec=ExecutionState)
    mock_result = None
    mock_state.operations.get.return_value = mock_result
    mock_state.durable_execution_arn = "test_arn"

    complex_result = {"key": "value", "number": 42, "list": [1, 2, 3]}
    mock_callable = Mock(return_value=complex_result)
    mock_logger = Mock(spec=logging.Logger)

    await step_handler(
        mock_callable,
        mock_state,
        OperationIdentifier("step6", OperationSubType.STEP, None, "test_step"),
        step_semantics=StepSemantics.AT_LEAST_ONCE_PER_RETRY,
        serdes=CustomDictSerDes(),
    )

    expected_checkpoointed_result = (
        '{"key": "VALUE", "number": "84", "list": [1, 2, 3]}'
    )

    success_call = mock_state.create_checkpoint.call_args_list[1]
    success_operation = success_call[1]["operation_update"]
    assert success_operation.payload == expected_checkpoointed_result


async def test_step_handler_returns_deserialized_serialized_custom_serdes_result() -> (
    None
):
    mock_state = Mock(spec=ExecutionState)
    mock_state.operations.get.return_value = None
    mock_state.durable_execution_arn = "test_arn"

    result = await step_handler(
        Mock(return_value="hello"),
        mock_state,
        OperationIdentifier("step_uppercase", OperationSubType.STEP, None, "uppercase"),
        serdes=UppercaseSerDes(),
    )

    success_call = mock_state.create_checkpoint.call_args_list[1]
    success_operation = success_call[1]["operation_update"]
    assert success_operation.payload == "HELLO"
    assert result == "HELLO"


async def test_step_handler_custom_serdes_already_succeeded() -> None:
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="step1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        step_details=StepDetails(
            result='{"key": "VALUE", "number": "84", "list": [1, 2, 3]}'
        ),
    )
    mock_result = operation
    mock_state.operations.get.return_value = mock_result

    mock_callable = Mock(return_value="should_not_call")
    mock_logger = Mock(spec=logging.Logger)

    result = await step_handler(
        mock_callable,
        mock_state,
        OperationIdentifier("step1", OperationSubType.STEP, None, "test_step"),
        serdes=CustomDictSerDes(),
    )

    assert result == {"key": "value", "number": 42, "list": [1, 2, 3]}


# Tests for start checkpoint refresh handling


async def test_step_start_does_not_refresh_checkpoint_after_start() -> None:
    """Test that start execution does not reread the checkpoint after START."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    # First call: not found (checkpoint doesn't exist).
    # start() should not call get_checkpointed_result again after START.
    not_found = None
    mock_state.operations.get.return_value = not_found

    mock_callable = Mock(return_value="success_result")
    mock_logger = Mock(spec=logging.Logger)

    result = await step_handler(
        mock_callable,
        mock_state,
        OperationIdentifier(
            "step_immediate_1", OperationSubType.STEP, None, "test_step"
        ),
        step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY,
    )

    assert mock_state.operations.get.call_count == 1
    assert result == "success_result"


async def test_step_immediate_response_create_checkpoint_sync_at_most_once() -> None:
    """Test that create_checkpoint is called with is_sync=True for AT_MOST_ONCE semantics."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    # First call: not found; start does not reread after the START checkpoint.
    not_found = None
    mock_state.operations.get.return_value = not_found

    mock_callable = Mock(return_value="success_result")
    mock_logger = Mock(spec=logging.Logger)

    await step_handler(
        mock_callable,
        mock_state,
        OperationIdentifier(
            "step_immediate_2", OperationSubType.STEP, None, "test_step"
        ),
        step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY,
    )

    # Verify START checkpoint was created with is_sync=True
    start_call = mock_state.create_checkpoint.call_args_list[0]
    assert start_call[1]["is_sync"] is True


async def test_step_immediate_response_create_checkpoint_async_at_least_once() -> None:
    """Test that create_checkpoint is called with is_sync=False for AT_LEAST_ONCE semantics."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    # For AT_LEAST_ONCE, only one call to direct state lookup (no second check)
    not_found = None
    mock_state.operations.get.return_value = not_found

    mock_callable = Mock(return_value="success_result")
    mock_logger = Mock(spec=logging.Logger)

    await step_handler(
        mock_callable,
        mock_state,
        OperationIdentifier(
            "step_immediate_3", OperationSubType.STEP, None, "test_step"
        ),
        step_semantics=StepSemantics.AT_LEAST_ONCE_PER_RETRY,
    )

    # Verify START checkpoint was created with is_sync=False
    start_call = mock_state.create_checkpoint.call_args_list[0]
    assert start_call[1]["is_sync"] is False


async def test_step_immediate_response_immediate_success() -> None:
    """Test successful execution without a checkpoint refresh after START."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    # First call: not found; start proceeds to execute without a second read.
    not_found = None
    mock_state.operations.get.return_value = not_found

    mock_callable = Mock(return_value="immediate_success_result")
    mock_logger = Mock(spec=logging.Logger)

    result = await step_handler(
        mock_callable,
        mock_state,
        OperationIdentifier(
            "step_immediate_4", OperationSubType.STEP, None, "test_step"
        ),
        step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY,
    )

    # Verify operation executed normally (no immediate response in current implementation)
    assert result == "immediate_success_result"
    mock_callable.assert_called_once()
    # Both START and SUCCEED checkpoints should be created
    assert mock_state.create_checkpoint.call_count == 2


async def test_step_handler_unrecognized_status_executes_again() -> None:
    """Statuses not handled specially fall through to normal execution."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="step_cancelled",
        operation_type=OperationType.STEP,
        status=OperationStatus.CANCELLED,
        step_details=StepDetails(attempt=2),
    )
    mock_state.operations.get.return_value = operation

    result = await step_handler(
        Mock(return_value="rerun"),
        mock_state,
        OperationIdentifier("step_cancelled", OperationSubType.STEP, None, "test_step"),
    )

    assert result == "rerun"


async def test_step_immediate_response_immediate_failure() -> None:
    """Test step failure without a checkpoint refresh after START."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    # First call: not found; start proceeds to execute without a second read.
    not_found = None
    mock_state.operations.get.return_value = not_found

    # Make the step function raise an error
    error = RuntimeError("Step execution error")
    mock_callable = Mock(side_effect=error)
    mock_logger = Mock(spec=logging.Logger)

    # Configure retry strategy to not retry
    mock_retry_strategy = Mock(return_value=None)
    # Verify operation raises error after executing step function
    with pytest.raises(CallableRuntimeError, match="Step execution error"):
        await step_handler(
            mock_callable,
            mock_state,
            OperationIdentifier(
                "step_immediate_5", OperationSubType.STEP, None, "test_step"
            ),
            retry_strategy=mock_retry_strategy,
            step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY,
        )

    mock_callable.assert_called_once()
    # Both START and FAIL checkpoints should be created
    assert mock_state.create_checkpoint.call_count == 2


async def test_step_start_executes_without_second_checkpoint_read() -> None:
    """Test start execution runs the step function after the initial checkpoint read."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    # First call: not found; start proceeds to execute without a second read.
    not_found = None
    mock_state.operations.get.return_value = not_found

    mock_callable = Mock(return_value="normal_execution_result")
    mock_logger = Mock(spec=logging.Logger)

    result = await step_handler(
        mock_callable,
        mock_state,
        OperationIdentifier(
            "step_immediate_6", OperationSubType.STEP, None, "test_step"
        ),
        step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY,
    )

    # Verify step function was executed
    assert result == "normal_execution_result"
    mock_callable.assert_called_once()
    # Both START and SUCCEED checkpoints should be created
    assert mock_state.create_checkpoint.call_count == 2


async def test_step_immediate_response_already_completed() -> None:
    """Test already completed: checkpoint is already SUCCEEDED on first check, no checkpoint created."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    # First call: already succeeded (replay scenario)
    succeeded_op = Operation(
        operation_id="step_immediate_7",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
        step_details=StepDetails(result=json.dumps("already_completed_result")),
    )
    succeeded = succeeded_op
    mock_state.operations.get.return_value = succeeded

    mock_callable = Mock(return_value="should_not_call")
    mock_logger = Mock(spec=logging.Logger)

    result = await step_handler(
        mock_callable,
        mock_state,
        OperationIdentifier(
            "step_immediate_7", OperationSubType.STEP, None, "test_step"
        ),
        step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY,
    )

    # Verify operation returned immediately without creating checkpoint
    assert result == "already_completed_result"
    mock_callable.assert_not_called()
    mock_state.create_checkpoint.assert_not_called()
    # Only one call to direct state lookup (no second check needed)
    assert mock_state.operations.get.call_count == 1


async def test_step_executes_function_when_second_check_returns_started() -> None:
    """Test backward compatibility: when the second checkpoint check returns
    STARTED (not terminal), the step function executes normally.
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    # First call: checkpoint doesn't exist
    # Second call: checkpoint returns STARTED (no immediate response)
    not_found = None
    started_op = Operation(
        operation_id="step-1",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
        step_details=StepDetails(attempt=1),
    )
    started = started_op
    mock_state.operations.get.side_effect = [not_found, started]

    mock_step_function = Mock(return_value="result")
    mock_logger = Mock(spec=logging.Logger)

    executor = StepOperationExecutor(
        func=_asyncify(mock_step_function),
        state=mock_state,
        operation_identifier=OperationIdentifier(
            "step-1", OperationSubType.STEP, None, "test_step"
        ),
        step_semantics=StepSemantics.AT_LEAST_ONCE_PER_RETRY,
    )
    result = await executor.process()

    # Assert - behaves like "old way"
    mock_step_function.assert_called_once()  # Function executed (not skipped)
    assert result == "result"
    assert mock_state.operations.get.call_count == 1  # Only one check for AT_LEAST_ONCE
    assert mock_state.create_checkpoint.call_count == 2  # START + SUCCEED checkpoints


async def test_step_creates_start_checkpoint_when_status_is_ready() -> None:
    """Test that create_checkpoint is called with START action when the step is in READY status."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    # Simulate a step that is in READY status (e.g., returned from a previous checkpoint)
    ready_op = Operation(
        operation_id="step_ready_1",
        operation_type=OperationType.STEP,
        status=OperationStatus.READY,
        step_details=StepDetails(attempt=0),
    )
    ready_result = ready_op

    # After creating the sync START checkpoint, the refreshed result returns STARTED
    started_op = Operation(
        operation_id="step_ready_1",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
        step_details=StepDetails(attempt=0),
    )
    started_result = started_op
    mock_state.operations.get.side_effect = [ready_result, started_result]

    mock_callable = Mock(return_value="ready_step_result")
    mock_logger = Mock(spec=logging.Logger)

    result = await step_handler(
        mock_callable,
        mock_state,
        OperationIdentifier("step_ready_1", OperationSubType.STEP, None, "test_step"),
        step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY,
    )

    assert result == "ready_step_result"
    mock_callable.assert_called_once()

    # Verify START checkpoint was created
    start_call = mock_state.create_checkpoint.call_args_list[0]
    start_operation = start_call[1]["operation_update"]
    assert start_operation.operation_id == "step_ready_1"
    assert start_operation.operation_type is OperationType.STEP
    assert start_operation.sub_type is OperationSubType.STEP
    assert start_operation.action is OperationAction.START

    # Verify SUCCEED checkpoint was also created after execution
    assert mock_state.create_checkpoint.call_count == 2
    success_call = mock_state.create_checkpoint.call_args_list[1]
    success_operation = success_call[1]["operation_update"]
    assert success_operation.action is OperationAction.SUCCEED
