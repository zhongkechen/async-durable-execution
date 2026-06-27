"""Unit tests for context."""

import asyncio
import json
import random
from datetime import timedelta
from functools import partial
from itertools import islice
from unittest.mock import ANY, AsyncMock, MagicMock, Mock, patch

import pytest

from async_durable_execution.context import (
    reset_current_context,
    set_current_context,
    get_current_context,
)
from async_durable_execution.primitive.callback import (
    Callback,
    CallbackError,
)
from async_durable_execution.composite.wait_for_condition import (
    WaitForConditionConfig,
    WaitForConditionDecision,
)
from async_durable_execution import (
    durable_callable,
    create_callback,
    step,
    wait,
    parallel,
    invoke,
    run_in_child_context,
    wait_for_condition,
    wait_for_callback,
    map as map_operation,
    StepContext,
    StepSemantics,
    DurableContext,
)
from async_durable_execution.exceptions import (
    SuspendExecution,
    ValidationError,
)
from async_durable_execution.models import OperationIdentifier
from async_durable_execution.models import (
    CallbackDetails,
    ErrorObject,
    Operation,
    OperationStatus,
    OperationSubType,
    OperationType,
)
from async_durable_execution.plugin import PluginExecutor
from async_durable_execution.state import ExecutionState
from async_durable_execution.primitive.base import CheckpointedResult

from .serdes_test import CustomDictSerDes
from .test_helpers import operation_id_sequence


async def run_async(awaitable):
    return await awaitable


async def run_with_context(context: DurableContext, awaitable):
    token = set_current_context(context)
    try:
        return await awaitable
    finally:
        reset_current_context(token)


async def test_current_context_is_isolated_between_asyncio_tasks():
    """Concurrent tasks should keep independent current-context bindings."""
    parent_context = create_test_context(parent_id="parent")
    task_a_context = create_test_context(parent_id="task-a")
    task_b_context = create_test_context(parent_id="task-b")
    task_a_bound = asyncio.Event()
    task_b_bound = asyncio.Event()

    async def worker(
        context: DurableContext,
        bound: asyncio.Event,
        other_bound: asyncio.Event,
    ) -> DurableContext:
        token = set_current_context(context)
        try:
            assert get_current_context() is context
            bound.set()
            await asyncio.wait_for(other_bound.wait(), timeout=1)
            await asyncio.sleep(0)
            assert get_current_context() is context
            return get_current_context()
        finally:
            reset_current_context(token)

    parent_token = set_current_context(parent_context)
    try:
        task_a = asyncio.create_task(worker(task_a_context, task_a_bound, task_b_bound))
        task_b = asyncio.create_task(worker(task_b_context, task_b_bound, task_a_bound))

        result_a, result_b = await asyncio.gather(task_a, task_b)

        assert result_a is task_a_context
        assert result_b is task_b_context
        assert get_current_context() is parent_context
    finally:
        reset_current_context(parent_token)


def make_async_executor(result):
    mock_executor = MagicMock()
    mock_executor.process = AsyncMock(return_value=result)
    return mock_executor


def create_async_child_state() -> Mock:
    state = Mock(spec=ExecutionState)
    state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    state.operations.get.return_value = CheckpointedResult.create_not_found()
    state.create_checkpoint = AsyncMock()
    state.wrap_user_function = lambda func, *args, **kwargs: func
    return state


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


async def test_durable_context():
    """Test the context module."""
    assert DurableContext is not None


async def test_step_context_exposes_lambda_context_from_operation_context():
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    lambda_context = Mock()

    context = StepContext(
        execution_state=mock_state,
        operation_identifier=OperationIdentifier(
            operation_id="step-1",
            sub_type=OperationSubType.STEP,
            parent_id=None,
        ),
        lambda_context=lambda_context,
        attempt=1,
    )

    assert context.lambda_context is lambda_context


async def test_child_context_inherits_lambda_context_from_operation_context():
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    lambda_context = Mock()
    parent_context = DurableContext(
        execution_state=mock_state,
        operation_identifier=OperationIdentifier.create_execution_op(),
        lambda_context=lambda_context,
    )

    child_context = parent_context.create_child_context("child-op-1")

    assert child_context.lambda_context is lambda_context


async def test_module_level_context_functions_delegate_to_durable_context():
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    context = create_test_context(state=mock_state)

    async def step_func() -> str:
        return "value"

    async def child_func() -> str:
        return "child"

    async def map_func(item: int) -> int:
        return item

    async def parallel_func() -> str:
        return "parallel"

    async def submitter() -> None:
        return None

    async def check(state: str) -> str:
        return state

    config = WaitForConditionConfig(
        wait_strategy=lambda state, attempt: WaitForConditionDecision.stop_polling(),
    )

    mock_wait = AsyncMock(return_value=None)
    mock_child = AsyncMock(return_value="child-result")
    mock_callback_child = AsyncMock(return_value="callback-wait-result")
    mock_map_child_executor = MagicMock(return_value=make_async_executor("map-result"))
    mock_parallel_child_executor = MagicMock(
        return_value=make_async_executor("parallel-result")
    )

    step_executor = AsyncMock()
    step_executor.process.return_value = "step-result"
    callback_executor = AsyncMock()
    callback_executor.process.return_value = "callback-id"
    invoke_executor = AsyncMock()
    invoke_executor.process.return_value = "invoke-result"
    wait_for_condition_executor = AsyncMock()
    wait_for_condition_executor.process.return_value = "condition-result"

    with (
        patch(
            "async_durable_execution.primitive.step.StepOperationExecutor"
        ) as mock_step_executor,
        patch(
            "async_durable_execution.primitive.callback.CallbackOperationExecutor"
        ) as mock_callback_executor,
        patch(
            "async_durable_execution.primitive.invoke.InvokeOperationExecutor"
        ) as mock_invoke_executor,
        patch(
            "async_durable_execution.composite.wait_for_condition.WaitForConditionOperationExecutor"
        ) as mock_wait_for_condition_executor,
        patch(
            "async_durable_execution.primitive.wait.WaitOperationExecutor"
        ) as mock_wait_executor,
        patch(
            "async_durable_execution.primitive.child.ChildOperationExecutor",
            MagicMock(return_value=make_async_executor("child-result")),
        ),
        patch(
            "async_durable_execution.composite.wait_for_callback.run_in_child_context",
            mock_callback_child,
        ),
        patch(
            "async_durable_execution.composite.map.ChildOperationExecutor",
            mock_map_child_executor,
        ),
        patch(
            "async_durable_execution.composite.parallel.ChildOperationExecutor",
            mock_parallel_child_executor,
        ),
    ):
        mock_step_executor.return_value = step_executor
        mock_callback_executor.return_value = callback_executor
        mock_invoke_executor.return_value = invoke_executor
        mock_wait_for_condition_executor.return_value = wait_for_condition_executor
        mock_wait_executor.return_value.process = mock_wait

        assert (
            await run_with_context(context, step(step_func, name="step-name"))
            == "step-result"
        )
        await run_with_context(context, wait(timedelta(seconds=1), name="wait-name"))
        callback_result = await run_with_context(
            context, create_callback(name="callback-name")
        )
        assert callback_result.callback_id == "callback-id"
        assert callback_result.operation_id is not None
        assert (
            await run_with_context(context, invoke("fn", {"x": 1}, name="invoke-name"))
            == "invoke-result"
        )
        assert (
            await run_with_context(
                context, run_in_child_context(child_func, name="child-name")
            )
            == "child-result"
        )
        assert (
            await run_with_context(
                context, map_operation(map_func, [1, 2], name="map-name")
            )
            == "map-result"
        )
        assert (
            await run_with_context(
                context, parallel([parallel_func], name="parallel-name")
            )
            == "parallel-result"
        )
        assert (
            await run_with_context(
                context, wait_for_callback(submitter, name="wait-callback-name")
            )
            == "callback-wait-result"
        )
        assert (
            await run_with_context(
                context,
                wait_for_condition(
                    check,
                    initial_state="pending",
                    name="condition-name",
                    wait_strategy=config.wait_strategy,
                ),
            )
            == "condition-result"
        )

    mock_step_executor.assert_called_once()
    step_executor.process.assert_awaited_once()
    mock_wait_executor.assert_called_once_with(
        seconds=1,
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
    mock_invoke_executor.assert_called_once_with(
        function_name="fn",
        payload={"x": 1},
        state=mock_state,
        operation_identifier=ANY,
        serdes_payload=None,
        serdes_result=None,
        tenant_id=None,
    )
    invoke_executor.process.assert_awaited_once()
    mock_callback_child.assert_awaited_once()
    assert mock_callback_child.await_args.kwargs["name"] == "wait-callback-name"
    assert mock_map_child_executor.call_count == 1
    assert mock_parallel_child_executor.call_count == 1
    mock_wait_for_condition_executor.assert_called_once_with(
        check=check,
        config=ANY,
        initial_state="pending",
        state=mock_state,
        operation_identifier=ANY,
    )
    created_config = mock_wait_for_condition_executor.call_args.kwargs["config"]
    assert created_config.wait_strategy is config.wait_strategy
    wait_for_condition_executor.process.assert_awaited_once()


async def test_durable_callable_can_be_passed_to_step():
    @durable_callable
    async def greet(name: str) -> str:
        return f"hello {name}"

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    context = create_test_context(state=mock_state)

    def build_executor(*, func, state, operation_identifier, **_kwargs):
        executor = AsyncMock()

        async def process():
            return f"{operation_identifier.name}:{await func()}"

        executor.process.side_effect = process
        return executor

    with patch(
        "async_durable_execution.primitive.step.StepOperationExecutor",
        side_effect=build_executor,
    ) as mock_executor_class:
        assert (
            await run_with_context(context, step(greet("Ada"), name="greet"))
            == "greet:hello Ada"
        )

    mock_executor_class.assert_called_once_with(
        func=ANY,
        state=mock_state,
        operation_identifier=ANY,
        retry_strategy=None,
        step_semantics=StepSemantics.AT_LEAST_ONCE_PER_RETRY,
        serdes=None,
    )


async def test_durable_callable_returns_bound_callable_inside_step_context():
    calls: list[str] = []

    @durable_callable
    async def record(value: str) -> str:
        calls.append(value)
        return value.upper()

    assert await record("inside-step")() == "INSIDE-STEP"
    assert calls == ["inside-step"]


async def test_durable_callable_returns_bound_callable_without_context():
    calls: list[int] = []

    @durable_callable
    async def increment(value: int) -> int:
        calls.append(value)
        return value + 1

    bound_increment = increment(2)
    assert await bound_increment() == 3
    assert calls == [2]


async def test_durable_callable_can_be_passed_to_run_in_child_context():
    @durable_callable
    async def greet(name: str) -> str:
        return f"hello {name}"

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    context = create_test_context(state=mock_state)

    with patch(
        "async_durable_execution.primitive.child.ChildOperationExecutor",
        new=MagicMock(return_value=make_async_executor("child:hello Ada")),
    ) as mock_child_executor:
        assert (
            await run_with_context(
                context,
                run_in_child_context(greet("Ada"), name="greet-child"),
            )
            == "child:hello Ada"
        )

    mock_child_executor.assert_called_once()
    assert mock_child_executor.call_args.args[1] is mock_state
    assert mock_child_executor.call_args.args[2].name == "greet-child"
    assert mock_child_executor.call_args.kwargs["serdes"] is None
    assert mock_child_executor.call_args.kwargs["summary_generator"] is None
    assert not mock_child_executor.call_args.kwargs["is_virtual"]
    assert await mock_child_executor.call_args.args[0]() == "hello Ada"


async def test_module_level_context_functions_raise_in_step_context():
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "arn:test"
    step_context = StepContext(
        attempt=1,
        execution_state=mock_state,
        operation_identifier=OperationIdentifier(
            operation_id="step-op",
            sub_type=OperationSubType.STEP,
        ),
    )

    async def noop() -> None:
        return None

    token = set_current_context(step_context)
    try:
        with pytest.raises(
            RuntimeError,
            match="Durable operations can only be used while a durable function or child context is executing\\.",
        ):
            await step(noop)
    finally:
        reset_current_context(token)


async def test_callback_init():
    """Test Callback initialization."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    callback = Callback("callback123", "op456", mock_state)

    assert callback.callback_id == "callback123"
    assert callback.operation_id == "op456"
    assert callback.state is mock_state


async def test_callback_result_succeeded():
    """Test Callback.result() when operation succeeded."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.SUCCEEDED,
        callback_details=CallbackDetails(
            callback_id="callback1", result=json.dumps("success_result")
        ),
    )
    mock_state.operations.get.return_value = operation

    callback = Callback("callback1", "op1", mock_state)
    result = await run_async(callback.result())

    assert result == '"success_result"'
    mock_state.operations.get.assert_called_once_with("op1")


async def test_callback_result_succeeded_with_plain_str():
    """Test Callback.result() when operation succeeded."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.SUCCEEDED,
        callback_details=CallbackDetails(
            callback_id="callback1", result="success_result"
        ),
    )
    mock_state.operations.get.return_value = operation

    callback = Callback("callback1", "op1", mock_state)
    result = await run_async(callback.result())

    assert result == "success_result"
    mock_state.operations.get.assert_called_once_with("op1")


async def test_callback_result_succeeded_none():
    """Test Callback.result() when operation succeeded with None result."""
    mock_state = Mock(spec=ExecutionState)
    operation = Operation(
        operation_id="op2",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.SUCCEEDED,
        callback_details=CallbackDetails(callback_id="callback2", result=None),
    )
    mock_state.operations.get.return_value = operation

    callback = Callback("callback2", "op2", mock_state)
    result = await run_async(callback.result())

    assert result is None


async def test_callback_result_started_no_timeout():
    """Test Callback.result() when operation started without timeout."""
    mock_state = Mock(spec=ExecutionState)
    operation = Operation(
        operation_id="op3",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=CallbackDetails(callback_id="callback3"),
    )
    mock_state.operations.get.return_value = operation

    callback = Callback("callback3", "op3", mock_state)

    with pytest.raises(SuspendExecution, match="Callback result not received yet"):
        await run_async(callback.result())


async def test_callback_result_started_with_timeout():
    """Test Callback.result() when operation started with timeout."""
    mock_state = Mock(spec=ExecutionState)
    operation = Operation(
        operation_id="op4",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.STARTED,
        callback_details=CallbackDetails(callback_id="callback4"),
    )
    mock_state.operations.get.return_value = operation

    callback = Callback("callback4", "op4", mock_state)

    with pytest.raises(SuspendExecution, match="Callback result not received yet"):
        await run_async(callback.result())


async def test_callback_result_failed():
    """Test Callback.result() when operation failed."""
    mock_state = Mock(spec=ExecutionState)
    error = ErrorObject(
        message="Callback failed", type="CallbackError", data=None, stack_trace=None
    )
    operation = Operation(
        operation_id="op5",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.FAILED,
        callback_details=CallbackDetails(callback_id="callback5", error=error),
    )
    mock_state.operations.get.return_value = operation

    callback = Callback("callback5", "op5", mock_state)

    with pytest.raises(CallbackError):
        await run_async(callback.result())


async def test_callback_result_not_started():
    """Test Callback.result() when operation not started."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.operations.get.return_value = None

    callback = Callback("callback6", "op6", mock_state)

    with pytest.raises(CallbackError, match="Callback operation must exist"):
        await run_async(callback.result())


async def test_callback_custom_serdes_result_succeeded():
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.SUCCEEDED,
        callback_details=CallbackDetails(
            callback_id="callback1",
            result='{"key": "VALUE", "number": "84", "list": [1, 2, 3]}',
        ),
    )
    mock_state.operations.get.return_value = operation

    callback = Callback("callback1", "op1", mock_state, CustomDictSerDes())
    result = await run_async(callback.result())

    expected_complex_result = {"key": "value", "number": 42, "list": [1, 2, 3]}

    assert result == expected_complex_result


async def test_callback_result_timed_out():
    """Test Callback.result() when operation timed out."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    error = ErrorObject(
        message="Callback timed out", type="TimeoutError", data=None, stack_trace=None
    )
    operation = Operation(
        operation_id="op_timeout",
        operation_type=OperationType.CALLBACK,
        status=OperationStatus.TIMED_OUT,
        callback_details=CallbackDetails(callback_id="callback_timeout", error=error),
    )
    mock_state.operations.get.return_value = operation

    callback = Callback("callback_timeout", "op_timeout", mock_state)

    with pytest.raises(CallbackError):
        await run_async(callback.result())


@patch("async_durable_execution.primitive.callback.CallbackOperationExecutor")
async def test_create_callback_basic(mock_executor_class):
    """Test create_callback with basic parameters."""
    mock_executor = make_async_executor("callback123")
    mock_executor_class.return_value = mock_executor

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    context = create_test_context(state=mock_state)
    operation_ids = operation_id_sequence()
    expected_operation_id = next(operation_ids)

    callback = await run_with_context(context, create_callback())

    assert isinstance(callback, Callback)
    assert callback.callback_id == "callback123"
    assert callback.operation_id == expected_operation_id
    assert callback.state is mock_state

    mock_executor_class.assert_called_once_with(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            expected_operation_id, OperationSubType.CALLBACK, None, None
        ),
        timeout=None,
        heartbeat_timeout=None,
    )
    mock_executor.process.assert_called_once()


@patch("async_durable_execution.primitive.callback.CallbackOperationExecutor")
async def test_create_callback_with_name_and_config(mock_executor_class):
    """Test create_callback with name and configuration fields."""
    mock_executor = make_async_executor("callback456")
    mock_executor_class.return_value = mock_executor

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    timeout = timedelta(seconds=30)
    heartbeat_timeout = timedelta(seconds=10)

    context = create_test_context(state=mock_state)
    operation_ids = operation_id_sequence()
    [next(operation_ids) for _ in range(5)]  # Skip 5 IDs
    expected_operation_id = next(operation_ids)  # Get the 6th ID
    [
        context.step_counter.create_step_id() for _ in range(5)
    ]  # Set counter to 5 # noqa: SLF001

    callback = await run_with_context(
        context,
        create_callback(
            timeout=timeout,
            heartbeat_timeout=heartbeat_timeout,
        ),
    )

    assert callback.callback_id == "callback456"
    assert callback.operation_id == expected_operation_id

    mock_executor_class.assert_called_once_with(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            expected_operation_id, OperationSubType.CALLBACK, None, None
        ),
        timeout=timeout,
        heartbeat_timeout=heartbeat_timeout,
    )
    mock_executor.process.assert_called_once()


@patch("async_durable_execution.primitive.callback.CallbackOperationExecutor")
async def test_create_callback_with_parent_id(mock_executor_class):
    """Test create_callback with parent_id."""

    mock_executor = make_async_executor("callback789")

    mock_executor_class.return_value = mock_executor
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    context = create_test_context(state=mock_state, parent_id="parent123")
    operation_ids = operation_id_sequence("parent123")
    [next(operation_ids) for _ in range(2)]  # Skip 2 IDs
    expected_operation_id = next(operation_ids)  # Get the 3rd ID
    [
        context.step_counter.create_step_id() for _ in range(2)
    ]  # Set counter to 2 # noqa: SLF001

    callback = await run_with_context(context, create_callback())

    assert callback.operation_id == expected_operation_id

    mock_executor_class.assert_called_once_with(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            expected_operation_id, OperationSubType.CALLBACK, "parent123"
        ),
        timeout=None,
        heartbeat_timeout=None,
    )


@patch("async_durable_execution.primitive.callback.CallbackOperationExecutor")
async def test_create_callback_increments_counter(mock_executor_class):
    """Test create_callback increments step counter."""
    mock_executor = make_async_executor("callback_test")

    mock_executor_class.return_value = mock_executor
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    context = create_test_context(state=mock_state)
    [
        context.step_counter.create_step_id() for _ in range(10)
    ]  # Set counter to 10 # noqa: SLF001

    callback1 = await run_with_context(context, create_callback())
    callback2 = await run_with_context(context, create_callback())

    # Use operation_id_sequence to get expected IDs
    seq = operation_id_sequence()
    [next(seq) for _ in range(10)]  # Skip first 10
    expected_id1 = next(seq)  # 11th
    expected_id2 = next(seq)  # 12th

    assert callback1.operation_id == expected_id1
    assert callback2.operation_id == expected_id2
    assert context.step_counter.get_current() == 12  # noqa: SLF001


@patch("async_durable_execution.primitive.step.StepOperationExecutor")
async def test_step_basic(mock_executor_class):
    """Test step with basic parameters."""
    mock_executor = make_async_executor("step_result")

    mock_executor_class.return_value = mock_executor
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    async def mock_callable() -> str:
        return "test_result"

    context = create_test_context(state=mock_state)
    operation_ids = operation_id_sequence()
    expected_operation_id = next(operation_ids)

    result = await run_with_context(context, step(mock_callable))

    assert result == "step_result"
    mock_executor_class.assert_called_once_with(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            expected_operation_id, OperationSubType.STEP, None, "mock_callable"
        ),
        func=mock_callable,
        retry_strategy=None,
        step_semantics=StepSemantics.AT_LEAST_ONCE_PER_RETRY,
        serdes=None,
    )
    mock_executor.process.assert_called_once()


@patch("async_durable_execution.primitive.step.StepOperationExecutor")
async def test_step_with_name_and_config_fields(mock_executor_class):
    """Test step with name and direct config fields."""
    mock_executor = make_async_executor("configured_result")

    mock_executor_class.return_value = mock_executor
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    async def mock_callable() -> None:
        return None

    retry_strategy = Mock()

    context = create_test_context(state=mock_state)
    [
        context.step_counter.create_step_id() for _ in range(5)
    ]  # Set counter to 5 # noqa: SLF001

    result = await run_with_context(
        context,
        step(
            mock_callable,
            name="mock-callable",
            retry_strategy=retry_strategy,
            step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY,
        ),
    )

    # Get expected ID
    seq = operation_id_sequence()
    [next(seq) for _ in range(5)]  # Skip first 5
    expected_id = next(seq)  # 6th

    assert result == "configured_result"
    mock_executor_class.assert_called_once_with(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            expected_id, OperationSubType.STEP, None, "mock-callable"
        ),
        func=mock_callable,
        retry_strategy=retry_strategy,
        step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY,
        serdes=None,
    )
    mock_executor.process.assert_called_once()


@patch("async_durable_execution.primitive.step.StepOperationExecutor")
async def test_step_with_parent_id(mock_executor_class):
    """Test step with parent_id."""
    mock_executor = make_async_executor("parent_result")

    mock_executor_class.return_value = mock_executor
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    async def mock_callable() -> None:
        return None

    context = create_test_context(state=mock_state, parent_id="parent123")
    [
        context.step_counter.create_step_id() for _ in range(2)
    ]  # Set counter to 2 # noqa: SLF001

    await run_with_context(context, step(mock_callable))

    # Get expected ID with parent
    seq = operation_id_sequence("parent123")
    [next(seq) for _ in range(2)]  # Skip first 2
    expected_id = next(seq)  # 3rd

    mock_executor_class.assert_called_once_with(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            expected_id, OperationSubType.STEP, "parent123", "mock_callable"
        ),
        func=mock_callable,
        retry_strategy=None,
        step_semantics=StepSemantics.AT_LEAST_ONCE_PER_RETRY,
        serdes=None,
    )
    mock_executor.process.assert_called_once()


@patch("async_durable_execution.primitive.step.StepOperationExecutor")
async def test_step_increments_counter(mock_executor_class):
    """Test step increments step counter."""
    mock_executor = make_async_executor("result")

    mock_executor_class.return_value = mock_executor
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    async def mock_callable() -> None:
        return None

    context = create_test_context(state=mock_state)
    [
        context.step_counter.create_step_id() for _ in range(10)
    ]  # Set counter to 10 # noqa: SLF001

    await run_with_context(context, step(mock_callable))
    await run_with_context(context, step(mock_callable))

    # Get expected IDs
    seq = operation_id_sequence()
    [next(seq) for _ in range(10)]  # Skip first 10
    expected_id1 = next(seq)  # 11th
    expected_id2 = next(seq)  # 12th

    assert context.step_counter.get_current() == 12  # noqa: SLF001
    assert mock_executor_class.call_args_list[0][1][
        "operation_identifier"
    ] == OperationIdentifier(expected_id1, OperationSubType.STEP, None, "mock_callable")
    assert mock_executor_class.call_args_list[1][1][
        "operation_identifier"
    ] == OperationIdentifier(expected_id2, OperationSubType.STEP, None, "mock_callable")


@patch("async_durable_execution.primitive.step.StepOperationExecutor")
async def test_step_with_callable_has_no_default_name(
    mock_executor_class,
):
    """Test step does not derive its name from the callable function."""
    mock_executor = make_async_executor("named_result")

    mock_executor_class.return_value = mock_executor
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    async def original_function(value: str) -> str:
        return value

    context = create_test_context(state=mock_state)

    mock_callable = partial(original_function, "value")

    await run_with_context(context, step(mock_callable))

    # Get expected ID
    seq = operation_id_sequence()
    expected_id = next(seq)  # 1st

    mock_executor_class.assert_called_once_with(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            expected_id, OperationSubType.STEP, None, None
        ),
        func=mock_callable,
        retry_strategy=None,
        step_semantics=StepSemantics.AT_LEAST_ONCE_PER_RETRY,
        serdes=None,
    )
    mock_executor.process.assert_called_once()


@patch("async_durable_execution.primitive.invoke.InvokeOperationExecutor")
async def test_invoke_basic(mock_executor_class):
    """Test invoke with basic parameters."""
    mock_executor = make_async_executor("invoke_result")

    mock_executor_class.return_value = mock_executor
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    context = create_test_context(state=mock_state)
    operation_ids = operation_id_sequence()
    expected_operation_id = next(operation_ids)

    result = await run_with_context(context, invoke("test_function", "test_payload"))

    assert result == "invoke_result"

    mock_executor_class.assert_called_once_with(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            expected_operation_id, OperationSubType.CHAINED_INVOKE, None, None
        ),
        function_name="test_function",
        payload="test_payload",
        serdes_payload=None,
        serdes_result=None,
        tenant_id=None,
    )
    mock_executor.process.assert_called_once()


@patch("async_durable_execution.primitive.invoke.InvokeOperationExecutor")
async def test_invoke_with_name_and_fields(mock_executor_class):
    """Test invoke with name and default fields."""
    mock_executor = make_async_executor("configured_result")
    mock_executor_class.return_value = mock_executor
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    context = create_test_context(state=mock_state)
    [
        context.step_counter.create_step_id() for _ in range(5)
    ]  # Set counter to 5 # noqa: SLF001

    result = await run_with_context(
        context,
        invoke("test_function", {"key": "value"}, name="named_invoke"),
    )

    # Get expected ID
    seq = operation_id_sequence()
    [next(seq) for _ in range(5)]  # Skip first 5
    expected_id = next(seq)  # 6th

    assert result == "configured_result"
    mock_executor_class.assert_called_once_with(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            expected_id, OperationSubType.CHAINED_INVOKE, None, "named_invoke"
        ),
        function_name="test_function",
        payload={"key": "value"},
        serdes_payload=None,
        serdes_result=None,
        tenant_id=None,
    )
    mock_executor.process.assert_called_once()


@patch("async_durable_execution.primitive.invoke.InvokeOperationExecutor")
async def test_invoke_with_parent_id(mock_executor_class):
    """Test invoke with parent_id."""
    mock_executor = make_async_executor("parent_result")
    mock_executor_class.return_value = mock_executor
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    context = create_test_context(state=mock_state, parent_id="parent123")
    [
        context.step_counter.create_step_id() for _ in range(2)
    ]  # Set counter to 2 # noqa: SLF001

    await run_with_context(context, invoke("test_function", None))

    seq = operation_id_sequence("parent123")
    [next(seq) for _ in range(2)]
    expected_id = next(seq)

    mock_executor_class.assert_called_once_with(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            expected_id, OperationSubType.CHAINED_INVOKE, "parent123", None
        ),
        function_name="test_function",
        payload=None,
        serdes_payload=None,
        serdes_result=None,
        tenant_id=None,
    )
    mock_executor.process.assert_called_once()


@patch("async_durable_execution.primitive.invoke.InvokeOperationExecutor")
async def test_invoke_increments_counter(mock_executor_class):
    """Test invoke increments step counter."""
    mock_executor = make_async_executor("result")
    mock_executor_class.return_value = mock_executor
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    context = create_test_context(state=mock_state)
    [
        context.step_counter.create_step_id() for _ in range(10)
    ]  # Set counter to 10 # noqa: SLF001

    await run_with_context(context, invoke("function1", "payload1"))
    await run_with_context(context, invoke("function2", "payload2"))

    seq = operation_id_sequence()
    [next(seq) for _ in range(10)]
    expected_id1 = next(seq)
    expected_id2 = next(seq)

    assert context.step_counter.get_current() == 12  # noqa: SLF001
    assert mock_executor_class.call_args_list[0][1][
        "operation_identifier"
    ] == OperationIdentifier(expected_id1, OperationSubType.CHAINED_INVOKE, None, None)
    assert mock_executor_class.call_args_list[1][1][
        "operation_identifier"
    ] == OperationIdentifier(expected_id2, OperationSubType.CHAINED_INVOKE, None, None)


@patch("async_durable_execution.primitive.invoke.InvokeOperationExecutor")
async def test_invoke_with_none_payload(mock_executor_class):
    """Test invoke with None payload."""
    mock_executor = make_async_executor(None)
    mock_executor_class.return_value = mock_executor
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    context = create_test_context(state=mock_state)

    result = await run_with_context(context, invoke("test_function", None))

    seq = operation_id_sequence()
    expected_id = next(seq)

    assert result is None

    mock_executor_class.assert_called_once_with(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            expected_id, OperationSubType.CHAINED_INVOKE, None, None
        ),
        function_name="test_function",
        payload=None,
        serdes_payload=None,
        serdes_result=None,
        tenant_id=None,
    )
    mock_executor.process.assert_called_once()


@patch("async_durable_execution.primitive.invoke.InvokeOperationExecutor")
async def test_invoke_with_custom_serdes(mock_executor_class):
    """Test invoke with custom serialization fields."""
    mock_executor = make_async_executor({"transformed": "data"})
    mock_executor_class.return_value = mock_executor
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    payload_serdes = CustomDictSerDes()
    result_serdes = CustomDictSerDes()
    context = create_test_context(state=mock_state)

    result = await run_with_context(
        context,
        invoke(
            "test_function",
            {"original": "data"},
            name="custom_serdes_invoke",
            serdes_payload=payload_serdes,
            serdes_result=result_serdes,
        ),
    )

    seq = operation_id_sequence()
    expected_id = next(seq)

    assert result == {"transformed": "data"}
    mock_executor_class.assert_called_once_with(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            expected_id, OperationSubType.CHAINED_INVOKE, None, "custom_serdes_invoke"
        ),
        function_name="test_function",
        payload={"original": "data"},
        serdes_payload=payload_serdes,
        serdes_result=result_serdes,
        tenant_id=None,
    )
    mock_executor.process.assert_called_once()


@patch("async_durable_execution.primitive.wait.WaitOperationExecutor")
async def test_wait_basic(mock_executor_class):
    """Test wait with basic parameters."""
    mock_executor = make_async_executor(None)
    mock_executor_class.return_value = mock_executor

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    context = create_test_context(state=mock_state)
    operation_ids = operation_id_sequence()
    expected_operation_id = next(operation_ids)

    await run_with_context(context, wait(timedelta(seconds=30)))

    mock_executor_class.assert_called_once_with(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            expected_operation_id, OperationSubType.WAIT, None, None
        ),
        seconds=30,
    )
    mock_executor.process.assert_called_once()


@patch("async_durable_execution.primitive.wait.WaitOperationExecutor")
async def test_wait_with_name(mock_executor_class):
    """Test wait with name parameter."""
    mock_executor = make_async_executor(None)
    mock_executor_class.return_value = mock_executor

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    context = create_test_context(state=mock_state)
    [
        context.step_counter.create_step_id() for _ in range(5)
    ]  # Set counter to 5 # noqa: SLF001

    await run_with_context(context, wait(timedelta(minutes=1), name="test_wait"))

    seq = operation_id_sequence()
    [next(seq) for _ in range(5)]
    expected_id = next(seq)

    mock_executor_class.assert_called_once_with(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            expected_id, OperationSubType.WAIT, None, "test_wait"
        ),
        seconds=60,
    )
    mock_executor.process.assert_called_once()


@patch("async_durable_execution.primitive.wait.WaitOperationExecutor")
async def test_wait_with_parent_id(mock_executor_class):
    """Test wait with parent_id."""
    mock_executor = make_async_executor(None)
    mock_executor_class.return_value = mock_executor

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    context = create_test_context(state=mock_state, parent_id="parent123")
    [
        context.step_counter.create_step_id() for _ in range(2)
    ]  # Set counter to 2 # noqa: SLF001

    await run_with_context(context, wait(timedelta(seconds=45)))

    seq = operation_id_sequence("parent123")
    [next(seq) for _ in range(2)]
    expected_id = next(seq)

    mock_executor_class.assert_called_once_with(
        state=mock_state,
        operation_identifier=OperationIdentifier(
            expected_id, OperationSubType.WAIT, "parent123"
        ),
        seconds=45,
    )
    mock_executor.process.assert_called_once()


@patch("async_durable_execution.primitive.wait.WaitOperationExecutor")
async def test_wait_increments_counter(mock_executor_class):
    """Test wait increments step counter."""
    mock_executor = make_async_executor(None)
    mock_executor_class.return_value = mock_executor

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    context = create_test_context(state=mock_state)
    [
        context.step_counter.create_step_id() for _ in range(10)
    ]  # Set counter to 10 # noqa: SLF001

    await run_with_context(context, wait(timedelta(seconds=15)))
    await run_with_context(context, wait(timedelta(seconds=25)))

    seq = operation_id_sequence()
    [next(seq) for _ in range(10)]
    expected_id1 = next(seq)
    expected_id2 = next(seq)

    assert context.step_counter.get_current() == 12  # noqa: SLF001
    assert mock_executor_class.call_args_list[0][1][
        "operation_identifier"
    ] == OperationIdentifier(expected_id1, OperationSubType.WAIT, None, None)
    assert mock_executor_class.call_args_list[1][1][
        "operation_identifier"
    ] == OperationIdentifier(expected_id2, OperationSubType.WAIT, None, None)


@patch("async_durable_execution.primitive.wait.WaitOperationExecutor")
async def test_wait_returns_none(mock_executor_class):
    """Test wait returns None."""
    mock_executor = make_async_executor(None)
    mock_executor_class.return_value = mock_executor

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    context = create_test_context(state=mock_state)

    result = await run_with_context(context, wait(timedelta(seconds=10)))

    assert result is None


@patch("async_durable_execution.primitive.wait.WaitOperationExecutor")
async def test_wait_with_time_less_than_one(mock_executor_class):
    """Test wait with time less than one."""
    mock_executor = make_async_executor(None)
    mock_executor_class.return_value = mock_executor

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    context = create_test_context(state=mock_state)

    with pytest.raises(ValidationError):
        await run_with_context(context, wait(timedelta(seconds=0)))


@patch("async_durable_execution.primitive.child.ChildOperationExecutor")
async def test_run_in_child_context_basic(mock_handler):
    """Test run_in_child_context with basic parameters."""
    mock_handler.return_value = make_async_executor("child_result")
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_callable = AsyncMock(return_value="test_result")
    del (
        mock_callable._original_name  # noqa: SLF001
    )  # Ensure _original_name doesn't exist

    context = create_test_context(state=mock_state)
    operation_ids = operation_id_sequence()
    expected_operation_id = next(operation_ids)

    result = await run_with_context(context, run_in_child_context(mock_callable))

    assert result == "child_result"
    assert mock_handler.call_count == 1

    # Verify the callable was wrapped with child context
    call_args = mock_handler.call_args
    assert call_args.args[1] is mock_state
    assert call_args.args[2] == OperationIdentifier(
        expected_operation_id,
        OperationSubType.RUN_IN_CHILD_CONTEXT,
        None,
        "AsyncMock",
    )
    assert call_args.kwargs["serdes"] is None
    assert call_args.kwargs["summary_generator"] is None
    assert not call_args.kwargs["is_virtual"]


@patch("async_durable_execution.primitive.child.ChildOperationExecutor")
async def test_run_in_child_context_with_name_and_config(mock_handler):
    """Test run_in_child_context with name and configuration fields."""
    mock_handler.return_value = make_async_executor("configured_child_result")
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_callable = AsyncMock()

    summary_generator = Mock()

    context = create_test_context(state=mock_state)
    [
        context.step_counter.create_step_id() for _ in range(3)
    ]  # Set counter to 3 # noqa: SLF001

    result = await run_with_context(
        context,
        run_in_child_context(
            mock_callable,
            name="original_function",
            summary_generator=summary_generator,
            is_virtual=True,
        ),
    )

    seq = operation_id_sequence()
    [next(seq) for _ in range(3)]
    expected_id = next(seq)

    assert result == "configured_child_result"
    call_args = mock_handler.call_args
    assert call_args.args[2] == OperationIdentifier(
        expected_id, OperationSubType.RUN_IN_CHILD_CONTEXT, None, "original_function"
    )
    assert call_args.kwargs["serdes"] is None
    assert call_args.kwargs["summary_generator"] is summary_generator
    assert call_args.kwargs["is_virtual"]


@patch("async_durable_execution.primitive.child.ChildOperationExecutor")
async def test_run_in_child_context_with_parent_id(mock_executor_class):
    """Test run_in_child_context with parent_id."""
    mock_executor = make_async_executor("parent_child_result")
    mock_executor_class.return_value = mock_executor
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_callable = AsyncMock()
    del (
        mock_callable._original_name  # noqa: SLF001
    )  # Ensure Mock doesn't have _original_name

    context = create_test_context(state=mock_state, parent_id="parent456")
    [
        context.step_counter.create_step_id() for _ in range(1)
    ]  # Set counter to 1 # noqa: SLF001

    await run_with_context(context, run_in_child_context(mock_callable))

    seq = operation_id_sequence("parent456")
    [next(seq) for _ in range(1)]
    expected_id = next(seq)

    call_args = mock_executor_class.call_args
    assert call_args.args[2] == OperationIdentifier(
        expected_id,
        OperationSubType.RUN_IN_CHILD_CONTEXT,
        "parent456",
        "AsyncMock",
    )


@patch("async_durable_execution.primitive.child.ChildOperationExecutor")
async def test_run_in_child_context_creates_child_context(mock_executor_class):
    """Test run_in_child_context creates proper child context."""
    mock_state = create_async_child_state()

    seq = operation_id_sequence()
    expected_parent_id = next(seq)

    async def capture_child_context():
        from async_durable_execution.primitive import child as child_module

        child_context = get_current_context()
        # Verify child context properties
        assert isinstance(child_context, child_module.DurableContext)
        assert child_context.execution_state is mock_state
        assert child_context.parent_id == expected_parent_id  # noqa: SLF001
        return "child_executed"

    mock_callable = AsyncMock(side_effect=capture_child_context)

    def execute_child_handler(func, *_args, **_kwargs):
        executor = MagicMock()
        executor.process = AsyncMock(side_effect=func)
        return executor

    mock_executor_class.side_effect = execute_child_handler

    context = create_test_context(state=mock_state)

    result = await run_with_context(context, run_in_child_context(mock_callable))

    assert result == "child_executed"
    mock_callable.assert_called_once()


@patch("async_durable_execution.primitive.child.ChildOperationExecutor")
async def test_run_in_child_context_increments_counter(mock_executor_class):
    """Test run_in_child_context increments step counter."""
    mock_executor = make_async_executor("result")
    mock_executor_class.return_value = mock_executor
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_callable = AsyncMock()
    del (
        mock_callable._original_name  # noqa: SLF001
    )  # Ensure _original_name doesn't exist

    context = create_test_context(state=mock_state)
    [
        context.step_counter.create_step_id() for _ in range(5)
    ]  # Set counter to 5 # noqa: SLF001

    await run_with_context(context, run_in_child_context(mock_callable))
    await run_with_context(context, run_in_child_context(mock_callable))

    seq = operation_id_sequence()
    [next(seq) for _ in range(5)]
    expected_id1 = next(seq)
    expected_id2 = next(seq)

    assert context.step_counter.get_current() == 7  # noqa: SLF001
    assert mock_executor_class.call_args_list[0].args[2] == OperationIdentifier(
        expected_id1, OperationSubType.RUN_IN_CHILD_CONTEXT, None, "AsyncMock"
    )
    assert mock_executor_class.call_args_list[1].args[2] == OperationIdentifier(
        expected_id2, OperationSubType.RUN_IN_CHILD_CONTEXT, None, "AsyncMock"
    )


@patch("async_durable_execution.primitive.child.ChildOperationExecutor")
async def test_run_in_child_context_uses_callable_name(mock_executor_class):
    """Test run_in_child_context uses func.__name__ when name is not provided."""
    mock_executor = make_async_executor("named_result")
    mock_executor_class.return_value = mock_executor
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_callable = AsyncMock()
    mock_callable._original_name = "original_function_name"  # noqa: SLF001

    context = create_test_context(state=mock_state)

    await run_with_context(context, run_in_child_context(mock_callable))

    call_args = mock_executor_class.call_args
    assert call_args.args[2].name == "AsyncMock"


@patch("async_durable_execution.composite.wait_for_callback.wait_for_callback_handler")
async def test_wait_for_callback_basic(mock_executor_class):
    """Test wait_for_callback with basic parameters."""
    mock_executor = make_async_executor("callback_result")
    mock_executor_class.return_value = mock_executor
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_submitter = AsyncMock()
    del (
        mock_submitter._original_name  # noqa: SLF001
    )  # Ensure _original_name doesn't exist

    with patch(
        "async_durable_execution.composite.wait_for_callback.run_in_child_context"
    ) as mock_run_in_child:
        mock_run_in_child.return_value = "callback_result"
        context = create_test_context(state=mock_state)

        result = await run_with_context(context, wait_for_callback(mock_submitter))

        assert result == "callback_result"
        mock_run_in_child.assert_called_once()

        # Verify the child context callable
        call_args = mock_run_in_child.call_args
        assert callable(call_args.args[0])
        assert call_args.kwargs["name"] == "AsyncMock"


@patch("async_durable_execution.composite.wait_for_callback.wait_for_callback_handler")
async def test_wait_for_callback_with_name_and_config(mock_executor_class):
    """Test wait_for_callback with name and configuration fields."""
    mock_executor = make_async_executor("configured_callback_result")
    mock_executor_class.return_value = mock_executor
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_submitter = AsyncMock()
    timeout = timedelta(seconds=30)
    heartbeat_timeout = timedelta(seconds=10)

    with patch(
        "async_durable_execution.composite.wait_for_callback.run_in_child_context"
    ) as mock_run_in_child:
        mock_run_in_child.return_value = "configured_callback_result"
        context = create_test_context(state=mock_state)

        result = await run_with_context(
            context,
            wait_for_callback(
                mock_submitter,
                name="submit_function",
                timeout=timeout,
                heartbeat_timeout=heartbeat_timeout,
            ),
        )

        assert result == "configured_callback_result"
        call_args = mock_run_in_child.call_args
        assert callable(call_args.args[0])
        assert call_args.kwargs["name"] == "submit_function"


@patch("async_durable_execution.composite.wait_for_callback.wait_for_callback_handler")
async def test_wait_for_callback_uses_submitter_name(mock_executor_class):
    """Test wait_for_callback uses submitter.__name__ when name is not provided."""
    mock_executor = make_async_executor("named_callback_result")
    mock_executor_class.return_value = mock_executor
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_submitter = AsyncMock()
    mock_submitter._original_name = "submit_task"  # noqa: SLF001

    with patch(
        "async_durable_execution.composite.wait_for_callback.run_in_child_context"
    ) as mock_run_in_child:
        mock_run_in_child.return_value = "named_callback_result"
        context = create_test_context(state=mock_state)

        await run_with_context(context, wait_for_callback(mock_submitter))

        call_args = mock_run_in_child.call_args
        assert callable(call_args.args[0])
        assert call_args.kwargs["name"] == "AsyncMock"


@patch("async_durable_execution.composite.wait_for_callback.wait_for_callback_handler")
async def test_wait_for_callback_passes_child_context(mock_executor_class):
    """Test wait_for_callback passes child context to handler."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    mock_submitter = AsyncMock()

    def capture_handler_call(
        submitter,
        name,
        *,
        timeout=None,
        heartbeat_timeout=None,
        serdes=None,
        retry_strategy=None,
    ):
        assert submitter is mock_submitter
        assert timeout is None
        assert heartbeat_timeout is None
        assert serdes is None
        assert retry_strategy is None

        async def bound_handler():
            return "handler_result"

        return bound_handler

    mock_executor_class.side_effect = capture_handler_call

    with patch(
        "async_durable_execution.composite.wait_for_callback.run_in_child_context"
    ) as mock_run_in_child:

        async def run_child_context(callable_func, *, name):
            # Execute the child context callable
            child_context = create_test_context(state=mock_state, parent_id="test")
            token = set_current_context(child_context)
            try:
                return await callable_func()
            finally:
                reset_current_context(token)

        mock_run_in_child.side_effect = run_child_context
        context = create_test_context(state=mock_state)

        result = await run_with_context(context, wait_for_callback(mock_submitter))

        assert result == "handler_result"
        mock_executor_class.assert_called_once()


@patch("async_durable_execution.composite.map.ChildOperationExecutor")
async def test_map_basic(mock_handler):
    """Test map with basic parameters."""
    mock_handler.return_value = make_async_executor("map_result")
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    async def test_function(item):
        return f"processed_{item}"

    items = [1, 2, 3]

    context = create_test_context(state=mock_state)

    result = await run_with_context(context, map_operation(test_function, items))

    assert result == "map_result"
    mock_handler.assert_called_once()

    # Verify the child handler was called with correct parameters
    call_args = mock_handler.call_args
    assert call_args.args[2].sub_type is OperationSubType.MAP
    assert call_args.args[2].name == "test_function"


@patch("async_durable_execution.composite.map.ChildOperationExecutor")
async def test_map_with_name_and_config(mock_handler):
    """Test map with name and configuration fields."""
    mock_handler.return_value = make_async_executor("configured_map_result")
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    async def test_function(item):
        return f"processed_{item}"

    test_function._original_name = "test_map_function"  # noqa: SLF001

    items = ["a", "b", "c"]
    context = create_test_context(state=mock_state)

    result = await run_with_context(
        context,
        map_operation(test_function, items, name="custom_map", max_concurrency=2),
    )

    assert result == "configured_map_result"
    call_args = mock_handler.call_args
    assert call_args.args[2].name == "custom_map"  # name should be custom_map


@patch("async_durable_execution.composite.map.ChildOperationExecutor")
async def test_map_calls_handler_correctly(mock_handler):
    """Test map calls map_handler with correct parameters."""
    mock_handler.return_value = make_async_executor("handler_result")
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    async def test_function(item):
        return item.upper()

    items = ["hello", "world"]

    context = create_test_context(state=mock_state)

    result = await run_with_context(context, map_operation(test_function, items))

    assert result == "handler_result"
    mock_handler.assert_called_once()


@patch("async_durable_execution.composite.map.ChildOperationExecutor")
async def test_map_with_empty_items(mock_handler):
    """Test map with empty items."""
    mock_handler.return_value = make_async_executor("empty_map_result")
    mock_state = create_async_child_state()

    async def test_function(item):
        return item

    items = []

    context = create_test_context(state=mock_state)
    result = await run_with_context(context, map_operation(test_function, items))
    assert result == "empty_map_result"


@patch("async_durable_execution.composite.map.ChildOperationExecutor")
async def test_map_with_different_input_types(mock_handler):
    """Test map with different item types."""
    mock_handler.return_value = make_async_executor("mixed_map_result")
    mock_state = create_async_child_state()

    async def test_function(item):
        return str(item)

    items = [1, "hello", {"key": "value"}, [1, 2, 3]]

    context = create_test_context(state=mock_state)
    result = await run_with_context(context, map_operation(test_function, items))
    assert result == "mixed_map_result"


@patch("async_durable_execution.composite.parallel.ChildOperationExecutor")
async def test_parallel_basic(mock_handler):
    """Test parallel with basic parameters."""
    mock_handler.return_value = make_async_executor("parallel_result")
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    async def task1(context):
        return "result1"

    async def task2(context):
        return "result2"

    callables = [task1, task2]

    context = create_test_context(state=mock_state)

    result = await run_with_context(context, parallel(callables))

    assert result == "parallel_result"
    mock_handler.assert_called_once()

    # Verify the child handler was called with correct parameters
    call_args = mock_handler.call_args
    assert call_args.args[2].sub_type is OperationSubType.PARALLEL


@patch("async_durable_execution.composite.parallel.ChildOperationExecutor")
async def test_parallel_with_name_and_config_fields(mock_handler):
    """Test parallel with name and direct config fields."""
    mock_handler.return_value = make_async_executor("configured_parallel_result")
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    async def task1(context):
        return "result1"

    async def task2(context):
        return "result2"

    callables = [task1, task2]
    serdes = Mock()

    context = create_test_context(state=mock_state)

    result = await run_with_context(
        context,
        parallel(
            callables,
            name="custom_parallel",
            max_concurrency=2,
            serdes=serdes,
        ),
    )

    assert result == "configured_parallel_result"
    call_args = mock_handler.call_args
    assert call_args.args[2].name == "custom_parallel"  # name should be custom_parallel
    assert call_args.kwargs["serdes"] is serdes


@patch("async_durable_execution.composite.parallel.ChildOperationExecutor")
async def test_parallel_has_no_default_name(mock_handler):
    """Test parallel has no name when no name is provided."""
    mock_handler.return_value = make_async_executor("unnamed_parallel_result")
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    async def task1(context):
        return "result1"

    async def task2(context):
        return "result2"

    callables = [task1, task2]

    context = create_test_context(state=mock_state)

    await run_with_context(context, parallel(callables))

    call_args = mock_handler.call_args
    assert call_args.args[2].name is None


@patch("async_durable_execution.composite.parallel.ChildOperationExecutor")
async def test_parallel_calls_handler_correctly(mock_handler):
    """Test parallel calls parallel_handler with correct parameters."""
    mock_handler.return_value = make_async_executor("handler_result")
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    async def task1(context):
        return "result1"

    async def task2(context):
        return "result2"

    callables = [task1, task2]

    context = create_test_context(state=mock_state)

    result = await run_with_context(context, parallel(callables))

    assert result == "handler_result"
    mock_handler.assert_called_once()


@patch("async_durable_execution.composite.parallel.parallel_handler")
async def test_parallel_with_empty_callables(mock_handler):
    """Test parallel with empty callables."""

    async def handler_result():
        return "empty_parallel_result"

    mock_handler.return_value = handler_result
    mock_state = create_async_child_state()

    callables = []

    context = create_test_context(state=mock_state)
    result = await run_with_context(context, parallel(callables))
    assert result == "empty_parallel_result"


@patch("async_durable_execution.composite.parallel.parallel_handler")
async def test_parallel_with_single_callable(mock_handler):
    """Test parallel with single callable."""

    async def handler_result():
        return "single_parallel_result"

    mock_handler.return_value = handler_result
    mock_state = create_async_child_state()

    async def single_task(context):
        return "single_result"

    callables = [single_task]

    context = create_test_context(state=mock_state)
    result = await run_with_context(context, parallel(callables))
    assert result == "single_parallel_result"


@patch("async_durable_execution.composite.parallel.parallel_handler")
async def test_parallel_with_many_callables(mock_handler):
    """Test parallel with many callables."""

    async def handler_result():
        return "many_parallel_result"

    mock_handler.return_value = handler_result
    mock_state = create_async_child_state()

    def create_task(i):
        async def task(context):
            return f"result_{i}"

        return task

    callables = [create_task(i) for i in range(10)]

    context = create_test_context(state=mock_state)
    result = await run_with_context(context, parallel(callables))
    assert result == "many_parallel_result"


@patch("async_durable_execution.composite.map.ChildOperationExecutor")
async def test_map_calls_handler(mock_handler):
    """Test map calls map_handler through run_in_child_context."""
    mock_handler.return_value = make_async_executor("map_result")
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    async def test_function(item):
        return f"processed_{item}"

    items = ["a", "b", "c"]
    context = create_test_context(state=mock_state)

    result = await run_with_context(context, map_operation(test_function, items))

    assert result == "map_result"
    mock_handler.assert_called_once()


@patch("async_durable_execution.composite.parallel.ChildOperationExecutor")
async def test_parallel_calls_handler(mock_handler):
    """Test parallel calls parallel_handler through run_in_child_context."""
    mock_handler.return_value = make_async_executor("parallel_result")
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    async def task1(context):
        return "result1"

    async def task2(context):
        return "result2"

    callables = [task1, task2]
    context = create_test_context(state=mock_state)

    result = await run_with_context(context, parallel(callables))

    assert result == "parallel_result"
    mock_handler.assert_called_once()


async def test_wait_for_condition_validation_errors():
    """Test wait_for_condition raises ValidationError for invalid inputs."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    context = create_test_context(state=mock_state)

    def dummy_wait_strategy(state, attempt):
        return None

    # Test None check function
    with pytest.raises(
        ValidationError, match="`check` is required for wait_for_condition"
    ):
        await run_with_context(
            context,
            wait_for_condition(None, wait_strategy=dummy_wait_strategy),
        )

    # None config is valid; check must return state and wait decision.
    async def dummy_check(state):
        return state, WaitForConditionDecision.stop_polling()

    with patch(
        "async_durable_execution.composite.wait_for_condition.WaitForConditionOperationExecutor"
    ) as mock_executor_class:
        mock_executor = make_async_executor("test")
        mock_executor_class.return_value = mock_executor

        result = await run_with_context(context, wait_for_condition(dummy_check))

    assert result == "test"


async def test_context_map_handler_call():
    """Test that map method calls through to map_handler (line 283)."""
    execution_calls = []

    async def test_function(item):
        execution_calls.append(f"item_{item}")
        return f"result_{item}"

    # Create mock state and context
    state = create_async_child_state()

    context = create_test_context(state=state)

    async def bound_map_handler():
        return "map_result"

    # Mock the handlers to track calls.
    with (
        patch("async_durable_execution.composite.map.map_handler") as mock_map_handler,
        patch(
            "async_durable_execution.composite.map.ChildOperationExecutor",
            new_callable=MagicMock,
        ) as mock_child_handler,
    ):
        mock_map_handler.return_value = bound_map_handler
        mock_child_handler.return_value = make_async_executor("map_result")

        result = await run_with_context(context, map_operation(test_function, [1, 2]))

        assert result == "map_result"
        mock_map_handler.assert_called_once()
        assert mock_child_handler.call_args.args[0] is bound_map_handler
        assert mock_map_handler.call_args.kwargs["summary_generator"] is None


async def test_context_parallel_handler_call():
    """Test that parallel method calls through to parallel_handler (line 306)."""
    execution_calls = []

    async def test_callable_1(context):
        execution_calls.append("callable_1")
        return "result_1"

    async def test_callable_2(context):
        execution_calls.append("callable_2")
        return "result_2"

    # Create mock state and context
    state = create_async_child_state()

    context = create_test_context(state=state)

    # Mock the handlers to track calls
    with patch(
        "async_durable_execution.composite.parallel.parallel_handler"
    ) as mock_parallel_handler:

        async def handler_result():
            return "parallel_result"

        mock_parallel_handler.return_value = handler_result

        await run_with_context(context, parallel([test_callable_1, test_callable_2]))
        mock_parallel_handler.assert_called_once()


async def test_context_wait_for_condition_handler_call():
    """Test that wait_for_condition method calls through to wait_for_condition_handler (line 425)."""
    execution_calls = []

    async def test_check(state):
        execution_calls.append("check_called")
        return state

    async def test_wait_strategy(state, attempt):
        return WaitForConditionDecision.stop_polling()

    # Create mock state and context
    state = Mock()
    state.durable_execution_arn = "test_arn"

    context = create_test_context(state=state)

    # Mock the executor to track calls
    with patch(
        "async_durable_execution.composite.wait_for_condition.WaitForConditionOperationExecutor"
    ) as mock_executor_class:
        mock_executor = make_async_executor("final_state")
        mock_executor_class.return_value = mock_executor

        # Call wait_for_condition method
        result = await run_with_context(
            context,
            wait_for_condition(test_check, wait_strategy=test_wait_strategy),
        )

        # Verify executor was called
        mock_executor_class.assert_called_once()
        mock_executor.process.assert_called_once()
        assert result == "final_state"


async def test_operation_id_conditional_on_parent():
    """
    - ensure that for all unique parents we produce unique sequences for the children
    """
    all_sequences = set()

    for i in range(10):
        parent = f"parent_{i}"
        seq = operation_id_sequence(parent)
        sequence = tuple(islice(seq, 10))
        all_sequences.add(sequence)

    assert len(all_sequences) == 10


async def test_operation_id_generation_conditional_on_name_and_parent():
    """
    ensure that for all given (name, parent), None included, we observe unique sequences
    """

    parents = [f"parent_{i}" for i in range(9)] + [None]
    random.shuffle(parents)
    all_sequences = set()

    for parent in parents:
        seq = operation_id_sequence(parent)
        sequence = tuple(islice(seq, 5))
        all_sequences.add(sequence)

    assert len(all_sequences) == 10


async def test_operation_id_generation_deterministic():
    """
    ensure that any sequence with any seed name and parent is deterministic
    """

    random.seed(43)
    parents = [f"parent_{i}" for i in range(9)] + [None]
    random.shuffle(parents)

    for parent in parents:
        seq1 = operation_id_sequence(parent)
        sequence1 = tuple(islice(seq1, 10))

        seq2 = operation_id_sequence(parent)
        sequence2 = tuple(islice(seq2, 10))

        assert sequence1 == sequence2


async def test_operation_id_generation_unique():
    """
    ensure that for any sequence, any two adjacent operation ids are unique
    """
    seq = operation_id_sequence()
    ids = [next(seq) for _ in range(100)]

    for i in range(len(ids) - 1):
        assert ids[i] != ids[i + 1]


@patch("async_durable_execution.primitive.invoke.InvokeOperationExecutor")
async def test_invoke_with_explicit_tenant_id(mock_executor_class):
    """Test invoke with explicit tenant_id field."""
    mock_executor = make_async_executor("result")
    mock_executor_class.return_value = mock_executor
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    context = create_test_context(state=mock_state)

    result = await run_with_context(
        context, invoke("test_function", "payload", tenant_id="explicit-tenant")
    )

    assert result == "result"
    call_args = mock_executor_class.call_args[1]
    assert call_args["tenant_id"] == "explicit-tenant"


@patch("async_durable_execution.primitive.invoke.InvokeOperationExecutor")
async def test_invoke_without_tenant_id_defaults_to_none(mock_executor_class):
    """Test invoke without tenant_id defaults to None."""
    mock_executor = make_async_executor("result")
    mock_executor_class.return_value = mock_executor
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    context = create_test_context(state=mock_state)

    result = await run_with_context(context, invoke("test_function", "payload"))

    assert result == "result"
    call_args = mock_executor_class.call_args[1]
    assert call_args["tenant_id"] is None


async def test_durable_execution_arn_exists_on_durable_context():
    """Test that DurableContext exposes durable_execution_arn directly."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test-execution"
    )

    context = create_test_context(state=mock_state)

    assert hasattr(context, "durable_execution_arn")
    assert context.durable_execution_arn is not None


async def test_durable_execution_arn_has_correct_value():
    """Test that DurableContext contains the correct durable_execution_arn."""
    expected_arn = "arn:aws:durable:us-west-2:987654321098:execution/my-execution"
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = expected_arn

    context = create_test_context(state=mock_state)

    assert context.durable_execution_arn == expected_arn


async def test_durable_execution_arn_is_derived_from_state_at_construction():
    """Test that DurableContext reflects durable_execution_arn from state."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    context = create_test_context(state=mock_state)

    mock_state.durable_execution_arn = "new-arn"
    assert context.durable_execution_arn == "new-arn"


async def test_durable_execution_arn_propagates_to_child_context():
    """Test that child contexts inherit the same durable_execution_arn."""
    parent_arn = "arn:aws:durable:eu-west-1:111222333444:execution/parent-exec"
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = parent_arn

    parent_context = create_test_context(state=mock_state)
    child_context = parent_context.create_child_context("parent-op-123")

    assert child_context.durable_execution_arn == parent_arn
    assert child_context.durable_execution_arn == parent_context.durable_execution_arn


async def test_from_lambda_context_sets_durable_execution_arn():
    """Test that from_lambda_context factory sets durable_execution_arn."""
    expected_arn = "arn:aws:durable:ap-south-1:555666777888:execution/lambda-exec"
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = expected_arn
    mock_lambda_context = Mock()

    context = DurableContext(
        execution_state=mock_state,
        operation_identifier=OperationIdentifier.create_execution_op(),
        lambda_context=mock_lambda_context,
    )

    assert context.durable_execution_arn == expected_arn


async def test_execution_arn_alias_matches_durable_execution_arn():
    """Test that the canonical durable_execution_arn field is available."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    context = create_test_context(state=mock_state)

    assert context.durable_execution_arn == mock_state.durable_execution_arn


async def test_should_default_step_id_prefix_to_parent_id_when_not_specified():
    """A non-virtual context derives the generator prefix from parent_id."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    ctx = DurableContext(
        execution_state=mock_state,
        operation_identifier=OperationIdentifier(
            operation_id=None,
            sub_type=OperationSubType.EXECUTION,
            parent_id="parent-op-1",
        ),
    )

    assert ctx.parent_id == "parent-op-1"  # noqa: SLF001
    assert ctx.step_id_prefix is None  # noqa: SLF001
    assert ctx.operation_id_generator_prefix == "parent-op-1"  # noqa: SLF001
    assert ctx.is_virtual is False


async def test_should_mark_context_virtual_when_parent_id_differs_from_step_prefix():
    """A virtual context holds parent_id and step_id_prefix with different values."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    ctx = DurableContext(
        execution_state=mock_state,
        operation_identifier=OperationIdentifier(
            operation_id=None,
            sub_type=OperationSubType.EXECUTION,
            parent_id="grandparent-op",
        ),
        step_id_prefix="branch-op",
    )

    assert ctx.parent_id == "grandparent-op"  # noqa: SLF001
    assert ctx.step_id_prefix == "branch-op"  # noqa: SLF001
    assert ctx.is_virtual is True


async def test_durable_callable_branches_bind_parallel_parameters():
    """Bound durable_callables can be passed directly to parallel()."""

    @durable_callable
    async def compute(a: int, b: int, op: str = "add") -> str:
        if op == "add":
            return f"{a + b}"
        return f"{a * b}"

    branch_a = compute(3, 4, op="mul")
    branch_b = compute(5, 6)

    assert await branch_a() == "12"
    assert await branch_b() == "11"
    assert callable(branch_a)
    assert callable(branch_b)


def create_replay_context() -> DurableContext:
    state = ExecutionState(
        durable_execution_arn="arn:aws:durable:us-east-1:123456789012:execution/test",
        initial_checkpoint_token="test_token",  # noqa: S106
        operations={},
        service_client=Mock(),
        plugin_executor=PluginExecutor(plugins=None),
    )
    return DurableContext(
        execution_state=state,
        operation_identifier=OperationIdentifier.create_execution_op(),
        replaying=True,
    )


def create_replay_operation(
    operation_id: str,
    status: OperationStatus,
    operation_type: OperationType = OperationType.STEP,
) -> Operation:
    return Operation(
        operation_id=operation_id,
        operation_type=operation_type,
        status=status,
    )


def test_replay_aware_flips_new_after_terminal_operation_without_next_operation():
    ctx = create_replay_context()
    operation_id = ctx._peek_next_operation_id()  # noqa: SLF001
    ctx.execution_state.operations[operation_id] = create_replay_operation(
        operation_id,
        OperationStatus.SUCCEEDED,
        OperationType.WAIT,
    )

    with ctx._replay_aware():  # noqa: SLF001
        ctx.step_counter.create_step_id()
        assert ctx.is_replaying() is True

    assert ctx.is_replaying() is False


def test_replay_aware_user_code_flips_new_before_retrying_operation():
    ctx = create_replay_context()
    operation_id = ctx._peek_next_operation_id()  # noqa: SLF001
    ctx.execution_state.operations[operation_id] = create_replay_operation(
        operation_id,
        OperationStatus.STARTED,
    )

    with ctx._replay_aware(executes_user_code=True):  # noqa: SLF001
        ctx.step_counter.create_step_id()
        assert ctx.is_replaying() is False

    assert ctx.is_replaying() is False


def test_child_context_refines_replay_status_independently():
    parent_ctx = create_replay_context()
    child_ctx = parent_ctx.create_child_context("child-op")
    operation_id = child_ctx._peek_next_operation_id()  # noqa: SLF001
    child_ctx.execution_state.operations[operation_id] = create_replay_operation(
        operation_id,
        OperationStatus.STARTED,
    )

    with child_ctx._replay_aware():  # noqa: SLF001
        child_ctx.step_counter.create_step_id()
        assert child_ctx.is_replaying() is True

    assert child_ctx.is_replaying() is False
    assert parent_ctx.is_replaying() is True
