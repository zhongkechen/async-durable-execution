"""Unit tests for child handler."""

from __future__ import annotations
from typing import Any

import asyncio
import hashlib
import inspect
import json
from typing import cast
from unittest.mock import Mock

import pytest
from async_durable_execution._core.context import (
    DurableContext,
    bind_current_context,
)
from async_durable_execution._core.exceptions import (
    CallableRuntimeError,
    ExecutionError,
    InvocationError,
    _decode_sdk_error_data,
)
from async_durable_execution._core.models import OperationIdentifier
from async_durable_execution._core.models import (
    ContextDetails,
    ErrorObject,
    Operation,
    OperationAction,
    OperationStatus,
    OperationSubType,
    OperationType,
)
from async_durable_execution._operation.child import (
    SummaryGenerator,
    run_in_child_context,
)
from async_durable_execution._operation.callback import CallbackError
from async_durable_execution._primitive.child import (
    ChildOperationExecutor,
    _run_in_child_context,
)
from async_durable_execution._core.serdes import SerDes
from async_durable_execution._core.state import ExecutionState

from ..serdes_test import CustomDictSerDes


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


async def child_handler(*args, **kwargs) -> Any:
    func = _asyncify(args[0] if args else kwargs.pop("func"))
    state = args[1] if len(args) > 1 else kwargs.pop("state")
    operation_identifier = (
        args[2] if len(args) > 2 else kwargs.pop("operation_identifier")
    )
    executor = ChildOperationExecutor(
        func,
        state,
        operation_identifier,
        **kwargs,
    )
    return await executor.process()


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


def test_run_in_child_context_name_is_keyword_only() -> None:
    """run_in_child_context operation name must be passed as a keyword."""
    parameters = inspect.signature(run_in_child_context).parameters

    assert parameters["func"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert parameters["name"].kind is inspect.Parameter.KEYWORD_ONLY


async def test_internal_run_in_child_context_uses_custom_sub_type() -> None:
    """Internal helper records the caller-supplied operation subtype."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_state.operations.get.return_value = None
    context = create_test_context(state=mock_state, parent_id="parent")

    async def child_func() -> str:
        return "custom_result"

    with bind_current_context(context):
        result = await _run_in_child_context(
            child_func,
            sub_type=OperationSubType.MAP,
            name="custom-child",
        )

    assert result == "custom_result"
    assert mock_state.create_checkpoint.call_count == 2

    start_operation = mock_state.create_checkpoint.call_args_list[0].kwargs[
        "operation_update"
    ]
    success_operation = mock_state.create_checkpoint.call_args_list[1].kwargs[
        "operation_update"
    ]

    assert start_operation.sub_type is OperationSubType.MAP
    assert start_operation.name == "custom-child"
    assert success_operation.sub_type is OperationSubType.MAP
    assert success_operation.name == "custom-child"


@pytest.mark.parametrize(
    "expected_sub_type", [OperationSubType.RUN_IN_CHILD_CONTEXT, OperationSubType.STEP]
)
async def test_child_handler_not_started(
    expected_sub_type: OperationSubType,
) -> None:
    """Test child_handler when operation not started.

    Verifies:
    - direct state lookup is called once (async checkpoint, no second check)
    - create_checkpoint is called with is_sync=False for START
    - Operation executes and creates SUCCEED checkpoint
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = False
    mock_result.is_failed.return_value = False
    mock_result.is_started.return_value = False
    mock_result.is_replay_children.return_value = False
    mock_result.is_existent.return_value = False
    mock_state.operations.get.return_value = None
    mock_callable = Mock(return_value="fresh_result")
    result = await child_handler(
        mock_callable,
        mock_state,
        OperationIdentifier("op1", expected_sub_type, None, "test_name"),
    )

    assert result == "fresh_result"

    # Verify direct state lookup called once (async checkpoint, no second check)
    assert mock_state.operations.get.call_count == 1

    # Verify create_checkpoint called twice (start and succeed)
    mock_state.create_checkpoint.assert_called()
    assert mock_state.create_checkpoint.call_count == 2

    # Verify start checkpoint with is_sync=False
    start_call = mock_state.create_checkpoint.call_args_list[0]
    start_operation = start_call[1]["operation_update"]
    assert start_operation.operation_id == "op1"
    assert start_operation.name == "test_name"
    assert start_operation.operation_type is OperationType.CONTEXT
    assert start_operation.sub_type is expected_sub_type
    assert start_operation.action is OperationAction.START
    # CRITICAL: Verify is_sync=False for START checkpoint (async, no immediate response)
    assert start_call[1]["is_sync"] is False

    # Verify success checkpoint
    success_call = mock_state.create_checkpoint.call_args_list[1]
    success_operation = success_call[1]["operation_update"]
    assert success_operation.operation_id == "op1"
    assert success_operation.name == "test_name"
    assert success_operation.operation_type is OperationType.CONTEXT
    assert success_operation.sub_type is expected_sub_type
    assert success_operation.action is OperationAction.SUCCEED
    assert success_operation.payload == json.dumps("fresh_result")

    mock_callable.assert_called_once()


async def test_child_handler_already_succeeded() -> None:
    """Test child_handler when operation already succeeded without replay_children.

    Verifies:
    - Returns cached result without executing function
    - No checkpoint created
    - direct state lookup called once
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="op2",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.SUCCEEDED,
        context_details=ContextDetails(result=json.dumps("cached_result")),
    )
    mock_result = operation
    mock_state.operations.get.return_value = mock_result
    mock_callable = Mock()

    result = await child_handler(
        mock_callable,
        mock_state,
        OperationIdentifier(
            "op2", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
        ),
    )

    assert result == "cached_result"
    # Verify function not executed
    mock_callable.assert_not_called()
    # Verify no checkpoint created
    mock_state.create_checkpoint.assert_not_called()
    # Verify direct state lookup called once
    assert mock_state.operations.get.call_count == 1


async def test_child_handler_already_succeeded_none_result() -> None:
    """Test child_handler when operation succeeded with None result."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="op3",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.SUCCEEDED,
        context_details=ContextDetails(result=None),
    )
    mock_result = operation
    mock_state.operations.get.return_value = mock_result
    mock_callable = Mock()

    result = await child_handler(
        mock_callable,
        mock_state,
        OperationIdentifier(
            "op3", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
        ),
    )

    assert result is None
    mock_callable.assert_not_called()


async def test_child_handler_already_succeeded_missing_context_details() -> None:
    """A succeeded child checkpoint without details replays as a None result."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="op3_missing_details",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.SUCCEEDED,
        context_details=None,
    )
    mock_state.operations.get.return_value = operation
    mock_callable = Mock()

    result = await child_handler(
        mock_callable,
        mock_state,
        OperationIdentifier(
            "op3_missing_details",
            OperationSubType.RUN_IN_CHILD_CONTEXT,
            None,
            "test_name",
        ),
    )

    assert result is None
    mock_callable.assert_not_called()


async def test_child_handler_already_failed() -> None:
    """Test child_handler when operation already failed.

    Verifies:
    - Already failed: raises error without executing function
    - No checkpoint created
    - direct state lookup called once
    """
    mock_state = Mock(spec=ExecutionState)
    error = ErrorObject(
        message="Previous failure",
        type="TestError",
        data=None,
        stack_trace=None,
    )
    operation = Operation(
        operation_id="op4",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.FAILED,
        context_details=ContextDetails(error=error),
    )
    mock_result = operation
    mock_state.operations.get.return_value = mock_result
    mock_callable = Mock()

    with pytest.raises(CallableRuntimeError, match="Previous failure"):
        await child_handler(
            mock_callable,
            mock_state,
            OperationIdentifier(
                "op4", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
            ),
        )


async def test_child_handler_already_failed_missing_error_details() -> None:
    """A failed child checkpoint without an ErrorObject raises an unknown error."""
    mock_state = Mock(spec=ExecutionState)
    operation = Operation(
        operation_id="op4_missing_error",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.FAILED,
        context_details=None,
    )
    mock_state.operations.get.return_value = operation
    mock_callable = Mock()

    with pytest.raises(CallableRuntimeError, match="Unknown error"):
        await child_handler(
            mock_callable,
            mock_state,
            OperationIdentifier(
                "op4_missing_error",
                OperationSubType.RUN_IN_CHILD_CONTEXT,
                None,
                "test_name",
            ),
        )

    mock_callable.assert_not_called()


async def test_child_handler_callback_error_checkpoints_callback_id() -> None:
    """A callback failure persists its callback id at the child boundary."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_state.operations.get.return_value = None
    callback_error = CallbackError("Callback failed", callback_id="callback-123")
    mock_callable = Mock(side_effect=callback_error)

    with pytest.raises(CallbackError) as exc_info:
        await child_handler(
            mock_callable,
            mock_state,
            OperationIdentifier(
                "wait-for-callback",
                OperationSubType.WAIT_FOR_CALLBACK,
                None,
                "test_callback",
            ),
        )

    assert exc_info.value is callback_error
    assert exc_info.value.callback_id == "callback-123"
    fail_operation = mock_state.create_checkpoint.call_args_list[1].kwargs[
        "operation_update"
    ]
    assert fail_operation.action is OperationAction.FAIL
    assert fail_operation.error.message == "Callback failed"
    assert fail_operation.error.type == "CallbackError"
    expected_exception_type = (
        "async_durable_execution._primitive.callback.CallbackError"
    )
    assert json.loads(fail_operation.error.data) == {
        "__async_durable_execution_error__": 1,
        "exception_type": expected_exception_type,
        "payload": "callback-123",
    }


@pytest.mark.parametrize(
    "exception_type",
    [
        "async_durable_execution._primitive.callback.CallbackError",
        "async_durable_execution.primitive.callback.CallbackError",
        "async_durable_execution.exceptions.CallbackError",
    ],
)
async def test_child_handler_replays_callback_error_with_callback_id(
    exception_type,
) -> None:
    """Current and legacy callback metadata reconstruct the callback id."""
    mock_state = Mock(spec=ExecutionState)
    operation = Operation(
        operation_id="wait-for-callback",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.FAILED,
        sub_type=OperationSubType.WAIT_FOR_CALLBACK,
        context_details=ContextDetails(
            error=ErrorObject(
                message="Callback failed",
                type="CallbackError",
                data=json.dumps(
                    {
                        "__async_durable_execution_error__": 1,
                        "exception_type": exception_type,
                        "payload": "callback-123",
                    }
                ),
            )
        ),
    )
    mock_state.operations.get.return_value = operation
    mock_callable = Mock()

    with pytest.raises(CallbackError, match="Callback failed") as exc_info:
        await child_handler(
            mock_callable,
            mock_state,
            OperationIdentifier(
                "wait-for-callback",
                OperationSubType.WAIT_FOR_CALLBACK,
                None,
                "test_callback",
            ),
        )

    assert exc_info.value.callback_id == "callback-123"
    mock_callable.assert_not_called()
    mock_state.create_checkpoint.assert_not_called()


async def test_child_handler_does_not_replay_user_callback_error_as_sdk_error() -> None:
    """A same-named user exception remains a generic callable failure on replay."""
    user_callback_error = type("CallbackError", (Exception,), {})("User failure")
    initial_state = Mock(spec=ExecutionState)
    initial_state.durable_execution_arn = "test_arn"
    initial_state.operations.get.return_value = None

    with pytest.raises(CallableRuntimeError, match="User failure"):
        await child_handler(
            Mock(side_effect=user_callback_error),
            initial_state,
            OperationIdentifier(
                "user-callback-error",
                OperationSubType.RUN_IN_CHILD_CONTEXT,
                None,
                "user_callback_error",
            ),
        )

    fail_operation = initial_state.create_checkpoint.call_args_list[1].kwargs[
        "operation_update"
    ]
    assert fail_operation.error.type == "CallbackError"
    assert fail_operation.error.data is None

    replay_state = Mock(spec=ExecutionState)
    replay_state.operations.get.return_value = Operation(
        operation_id="user-callback-error",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.FAILED,
        context_details=ContextDetails(error=fail_operation.error),
    )
    replay_callable = Mock()

    with pytest.raises(CallableRuntimeError, match="User failure") as exc_info:
        await child_handler(
            replay_callable,
            replay_state,
            OperationIdentifier(
                "user-callback-error",
                OperationSubType.RUN_IN_CHILD_CONTEXT,
                None,
                "user_callback_error",
            ),
        )

    assert exc_info.value.error_type == "CallbackError"
    replay_callable.assert_not_called()


async def test_should_use_step_id_prefix_when_generating_step_ids() -> None:
    """Step ids derive from the step_id_prefix, not parent_id.

    For virtual contexts this is load-bearing: step ids must stay stable
    across virtual/non-virtual construction so replay ids match.
    """

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    virtual = DurableContext(
        execution_state=mock_state,
        operation_identifier=OperationIdentifier(
            operation_id=None,
            sub_type=OperationSubType.EXECUTION,
            parent_id="grandparent-op",
        ),
        step_id_prefix="branch-op",
    )
    expected_prefixed = hashlib.blake2b(b"branch-op-1").hexdigest()[:64]

    assert virtual.step_counter._create_step_id_for_logical_step(1) == expected_prefixed  # noqa: SLF001


async def test_should_use_parent_id_as_step_prefix_when_non_virtual() -> None:
    """Non-virtual contexts prefix step ids with parent_id (default fallback).

    For the non-virtual case `step_id_prefix` is not passed explicitly;
    it defaults to `parent_id`. Replay stability for executions produced
    before the virtual-context refactor depends on this fallback
    matching the pre-refactor behaviour exactly.
    """

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    non_virtual = DurableContext(
        execution_state=mock_state,
        operation_identifier=OperationIdentifier(
            operation_id=None,
            sub_type=OperationSubType.EXECUTION,
            parent_id="parent-op",
        ),
    )
    expected = hashlib.blake2b(b"parent-op-1").hexdigest()[:64]

    assert non_virtual.step_counter._create_step_id_for_logical_step(1) == expected  # noqa: SLF001
    assert non_virtual.is_virtual is False


async def test_should_create_non_virtual_child_when_is_virtual_false() -> None:
    """create_child_context(op_id) returns a non-virtual child."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    parent = create_test_context(state=mock_state, parent_id="parent-op")

    child = parent.create_child_context("child-op")

    assert child.parent_id == "child-op"  # noqa: SLF001
    assert child.step_id_prefix == "child-op"  # noqa: SLF001
    assert child.is_virtual is False


async def test_should_create_virtual_child_that_propagates_grandparent_id() -> None:
    """create_child_context(op_id, is_virtual=True) propagates the grandparent as parent_id."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    parent = create_test_context(state=mock_state, parent_id="grandparent-op")

    child = parent.create_child_context("child-op", is_virtual=True)

    assert child.parent_id == "grandparent-op"  # noqa: SLF001
    assert child.step_id_prefix == "child-op"  # noqa: SLF001
    assert child.is_virtual is True


async def test_should_create_virtual_child_with_none_parent_when_parent_is_root() -> (
    None
):
    """Virtual child of a root context (parent_id=None) keeps parent_id=None.

    Inner operations then report at the top level; step ids still prefix
    on the child's own operation id.
    """

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    root_parent = create_test_context(state=mock_state, parent_id=None)

    child = root_parent.create_child_context("child-op", is_virtual=True)

    assert child.parent_id is None  # noqa: SLF001
    assert child.step_id_prefix == "child-op"  # noqa: SLF001
    assert child.is_virtual is True

    expected = hashlib.blake2b(b"child-op-1").hexdigest()[:64]
    assert child.step_counter._create_step_id_for_logical_step(1) == expected  # noqa: SLF001


async def test_next_operation_is_terminal_checkpoint_returns_false_when_missing() -> (
    None
):
    """Replay lookahead treats a missing next operation as non-terminal."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.operations.get.return_value = None
    context = create_test_context(state=mock_state, parent_id="parent-op")

    assert context._next_operation_is_terminal_checkpoint() is False  # noqa: SLF001


async def test_should_propagate_outer_parent_id_when_virtual_is_nested_in_virtual() -> (
    None
):
    """A virtual child of a virtual parent still reports to the outer non-virtual ancestor.

    Nested concurrency is a real scenario: e.g. a FLAT `map` inside a
    FLAT `parallel`. Each layer creates a virtual child. The inner
    virtual child inherits `_parent_id` from its immediate (virtual)
    parent, which in turn inherited it from its non-virtual
    grandparent. The expected end result is that inner operations in
    the doubly-nested virtual branch still stamp the outer
    non-virtual ancestor's id — every virtual layer collapses out of
    the observable hierarchy without accumulating.
    """

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )

    outer = create_test_context(state=mock_state, parent_id="outer-parallel-op")

    outer_branch = outer.create_child_context("outer-branch-op", is_virtual=True)
    assert outer_branch.parent_id == "outer-parallel-op"  # noqa: SLF001
    assert outer_branch.step_id_prefix == "outer-branch-op"  # noqa: SLF001
    assert outer_branch.is_virtual is True

    inner_branch = outer_branch.create_child_context("inner-branch-op", is_virtual=True)
    assert inner_branch.parent_id == "outer-parallel-op"  # noqa: SLF001
    assert inner_branch.step_id_prefix == "inner-branch-op"  # noqa: SLF001
    assert inner_branch.is_virtual is True

    expected = hashlib.blake2b(b"inner-branch-op-1").hexdigest()[:64]
    assert inner_branch.step_counter._create_step_id_for_logical_step(1) == expected  # noqa: SLF001


@pytest.mark.parametrize(
    "expected_sub_type", [OperationSubType.RUN_IN_CHILD_CONTEXT, OperationSubType.STEP]
)
async def test_child_handler_already_started(
    expected_sub_type: OperationSubType,
) -> None:
    """Test child_handler when operation already started.

    Verifies:
    - Operation executes when already started
    - Only SUCCEED checkpoint created (no START)
    - direct state lookup called once
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="op5",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.STARTED,
    )
    mock_result = operation
    mock_state.operations.get.return_value = mock_result
    mock_callable = Mock(return_value="started_result")
    result = await child_handler(
        mock_callable,
        mock_state,
        OperationIdentifier("op5", expected_sub_type, None, "test_name"),
    )

    assert result == "started_result"

    # Verify direct state lookup called once
    assert mock_state.operations.get.call_count == 1

    # Verify only success checkpoint (no START since already started)
    assert mock_state.create_checkpoint.call_count == 1
    success_call = mock_state.create_checkpoint.call_args_list[0]
    success_operation = success_call[1]["operation_update"]
    assert success_operation.operation_id == "op5"
    assert success_operation.name == "test_name"
    assert success_operation.operation_type is OperationType.CONTEXT
    assert success_operation.sub_type == expected_sub_type
    assert success_operation.action is OperationAction.SUCCEED
    assert success_operation.payload == json.dumps("started_result")

    mock_callable.assert_called_once()


@pytest.mark.parametrize(
    "expected_sub_type", [OperationSubType.RUN_IN_CHILD_CONTEXT, OperationSubType.STEP]
)
async def test_child_handler_callable_exception(
    expected_sub_type: OperationSubType,
) -> None:
    """Test child_handler when callable raises exception.

    Verifies:
    - Error handling: checkpoints FAIL and raises wrapped error
    - direct state lookup called once
    - create_checkpoint called with is_sync=False for START
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = False
    mock_result.is_failed.return_value = False
    mock_result.is_started.return_value = False
    mock_result.is_existent.return_value = False
    mock_state.operations.get.return_value = None
    mock_callable = Mock(side_effect=ValueError("Test error"))
    with pytest.raises(CallableRuntimeError):
        await child_handler(
            mock_callable,
            mock_state,
            OperationIdentifier("op6", expected_sub_type, None, "test_name"),
        )

    # Verify direct state lookup called once
    assert mock_state.operations.get.call_count == 1

    # Verify create_checkpoint called twice (start and fail)
    mock_state.create_checkpoint.assert_called()
    assert mock_state.create_checkpoint.call_count == 2

    # Verify start checkpoint with is_sync=False
    start_call = mock_state.create_checkpoint.call_args_list[0]
    start_operation = start_call[1]["operation_update"]
    assert start_operation.operation_id == "op6"
    assert start_operation.name == "test_name"
    assert start_operation.operation_type is OperationType.CONTEXT
    assert start_operation.sub_type is expected_sub_type
    assert start_operation.action is OperationAction.START
    assert start_call[1]["is_sync"] is False

    # Verify fail checkpoint
    fail_call = mock_state.create_checkpoint.call_args_list[1]
    fail_operation = fail_call[1]["operation_update"]
    assert fail_operation.operation_id == "op6"
    assert fail_operation.name == "test_name"
    assert fail_operation.operation_type is OperationType.CONTEXT
    assert fail_operation.sub_type is expected_sub_type
    assert fail_operation.action is OperationAction.FAIL
    assert fail_operation.error == ErrorObject.from_exception(ValueError("Test error"))


async def test_child_handler_error_wrapped() -> None:
    """Test child_handler wraps regular errors as CallableRuntimeError.

    Verifies:
    - Regular exceptions are wrapped as CallableRuntimeError
    - FAIL checkpoint is created
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = False
    mock_result.is_failed.return_value = False
    mock_result.is_started.return_value = False
    mock_result.is_existent.return_value = False
    mock_state.operations.get.return_value = None
    test_error = RuntimeError("Test error")
    mock_callable = Mock(side_effect=test_error)
    with pytest.raises(CallableRuntimeError):
        await child_handler(
            mock_callable,
            mock_state,
            OperationIdentifier(
                "op7", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
            ),
        )

    # Verify FAIL checkpoint was created
    assert mock_state.create_checkpoint.call_count == 2  # start and fail


async def test_child_handler_checkpoints_sdk_error_metadata() -> None:
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_state.operations.get.return_value = None

    with pytest.raises(ExecutionError, match="Execution failed"):
        await child_handler(
            Mock(side_effect=ExecutionError("Execution failed")),
            mock_state,
            OperationIdentifier(
                "sdk-error",
                OperationSubType.RUN_IN_CHILD_CONTEXT,
                None,
                "test_name",
            ),
        )

    fail_operation = mock_state.create_checkpoint.call_args_list[1].kwargs[
        "operation_update"
    ]
    is_sdk_error, _ = _decode_sdk_error_data(
        fail_operation.error.data,
        ExecutionError,
    )
    assert fail_operation.error.type == "ExecutionError"
    assert is_sdk_error


async def test_child_handler_retryable_invocation_error_replays_without_fail() -> None:
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_state.operations.get.return_value = None
    test_error = InvocationError("Invocation failed")
    mock_callable = Mock(side_effect=[test_error, "replayed result"])
    operation_identifier = OperationIdentifier(
        "op7b",
        OperationSubType.RUN_IN_CHILD_CONTEXT,
        None,
        "test_name",
    )

    with pytest.raises(InvocationError, match="Invocation failed"):
        await child_handler(
            mock_callable,
            mock_state,
            operation_identifier,
        )

    mock_state.create_checkpoint.assert_called_once()
    start_operation = mock_state.create_checkpoint.call_args.kwargs["operation_update"]
    assert start_operation.action is OperationAction.START

    mock_state.reset_mock()
    mock_state.operations.get.return_value = Operation(
        operation_id="op7b",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.STARTED,
    )

    result = await child_handler(
        mock_callable,
        mock_state,
        operation_identifier,
    )

    assert result == "replayed result"
    assert mock_callable.call_count == 2
    mock_state.create_checkpoint.assert_called_once()
    success_operation = mock_state.create_checkpoint.call_args.kwargs[
        "operation_update"
    ]
    assert success_operation.action is OperationAction.SUCCEED


async def test_child_handler_non_retryable_invocation_error_checkpoints_fail() -> None:
    class NonRetryableInvocationError(InvocationError):
        def is_retryable(self) -> bool:
            return False

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_state.operations.get.return_value = None
    test_error = NonRetryableInvocationError("Invocation failed")

    with pytest.raises(NonRetryableInvocationError, match="Invocation failed"):
        await child_handler(
            Mock(side_effect=test_error),
            mock_state,
            OperationIdentifier(
                "op7c",
                OperationSubType.RUN_IN_CHILD_CONTEXT,
                None,
                "test_name",
            ),
        )

    assert mock_state.create_checkpoint.call_count == 2
    fail_operation = mock_state.create_checkpoint.call_args_list[1].kwargs[
        "operation_update"
    ]
    assert fail_operation.action is OperationAction.FAIL


async def test_child_handler_with_config() -> None:
    """Test child_handler with direct field parameters."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = False
    mock_result.is_failed.return_value = False
    mock_result.is_started.return_value = False
    mock_result.is_existent.return_value = False
    mock_state.operations.get.return_value = None
    mock_callable = Mock(return_value="config_result")
    result = await child_handler(
        mock_callable,
        mock_state,
        OperationIdentifier(
            "op8", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
        ),
    )

    assert result == "config_result"
    mock_callable.assert_called_once()
    # Verify direct state lookup called once
    assert mock_state.operations.get.call_count == 1


async def test_child_handler_default_serialization() -> None:
    """Test child_handler properly serializes complex result."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = False
    mock_result.is_failed.return_value = False
    mock_result.is_started.return_value = False
    mock_result.is_replay_children.return_value = False
    mock_result.is_existent.return_value = False
    mock_state.operations.get.return_value = None
    complex_result = {"key": "value", "number": 42, "list": [1, 2, 3]}
    mock_callable = Mock(return_value=complex_result)
    result = await child_handler(
        mock_callable,
        mock_state,
        OperationIdentifier(
            "op9", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
        ),
    )

    assert result == complex_result
    # Verify direct state lookup called once
    assert mock_state.operations.get.call_count == 1
    # Verify JSON serialization was used in checkpoint
    success_call = [
        call
        for call in mock_state.create_checkpoint.call_args_list
        if "SUCCEED" in str(call)
    ]
    assert len(success_call) == 1


async def test_child_handler_custom_serdes_not_start() -> None:
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = False
    mock_result.is_failed.return_value = False
    mock_result.is_started.return_value = False
    mock_result.is_replay_children.return_value = False
    mock_result.is_existent.return_value = False
    mock_state.operations.get.return_value = None
    complex_result = {"key": "value", "number": 42, "list": [1, 2, 3]}
    mock_callable = Mock(return_value=complex_result)
    await child_handler(
        mock_callable,
        mock_state,
        OperationIdentifier(
            "op9", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
        ),
        serdes=CustomDictSerDes(),
    )

    expected_checkpoointed_result = (
        '{"key": "VALUE", "number": "84", "list": [1, 2, 3]}'
    )

    success_call = mock_state.create_checkpoint.call_args_list[1]
    success_operation = success_call[1]["operation_update"]
    assert success_operation.payload == expected_checkpoointed_result


async def test_result_preparation_cancellation_calls_child_hook() -> None:
    class CancellingSerDes(SerDes[str]):
        async def serialize(self, _value: str) -> str:
            raise asyncio.CancelledError

        async def deserialize(self, data: str) -> str:
            return data

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_state.operations.get.return_value = None
    cancellations: list[BaseException] = []

    async def on_result_preparation_error(error: BaseException) -> None:
        cancellations.append(error)

    with pytest.raises(asyncio.CancelledError):
        await child_handler(
            Mock(return_value="result"),
            mock_state,
            OperationIdentifier(
                "cancel-result",
                OperationSubType.RUN_IN_CHILD_CONTEXT,
                None,
                "cancel-result",
            ),
            serdes=CancellingSerDes(),
            on_result_preparation_error=on_result_preparation_error,
        )

    assert len(cancellations) == 1
    assert isinstance(cancellations[0], asyncio.CancelledError)
    assert mock_state.create_checkpoint.call_count == 1


async def test_child_handler_returns_deserialized_serialized_custom_serdes_result() -> (
    None
):
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_state.operations.get.return_value = None

    result = await child_handler(
        Mock(return_value="hello"),
        mock_state,
        OperationIdentifier(
            "op_uppercase", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "uppercase"
        ),
        serdes=UppercaseSerDes(),
    )

    success_call = mock_state.create_checkpoint.call_args_list[1]
    success_operation = success_call[1]["operation_update"]
    assert success_operation.payload == "HELLO"
    assert result == "HELLO"


async def test_child_handler_custom_serdes_already_succeeded() -> None:
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="op9",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.SUCCEEDED,
        context_details=ContextDetails(
            result='{"key": "VALUE", "number": "84", "list": [1, 2, 3]}'
        ),
    )
    mock_result = operation
    mock_state.operations.get.return_value = mock_result
    mock_callable = Mock()

    actual_result = await child_handler(
        mock_callable,
        mock_state,
        OperationIdentifier(
            "op9", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
        ),
        serdes=CustomDictSerDes(),
    )

    expected_checkpoointed_result = {"key": "value", "number": 42, "list": [1, 2, 3]}

    assert actual_result == expected_checkpoointed_result
    # Verify direct state lookup called once
    assert mock_state.operations.get.call_count == 1


# large payload with summary generator
async def test_child_handler_large_payload_with_summary_generator() -> None:
    """Test child_handler with large payload and summary generator.

    Verifies:
    - Large payload: uses ReplayChildren mode with summary_generator
    - direct state lookup called once
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = False
    mock_result.is_failed.return_value = False
    mock_result.is_started.return_value = False
    mock_result.is_replay_children.return_value = False
    mock_result.is_existent.return_value = False
    mock_state.operations.get.return_value = None
    large_result = "large" * 256 * 1024
    mock_callable = Mock(return_value=large_result)

    def my_summary(result: str) -> str:
        return "summary"

    actual_result = await child_handler(
        mock_callable,
        mock_state,
        OperationIdentifier(
            "op9", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
        ),
        summary_generator=cast("SummaryGenerator", my_summary),
    )

    assert large_result == actual_result
    # Verify direct state lookup called once
    assert mock_state.operations.get.call_count == 1
    # Verify replay_children mode with summary
    success_call = mock_state.create_checkpoint.call_args_list[1]
    success_operation = success_call[1]["operation_update"]
    assert success_operation.context_options.replay_children
    expected_checkpoointed_result = "summary"
    assert success_operation.payload == expected_checkpoointed_result


# large payload without summary generator
async def test_child_handler_large_payload_without_summary_generator() -> None:
    """Test child_handler with large payload and no summary generator.

    Verifies:
    - Large payload without summary_generator: uses ReplayChildren mode with empty string
    - direct state lookup called once
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = False
    mock_result.is_failed.return_value = False
    mock_result.is_started.return_value = False
    mock_result.is_replay_children.return_value = False
    mock_result.is_existent.return_value = False
    mock_state.operations.get.return_value = None
    large_result = "large" * 256 * 1024
    mock_callable = Mock(return_value=large_result)
    actual_result = await child_handler(
        mock_callable,
        mock_state,
        OperationIdentifier(
            "op9", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
        ),
    )

    assert large_result == actual_result
    # Verify direct state lookup called once
    assert mock_state.operations.get.call_count == 1
    # Verify replay_children mode with empty string
    success_call = mock_state.create_checkpoint.call_args_list[1]
    success_operation = success_call[1]["operation_update"]
    assert success_operation.context_options.replay_children
    expected_checkpoointed_result = ""
    assert success_operation.payload == expected_checkpoointed_result


# mocked children replay mode execute the function again
async def test_child_handler_replay_children_mode() -> None:
    """Test child_handler in ReplayChildren mode.

    Verifies:
    - Already succeeded with replay_children: re-executes function
    - No checkpoint created (returns without checkpointing)
    - direct state lookup called once
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_state.operations.get.return_value = Operation(
        operation_id="op9",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.SUCCEEDED,
        context_details=ContextDetails(replay_children=True),
    )
    complex_result = {"key": "value", "number": 42, "list": [1, 2, 3]}
    mock_callable = Mock(return_value=complex_result)
    actual_result = await child_handler(
        mock_callable,
        mock_state,
        OperationIdentifier(
            "op9", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
        ),
    )

    assert actual_result == complex_result
    # Verify function was executed (replay_children mode)
    mock_callable.assert_called_once()
    # Verify no checkpoint created (returns without checkpointing in replay mode)
    mock_state.create_checkpoint.assert_not_called()
    # Verify direct state lookup called once
    assert mock_state.operations.get.call_count == 1


async def test_small_payload_with_summary_generator() -> None:
    """Test: Small payload with summary_generator -> replay_children = False

    Verifies:
    - Small payload does NOT trigger replay_children even with summary_generator
    - direct state lookup called once
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = False
    mock_result.is_failed.return_value = False
    mock_result.is_started.return_value = False
    mock_result.is_replay_children.return_value = False
    mock_result.is_existent.return_value = False
    mock_state.operations.get.return_value = None

    # Small payload (< 256KB)
    small_result = "small_payload"
    mock_callable = Mock(return_value=small_result)

    def my_summary(result: str) -> str:
        return "summary_of_small_payload"

    actual_result = await child_handler(
        mock_callable,
        mock_state,
        OperationIdentifier(
            "op1", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
        ),
        summary_generator=my_summary,
    )

    assert actual_result == small_result
    # Verify direct state lookup called once
    assert mock_state.operations.get.call_count == 1
    success_call = mock_state.create_checkpoint.call_args_list[1]
    success_operation = success_call[1]["operation_update"]

    # Small payload should NOT trigger replay_children, even with summary_generator
    assert not success_operation.context_options.replay_children
    # Should checkpoint the actual result, not the summary
    assert success_operation.payload == '"small_payload"'  # JSON serialized


async def test_small_payload_without_summary_generator() -> None:
    """Test: small payload without summary_generator -> replay_children=False.

    Restored from pre-PR #351. For small payloads we always checkpoint
    the actual result (JSON-serialized); ReplayChildren mode exists only
    to handle payloads that exceed the size limit, so a small payload
    without a summary generator must still round-trip through a normal
    SUCCEED checkpoint.
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = False
    mock_result.is_failed.return_value = False
    mock_result.is_started.return_value = False
    mock_result.is_replay_children.return_value = False
    mock_result.is_existent.return_value = False
    mock_state.operations.get.return_value = None

    # Small payload (< 256KB); no summary_generator provided
    small_result = "small_payload"
    mock_callable = Mock(return_value=small_result)
    actual_result = await child_handler(
        mock_callable,
        mock_state,
        OperationIdentifier(
            "op1", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
        ),
    )

    assert actual_result == small_result
    assert mock_state.operations.get.call_count == 1

    success_call = mock_state.create_checkpoint.call_args_list[1]
    success_operation = success_call[1]["operation_update"]

    # Small payload MUST NOT trigger replay_children.
    assert not success_operation.context_options.replay_children
    # Payload MUST be the JSON-serialized result, not a summary.
    assert success_operation.payload == '"small_payload"'


async def test_child_handler_is_virtual_no_start() -> None:
    """Skip the START checkpoint when is_virtual=True.

    A virtual branch is a logical scope for step-id prefixing but does
    not appear in the execution history, so no START entry is emitted.
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = False
    mock_result.is_failed.return_value = False
    mock_result.is_started.return_value = False
    mock_result.is_replay_children.return_value = False
    mock_result.is_existent.return_value = False
    mock_state.operations.get.return_value = None
    mock_callable = Mock(return_value="no_checkpoint_result")
    result = await child_handler(
        mock_callable,
        mock_state,
        OperationIdentifier(
            "op1", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
        ),
        is_virtual=True,
    )

    assert result == "no_checkpoint_result"

    # Verify direct state lookup called once
    assert mock_state.operations.get.call_count == 1

    # Verify no checkpoints created (virtual context writes none)
    assert mock_state.create_checkpoint.call_count == 0

    mock_callable.assert_called_once()


async def test_child_handler_is_virtual_no_succeed() -> None:
    """Skip the SUCCEED checkpoint when is_virtual=True.

    A virtual branch is not represented in the execution history; its
    successful completion is observable only via the values returned
    to the calling parallel executor.
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = False
    mock_result.is_failed.return_value = False
    mock_result.is_started.return_value = False
    mock_result.is_replay_children.return_value = False
    mock_result.is_existent.return_value = False
    mock_state.operations.get.return_value = None
    mock_callable = Mock(return_value="no_checkpoint_result")
    result = await child_handler(
        mock_callable,
        mock_state,
        OperationIdentifier(
            "op2", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
        ),
        is_virtual=True,
    )

    assert result == "no_checkpoint_result"

    # Verify no checkpoints created
    mock_state.create_checkpoint.assert_not_called()

    mock_callable.assert_called_once()


async def test_child_handler_not_is_virtual_finish_mode() -> None:
    """Create START + SUCCEED checkpoints when is_virtual=False."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = False
    mock_result.is_failed.return_value = False
    mock_result.is_started.return_value = False
    mock_result.is_replay_children.return_value = False
    mock_result.is_existent.return_value = False
    mock_state.operations.get.return_value = None
    mock_callable = Mock(return_value="checkpoint_result")
    result = await child_handler(
        mock_callable,
        mock_state,
        OperationIdentifier(
            "op3", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
        ),
        is_virtual=False,
    )

    assert result == "checkpoint_result"

    # Verify both START and SUCCEED checkpoints created
    assert mock_state.create_checkpoint.call_count == 2

    # Verify START checkpoint
    start_call = mock_state.create_checkpoint.call_args_list[0]
    start_operation = start_call[1]["operation_update"]
    assert start_operation.action.value == "START"
    assert start_call[1]["is_sync"] is False

    # Verify SUCCEED checkpoint
    success_call = mock_state.create_checkpoint.call_args_list[1]
    success_operation = success_call[1]["operation_update"]
    assert success_operation.action.value == "SUCCEED"

    mock_callable.assert_called_once()


async def test_child_handler_is_virtual_with_exception() -> None:
    """Skip the FAIL checkpoint when is_virtual=True and the user function raises.

    A virtual branch emits no lifecycle entries in the execution
    history, so a failure inside the branch does not get its own FAIL
    checkpoint. The exception still propagates (wrapped as
    CallableRuntimeError for non-InvocationError exceptions) so the
    parallel executor records the failure in the BatchResult and
    its completion-tolerance logic still applies.
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = False
    mock_result.is_failed.return_value = False
    mock_result.is_started.return_value = False
    mock_result.is_replay_children.return_value = False
    mock_result.is_existent.return_value = False
    mock_state.operations.get.return_value = None
    mock_callable = Mock(side_effect=ValueError("Test error"))
    with pytest.raises(CallableRuntimeError):
        await child_handler(
            mock_callable,
            mock_state,
            OperationIdentifier(
                "op4", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
            ),
            is_virtual=True,
        )

    # Verify NO FAIL checkpoint created (virtual contexts suppress all lifecycle checkpoints).
    assert mock_state.create_checkpoint.call_count == 0

    mock_callable.assert_called_once()


async def test_child_handler_not_is_virtual_with_exception() -> None:
    """Create a FAIL checkpoint when is_virtual=False and the user function raises."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_result = Mock()
    mock_result.is_succeeded.return_value = False
    mock_result.is_failed.return_value = False
    mock_result.is_started.return_value = False
    mock_result.is_replay_children.return_value = False
    mock_result.is_existent.return_value = False
    mock_state.operations.get.return_value = None
    mock_callable = Mock(side_effect=ValueError("Test error"))
    with pytest.raises(CallableRuntimeError):
        await child_handler(
            mock_callable,
            mock_state,
            OperationIdentifier(
                "op5", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
            ),
            is_virtual=False,
        )

    # Verify START + FAIL checkpoints created (non-virtual path).
    assert mock_state.create_checkpoint.call_count == 2
    start_call = mock_state.create_checkpoint.call_args_list[0]
    start_operation = start_call[1]["operation_update"]
    assert start_operation.action.value == "START"
    fail_call = mock_state.create_checkpoint.call_args_list[1]
    fail_operation = fail_call[1]["operation_update"]
    assert fail_operation.action.value == "FAIL"

    mock_callable.assert_called_once()


async def test_child_handler_is_virtual_comparison() -> None:
    """Compare checkpoint counts between is_virtual=True and is_virtual=False for success.

    - is_virtual=False: 2 checkpoints (START + SUCCEED)
    - is_virtual=True:  0 checkpoints
    """

    # Setup common mocks
    def setup_mocks() -> Any:
        mock_state = Mock(spec=ExecutionState)
        mock_state.durable_execution_arn = "test_arn"
        mock_result = Mock()
        mock_result.is_succeeded.return_value = False
        mock_result.is_failed.return_value = False
        mock_result.is_started.return_value = False
        mock_result.is_replay_children.return_value = False
        mock_result.is_existent.return_value = False
        mock_state.operations.get.return_value = None
        mock_callable = Mock(return_value="test_result")
        return mock_state, mock_callable

    # is_virtual=False: 2 checkpoints
    mock_state1, mock_callable1 = setup_mocks()

    result1 = await child_handler(
        mock_callable1,
        mock_state1,
        OperationIdentifier(
            "op1", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
        ),
        is_virtual=False,
    )

    assert result1 == "test_result"
    assert mock_state1.create_checkpoint.call_count == 2  # START + SUCCEED

    # is_virtual=True: 0 checkpoints
    mock_state2, mock_callable2 = setup_mocks()

    result2 = await child_handler(
        mock_callable2,
        mock_state2,
        OperationIdentifier(
            "op2", OperationSubType.RUN_IN_CHILD_CONTEXT, None, "test_name"
        ),
        is_virtual=True,
    )

    assert result2 == "test_result"
    assert mock_state2.create_checkpoint.call_count == 0  # No checkpoints
