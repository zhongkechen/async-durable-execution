"""Integration tests for immediate checkpoint response handling.

Tests end-to-end operation execution with the immediate response handling
that's implemented via the OperationExecutor base class pattern.
"""

from __future__ import annotations

from datetime import timedelta
from typing import cast
from unittest.mock import Mock, patch

import pytest

from async_durable_execution.config import ChildConfig
from async_durable_execution.context import DurableContext, get_context
from async_durable_execution.exceptions import InvocationError
from async_durable_execution.execution import (
    InvocationStatus,
    durable_execution,
)
from async_durable_execution.models import (
    CallbackDetails,
    CheckpointOutput,
    CheckpointUpdatedExecutionState,
    Operation,
    OperationStatus,
    OperationType,
)


async def run_handler(handler, event, lambda_context):
    return await handler._async_handler(event, lambda_context)


def create_mock_checkpoint_with_operations():
    """Create a mock checkpoint function that properly tracks operations.

    Returns a tuple of (mock_checkpoint_function, checkpoint_calls_list).
    The mock properly maintains an operations list that gets updated with each checkpoint.
    """
    checkpoint_calls = []
    operations = [
        Operation(
            operation_id="execution-1",
            operation_type=OperationType.EXECUTION,
            status=OperationStatus.STARTED,
        )
    ]

    async def mock_checkpoint(
        durable_execution_arn,
        checkpoint_token,
        updates,
        client_token="token",  # noqa: S107
    ):
        checkpoint_calls.append(updates)

        # Convert updates to Operation objects and add to operations list
        for update in updates:
            op = Operation(
                operation_id=update.operation_id,
                operation_type=update.operation_type,
                status=OperationStatus.STARTED,
                parent_id=update.parent_id,
            )
            operations.append(op)

        return CheckpointOutput(
            checkpoint_token="new_token",  # noqa: S106
            new_execution_state=CheckpointUpdatedExecutionState(
                operations=operations.copy()
            ),
        )

    return mock_checkpoint, checkpoint_calls


async def test_end_to_end_step_operation_with_double_check():
    """Test end-to-end step operation execution with double-check pattern.

    Verifies that the OperationExecutor.process() method properly calls
    check_result_status() twice when a checkpoint is created, enabling
    immediate response handling.
    """

    async def my_step() -> str:
        return "step_result"

    @durable_execution
    async def my_handler(event, context: DurableContext) -> str:
        result: str = await context.step(my_step)
        return result

    with patch(
        "async_durable_execution.execution.ThreadedSyncLambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_client.return_value = mock_client

        mock_checkpoint, checkpoint_calls = create_mock_checkpoint_with_operations()
        mock_client.checkpoint = mock_checkpoint

        event = {
            "DurableExecutionArn": "test-arn/execution-1",
            "CheckpointToken": "test-token",
            "InitialExecutionState": {
                "Operations": [
                    {
                        "Id": "execution-1",
                        "Type": "EXECUTION",
                        "Status": "STARTED",
                        "ExecutionDetails": {"InputPayload": "{}"},
                    }
                ],
                "NextMarker": "",
            },
            "LocalRunner": True,
        }

        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request-id"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
        lambda_context.invoked_function_arn = "test-arn"
        lambda_context.tenant_id = None

        result = await run_handler(my_handler, event, lambda_context)

        assert result["Status"] == InvocationStatus.SUCCEEDED.value
        assert result["Result"] == '"step_result"'

        # Verify checkpoints were created (START + SUCCEED)
        all_operations = [op for batch in checkpoint_calls for op in batch]
        assert len(all_operations) == 2


async def test_end_to_end_multiple_operations_execute_sequentially():
    """Test end-to-end execution with multiple operations.

    Verifies that multiple operations in a workflow execute correctly
    with the immediate response handling pattern.
    """

    async def step1() -> str:
        return "result1"

    async def step2() -> str:
        return "result2"

    @durable_execution
    async def my_handler(event, context: DurableContext) -> list[str]:
        return [await context.step(step1), await context.step(step2)]

    with patch(
        "async_durable_execution.execution.ThreadedSyncLambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_client.return_value = mock_client

        mock_checkpoint, checkpoint_calls = create_mock_checkpoint_with_operations()
        mock_client.checkpoint = mock_checkpoint

        event = {
            "DurableExecutionArn": "test-arn/execution-1",
            "CheckpointToken": "test-token",
            "InitialExecutionState": {
                "Operations": [
                    {
                        "Id": "execution-1",
                        "Type": "EXECUTION",
                        "Status": "STARTED",
                        "ExecutionDetails": {"InputPayload": "{}"},
                    }
                ],
                "NextMarker": "",
            },
            "LocalRunner": True,
        }

        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request-id"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
        lambda_context.invoked_function_arn = "test-arn"
        lambda_context.tenant_id = None

        result = await run_handler(my_handler, event, lambda_context)

        assert result["Status"] == InvocationStatus.SUCCEEDED.value
        assert result["Result"] == '["result1", "result2"]'

        # Verify all checkpoints were created (2 START + 2 SUCCEED)
        all_operations = [op for batch in checkpoint_calls for op in batch]
        assert len(all_operations) == 4


async def test_end_to_end_wait_operation_with_double_check():
    """Test end-to-end wait operation execution with double-check pattern.

    Verifies that wait operations properly use the double-check pattern
    for immediate response handling.
    """

    @durable_execution
    async def my_handler(event, context: DurableContext) -> str:
        await context.wait(timedelta(seconds=5))
        return "completed"

    with patch(
        "async_durable_execution.execution.ThreadedSyncLambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_client.return_value = mock_client

        mock_checkpoint, checkpoint_calls = create_mock_checkpoint_with_operations()
        mock_client.checkpoint = mock_checkpoint

        event = {
            "DurableExecutionArn": "test-arn/execution-1",
            "CheckpointToken": "test-token",
            "InitialExecutionState": {
                "Operations": [
                    {
                        "Id": "execution-1",
                        "Type": "EXECUTION",
                        "Status": "STARTED",
                        "ExecutionDetails": {"InputPayload": "{}"},
                    }
                ],
                "NextMarker": "",
            },
            "LocalRunner": True,
        }

        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request-id"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
        lambda_context.invoked_function_arn = "test-arn"
        lambda_context.tenant_id = None

        # Wait will suspend, so we expect PENDING status
        result = await run_handler(my_handler, event, lambda_context)

        assert result["Status"] == InvocationStatus.PENDING.value

        # Verify wait checkpoint was created
        all_operations = [op for batch in checkpoint_calls for op in batch]
        assert len(all_operations) >= 1


async def test_end_to_end_checkpoint_synchronization_with_operations_list():
    """Test that synchronous checkpoints properly update operations list.

    Verifies that when is_sync=True, the operations list is updated
    before the second status check occurs.
    """

    async def my_step() -> str:
        return "result"

    @durable_execution
    async def my_handler(event, context: DurableContext) -> str:
        return await context.step(my_step)

    with patch(
        "async_durable_execution.execution.ThreadedSyncLambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_client.return_value = mock_client

        mock_checkpoint, checkpoint_calls = create_mock_checkpoint_with_operations()
        mock_client.checkpoint = mock_checkpoint

        event = {
            "DurableExecutionArn": "test-arn/execution-1",
            "CheckpointToken": "test-token",
            "InitialExecutionState": {
                "Operations": [
                    {
                        "Id": "execution-1",
                        "Type": "EXECUTION",
                        "Status": "STARTED",
                        "ExecutionDetails": {"InputPayload": "{}"},
                    }
                ],
                "NextMarker": "",
            },
            "LocalRunner": True,
        }

        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request-id"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
        lambda_context.invoked_function_arn = "test-arn"
        lambda_context.tenant_id = None

        result = await run_handler(my_handler, event, lambda_context)

        assert result["Status"] == InvocationStatus.SUCCEEDED.value

        # Verify operations list was properly maintained
        all_operations = [op for batch in checkpoint_calls for op in batch]
        assert len(all_operations) >= 2  # At least START and SUCCEED


async def test_callback_deferred_error_handling_to_result():
    """Test callback deferred error handling pattern.

    Verifies that callback operations properly return callback_id through
    the immediate response handling pattern, enabling deferred error handling.
    """

    async def step_after_callback() -> str:
        return "code_executed_after_callback"

    @durable_execution
    async def my_handler(event, context: DurableContext) -> str:
        # Create callback
        callback = await context.create_callback("test_callback")

        # This code executes even if callback will eventually fail
        # This is the deferred error handling pattern
        result = await context.step(step_after_callback)

        return f"{callback.callback_id}:{result}"

    with patch(
        "async_durable_execution.execution.ThreadedSyncLambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_client.return_value = mock_client

        checkpoint_calls = []
        operations = [
            Operation(
                operation_id="execution-1",
                operation_type=OperationType.EXECUTION,
                status=OperationStatus.STARTED,
            )
        ]

        async def mock_checkpoint(
            durable_execution_arn,
            checkpoint_token,
            updates,
            client_token="token",  # noqa: S107
        ):
            checkpoint_calls.append(updates)

            # Add operations with proper details
            for update in updates:
                if update.operation_type == OperationType.CALLBACK:
                    op = Operation(
                        operation_id=update.operation_id,
                        operation_type=update.operation_type,
                        status=OperationStatus.STARTED,
                        parent_id=update.parent_id,
                        callback_details=CallbackDetails(
                            callback_id=f"cb-{update.operation_id[:8]}"
                        ),
                    )
                else:
                    op = Operation(
                        operation_id=update.operation_id,
                        operation_type=update.operation_type,
                        status=OperationStatus.STARTED,
                        parent_id=update.parent_id,
                    )
                operations.append(op)

            return CheckpointOutput(
                checkpoint_token="new_token",  # noqa: S106
                new_execution_state=CheckpointUpdatedExecutionState(
                    operations=operations.copy()
                ),
            )

        mock_client.checkpoint = mock_checkpoint

        event = {
            "DurableExecutionArn": "test-arn/execution-1",
            "CheckpointToken": "test-token",
            "InitialExecutionState": {
                "Operations": [
                    {
                        "Id": "execution-1",
                        "Type": "EXECUTION",
                        "Status": "STARTED",
                        "ExecutionDetails": {"InputPayload": "{}"},
                    }
                ],
                "NextMarker": "",
            },
            "LocalRunner": True,
        }

        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request-id"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
        lambda_context.invoked_function_arn = "test-arn"
        lambda_context.tenant_id = None

        result = await run_handler(my_handler, event, lambda_context)

        # Verify execution succeeded and code after callback executed
        assert result["Status"] == InvocationStatus.SUCCEEDED.value
        assert "code_executed_after_callback" in result["Result"]


async def test_end_to_end_invoke_operation_with_double_check():
    """Test end-to-end invoke operation execution with double-check pattern.

    Verifies that invoke operations properly use the double-check pattern
    for immediate response handling.
    """

    @durable_execution
    async def my_handler(event, context: DurableContext):
        await context.invoke("my-function", {"data": "test"})

    with patch(
        "async_durable_execution.execution.ThreadedSyncLambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_client.return_value = mock_client

        mock_checkpoint, checkpoint_calls = create_mock_checkpoint_with_operations()
        mock_client.checkpoint = mock_checkpoint

        event = {
            "DurableExecutionArn": "test-arn/execution-1",
            "CheckpointToken": "test-token",
            "InitialExecutionState": {
                "Operations": [
                    {
                        "Id": "execution-1",
                        "Type": "EXECUTION",
                        "Status": "STARTED",
                        "ExecutionDetails": {"InputPayload": "{}"},
                    }
                ],
                "NextMarker": "",
            },
            "LocalRunner": True,
        }

        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request-id"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
        lambda_context.invoked_function_arn = "test-arn"
        lambda_context.tenant_id = None

        # Invoke will suspend, so we expect PENDING status
        result = await run_handler(my_handler, event, lambda_context)

        assert result["Status"] == InvocationStatus.PENDING.value

        # Verify invoke checkpoint was created
        all_operations = [op for batch in checkpoint_calls for op in batch]
        assert len(all_operations) >= 1


async def test_end_to_end_child_context_with_async_checkpoint():
    """Test end-to-end child context execution with async checkpoint.

    Verifies that child context operations use async checkpoint (is_sync=False)
    and execute correctly without waiting for immediate response.
    """

    async def child_function() -> str:
        _ = cast(DurableContext, get_context())
        return "child_result"

    @durable_execution
    async def my_handler(event, context: DurableContext) -> str:
        result: str = await context.run_in_child_context(child_function)
        return result

    with patch(
        "async_durable_execution.execution.ThreadedSyncLambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_client.return_value = mock_client

        mock_checkpoint, checkpoint_calls = create_mock_checkpoint_with_operations()
        mock_client.checkpoint = mock_checkpoint

        event = {
            "DurableExecutionArn": "test-arn/execution-1",
            "CheckpointToken": "test-token",
            "InitialExecutionState": {
                "Operations": [
                    {
                        "Id": "execution-1",
                        "Type": "EXECUTION",
                        "Status": "STARTED",
                        "ExecutionDetails": {"InputPayload": "{}"},
                    }
                ],
                "NextMarker": "",
            },
            "LocalRunner": True,
        }

        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request-id"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
        lambda_context.invoked_function_arn = "test-arn"
        lambda_context.tenant_id = None

        result = await run_handler(my_handler, event, lambda_context)

        assert result["Status"] == InvocationStatus.SUCCEEDED.value
        assert result["Result"] == '"child_result"'

        # Verify checkpoints were created (START + SUCCEED)
        all_operations = [op for batch in checkpoint_calls for op in batch]
        assert len(all_operations) == 2


async def test_end_to_end_child_context_replay_children_mode():
    """Test end-to-end child context with large payload and ReplayChildren mode.

    Verifies that child context with large result (>256KB) triggers replay_children mode,
    uses summary generator if provided, and re-executes function on replay.
    """
    execution_count = {"count": 0}

    async def child_function_with_large_result() -> str:
        _ = cast(DurableContext, get_context())
        execution_count["count"] += 1
        return "large" * 256 * 1024

    def summary_generator(result: str) -> str:
        return f"summary_of_{len(result)}_bytes"

    @durable_execution
    async def my_handler(event, context: DurableContext) -> str:
        await context.run_in_child_context(
            child_function_with_large_result,
            config=ChildConfig(summary_generator=summary_generator),
        )
        return f"executed_{execution_count['count']}_times"

    with patch(
        "async_durable_execution.execution.ThreadedSyncLambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_client.return_value = mock_client

        checkpoint_calls = []
        operations = [
            Operation(
                operation_id="execution-1",
                operation_type=OperationType.EXECUTION,
                status=OperationStatus.STARTED,
            )
        ]

        async def mock_checkpoint(
            durable_execution_arn,
            checkpoint_token,
            updates,
            client_token="token",  # noqa: S107
        ):
            checkpoint_calls.append(updates)

            for update in updates:
                op = Operation(
                    operation_id=update.operation_id,
                    operation_type=update.operation_type,
                    status=OperationStatus.STARTED,
                    parent_id=update.parent_id,
                )
                operations.append(op)

            return CheckpointOutput(
                checkpoint_token="new_token",  # noqa: S106
                new_execution_state=CheckpointUpdatedExecutionState(
                    operations=operations.copy()
                ),
            )

        mock_client.checkpoint = mock_checkpoint

        event = {
            "DurableExecutionArn": "test-arn/execution-1",
            "CheckpointToken": "test-token",
            "InitialExecutionState": {
                "Operations": [
                    {
                        "Id": "execution-1",
                        "Type": "EXECUTION",
                        "Status": "STARTED",
                        "ExecutionDetails": {"InputPayload": "{}"},
                    }
                ],
                "NextMarker": "",
            },
            "LocalRunner": True,
        }

        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request-id"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
        lambda_context.invoked_function_arn = "test-arn"
        lambda_context.tenant_id = None

        result = await run_handler(my_handler, event, lambda_context)

        assert result["Status"] == InvocationStatus.SUCCEEDED.value
        # Function executed once during initial execution
        assert execution_count["count"] == 1

        # Verify replay_children was set in SUCCEED checkpoint
        all_operations = [op for batch in checkpoint_calls for op in batch]
        succeed_updates = [
            op
            for op in all_operations
            if hasattr(op, "action") and op.action.value == "SUCCEED"
        ]
        assert len(succeed_updates) == 1
        assert succeed_updates[0].context_options.replay_children is True


async def test_end_to_end_child_context_error_handling():
    """Test end-to-end child context error handling.

    Verifies that child context that raises exception creates FAIL checkpoint
    and error is wrapped as CallableRuntimeError.
    """

    async def child_function_that_fails() -> str:
        _ = cast(DurableContext, get_context())
        msg = "Child function error"
        raise ValueError(msg)

    @durable_execution
    async def my_handler(event, context: DurableContext) -> str:
        result: str = await context.run_in_child_context(child_function_that_fails)
        return result

    with patch(
        "async_durable_execution.execution.ThreadedSyncLambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_client.return_value = mock_client

        mock_checkpoint, checkpoint_calls = create_mock_checkpoint_with_operations()
        mock_client.checkpoint = mock_checkpoint

        event = {
            "DurableExecutionArn": "test-arn/execution-1",
            "CheckpointToken": "test-token",
            "InitialExecutionState": {
                "Operations": [
                    {
                        "Id": "execution-1",
                        "Type": "EXECUTION",
                        "Status": "STARTED",
                        "ExecutionDetails": {"InputPayload": "{}"},
                    }
                ],
                "NextMarker": "",
            },
            "LocalRunner": True,
        }

        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request-id"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
        lambda_context.invoked_function_arn = "test-arn"
        lambda_context.tenant_id = None

        result = await run_handler(my_handler, event, lambda_context)

        # Verify execution failed
        assert result["Status"] == InvocationStatus.FAILED.value

        # Verify FAIL checkpoint was created
        all_operations = [op for batch in checkpoint_calls for op in batch]
        fail_updates = [
            op
            for op in all_operations
            if hasattr(op, "action") and op.action.value == "FAIL"
        ]
        assert len(fail_updates) == 1


async def test_end_to_end_child_context_invocation_error_reraised():
    """Test end-to-end child context InvocationError re-raising.

    Verifies that child context that raises InvocationError creates FAIL checkpoint
    and re-raises InvocationError (not wrapped) to enable retry at execution handler level.
    """

    async def child_function_with_invocation_error() -> str:
        _ = cast(DurableContext, get_context())
        msg = "Invocation failed in child"
        raise InvocationError(msg)

    @durable_execution
    async def my_handler(event, context: DurableContext) -> str:
        result: str = await context.run_in_child_context(
            child_function_with_invocation_error
        )
        return result

    with patch(
        "async_durable_execution.execution.ThreadedSyncLambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_client.return_value = mock_client

        mock_checkpoint, checkpoint_calls = create_mock_checkpoint_with_operations()
        mock_client.checkpoint = mock_checkpoint

        event = {
            "DurableExecutionArn": "test-arn/execution-1",
            "CheckpointToken": "test-token",
            "InitialExecutionState": {
                "Operations": [
                    {
                        "Id": "execution-1",
                        "Type": "EXECUTION",
                        "Status": "STARTED",
                        "ExecutionDetails": {"InputPayload": "{}"},
                    }
                ],
                "NextMarker": "",
            },
            "LocalRunner": True,
        }

        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request-id"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
        lambda_context.invoked_function_arn = "test-arn"
        lambda_context.tenant_id = None

        # InvocationError should be re-raised (not wrapped) to trigger Lambda retry
        with pytest.raises(InvocationError, match="Invocation failed in child"):
            await run_handler(my_handler, event, lambda_context)

        # Verify FAIL checkpoint was created before re-raising
        all_operations = [op for batch in checkpoint_calls for op in batch]
        fail_updates = [
            op
            for op in all_operations
            if hasattr(op, "action") and op.action.value == "FAIL"
        ]
        assert len(fail_updates) == 1
