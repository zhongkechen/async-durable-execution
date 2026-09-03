"""Local and cloud tests for the terminal-scope MicroVM example."""

from async_durable_execution import (
    InvocationStatus,
    OperationStatus,
    OperationSubType,
)
from examples.terminal_scope import terminal_scope_microvm


async def test_terminal_scope_microvm_success(durable_runner) -> None:
    async with durable_runner(
        handler=terminal_scope_microvm.handler,
        input={"review_id": "review-123"},
        timeout=45,
    ) as runner:
        execution_arn = await runner.run_async()
        callback_id = await runner.wait_for_callback(execution_arn=execution_arn)
        await runner.send_callback_success(callback_id, b"approved")
        result = await runner.wait_for_result(execution_arn=execution_arn)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == {
        "microvm_id": "microvm-review-123",
        "review": "approved",
    }

    scope = result.get_context("review-with-microvm")
    assert scope.sub_type is OperationSubType.TERMINAL_SCOPE
    assert scope.status is OperationStatus.SUCCEEDED
    children = result.get_child_operations(scope)
    assert [operation.name for operation in children] == [
        "launch-microvm",
        "review-complete",
        "dispatch-review",
        "terminate-microvm",
    ]
    assert children[-1].sub_type is OperationSubType.TERMINAL_CLEANUP
    assert all(operation.name != "cancel-review" for operation in children)


async def test_terminal_scope_microvm_failure(durable_runner) -> None:
    async with durable_runner(
        handler=terminal_scope_microvm.handler,
        input={"review_id": "review-456", "fail": True},
        timeout=45,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.FAILED

    scope = result.get_context("review-with-microvm")
    assert scope.sub_type is OperationSubType.TERMINAL_SCOPE
    assert scope.status is OperationStatus.FAILED
    children = result.get_child_operations(scope)
    assert [operation.name for operation in children] == [
        "launch-microvm",
        "fail-review",
        "cancel-review",
        "terminate-microvm",
    ]
    assert children[-2].sub_type is OperationSubType.TERMINAL_COMPENSATION
    assert children[-1].sub_type is OperationSubType.TERMINAL_CLEANUP
