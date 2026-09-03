"""End-to-end local-runner coverage for durable terminal scopes."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any, cast

from async_durable_execution import (
    CompletionConfig,
    DurableFunctionLocalTestRunner,
    DurableTerminalActions,
    InvocationStatus,
    JitterStrategy,
    OperationStatus,
    OperationSubType,
    RetryStrategy,
    TerminalFailurePhase,
    TerminalScopeError,
    create_callback,
    durable_dag,
    durable_callable,
    durable_execution,
    durable_node,
    flow,
    get_step_context,
    map as durable_map,
    node,
    parallel,
    step,
    terminal_scope,
    wait,
)
from async_durable_execution._core.exceptions import _restore_sdk_control_error


async def test_callback_suspension_does_not_run_cleanup() -> None:
    cleanup_calls = 0

    @durable_callable
    async def cleanup() -> None:
        nonlocal cleanup_calls
        cleanup_calls += 1

    async def scoped(actions: DurableTerminalActions) -> str:
        actions.cleanup(cleanup(), name="cleanup")
        callback = await create_callback(name="approval")
        result = await callback.result()
        return str(result)

    @durable_execution
    async def handler(_event: Any) -> str:
        return await terminal_scope(scoped, name="approval-scope")

    async with DurableFunctionLocalTestRunner(
        handler=handler,
        input=None,
        timeout=10,
    ) as runner:
        execution_arn = await runner.run_async()
        callback_id = await runner.wait_for_callback(execution_arn=execution_arn)
        assert cleanup_calls == 0

        await runner.send_callback_success(callback_id, b"approved")
        result = await runner.wait_for_result(execution_arn=execution_arn)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == "approved"
    assert cleanup_calls == 1

    scope = result.get_context("approval-scope")
    assert scope.sub_type is OperationSubType.TERMINAL_SCOPE
    children = result.get_child_operations(scope)
    assert [operation.name for operation in children] == ["approval", "cleanup"]
    assert children[1].sub_type is OperationSubType.TERMINAL_CLEANUP


async def test_cleanup_retry_resumes_without_repeating_completed_cleanup(
    monkeypatch,
) -> None:
    monkeypatch.setenv("DURABLE_EXECUTION_TIME_SCALE", "0.01")
    completed_cleanup_calls = 0
    retry_cleanup_calls = 0

    @durable_callable
    async def completed_cleanup() -> None:
        nonlocal completed_cleanup_calls
        completed_cleanup_calls += 1

    @durable_callable
    async def retry_cleanup() -> None:
        nonlocal retry_cleanup_calls
        retry_cleanup_calls += 1
        if (get_step_context().attempt or 1) < 2:
            msg = "retry cleanup"
            raise RuntimeError(msg)

    retry_strategy = RetryStrategy(
        max_attempts=3,
        initial_delay=timedelta(seconds=1),
        max_delay=timedelta(seconds=1),
        jitter_strategy=JitterStrategy.NONE,
        retryable_error_types=[RuntimeError],
    )

    async def scoped(actions: DurableTerminalActions) -> str:
        actions.cleanup(
            retry_cleanup(),
            name="retry-cleanup",
            retry_strategy=retry_strategy,
        )
        actions.cleanup(completed_cleanup(), name="completed-cleanup")
        return "done"

    @durable_execution
    async def handler(_event: Any) -> str:
        return await terminal_scope(scoped, name="retrying-cleanup-scope")

    async with DurableFunctionLocalTestRunner(
        handler=handler,
        input=None,
        timeout=10,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert completed_cleanup_calls == 1
    assert retry_cleanup_calls == 2

    scope = result.get_context("retrying-cleanup-scope")
    cleanup_operations = {
        operation.name: operation for operation in result.get_child_operations(scope)
    }
    completed = cleanup_operations["completed-cleanup"]
    retried = cleanup_operations["retry-cleanup"]
    assert completed.status is OperationStatus.SUCCEEDED
    assert completed.step_details is not None
    assert completed.step_details.attempt == 1
    assert retried.status is OperationStatus.SUCCEEDED
    assert retried.step_details is not None
    assert retried.step_details.attempt == 2


async def test_nested_terminal_scopes_unwind_inner_before_outer() -> None:
    events: list[str] = []

    @durable_callable
    async def record(value: str) -> None:
        events.append(value)

    async def inner(actions: DurableTerminalActions) -> str:
        actions.cleanup(record("inner-cleanup"), name="inner-cleanup")
        await wait(timedelta(seconds=1), name="inner-wait")
        return "inner"

    async def outer(actions: DurableTerminalActions) -> str:
        actions.cleanup(record("outer-cleanup"), name="outer-cleanup")
        return await terminal_scope(inner, name="inner-scope")

    @durable_execution
    async def handler(_event: Any) -> str:
        return await terminal_scope(outer, name="outer-scope")

    async with DurableFunctionLocalTestRunner(
        handler=handler,
        input=None,
        timeout=10,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert events == ["inner-cleanup", "outer-cleanup"]

    outer_scope = result.get_context("outer-scope")
    outer_children = result.get_child_operations(outer_scope)
    inner_scope = next(
        operation for operation in outer_children if operation.name == "inner-scope"
    )
    assert inner_scope.sub_type is OperationSubType.TERMINAL_SCOPE
    assert [
        operation.name for operation in result.get_child_operations(inner_scope)
    ] == [
        "inner-wait",
        "inner-cleanup",
    ]


async def test_parallel_early_completion_cancels_scope_without_cleanup() -> None:
    cleanup_calls = 0

    @durable_callable
    async def cleanup() -> None:
        nonlocal cleanup_calls
        cleanup_calls += 1

    @durable_execution
    async def handler(_event: Any) -> str:
        slow_branch_started = asyncio.Event()

        async def slow_branch() -> str:
            async def scoped(actions: DurableTerminalActions) -> str:
                actions.cleanup(cleanup(), name="cancelled-branch-cleanup")
                slow_branch_started.set()
                await asyncio.Event().wait()
                return "slow"

            return await terminal_scope(scoped, name="cancelled-branch-scope")

        async def fast_branch() -> str:
            await slow_branch_started.wait()
            return "fast"

        result = await parallel(
            [slow_branch, fast_branch],
            completion_config=CompletionConfig.first_successful(),
            name="first-result",
        )
        return str(result.succeeded()[0].result)

    async with DurableFunctionLocalTestRunner(
        handler=handler,
        input=None,
        timeout=10,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == "fast"
    assert cleanup_calls == 0
    assert all(
        operation.name != "cancelled-branch-cleanup"
        for operation in result.get_all_operations()
    )


async def test_terminal_scope_runs_compensation_and_cleanup_after_body_failure() -> (
    None
):
    events: list[str] = []

    @durable_callable
    async def record(value: str) -> None:
        events.append(value)

    @durable_callable
    async def fail_body() -> None:
        msg = "body failed"
        raise ValueError(msg)

    async def scoped(actions: DurableTerminalActions) -> None:
        actions.compensate(record("compensate"), name="compensate")
        actions.cleanup(record("cleanup"), name="cleanup")
        await step(
            fail_body(),
            name="fail-body",
            retry_strategy=RetryStrategy.none(),
        )

    @durable_execution
    async def handler(_event: Any) -> None:
        await terminal_scope(scoped, name="failing-scope")

    async with DurableFunctionLocalTestRunner(
        handler=handler,
        input=None,
        timeout=10,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.FAILED
    assert events == ["compensate", "cleanup"]

    scope = result.get_context("failing-scope")
    children = result.get_child_operations(scope)
    assert [operation.name for operation in children] == [
        "fail-body",
        "compensate",
        "cleanup",
    ]
    assert children[1].sub_type is OperationSubType.TERMINAL_COMPENSATION
    assert children[2].sub_type is OperationSubType.TERMINAL_CLEANUP


async def test_map_items_own_isolated_terminal_scopes() -> None:
    cleaned: list[int] = []

    @durable_callable
    async def cleanup(item: int) -> None:
        cleaned.append(item)

    async def process_item(item: int) -> int:
        async def scoped(actions: DurableTerminalActions) -> int:
            actions.cleanup(cleanup(item), name=f"cleanup-{item}")
            return item * 2

        return await terminal_scope(scoped, name=f"item-{item}-scope")

    @durable_execution
    async def handler(_event: Any) -> list[int]:
        result = await durable_map(
            cast(Any, process_item),
            [1, 2],
            name="terminal-map",
        )
        return result.get_results()

    async with DurableFunctionLocalTestRunner(
        handler=handler,
        input=None,
        timeout=10,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == [2, 4]
    assert sorted(cleaned) == [1, 2]

    scopes = [
        operation
        for operation in result.get_all_operations()
        if operation.sub_type is OperationSubType.TERMINAL_SCOPE
    ]
    assert {scope.name for scope in scopes} == {"item-1-scope", "item-2-scope"}
    for scope in scopes:
        cleanup_operations = result.get_child_operations(scope)
        assert len(cleanup_operations) == 1
        assert cleanup_operations[0].sub_type is OperationSubType.TERMINAL_CLEANUP


async def test_flow_node_terminal_scope_suspends_and_cleans_up() -> None:
    events: list[str] = []

    @durable_callable
    async def cleanup() -> None:
        events.append("cleanup")

    @durable_node
    async def scoped_node() -> str:
        async def scoped(actions: DurableTerminalActions) -> str:
            actions.cleanup(cleanup(), name="flow-cleanup")
            await wait(timedelta(seconds=1), name="flow-wait")
            return "done"

        return await terminal_scope(scoped, name="flow-terminal-scope")

    @durable_dag
    def scoped_flow():
        terminal_node = node(scoped_node(), name="terminal-node")
        return terminal_node.outcome

    @durable_execution
    async def handler(_event: Any) -> str:
        result = await flow(scoped_flow(), name="terminal-flow")
        return cast(str, result.output)

    async with DurableFunctionLocalTestRunner(
        handler=handler,
        input=None,
        timeout=10,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == "done"
    assert events == ["cleanup"]

    scope = next(
        operation
        for operation in result.get_all_operations()
        if operation.name == "flow-terminal-scope"
    )
    assert scope.sub_type is OperationSubType.TERMINAL_SCOPE
    assert [operation.name for operation in result.get_child_operations(scope)] == [
        "flow-wait",
        "flow-cleanup",
    ]


async def test_multiple_terminal_failures_are_checkpointed_and_restored() -> None:
    events: list[str] = []

    @durable_callable
    async def fail(value: str) -> None:
        events.append(value)
        raise RuntimeError(value)

    @durable_callable
    async def succeed(value: str) -> None:
        events.append(value)

    @durable_callable
    async def fail_body() -> None:
        msg = "body failed"
        raise ValueError(msg)

    async def scoped(actions: DurableTerminalActions) -> None:
        actions.compensate(
            fail("compensation failed"),
            name="failed-compensation",
            retry_strategy=RetryStrategy.none(),
        )
        actions.cleanup(
            succeed("cleanup succeeded"),
            name="successful-cleanup",
        )
        actions.cleanup(
            fail("cleanup failed"),
            name="failed-cleanup",
            retry_strategy=RetryStrategy.none(),
        )
        await step(
            fail_body(),
            name="failed-body",
            retry_strategy=RetryStrategy.none(),
        )

    @durable_execution
    async def handler(_event: Any) -> None:
        await terminal_scope(scoped, name="multiple-failure-scope")

    async with DurableFunctionLocalTestRunner(
        handler=handler,
        input=None,
        timeout=10,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.FAILED
    assert result.error is not None
    assert result.error.type == "TerminalScopeError"
    assert events == [
        "compensation failed",
        "cleanup failed",
        "cleanup succeeded",
    ]

    scope = result.get_context("multiple-failure-scope")
    assert scope.context_details is not None
    checkpoint_error = scope.context_details.error
    assert checkpoint_error is not None
    restored = _restore_sdk_control_error(
        checkpoint_error.message or "",
        checkpoint_error.type,
        checkpoint_error.data,
    )
    assert isinstance(restored, TerminalScopeError)
    assert restored.body_failure is not None
    assert restored.body_failure.phase is TerminalFailurePhase.BODY
    assert [failure.phase for failure in restored.action_failures] == [
        TerminalFailurePhase.COMPENSATION,
        TerminalFailurePhase.CLEANUP,
    ]
