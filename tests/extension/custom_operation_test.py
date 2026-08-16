from __future__ import annotations

import hashlib

import pytest

from async_durable_execution import (
    ExtensionStepResult,
    InvocationStatus,
    NestingType,
    OperationStatus,
    OperationType,
    StepSemantics,
    create_local_runner,
    durable_execution,
    get_extension_context,
    get_with_retry_context,
    parallel,
    wait,
    with_retry,
)


def _operation_id(
    local_id: str,
    parent_id: str | None = None,
    *,
    explicit: bool = False,
) -> str:
    value = f"local:{local_id}" if explicit else local_id
    namespaced = f"{parent_id}-{value}" if parent_id else value
    return hashlib.blake2b(namespaced.encode()).hexdigest()[:64]


async def _run(handler):
    async with create_local_runner(
        handler=handler,
        input={},
        poll_interval=0.01,
        timeout=10,
    ) as runner:
        return await runner.run()


async def test_reserved_operations_keep_ids_when_launch_order_changes(monkeypatch):
    monkeypatch.setenv("DURABLE_EXECUTION_TIME_SCALE", "0.01")
    executions = 0

    async def left(_state):
        return ExtensionStepResult.succeed("L")

    async def right(_state):
        return ExtensionStepResult.succeed("R")

    @durable_execution
    async def handler(_event):
        nonlocal executions
        extension = get_extension_context()
        left_operation = extension.reserve("left")
        right_operation = extension.reserve("right")
        pause_operation = extension.reserve("pause")

        executions += 1
        if executions % 2:
            left_task = left_operation.step(left, sub_type="AcmeLeft")
            right_task = right_operation.step(right, sub_type="AcmeRight")
        else:
            right_task = right_operation.step(right, sub_type="AcmeRight")
            left_task = left_operation.step(left, sub_type="AcmeLeft")

        result = (await left_task) + (await right_task)
        await pause_operation.wait(1, sub_type="AcmePause")
        return result

    result = await _run(handler)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == "LR"
    assert executions >= 2

    left_operation = result.get_step("left")
    right_operation = result.get_step("right")
    pause_operation = result.get_wait("pause")
    assert left_operation.operation_id == _operation_id("1")
    assert right_operation.operation_id == _operation_id("2")
    assert pause_operation.operation_id == _operation_id("3")
    assert left_operation.sub_type == "AcmeLeft"
    assert right_operation.sub_type == "AcmeRight"
    assert pause_operation.sub_type == "AcmePause"


async def test_custom_local_ids_survive_reservation_reordering(monkeypatch):
    monkeypatch.setenv("DURABLE_EXECUTION_TIME_SCALE", "0.01")
    executions = 0

    async def value(state):
        return ExtensionStepResult.succeed(state)

    @durable_execution
    async def handler(_event):
        nonlocal executions
        extension = get_extension_context()
        executions += 1
        if executions % 2:
            first = extension.reserve("first", local_operation_id="node-a")
            second = extension.reserve("second", local_operation_id="node-b")
        else:
            second = extension.reserve("second", local_operation_id="node-b")
            first = extension.reserve("first", local_operation_id="node-a")
        pause = extension.reserve("pause", local_operation_id="pause")

        first_value = await first.step(
            value,
            sub_type="AcmeNode",
            initial_state="A",
        )
        second_value = await second.step(
            value,
            sub_type="AcmeNode",
            initial_state="B",
        )
        await pause.wait(1, sub_type="AcmePause")
        return first_value + second_value

    result = await _run(handler)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == "AB"
    assert executions >= 2
    assert result.get_step("first").operation_id == _operation_id(
        "node-a",
        explicit=True,
    )
    assert result.get_step("second").operation_id == _operation_id(
        "node-b",
        explicit=True,
    )
    assert result.get_wait("pause").operation_id == _operation_id(
        "pause",
        explicit=True,
    )


async def test_stateful_extension_step_checkpoints_state_between_attempts(monkeypatch):
    monkeypatch.setenv("DURABLE_EXECUTION_TIME_SCALE", "0.01")
    observed_states = []

    async def poll(state):
        observed_states.append(state)
        if state < 2:
            return ExtensionStepResult.retry(state + 1, 1)
        return ExtensionStepResult.succeed(state)

    @durable_execution
    async def handler(_event):
        return (
            await get_extension_context()
            .reserve("poll")
            .step(
                poll,
                sub_type="AcmePoll",
                initial_state=0,
            )
        )

    result = await _run(handler)

    operation = result.get_step("poll")
    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == 2
    assert observed_states == [0, 1, 2]
    assert operation.status is OperationStatus.SUCCEEDED
    assert operation.sub_type == "AcmePoll"
    assert operation.step_details is not None
    assert operation.step_details.attempt == 3


async def test_extension_step_exception_retry_strategy_can_replace_state(monkeypatch):
    monkeypatch.setenv("DURABLE_EXECUTION_TIME_SCALE", "0.01")
    observed_states = []

    async def work(state):
        observed_states.append(state)
        if state == "initial":
            raise RuntimeError("retry")
        return ExtensionStepResult.succeed(state)

    def retry_strategy(error, state, attempt):
        assert isinstance(error, RuntimeError)
        assert state == "initial"
        assert attempt == 1
        return ExtensionStepResult.retry("retried", 1)

    @durable_execution
    async def handler(_event):
        return (
            await get_extension_context()
            .reserve("retry")
            .step(
                work,
                sub_type="AcmeRetry",
                initial_state="initial",
                retry_strategy=retry_strategy,
                step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY,
            )
        )

    result = await _run(handler)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == "retried"
    assert observed_states == ["initial", "retried"]
    assert result.get_step("retry").step_details.attempt == 2


async def test_extension_can_create_custom_child_context_with_nested_reservation():
    async def nested_step(_state):
        return ExtensionStepResult.succeed("nested")

    async def child():
        return (
            await get_extension_context()
            .reserve(
                "nested",
                local_operation_id="node",
            )
            .step(
                nested_step,
                sub_type="AcmeNestedStep",
            )
        )

    @durable_execution
    async def handler(_event):
        return (
            await get_extension_context()
            .reserve(
                "child",
                local_operation_id="child",
            )
            .run_in_child_context(
                child,
                sub_type="AcmeContext",
            )
        )

    result = await _run(handler)

    child_operation = result.get_context("child")
    nested_operation = result.get_child_operations(child_operation)[0]
    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == "nested"
    assert child_operation.operation_type is OperationType.CONTEXT
    assert child_operation.sub_type == "AcmeContext"
    assert child_operation.operation_id == _operation_id("child", explicit=True)
    assert nested_operation.operation_type is OperationType.STEP
    assert nested_operation.sub_type == "AcmeNestedStep"
    assert nested_operation.operation_id == _operation_id(
        "node",
        child_operation.operation_id,
        explicit=True,
    )


async def test_bounded_parallel_restarts_suspended_spi_branches(monkeypatch):
    monkeypatch.setenv("DURABLE_EXECUTION_TIME_SCALE", "0.01")
    branch_attempts = [0, 0]

    def create_branch(index):
        async def branch():
            branch_attempts[index] += 1
            await wait(1, name=f"pause-{index}")
            return index

        return branch

    @durable_execution
    async def handler(_event):
        result = await parallel(
            [create_branch(0), create_branch(1)],
            name="bounded",
            max_concurrency=1,
        )
        return result.get_results()

    result = await _run(handler)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == [0, 1]
    assert branch_attempts == [2, 2]
    waits = {
        operation.name: operation
        for operation in result.get_all_operations()
        if operation.operation_type is OperationType.WAIT
    }
    assert waits["pause-0"].status is OperationStatus.SUCCEEDED
    assert waits["pause-1"].status is OperationStatus.SUCCEEDED
    parallel_operation = result.get_context("bounded")
    branches = {
        operation.name: operation
        for operation in result.get_child_operations(parallel_operation)
    }
    assert branches["parallel-branch-0"].operation_id == _operation_id(
        "0",
        parallel_operation.operation_id,
    )
    assert branches["parallel-branch-1"].operation_id == _operation_id(
        "1",
        parallel_operation.operation_id,
    )


async def test_parallel_checkpointed_siblings_each_replay(monkeypatch):
    monkeypatch.setenv("DURABLE_EXECUTION_TIME_SCALE", "0.01")
    observed_replay = [[], []]
    delays = [1, 100]

    def create_branch(index):
        async def branch():
            observed_replay[index].append(get_extension_context().is_replaying())
            await wait(delays[index], name=f"pause-{index}")
            return index

        return branch

    @durable_execution
    async def handler(_event):
        result = await parallel(
            [create_branch(0), create_branch(1)],
            name="siblings",
        )
        return result.get_results()

    result = await _run(handler)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == [0, 1]
    assert observed_replay[0] == [False, True]
    assert observed_replay[1][0] is False
    assert all(observed_replay[1][1:])


async def test_extension_reservations_are_one_shot():
    async def work(_state):
        return ExtensionStepResult.succeed("done")

    @durable_execution
    async def handler(_event):
        reservation = get_extension_context().reserve("one-shot")
        task = reservation.step(work, sub_type="AcmeStep")
        with pytest.raises(
            RuntimeError,
            match="reservation can only be used once",
        ):
            reservation.step(work, sub_type="AcmeStep")
        return await task

    result = await _run(handler)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == "done"


async def test_captured_extension_context_cannot_reserve_inside_step():
    async def outer(_state):
        assert captured is not None
        captured.reserve("nested")
        return ExtensionStepResult.succeed("done")

    @durable_execution
    async def handler(_event):
        nonlocal captured
        captured = get_extension_context()
        return await captured.reserve("outer").step(
            outer,
            sub_type="AcmeOuter",
        )

    captured = None
    result = await _run(handler)

    assert result.status is InvocationStatus.FAILED
    assert result.get_step("outer").status is OperationStatus.FAILED


async def test_reserved_operation_cannot_be_claimed_inside_step():
    async def inner(_state):
        return ExtensionStepResult.succeed("nested")

    async def outer(_state):
        assert nested is not None
        return ExtensionStepResult.succeed(
            await nested.step(inner, sub_type="AcmeNested")
        )

    @durable_execution
    async def handler(_event):
        nonlocal nested
        extension = get_extension_context()
        nested = extension.reserve("nested")
        return await extension.reserve("outer").step(
            outer,
            sub_type="AcmeOuter",
        )

    nested = None
    result = await _run(handler)

    assert result.status is InvocationStatus.FAILED
    assert result.get_step("outer").status is OperationStatus.FAILED


async def test_extension_operation_names_must_be_nonblank():
    async def work(_state):
        return ExtensionStepResult.succeed("done")

    @durable_execution
    async def handler(_event):
        extension = get_extension_context()
        for name in ("", "   "):
            with pytest.raises(ValueError, match="name must not be blank"):
                extension.reserve(name)

        return await extension.reserve("valid").step(
            work,
            sub_type="AcmeStep",
        )

    result = await _run(handler)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_step("valid").operation_id == _operation_id("1")


async def test_virtual_child_preserves_replay_state_and_advances_parent(monkeypatch):
    monkeypatch.setenv("DURABLE_EXECUTION_TIME_SCALE", "0.01")
    observed_parent_replay = []
    observed_replay = []

    async def child():
        observed_replay.append(get_extension_context().is_replaying())
        await wait(1, name="pause")
        return "done"

    @durable_execution
    async def handler(_event):
        result = (
            await get_extension_context()
            .reserve("virtual")
            .run_in_child_context(
                child,
                sub_type="AcmeVirtual",
                is_virtual=True,
            )
        )
        observed_parent_replay.append(get_extension_context().is_replaying())
        return result

    result = await _run(handler)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == "done"
    assert observed_replay == [False, True]
    assert observed_parent_replay == [False]


async def test_flat_parallel_branch_preserves_replay_state(monkeypatch):
    monkeypatch.setenv("DURABLE_EXECUTION_TIME_SCALE", "0.01")
    observed_replay = []

    async def branch():
        observed_replay.append(get_extension_context().is_replaying())
        await wait(1, name="branch-pause")
        return "done"

    @durable_execution
    async def handler(_event):
        result = await parallel(
            [branch],
            nesting_type=NestingType.FLAT,
        )
        return result.get_results()

    result = await _run(handler)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == ["done"]
    assert observed_replay == [False, True]


async def test_late_flat_parallel_branch_preserves_initial_replay_state(monkeypatch):
    monkeypatch.setenv("DURABLE_EXECUTION_TIME_SCALE", "0.01")
    observed_after_parallel = []
    observed_replay = {0: [], 1: []}

    def branch(index):
        async def run():
            observed_replay[index].append(get_extension_context().is_replaying())
            await wait(1, name=f"branch-{index}-pause")
            return index

        return run

    @durable_execution
    async def handler(_event):
        result = await parallel(
            [branch(0), branch(1)],
            max_concurrency=1,
            nesting_type=NestingType.FLAT,
        )
        observed_after_parallel.append(get_extension_context().is_replaying())
        return result.get_results()

    result = await _run(handler)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == [0, 1]
    assert observed_replay[1] == [True, True]
    assert observed_after_parallel == [False]


async def test_virtual_retry_scope_preserves_replay_state(monkeypatch):
    monkeypatch.setenv("DURABLE_EXECUTION_TIME_SCALE", "0.01")
    observed_replay = []

    async def operation():
        observed_replay.append(get_with_retry_context().is_replaying())
        await wait(1, name="retry-pause")
        return "done"

    @durable_execution
    async def handler(_event):
        return await with_retry(
            operation,
            name="virtual-retry",
            is_virtual=True,
        )

    result = await _run(handler)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == "done"
    assert observed_replay == [False, True]
