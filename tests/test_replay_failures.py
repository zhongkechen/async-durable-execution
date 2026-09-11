"""Failure and interruption contracts exercised through public durable workflows."""

import asyncio

import pytest

from async_durable_execution import (
    Callback,
    CallableRuntimeError,
    ExtensionStepResult,
    InvocationError,
    InvocationStatus,
    RetryStrategy,
    StepInterruptedError,
    StepSemantics,
    create_local_runner,
    create_callback,
    durable_execution,
    get_extension_context,
    get_step_context,
    step,
    wait,
)


@pytest.mark.parametrize("semantics", list(StepSemantics))
async def test_interrupted_step_obeys_attempt_semantics(semantics):
    attempts = []
    entries = []

    async def work():
        attempts.append(get_step_context().attempt)
        if len(attempts) == 1:
            raise InvocationError("invocation interrupted after starting work")
        return {"saved": True}

    @durable_execution
    async def handler(event):
        entries.append(event)
        result = await step(
            work,
            name="interrupted",
            step_semantics=semantics,
            retry_strategy=RetryStrategy(max_attempts=2, initial_delay=1),
        )
        await wait(1, name="replay-completion")
        return result

    async with create_local_runner(handler=handler, timeout=3) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED, result.error
    assert result.get_deserialized_result() == {"saved": True}
    # At-most-once skips the interrupted attempt and only runs work in a new retry.
    expected = [1, 2] if semantics is StepSemantics.AT_MOST_ONCE_PER_RETRY else [1, 1]
    assert attempts == expected
    assert result.get_step("interrupted").step_details.attempt == expected[-1]
    assert len(entries) >= 3


async def test_at_most_once_without_retry_does_not_repeat_interrupted_work():
    calls = []

    async def work():
        calls.append(get_step_context().operation_id)
        raise InvocationError("lost invocation")

    @durable_execution
    async def handler(event):
        try:
            await step(
                work,
                name="once",
                step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY,
                retry_strategy=RetryStrategy.none(),
            )
        except StepInterruptedError as error:
            return error.step_id

    async with create_local_runner(handler=handler, timeout=3) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED, result.error
    assert calls == [result.get_step("once").operation_id]
    assert result.get_deserialized_result() == calls[0]


async def test_retry_policy_failure_is_checkpointed_and_replayed():
    effects = []
    decisions = []
    observed = []

    async def work():
        effects.append("work")
        raise ValueError("temporary")

    def broken_policy(error, attempt):
        decisions.append((str(error), attempt))
        raise TypeError("invalid retry configuration")

    @durable_execution
    async def handler(event):
        try:
            await step(work, name="failure", retry_strategy=broken_policy)
        except CallableRuntimeError as error:
            observed.append((error.error_type, str(error)))
        await wait(1, name="replay")
        return "handled"

    async with create_local_runner(handler=handler, timeout=3) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED, result.error
    assert effects == ["work"]
    assert decisions == [("temporary", 1)]
    assert len(observed) == 2 and observed[0] == observed[1]
    assert observed[0][0] == "TypeError"
    assert "invalid retry configuration" in observed[0][1]


async def test_stateful_error_retry_checkpoints_replacement_state():
    states = []
    decisions = []

    async def advance(state):
        states.append((state, get_step_context().attempt))
        if state["cursor"] == 0:
            raise ValueError("resume at next page")
        return ExtensionStepResult.succeed(state)

    def retry(error, state, attempt):
        decisions.append((str(error), state, attempt))
        return ExtensionStepResult.retry({"cursor": 1}, 1)

    @durable_execution
    async def handler(event):
        result = (
            await get_extension_context()
            .reserve("pages")
            .step(
                advance,
                sub_type="PageFetch",
                initial_state={"cursor": 0},
                retry_strategy=retry,
            )
        )
        await wait(1)
        return result

    async with create_local_runner(handler=handler, timeout=3) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED, result.error
    assert result.get_deserialized_result() == {"cursor": 1}
    assert states == [({"cursor": 0}, 1), ({"cursor": 1}, 2)]
    assert decisions == [("resume at next page", {"cursor": 0}, 1)]


@pytest.mark.parametrize("policy_result", [1, ExtensionStepResult.succeed("done")])
async def test_invalid_stateful_retry_result_is_a_durable_failure(policy_result):
    calls = []
    failures = []

    async def work(state):
        calls.append(state)
        raise ValueError("failed")

    @durable_execution
    async def handler(event):
        try:
            await (
                get_extension_context()
                .reserve("stateful")
                .step(
                    work,
                    sub_type="CustomStep",
                    initial_state="initial",
                    retry_strategy=lambda error, state, attempt: policy_result,
                )
            )
        except CallableRuntimeError as error:
            failures.append(error.error_type)
        await wait(1)

    async with create_local_runner(handler=handler, timeout=3) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED, result.error
    assert calls == ["initial"]
    assert failures == ["TypeError", "TypeError"]


async def test_result_wait_timeout_leaves_callback_execution_running():
    @durable_execution
    async def handler(event):
        callback: Callback = await create_callback(name="approval")
        return await callback.result()

    async with create_local_runner(handler=handler, timeout=3) as runner:
        arn = await runner.run_async()
        callback = await runner.wait_for_callback(arn, name="approval")
        with pytest.raises(asyncio.TimeoutError):
            await runner.wait_for_result(arn, timeout=0)
        with pytest.raises(RuntimeError, match="active"):
            await runner.run_async()
        await runner.send_callback_success(callback, b"approved")
        result = await runner.wait_for_result(arn)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == "approved"
