"""Durable behavior verified entirely through public workflows and runners."""

import asyncio
import json
from dataclasses import dataclass
from datetime import timedelta
import pytest
from async_durable_execution import *


async def execute(body, value=None):
    async with create_local_runner(
        handler=durable_execution(body), input=value, timeout=3
    ) as runner:
        result = await runner.run()
    assert result.status is InvocationStatus.SUCCEEDED, result.error
    return result


async def test_step_wait_replays_only_workflow_code():
    effects = []
    entries = []

    async def work():
        effects.append(get_step_context().attempt)
        return {"value": 7}

    async def handler(event):
        entries.append(event)
        task = step(work, name="work")
        assert isinstance(task, asyncio.Task)
        value = await task
        await wait(timedelta(seconds=20), name="delay")
        return value

    result = await execute(handler, {"input": 1})
    assert effects == [1]
    assert len(entries) == 2
    assert result.get_deserialized_result() == {"value": 7}
    assert result.get_step("work").status is OperationStatus.SUCCEEDED
    assert result.get_wait("delay").status is OperationStatus.SUCCEEDED


@pytest.mark.parametrize("semantics", list(StepSemantics))
async def test_step_retry_then_success(semantics):
    attempts = []

    async def work():
        count = get_step_context().attempt
        attempts.append(count)
        if count < 3:
            raise ValueError("try again")
        return count

    async def handler(event):
        return await step(
            work,
            retry_strategy=RetryStrategy(
                initial_delay=1, jitter_strategy=JitterStrategy.NONE
            ),
            step_semantics=semantics,
            name="retry",
        )

    result = await execute(handler)
    assert attempts == [1, 2, 3]
    assert result.get_deserialized_result() == 3
    assert result.get_step("retry").step_details.attempt == 3


async def test_exhausted_step_failure_replays_without_work():
    effects = []

    async def work():
        effects.append(1)
        raise ValueError("permanent")

    async def handler(event):
        try:
            await step(work, name="fails", retry_strategy=RetryStrategy.none())
        except CallableRuntimeError as error:
            assert error.error_type == "ValueError"
        await wait(1)
        return "handled"

    result = await execute(handler)
    assert effects == [1]
    assert result.get_deserialized_result() == "handled"


@pytest.mark.parametrize("count", [0, 1, 4])
@pytest.mark.parametrize("nesting", list(NestingType))
async def test_map_waits_preserve_bounds_and_results(count, nesting):
    effects = []

    async def item(value):
        context = get_map_item_context()
        assert context.index == value

        async def work():
            effects.append(value)
            return value * 2

        result = await step(work, name="item-work")
        await wait(1)
        return result

    async def handler(event):
        result = await map(
            item, range(count), max_concurrency=2, nesting_type=nesting, name="items"
        )
        return result.get_results()

    result = await execute(handler)
    assert result.get_deserialized_result() == [i * 2 for i in range(count)]
    assert sorted(effects) == list(range(count))


@pytest.mark.parametrize("nesting", list(NestingType))
@pytest.mark.parametrize("size", [10, 100000])
async def test_early_completed_group_never_restarts_cancelled_effect(nesting, size):
    effects = []
    results = []

    async def fragment():
        return "x" * size

    async def winner():
        return "".join([await step(fragment, name=f"fragment-{i}") for i in range(3)])

    async def slow():
        effects.append("slow")
        await asyncio.sleep(10)
        return "loser"

    async def loser():
        return await step(slow, name="losing-effect")

    @durable_execution
    async def handler(event):
        result = await parallel(
            [winner, loser],
            name="race",
            completion_config=CompletionConfig.first_successful(),
            nesting_type=nesting,
        )
        results.append((result.get_results(), result.cancelled_count))
        callback: Callback = await create_callback(name="approval")
        return await callback.result()

    async with create_local_runner(
        handler=handler, timeout=3, poll_interval=0.001
    ) as runner:
        arn = await runner.run_async()
        callback = await runner.wait_for_callback(arn, name="approval")
        before = list(effects)
        await runner.send_callback_success(callback, b"approved")
        result = await runner.wait_for_result(arn)
    assert before == ["slow"] and effects == before
    assert len(results) == 2 and results[0] == results[1]
    assert results[0] == (["x" * (3 * size)], 1)
    assert result.get_deserialized_result() == "approved"


async def test_extension_reservation_order_is_independent_of_start_order():
    effects = []

    async def body(event):
        extension = get_extension_context()
        left = extension.reserve("left", local_operation_id="left")
        right = extension.reserve("right", local_operation_id="right")

        async def run_left(state):
            effects.append("left")
            return ExtensionStepResult.succeed("L")

        async def run_right(state):
            effects.append("right")
            return ExtensionStepResult.succeed("R")

        r = right.step(run_right, sub_type="AcmeStep")
        l = left.step(run_left, sub_type="AcmeStep")
        values = await asyncio.gather(l, r)
        await wait(1)
        return values

    result = await execute(body)
    assert sorted(effects) == ["left", "right"]
    assert result.get_deserialized_result() == ["L", "R"]


async def test_stateful_extension_and_polling_resume_state():
    states = []

    async def advance(state):
        states.append(state)
        return (
            ExtensionStepResult.succeed(state)
            if state == 2
            else ExtensionStepResult.retry(state + 1, 1)
        )

    async def check(state):
        return (state or 0) + 1

    async def handler(event):
        value = (
            await get_extension_context()
            .reserve("stateful")
            .step(advance, initial_state=0, sub_type="AcmePoll")
        )
        polled = await wait_for_condition(
            check, polling_strategy=lambda value, attempt: None if value == 3 else 1
        )
        return value, polled

    result = await execute(handler)
    assert states == [0, 1, 2]
    assert result.get_deserialized_result() == [2, 3]


async def test_polling_exhaustion_is_durable_and_catchable():
    calls = []

    async def check(state):
        calls.append(1)
        return False

    async def handler(event):
        try:
            await wait_for_condition(
                check, polling_strategy=PollingStrategy(max_attempts=2, initial_delay=1)
            )
        except WaitForConditionError:
            pass
        await wait(1)
        return "handled"

    result = await execute(handler)
    assert len(calls) == 2
    assert result.get_deserialized_result() == "handled"


async def test_scope_namespace_and_step_context_restriction():
    effects = []

    async def work():
        with pytest.raises(RuntimeError):
            get_durable_context()
        with pytest.raises(RuntimeError):
            wait(1)
        effects.append(get_step_context().operation_id)
        return 1

    async def child():
        return await step(work, name="work")

    async def handler(event):
        a, b = await asyncio.gather(
            run_in_child_context(child, name="left"),
            run_in_child_context(child, name="right"),
        )
        await wait(1)
        return a + b

    result = await execute(handler)
    assert len(set(effects)) == 2
    assert len(result.get_child_operations(result.get_context("left"))) == 1


async def test_retry_scope_context_attempts():
    attempts = []

    async def body():
        attempt = get_with_retry_context().attempt
        attempts.append(attempt)
        if attempt < 2:
            raise ValueError("again")
        return await step(lambda: asyncio.sleep(0, result=8))

    async def handler(event):
        return await with_retry(body, retry_strategy=RetryStrategy(initial_delay=1))

    result = await execute(handler)
    assert result.get_deserialized_result() == 8
    assert 2 in attempts


async def test_callback_submission_and_failure():
    submissions = []

    async def submit():
        submissions.append(get_wait_for_callback_context().callback_id)

    @durable_execution
    async def handler(event):
        try:
            return await wait_for_callback(submit, name="job")
        except CallbackError:
            return "rejected"

    async with create_local_runner(
        handler=handler, timeout=3, poll_interval=0.001
    ) as runner:
        arn = await runner.run_async()
        callback = await runner.wait_for_callback(arn, name="job-callback")
        await runner.send_callback_failure(callback, ErrorObject("denied", "Denied"))
        result = await runner.wait_for_result(arn)
    assert submissions == [callback]
    assert result.get_deserialized_result() == "rejected"


async def test_invoke_mock_and_custom_serialization():
    @durable_execution
    async def handler(event):
        return await invoke("worker:1", {"value": 2}, name="invoke")

    async with create_local_runner(handler=handler) as runner:
        runner.mock_invoke_result("worker:1", {"result": 3})
        result = await runner.run()
    assert result.get_deserialized_result() == {"result": 3}


@pytest.mark.parametrize("fail", [False, True])
async def test_terminal_actions_suspend_then_run_in_reverse_order(fail):
    actions = []

    async def action(label):
        actions.append(label)

    async def body(registry):
        registry.compensate(lambda: action("undo1"))
        registry.compensate(lambda: action("undo2"))
        registry.cleanup(lambda: action("clean1"))
        registry.cleanup(lambda: action("clean2"))
        await wait(1)
        if fail:
            raise ValueError("body failure")
        return "ok"

    async def handler(event):
        try:
            return await terminal_scope(body)
        except CallableRuntimeError:
            return "failed"

    result = await execute(handler)
    assert actions == (["undo2", "undo1"] if fail else []) + ["clean2", "clean1"]
    assert result.get_deserialized_result() == ("failed" if fail else "ok")


async def test_terminal_failure_summaries_survive_replay():
    effects = []

    async def bad():
        effects.append(1)
        raise ValueError("cleanup")

    async def body(actions):
        actions.cleanup(bad, retry_strategy=RetryStrategy.none())
        return 1

    async def handler(event):
        try:
            await terminal_scope(body)
        except TerminalScopeError as error:
            assert error.action_failures[0].phase is TerminalFailurePhase.CLEANUP
        await wait(1)
        return "done"

    result = await execute(handler)
    assert effects == [1]
    assert result.get_deserialized_result() == "done"


async def test_cancelled_terminal_scope_defaults_skip_actions():
    effects = []

    async def cleanup():
        effects.append("cleanup")

    async def body(actions):
        actions.cleanup(cleanup)
        await asyncio.sleep(10)

    async def handler(event):
        task = terminal_scope(body)
        await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        return 1

    await execute(handler)
    assert effects == []


@pytest.mark.parametrize("value", [0, False, "", None, [], {}])
async def test_falsey_step_results(value):
    async def handler(event):
        result = await step(lambda: asyncio.sleep(0, result=value))
        await wait(1)
        return result

    assert (await execute(handler)).get_deserialized_result() == value


async def test_recursive_helpers_and_metadata_remain_public():
    values = []

    async def work():
        context = get_step_context()
        return context.recursive_level

    @durable_execution
    async def handler(event):
        snapshot = (
            (await now()).isoformat(),
            await timestamp(),
            str(await uuid()),
            await random(),
        )
        values.append(snapshot)
        await wait(1)
        return [
            await step(work),
            await recurse(
                {"level": 2}, function_name="self:1", with_recursive_level=True
            ),
        ]

    async with create_local_runner(handler=handler, input={}, timeout=3) as runner:
        runner.mock_invoke_result("self:1", {"ok": True})
        result = await runner.run()
    assert result.status is InvocationStatus.SUCCEEDED, result.error
    assert values[0] == values[1]
    assert result.get_deserialized_result() == [0, {"ok": True}]


async def test_nonretryable_invocation_error_fails_once():
    calls = []

    class Permanent(InvocationError):
        def is_retryable(self):
            return False

    @durable_execution
    async def handler(event):
        calls.append(1)
        raise Permanent("configuration")

    async with create_local_runner(handler=handler) as runner:
        result = await runner.run()
    assert result.status is InvocationStatus.FAILED
    assert calls == [1]


async def test_configured_cancellation_runs_terminal_actions():
    effects = []

    async def cleanup():
        effects.append("cleanup")

    async def body(actions):
        actions.cleanup(cleanup)
        await asyncio.sleep(10)

    async def handler(event):
        task = terminal_scope(
            body, config=TerminalScopeConfig(cleanup_on_cancellation=True)
        )
        await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        return "cancelled"

    await execute(handler)
    assert effects == ["cleanup"]


async def test_extension_all_primitives_share_the_journal():
    @durable_execution
    async def handler(event):
        extension = get_extension_context()
        child = extension.reserve("child")

        async def nested():
            callback: Callback = (
                await get_extension_context()
                .reserve("callback")
                .create_callback(sub_type="CustomCallback")
            )
            return await callback.result()

        value = await child.run_in_child_context(nested, sub_type="CustomContext")
        await extension.reserve("wait").wait(1, sub_type="CustomWait")
        result = await extension.reserve("invoke").invoke(
            "worker:1", {}, sub_type="CustomInvoke"
        )
        return value, result

    async with create_local_runner(
        handler=handler, timeout=3, poll_interval=0.001
    ) as runner:
        runner.mock_invoke_result("worker:1", 7)
        arn = await runner.run_async()
        callback = await runner.wait_for_callback(arn, name="callback")
        await runner.send_callback_success(callback, b"ok")
        result = await runner.wait_for_result(arn)
    assert result.status is InvocationStatus.SUCCEEDED, result.error
    assert result.get_deserialized_result() == ["ok", 7]


async def test_heartbeat_extends_callback_deadline():
    @durable_execution
    async def handler(event):
        callback: Callback = await create_callback(
            name="heartbeat", timeout=5, heartbeat_timeout=1
        )
        return await callback.result()

    async with create_local_runner(
        handler=handler, poll_interval=0.001, timeout=3
    ) as runner:
        arn = await runner.run_async()
        callback = await runner.wait_for_callback(arn, name="heartbeat")
        await asyncio.sleep(0.6)
        await runner.send_callback_heartbeat(callback)
        await asyncio.sleep(0.6)
        await runner.send_callback_success(callback, b"alive")
        result = await runner.wait_for_result(arn)
    assert result.status is InvocationStatus.SUCCEEDED, result.error
    assert result.get_deserialized_result() == "alive"


async def test_permanent_invocation_failure_in_child_is_checkpointed():
    calls = []

    class Permanent(InvocationError):
        def is_retryable(self):
            return False

    async def child():
        calls.append(1)
        raise Permanent("fatal")

    async def handler(event):
        try:
            await run_in_child_context(child)
        except InvocationError as error:
            assert not error.is_retryable()
        await wait(1)
        return "handled"

    result = await execute(handler)
    assert calls == [1] and result.get_deserialized_result() == "handled"


async def test_lambda_context_exposes_standard_optional_metadata():
    async def handler(event):
        context = get_durable_context().lambda_context
        for attribute in (
            "aws_request_id",
            "log_group_name",
            "log_stream_name",
            "function_name",
            "memory_limit_in_mb",
            "function_version",
            "invoked_function_arn",
            "tenant_id",
            "client_context",
            "identity",
        ):
            assert hasattr(context, attribute), attribute
        assert context.get_remaining_time_in_millis() >= 0
        return "ok"

    await execute(handler)


async def test_callback_delivered_inside_submitter_is_visible_immediately():
    submitted = []

    async def submit():
        callback = get_wait_for_callback_context().callback_id
        submitted.append(callback)
        await runner.send_callback_success(callback, b"immediate")

    @durable_execution
    async def handler(event):
        return await wait_for_callback(submit, name="inline")

    async with create_local_runner(handler=handler, timeout=3) as runner:
        result = await runner.run()
    assert result.status is InvocationStatus.SUCCEEDED, result.error
    assert result.get_deserialized_result() == "immediate" and len(submitted) == 1
