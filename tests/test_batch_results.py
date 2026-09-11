"""Concurrent completion policies preserve observable outcomes across replay."""

import pytest

from async_durable_execution import (
    BatchItemStatus,
    CallableRuntimeError,
    CompletionConfig,
    CompletionDecision,
    CompletionReason,
    InvocationStatus,
    NestingType,
    RetryStrategy,
    ValidationError,
    create_local_runner,
    durable_execution,
    map,
    parallel,
    step,
    wait,
)


@pytest.mark.parametrize("nesting", list(NestingType))
@pytest.mark.parametrize("tolerated", [0, 1])
async def test_failed_batch_items_and_completion_reason_survive_replay(
    nesting, tolerated
):
    effects = []
    snapshots = []

    async def item(value):
        async def work():
            effects.append(value)
            if value == "bad":
                raise ValueError("permanent item failure")
            return value

        return await step(work, name="work", retry_strategy=RetryStrategy.none())

    @durable_execution
    async def handler(event):
        batch = await map(
            item,
            [None, "bad", "last"],
            max_concurrency=1,
            completion_config=CompletionConfig.thresholds(
                tolerated_failure_count=tolerated
            ),
            nesting_type=nesting,
            name="batch",
        )
        snapshots.append(batch)
        await wait(1, name="replay")
        return batch.to_dict()

    async with create_local_runner(handler=handler, timeout=3) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED, result.error
    assert len(snapshots) == 2 and snapshots[0] == snapshots[1]
    batch = snapshots[0]
    assert effects == ([None, "bad", "last"] if tolerated else [None, "bad"])
    assert batch.total_count == len(effects)
    assert batch.success_count == (2 if tolerated else 1)
    assert batch.failure_count == 1 and batch.has_failure
    assert batch.status is BatchItemStatus.FAILED
    assert batch.started_count == batch.cancelled_count == 0
    assert batch.started() == batch.cancelled() == []
    assert batch.get_results() == (["last"] if tolerated else [])
    assert batch.all[0].status is BatchItemStatus.SUCCEEDED
    assert batch.all[0].result is None
    assert [item.index for item in batch.failed()] == [1]
    assert batch.get_errors()[0].type == "ValueError"
    assert batch.get_errors()[0].message == "permanent item failure"
    assert batch.completion_reason is (
        CompletionReason.ALL_COMPLETED
        if tolerated
        else CompletionReason.FAILURE_TOLERANCE_EXCEEDED
    )
    with pytest.raises(CallableRuntimeError, match="permanent item failure"):
        batch.throw_if_error()


@pytest.mark.parametrize("nesting", list(NestingType))
async def test_custom_completion_omits_unstarted_branches_on_replay(nesting):
    effects = []
    snapshots = []

    async def item(value):
        effects.append(value)
        return value

    def complete(status):
        assert status.total_count == 4
        assert status.completed_count == status.success_count
        assert not status.all_completed
        return (
            CompletionDecision.complete(CompletionReason.CUSTOM_COMPLETION_SUCCEEDED)
            if status.success_count == 2
            else CompletionDecision.continue_execution()
        )

    @durable_execution
    async def handler(event):
        batch = await map(
            item,
            range(4),
            max_concurrency=1,
            completion_config=CompletionConfig.custom(complete),
            nesting_type=nesting,
            item_namer=lambda value, index: f"item-{index}",
        )
        snapshots.append(batch)
        await wait(1)
        return batch.get_results()

    async with create_local_runner(handler=handler, timeout=3) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED, result.error
    assert result.get_deserialized_result() == [0, 1]
    assert effects == [0, 1]
    assert len(snapshots) == 2 and snapshots[0] == snapshots[1]
    assert (
        snapshots[0].completion_reason is CompletionReason.CUSTOM_COMPLETION_SUCCEEDED
    )
    assert snapshots[0].total_count == 2


@pytest.mark.parametrize("operation", [map, parallel])
@pytest.mark.parametrize("invalid", [0, -1, True, 1.5, "2"])
async def test_invalid_concurrency_fails_before_starting_durable_work(
    operation, invalid
):
    async def unused(value=None):
        pytest.fail("Invalid concurrency must be rejected before launching work")

    @durable_execution
    async def handler(event):
        with pytest.raises(ValidationError, match="positive integer"):
            if operation is map:
                map(unused, [1], max_concurrency=invalid)
            else:
                parallel([unused], max_concurrency=invalid)

    async with create_local_runner(handler=handler, timeout=3) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED, result.error
    assert result.get_all_operations() == []


@pytest.mark.parametrize(
    "decision, error_type, message",
    [
        (True, "TypeError", "Completion policies must return CompletionDecision"),
        (
            CompletionDecision.continue_execution(),
            "InvalidStateError",
            "Completion policy did not finish",
        ),
    ],
)
async def test_invalid_custom_policy_fails_durably_instead_of_hanging(
    decision, error_type, message
):
    calls = []
    failures = []

    async def item():
        calls.append("item")
        return 1

    @durable_execution
    async def handler(event):
        try:
            await parallel(
                [item], completion_config=CompletionConfig.custom(lambda _: decision)
            )
        except CallableRuntimeError as error:
            failures.append(error.error_type)
            assert message in str(error)
        await wait(1)

    async with create_local_runner(handler=handler, timeout=3) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED, result.error
    assert calls == ["item"]
    assert failures == [error_type, error_type]
