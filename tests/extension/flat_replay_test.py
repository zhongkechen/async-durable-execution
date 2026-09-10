"""Completed flat aggregates replay through the public Lambda handler contract."""

import asyncio
import copy
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
import json
from types import SimpleNamespace

import pytest

from async_durable_execution import (
    CompletionConfig,
    CompletionDecision,
    CompletionReason,
    ExecutionError,
    NestingType,
    SerDes,
    create_callback,
    durable_execution,
    map as durable_map,
    parallel,
    step,
)


class LambdaHistory:
    """Minimal STEP/CONTEXT service fixture retaining the exact checkpoint payloads."""

    def __init__(self):
        self.operations = {
            "root": {
                "Id": "root",
                "Type": "EXECUTION",
                "Status": "STARTED",
                "StartTimestamp": datetime(2026, 9, 10, tzinfo=timezone.utc),
                "ExecutionDetails": {"InputPayload": "{}"},
            }
        }
        self.calls = 0
        self.token = "token-0"

    async def checkpoint_durable_execution(self, **request):
        assert request["CheckpointToken"] == self.token
        touched = {}
        for update in request["Updates"]:
            assert update["Type"] in ("STEP", "CONTEXT", "CALLBACK")
            record = self.operations.setdefault(
                update["Id"], {"Id": update["Id"], "Type": update["Type"]}
            )
            record.update(
                {
                    key: update[key]
                    for key in ("Name", "ParentId", "SubType")
                    if key in update
                }
            )
            record["Status"] = {
                "START": "STARTED",
                "SUCCEED": "SUCCEEDED",
                "FAIL": "FAILED",
            }[update["Action"]]
            field = {
                "STEP": "StepDetails",
                "CONTEXT": "ContextDetails",
                "CALLBACK": "CallbackDetails",
            }[update["Type"]]
            details = record.setdefault(field, {})
            if update["Type"] == "CALLBACK":
                details.setdefault("CallbackId", "callback-" + update["Id"])
            if update["Type"] == "STEP":
                details["Attempt"] = 1
            if "Payload" in update:
                details["Result"] = update["Payload"]
            if "Error" in update:
                details["Error"] = update["Error"]
            if "ContextOptions" in update:
                details["ReplayChildren"] = update["ContextOptions"]["ReplayChildren"]
            touched[update["Id"]] = record
        self.calls += 1
        self.token = f"token-{self.calls}"
        return {
            "CheckpointToken": self.token,
            "NewExecutionState": {"Operations": copy.deepcopy(list(touched.values()))},
        }

    def event(self):
        operations = json.loads(
            json.dumps(
                list(self.operations.values()),
                default=lambda d: int(d.timestamp() * 1000),
            )
        )
        return {
            "DurableExecutionArn": "arn:aws:lambda:us-west-2:123456789012:function:test:1/durable-execution/run/root",
            "CheckpointToken": self.token,
            "InitialExecutionState": {"Operations": operations},
        }

    def call(self, handler):
        context = SimpleNamespace(
            aws_request_id="test",
            function_name="test",
            function_version="1",
            get_remaining_time_in_millis=lambda: 900000,
        )
        return handler(self.event(), context)

    def legacy_summary(self, *, absent=False):
        for operation in self.operations.values():
            if operation["Type"] == "CONTEXT" and operation.get(
                "ContextDetails", {}
            ).get("ReplayChildren"):
                if absent:
                    operation["ContextDetails"].pop("Result", None)
                else:
                    operation["ContextDetails"]["Result"] = ""


def describe(result):
    return {
        "bytes": sum(len(value) for value in result.get_results()),
        "items": [
            [item.index, item.status.value, item.error.message if item.error else None]
            for item in result.all
        ],
        "reason": result.completion_reason.value,
    }


@pytest.mark.parametrize("kind", ["map", "parallel"])
def test_large_flat_result_replays_without_effects_or_checkpoints(kind):
    api = LambdaHistory()
    effects = []
    body_calls = []

    async def branch(index):
        body_calls.append(index)

        async def effect():
            effects.append(index)
            await asyncio.sleep(0)
            return "x" * 80000

        return await step(effect, name="value")

    @durable_execution(boto3_client=api)
    async def handler(event):
        if kind == "map":
            batch = await durable_map(branch, range(4), nesting_type=NestingType.FLAT)
        else:
            batch = await parallel(
                [lambda i=i: branch(i) for i in range(4)], nesting_type=NestingType.FLAT
            )
        return describe(batch)

    first = api.call(handler)
    assert first["Status"] == "SUCCEEDED"
    result = json.loads(first["Result"])
    assert result["bytes"] == 320000 and len(result["items"]) == 4
    calls = api.calls
    second = api.call(handler)
    assert second == first
    assert api.calls == calls
    assert sorted(effects) == list(range(4))
    assert sorted(body_calls) == sorted(list(range(4)) * 2)


@pytest.mark.parametrize("policy", ["first_successful", "custom", "failure"])
def test_large_flat_terminal_decision_does_not_restart_other_effects(policy):
    api = LambdaHistory()
    effects = []
    entries = []
    decisions = []

    async def winner():
        entries.append("winner")

        async def part():
            effects.append("part")
            return "x" * 80000

        return "".join([await step(part, name=f"part-{i}") for i in range(4)])

    async def other():
        entries.append("other")
        if policy == "failure":
            raise ValueError("failed before a durable operation")

        async def slow():
            effects.append("slow")
            await asyncio.Future()

        return await step(slow, name="unfinished")

    async def never():
        entries.append("never")

        async def work():
            effects.append("never")
            return "unexpected"

        return await step(work)

    def decide(status):
        decisions.append(status.success_count)
        return CompletionDecision(
            status.success_count > 0, CompletionReason.CUSTOM_COMPLETION_SUCCEEDED
        )

    config = (
        CompletionConfig.custom(decide)
        if policy == "custom"
        else CompletionConfig.all_successful()
        if policy == "failure"
        else CompletionConfig.first_successful()
    )

    @durable_execution(boto3_client=api)
    async def handler(event):
        return describe(
            await parallel(
                [winner, other, never],
                nesting_type=NestingType.FLAT,
                max_concurrency=1 if policy == "failure" else 2,
                completion_config=config,
            )
        )

    first = api.call(handler)
    assert first["Status"] == "SUCCEEDED"
    before = list(effects)
    calls = api.calls
    old_decisions = len(decisions)
    second = api.call(handler)
    assert second == first
    assert api.calls == calls and effects == before
    assert entries.count("winner") == 2
    assert "never" not in entries
    assert len(decisions) == old_decisions
    if policy != "failure":
        # Legacy summaries cannot prove the cancelled branch's outcome. Fail
        # explicitly before resuming its unfinished effect, without rewriting history.
        api.legacy_summary()
        failed = api.call(handler)
        assert failed["Status"] == "FAILED"
        assert "lacks terminal decision metadata" in str(failed)
        assert api.calls == calls and effects == before


@pytest.mark.parametrize(
    "incomplete", ["missing", "started", "cancelled", "timed_out", "stopped"]
)
def test_completed_flat_replay_never_restarts_an_unfinished_step(incomplete):
    api = LambdaHistory()
    effects = []

    async def branch():
        async def work():
            effects.append("work")
            return "x" * 80000

        return await step(work)

    @durable_execution(boto3_client=api)
    async def handler(event):
        return describe(await parallel([branch] * 4, nesting_type=NestingType.FLAT))

    assert api.call(handler)["Status"] == "SUCCEEDED"
    step_id = next(
        key for key, value in api.operations.items() if value["Type"] == "STEP"
    )
    if incomplete == "missing":
        del api.operations[step_id]
    else:
        api.operations[step_id]["Status"] = incomplete.upper()
    calls = api.calls
    result = api.call(handler)
    assert result["Status"] == "FAILED"
    assert "without a cached result or error" in str(result)
    assert len(effects) == 4 and api.calls == calls


def test_completed_flat_branch_can_read_an_existing_pending_callback_id():
    api = LambdaHistory()

    async def branch():
        callback = await create_callback(name="callback")
        return callback.callback_id + "x" * 80000

    @durable_execution(boto3_client=api)
    async def handler(event):
        return describe(await parallel([branch] * 4, nesting_type=NestingType.FLAT))

    first = api.call(handler)
    assert first["Status"] == "SUCCEEDED"
    calls = api.calls
    assert api.call(handler) == first
    assert api.calls == calls


@pytest.mark.parametrize("kind", ["map", "parallel"])
def test_completed_flat_replay_preserves_coordinated_branch_concurrency(kind):
    api = LambdaHistory()
    entries = []
    effects = []

    @durable_execution(boto3_client=api)
    async def handler(event):
        ready = asyncio.Event()
        started = []
        entries.append(started)

        async def branch(index):
            started.append(index)
            if index == 0:
                await ready.wait()
            else:
                ready.set()

            async def work():
                effects.append(index)
                return str(index) * 160000

            return await step(work, name="work")

        if kind == "map":
            task = durable_map(
                branch, range(2), nesting_type=NestingType.FLAT, max_concurrency=2
            )
        else:
            task = parallel(
                [lambda: branch(0), lambda: branch(1)],
                nesting_type=NestingType.FLAT,
                max_concurrency=2,
            )
        batch = await asyncio.wait_for(task, 2)
        return {
            "values": [value[0] for value in batch.get_results()],
            **describe(batch),
        }

    first = api.call(handler)
    assert first["Status"] == "SUCCEEDED"
    result = json.loads(first["Result"])
    assert result["bytes"] == 320000 and result["values"] == ["0", "1"]
    calls = api.calls
    assert api.call(handler) == first
    assert entries == [[0, 1], [0, 1]]
    assert sorted(effects) == [0, 1] and api.calls == calls


@pytest.mark.parametrize("limit", [1, 2, None])
def test_completed_flat_replay_respects_max_concurrency(limit):
    api = LambdaHistory()
    peaks = []
    effects = []

    @durable_execution(boto3_client=api)
    async def handler(event):
        active = peak = 0

        async def branch(index):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            try:
                await asyncio.sleep(0)

                async def work():
                    effects.append(index)
                    return str(index) * 80000

                return await step(work)
            finally:
                active -= 1

        result = await parallel(
            [lambda i=i: branch(i) for i in range(4)],
            nesting_type=NestingType.FLAT,
            max_concurrency=limit,
        )
        peaks.append(peak)
        assert active == 0
        return [value[0] for value in result.get_results()]

    first = api.call(handler)
    assert first["Status"] == "SUCCEEDED"
    assert json.loads(first["Result"]) == ["0", "1", "2", "3"]
    calls = api.calls
    assert api.call(handler) == first
    assert peaks == [limit or 4, limit or 4]
    assert sorted(effects) == list(range(4)) and api.calls == calls


@contextmanager
def track_created_tasks():
    """Track tasks in this context without including background queue readers."""
    loop = asyncio.get_running_loop()
    previous_factory = loop.get_task_factory()
    tracking = ContextVar("track_aggregate_tasks", default=False)
    created = []

    def factory(loop, coro, **kwargs):
        task = (
            previous_factory(loop, coro, **kwargs)
            if previous_factory is not None
            else asyncio.Task(coro, loop=loop, **kwargs)
        )
        context = kwargs.get("context")
        if context.get(tracking, False) if context is not None else tracking.get():
            created.append(task)
        return task

    token = tracking.set(True)
    loop.set_task_factory(factory)
    try:
        yield created
    finally:
        loop.set_task_factory(previous_factory)
        tracking.reset(token)


@pytest.mark.parametrize("mode", ["failure", "cancel"])
@pytest.mark.parametrize("terminal_helper", [False, True, "callback"])
def test_completed_flat_replay_drains_workers_before_returning(mode, terminal_helper):
    api = LambdaHistory()
    effects = []
    replay = False

    @durable_execution(boto3_client=api)
    async def handler(event):
        started = []
        finished = []
        both_started = asyncio.Event()

        async def branch(index):
            started.append(index)
            if len(started) == 2:
                both_started.set()
            try:
                if terminal_helper and index == 0:
                    if terminal_helper == "callback":
                        callback = await create_callback(name="parked-helper")
                        await callback.result()
                    await asyncio.Future()
                if replay:
                    if index == 0 or mode == "cancel":
                        await asyncio.Future()
                    await both_started.wait()

                async def work():
                    effects.append(index)
                    return "x" * 160000

                return await step(work, name=f"work-{index}")
            finally:
                finished.append(index)

        with track_created_tasks() as aggregate_tasks:
            task = parallel(
                [lambda i=i: branch(i) for i in range(3)],
                nesting_type=NestingType.FLAT,
                max_concurrency=2,
                completion_config=CompletionConfig(min_successful=2)
                if terminal_helper
                else None,
            )
            if not replay:
                return describe(await task)
            if mode == "cancel":
                try:
                    await asyncio.wait_for(both_started.wait(), 2)
                finally:
                    task.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await task
            else:
                with pytest.raises(
                    ExecutionError, match="without a cached result or error"
                ):
                    await asyncio.wait_for(task, 2)
            # Check before the invocation's global task cleanup can hide leaked workers.
            assert started == [0, 1]
            assert sorted(finished) == [0, 1]
            if terminal_helper == "callback":
                assert aggregate_tasks and all(task.done() for task in aggregate_tasks)
            return "drained"

    assert api.call(handler)["Status"] == "SUCCEEDED"
    if mode == "failure":
        key = next(
            key for key, op in api.operations.items() if op.get("Name") == "work-1"
        )
        del api.operations[key]
    replay = True
    calls = api.calls
    result = api.call(handler)
    assert result["Status"] == "SUCCEEDED", result
    assert json.loads(result["Result"]) == "drained"
    assert sorted(effects) == ([1, 2] if terminal_helper else [0, 1, 2])
    assert api.calls == calls


@pytest.mark.parametrize("kind", ["map", "parallel"])
@pytest.mark.parametrize(
    ("helper_mode", "late_status"),
    [
        (mode, None)
        for mode in (
            "failed",
            "failed_execution",
            "failed_step",
            "cancelled",
            "cancelled_step",
            "callback",
        )
    ]
    + [
        (mode, status)
        for mode in ("cancelled_step", "callback")
        for status in ("SUCCEEDED", "FAILED")
    ],
)
def test_completed_flat_replay_coordinates_with_terminal_helpers(
    kind, helper_mode, late_status
):
    api = LambdaHistory()
    entries = []
    exits = []
    effects = []
    failed = helper_mode.startswith("failed")

    @durable_execution(boto3_client=api)
    async def handler(event):
        ready = asyncio.Event()
        started, finished = [], []
        entries.append(started)
        exits.append(finished)

        async def branch(index):
            started.append(index)
            try:
                if index == 1:
                    callback = (
                        await create_callback(name="helper-callback")
                        if helper_mode == "callback"
                        else None
                    )
                    ready.set()
                    if helper_mode == "failed":
                        raise ValueError("expected helper failure")
                    if helper_mode == "failed_execution":
                        raise ExecutionError("recorded helper execution failure")
                    if helper_mode.endswith("_step"):

                        async def helper_effect():
                            effects.append("helper")
                            if failed:
                                raise ValueError("expected durable failure")
                            await asyncio.Future()

                        return await step(
                            helper_effect,
                            name="helper-step",
                            retry_strategy=lambda *_: None,
                        )
                    if callback is not None:
                        return await callback.result()
                    await asyncio.Future()
                await ready.wait()

                async def part():
                    effects.append("part")
                    return "x" * 80000

                return "".join([await step(part, name=f"part-{i}") for i in range(4)])
            finally:
                finished.append(index)

        config = (
            CompletionConfig(tolerated_failure_count=1)
            if failed
            else CompletionConfig.first_successful()
        )
        if kind == "map":
            task = durable_map(
                branch,
                range(2),
                nesting_type=NestingType.FLAT,
                max_concurrency=2,
                completion_config=config,
            )
        else:
            task = parallel(
                [lambda: branch(0), lambda: branch(1)],
                nesting_type=NestingType.FLAT,
                max_concurrency=2,
                completion_config=config,
            )
        batch = await asyncio.wait_for(task, 2)
        # Assert cleanup before the Lambda wrapper can cancel leaked helpers.
        assert sorted(finished) == [0, 1]
        return describe(batch)

    first = api.call(handler)
    assert first["Status"] == "SUCCEEDED", first
    result = json.loads(first["Result"])
    assert result["bytes"] == 320000
    assert [item[1] for item in result["items"]] == [
        "SUCCEEDED",
        "FAILED" if failed else "CANCELLED",
    ]
    if late_status is not None:
        # A durable outcome arriving after the aggregate stopped must not change
        # the branch's recorded cancellation or contribute another result.
        name = "helper-callback" if helper_mode == "callback" else "helper-step"
        field = "CallbackDetails" if helper_mode == "callback" else "StepDetails"
        operation = next(op for op in api.operations.values() if op.get("Name") == name)
        operation["Status"] = late_status
        operation[field].update(
            {"Result": json.dumps("late helper result")}
            if late_status == "SUCCEEDED"
            else {
                "Error": {
                    "ErrorMessage": "late helper failure",
                    "ErrorType": "ValueError",
                }
            }
        )
    calls, original_effects = api.calls, list(effects)
    assert original_effects.count("part") == 4
    assert original_effects.count("helper") == int(helper_mode.endswith("_step"))
    assert api.call(handler) == first
    assert entries == [[0, 1], [0, 1]]
    assert all(sorted(finished) == [0, 1] for finished in exits)
    assert api.calls == calls and effects == original_effects


@pytest.mark.parametrize("limit", [1, 2])
def test_completed_flat_replay_limits_terminal_helpers_and_keeps_item_order(limit):
    api = LambdaHistory()
    peaks = []
    entries = []
    effects = []

    @durable_execution(boto3_client=api)
    async def handler(event):
        active = peak = 0
        started = []
        entries.append(started)

        async def branch(index):
            nonlocal active, peak
            started.append(index)
            active += 1
            peak = max(peak, active)
            try:
                await asyncio.sleep(0)
                if index % 2 == 0:
                    raise ValueError(f"failure {index}")

                async def work():
                    effects.append(index)
                    return str(index) * 160000

                return await step(work, name="value")
            finally:
                active -= 1

        batch = await asyncio.wait_for(
            parallel(
                [lambda i=i: branch(i) for i in range(4)],
                nesting_type=NestingType.FLAT,
                max_concurrency=limit,
                completion_config=CompletionConfig(tolerated_failure_count=2),
            ),
            2,
        )
        peaks.append(peak)
        assert active == 0
        return {
            "values": [value[0] for value in batch.get_results()],
            **describe(batch),
        }

    first = api.call(handler)
    assert first["Status"] == "SUCCEEDED", first
    assert json.loads(first["Result"])["values"] == ["1", "3"]
    calls = api.calls
    assert api.call(handler) == first
    assert entries == [list(range(4)), list(range(4))]
    assert peaks == [limit, limit]
    assert effects == [1, 3] and api.calls == calls


@pytest.mark.parametrize("kind", ["map", "parallel"])
@pytest.mark.parametrize(
    ("historical_type", "historical_status"),
    [
        ("CALLBACK", "STARTED"),
        ("CALLBACK", "SUCCEEDED"),
        ("CHAINED_INVOKE", "STOPPED"),
        ("CHAINED_INVOKE", "TIMED_OUT"),
    ],
)
def test_completed_flat_replay_rejects_operation_type_mismatches(
    kind, historical_type, historical_status
):
    api = LambdaHistory()
    effects = []

    async def branch():
        async def work():
            effects.append("work")
            return "x" * 80000

        return await step(work, name="value")

    @durable_execution(boto3_client=api)
    async def handler(event):
        task = (
            durable_map(lambda _: branch(), range(4), nesting_type=NestingType.FLAT)
            if kind == "map"
            else parallel([branch] * 4, nesting_type=NestingType.FLAT)
        )
        return describe(await task)

    assert api.call(handler)["Status"] == "SUCCEEDED"
    # Model an operation occupying the same slot after a workflow code change.
    operation = next(op for op in api.operations.values() if op["Type"] == "STEP")
    operation.pop("StepDetails")
    operation.update(Type=historical_type, Status=historical_status)
    if historical_type == "CALLBACK":
        operation.update(SubType="Callback", CallbackDetails={"CallbackId": "existing"})
    else:
        operation.update(SubType="ChainedInvoke", ChainedInvokeDetails={})
    calls, history = api.calls, copy.deepcopy(api.operations)
    replay = api.call(handler)
    assert replay["Status"] == "FAILED", replay
    assert replay["Error"]["ErrorType"] == "ExecutionError"
    assert "operation type mismatch" in replay["Error"]["ErrorMessage"]
    assert effects == ["work"] * 4 and api.calls == calls
    assert api.operations == history


@pytest.mark.parametrize("kind", ["map", "parallel"])
@pytest.mark.parametrize(
    "payload",
    [
        "{",
        " ",
        "null",
        '""',
        "[]",
        "{}",
        '{"items":[]}',
        '{"__ade_flat_replay__":2}',
        '{"__ade_flat_replay__":true}',
        '{"__ade_flat_replay__":1,"completionReason":"ALL_COMPLETED","items":null}',
        '{"__ade_flat_replay__":1,"completionReason":"ALL_COMPLETED","items":[]}',
    ],
)
def test_completed_flat_replay_rejects_invalid_nonempty_metadata(kind, payload):
    api = LambdaHistory()
    entries = []
    effects = []

    async def branch():
        entries.append("branch")

        async def work():
            effects.append("work")
            return "x" * 80000

        return await step(work)

    @durable_execution(boto3_client=api)
    async def handler(event):
        task = (
            durable_map(lambda _: branch(), range(4), nesting_type=NestingType.FLAT)
            if kind == "map"
            else parallel([branch] * 4, nesting_type=NestingType.FLAT)
        )
        return describe(await task)

    assert api.call(handler)["Status"] == "SUCCEEDED"
    parent = next(
        op
        for op in api.operations.values()
        if op["Type"] == "CONTEXT"
        and op.get("ContextDetails", {}).get("ReplayChildren")
    )
    parent["ContextDetails"]["Result"] = payload
    calls, history = api.calls, copy.deepcopy(api.operations)
    replay = api.call(handler)
    assert replay["Status"] == "FAILED", replay
    assert replay["Error"]["ErrorType"] == "ExecutionError"
    assert (
        "Invalid completed flat aggregate replay metadata"
        in replay["Error"]["ErrorMessage"]
    )
    assert entries == ["branch"] * 4 and effects == ["work"] * 4
    assert api.calls == calls and api.operations == history


@pytest.mark.parametrize("kind", ["map", "parallel"])
@pytest.mark.parametrize("terminal", ["FAILED", "CANCELLED"])
@pytest.mark.parametrize("signal_first", [False, True])
@pytest.mark.parametrize("cause", ["type", "metadata", "execution", "cleanup"])
def test_completed_flat_replay_propagates_helper_integrity_errors(
    kind, terminal, signal_first, cause
):
    api = LambdaHistory()
    changed = False
    effects = []

    @durable_execution(boto3_client=api)
    async def handler(event):
        ready = asyncio.Event()
        finished = []

        async def value():
            effects.append("value")
            return "x" * 80000

        async def branch(index):
            try:
                if index == 1:
                    if signal_first:
                        ready.set()
                    if cause == "metadata":
                        await parallel(
                            [lambda: step(value)] * 4,
                            name="helper-inner",
                            nesting_type=NestingType.FLAT,
                        )
                    elif changed and cause == "type":
                        await step(value, name="helper-slot")
                    else:
                        await create_callback(name="helper-slot")
                    if changed and cause == "execution":
                        raise ExecutionError("new helper reconstruction error")
                    ready.set()
                    if terminal == "FAILED":
                        raise ValueError("recorded helper failure")
                    await asyncio.Future()
                await ready.wait()
                return "".join([await step(value, name=f"part-{i}") for i in range(4)])
            finally:
                finished.append(index)
                if changed and cause == "cleanup" and index == 1:
                    raise ExecutionError("helper cleanup integrity error")

        config = (
            CompletionConfig(tolerated_failure_count=1)
            if terminal == "FAILED"
            else CompletionConfig.first_successful()
        )
        task = (
            durable_map(
                branch,
                range(2),
                nesting_type=NestingType.FLAT,
                max_concurrency=2,
                completion_config=config,
            )
            if kind == "map"
            else parallel(
                [lambda: branch(0), lambda: branch(1)],
                nesting_type=NestingType.FLAT,
                max_concurrency=2,
                completion_config=config,
            )
        )
        try:
            return describe(await asyncio.wait_for(task, 1))
        except ExecutionError:
            # Cleanup must finish before the wrapper's global task teardown.
            assert sorted(finished) == [0, 1]
            raise

    first = api.call(handler)
    assert first["Status"] == "SUCCEEDED", first
    if cause == "metadata":
        parent = next(
            op for op in api.operations.values() if op.get("Name") == "helper-inner"
        )
        assert parent["ContextDetails"]["ReplayChildren"]
        parent["ContextDetails"]["Result"] = "{"
    changed = True
    calls, original_effects = api.calls, list(effects)
    history = copy.deepcopy(api.operations)
    result = api.call(handler)
    assert result["Status"] == "FAILED", result
    assert result["Error"]["ErrorType"] == "ExecutionError", result
    expected = {
        "type": "operation type mismatch",
        "metadata": "Invalid completed flat",
        "execution": "new helper reconstruction error",
        "cleanup": "helper cleanup integrity error",
    }[cause]
    assert expected in result["Error"]["ErrorMessage"]
    assert api.calls == calls and effects == original_effects
    assert api.operations == history


@pytest.mark.parametrize("kind", ["map", "parallel"])
@pytest.mark.parametrize("absent", [False, True])
@pytest.mark.parametrize(
    "policy", ["all_success", "zero_tolerance", "one_tolerated", "no_thresholds"]
)
@pytest.mark.parametrize("changed", [False, True])
def test_legacy_flat_replay_refuses_unverifiable_branch_decisions(
    kind, absent, policy, changed
):
    api = LambdaHistory()
    repaired = False
    entries = []
    effects = []

    async def branch(index):
        entries.append(index)
        if index == 0:

            async def value():
                effects.append("value")
                return "x" * 80000

            return "".join([await step(value, name=f"part-{i}") for i in range(4)])
        if index == 1 and policy != "all_success" and not repaired:
            raise ValueError("original branch failure")
        return f"pure result {index}"

    config = (
        CompletionConfig(tolerated_failure_count=1)
        if policy == "one_tolerated"
        else CompletionConfig()
        if policy == "no_thresholds"
        else CompletionConfig.all_successful()
    )

    @durable_execution(boto3_client=api)
    async def handler(event):
        task = (
            durable_map(
                branch,
                range(4),
                nesting_type=NestingType.FLAT,
                max_concurrency=1,
                completion_config=config,
            )
            if kind == "map"
            else parallel(
                [lambda i=i: branch(i) for i in range(4)],
                nesting_type=NestingType.FLAT,
                max_concurrency=1,
                completion_config=config,
            )
        )
        return describe(await task)

    first = api.call(handler)
    assert first["Status"] == "SUCCEEDED", first
    api.legacy_summary(absent=absent)
    calls, original_entries, original_effects = api.calls, list(entries), list(effects)
    history = copy.deepcopy(api.operations)
    repaired = changed
    result = api.call(handler)
    assert result["Status"] == "FAILED", result
    assert result["Error"]["ErrorType"] == "ExecutionError"
    assert "lacks terminal decision metadata" in result["Error"]["ErrorMessage"]
    assert entries == original_entries and effects == original_effects
    assert api.calls == calls and api.operations == history


def test_completed_flat_replay_does_not_suppress_a_required_cached_callback_failure():
    api = LambdaHistory()
    read_result = False

    async def branch():
        callback = await create_callback(name="callback")
        if read_result:
            await callback.result()
        return callback.callback_id + "x" * 80000

    @durable_execution(boto3_client=api)
    async def handler(event):
        return describe(
            await asyncio.wait_for(
                parallel([branch] * 4, nesting_type=NestingType.FLAT), 1
            )
        )

    assert api.call(handler)["Status"] == "SUCCEEDED"
    for operation in api.operations.values():
        if operation["Type"] == "CALLBACK":
            operation["Status"] = "FAILED"
            operation["CallbackDetails"]["Error"] = {
                "ErrorMessage": "late callback failure"
            }
    calls = api.calls
    history = copy.deepcopy(api.operations)
    read_result = True
    result = api.call(handler)
    assert result["Status"] == "FAILED", result
    assert result["Error"]["ErrorType"] == "CallbackError", result
    assert "late callback failure" in result["Error"]["ErrorMessage"]
    assert api.calls == calls and api.operations == history


@pytest.mark.parametrize("kind", ["map", "parallel"])
@pytest.mark.parametrize("helper_kind", ["callback", "step"])
@pytest.mark.parametrize("late_status", [None, "SUCCEEDED", "FAILED"])
def test_cancelled_flat_helper_retains_its_concurrency_slot(
    kind, helper_kind, late_status
):
    api = LambdaHistory()
    effects = []
    observed = []

    @durable_execution(boto3_client=api)
    async def handler(event):
        later_started = False

        async def branch(index):
            nonlocal later_started
            if index == 0:
                if helper_kind == "callback":
                    callback = await create_callback(name="slot-blocker")
                    return await callback.result()

                async def pending():
                    effects.append("pending")
                    await asyncio.Future()

                return await step(
                    pending, name="slot-blocker", retry_strategy=lambda *_: None
                )
            if index == 2:
                later_started = True

            async def value():
                effects.append(index)
                return "x" * 160000

            payload = await step(value, name="value")
            await asyncio.sleep(0)
            if index == 1:
                observed.append(later_started)
                return ("early" if later_started else "ordered") + payload
            return "last" + payload

        config = CompletionConfig(min_successful=2)
        task = (
            durable_map(
                branch,
                range(3),
                nesting_type=NestingType.FLAT,
                max_concurrency=2,
                completion_config=config,
            )
            if kind == "map"
            else parallel(
                [lambda i=i: branch(i) for i in range(3)],
                nesting_type=NestingType.FLAT,
                max_concurrency=2,
                completion_config=config,
            )
        )
        batch = await asyncio.wait_for(task, 2)
        return {
            "prefixes": [value[:7] for value in batch.get_results()],
            **describe(batch),
        }

    first = api.call(handler)
    assert first["Status"] == "SUCCEEDED", first
    assert observed == [False]
    assert [item[1] for item in json.loads(first["Result"])["items"]] == [
        "CANCELLED",
        "SUCCEEDED",
        "SUCCEEDED",
    ]
    if late_status is not None:
        operation = next(
            op for op in api.operations.values() if op.get("Name") == "slot-blocker"
        )
        operation["Status"] = late_status
        field = "CallbackDetails" if helper_kind == "callback" else "StepDetails"
        operation[field].update(
            {"Result": json.dumps("late result")}
            if late_status == "SUCCEEDED"
            else {"Error": {"ErrorType": "ValueError", "ErrorMessage": "late failure"}}
        )
    calls, original_effects = api.calls, list(effects)
    history = copy.deepcopy(api.operations)
    assert api.call(handler) == first
    assert observed == [False, False]
    assert effects == original_effects and api.calls == calls
    assert api.operations == history


@pytest.mark.parametrize("kind", ["map", "parallel"])
@pytest.mark.parametrize(
    "error_payload",
    [
        pytest.param(..., id="missing-error-field"),
        None,
        "",
        "broken",
        42,
        False,
        [],
        {},
        {"unknown": "value"},
        {"ErrorMessage": None},
        {"ErrorMessage": 42},
        {"ErrorType": []},
        {"ErrorData": {}},
        {"StackTrace": "not a list"},
        {"StackTrace": [7]},
    ],
)
def test_flat_replay_rejects_invalid_failure_details_before_running_branches(
    kind, error_payload
):
    api = LambdaHistory()
    entries = []
    effects = []

    async def branch(index):
        entries.append(index)
        if index == 1:
            raise ValueError("original failure")

        async def value():
            effects.append("value")
            return "x" * 80000

        return "".join([await step(value, name=f"part-{i}") for i in range(4)])

    @durable_execution(boto3_client=api)
    async def handler(event):
        task = (
            durable_map(
                branch, range(2), nesting_type=NestingType.FLAT, max_concurrency=1
            )
            if kind == "map"
            else parallel(
                [lambda: branch(0), lambda: branch(1)],
                nesting_type=NestingType.FLAT,
                max_concurrency=1,
            )
        )
        return describe(await task)

    assert api.call(handler)["Status"] == "SUCCEEDED"
    parent = next(
        op
        for op in api.operations.values()
        if op["Type"] == "CONTEXT"
        and op.get("ContextDetails", {}).get("ReplayChildren")
    )
    metadata = json.loads(parent["ContextDetails"]["Result"])
    assert metadata["items"][1]["status"] == "FAILED"
    if error_payload is ...:
        del metadata["items"][1]["error"]
    else:
        metadata["items"][1]["error"] = error_payload
    parent["ContextDetails"]["Result"] = json.dumps(metadata)
    calls, history = api.calls, copy.deepcopy(api.operations)
    result = api.call(handler)
    assert result["Status"] == "FAILED", result
    assert result["Error"]["ErrorType"] == "ExecutionError", result
    assert (
        "Invalid completed flat aggregate replay metadata"
        in result["Error"]["ErrorMessage"]
    )
    assert entries == [0, 1] and effects == ["value"] * 4
    assert api.calls == calls and api.operations == history


@pytest.mark.parametrize("kind", ["map", "parallel"])
def test_empty_flat_workload_can_replay_an_oversized_custom_serialization(kind):
    class LargeSerDes(SerDes):
        async def serialize(self, value):
            return "x" * 300000

        async def deserialize(self, data):
            raise AssertionError("ReplayChildren reconstructs the result")

    api = LambdaHistory()

    @durable_execution(boto3_client=api)
    async def handler(event):
        task = (
            durable_map(
                lambda _: None, [], nesting_type=NestingType.FLAT, serdes=LargeSerDes()
            )
            if kind == "map"
            else parallel([], nesting_type=NestingType.FLAT, serdes=LargeSerDes())
        )
        return describe(await task)

    first = api.call(handler)
    assert first["Status"] == "SUCCEEDED", first
    parent = next(
        op
        for op in api.operations.values()
        if op["Type"] == "CONTEXT"
        and op.get("ContextDetails", {}).get("ReplayChildren")
    )
    assert json.loads(parent["ContextDetails"]["Result"])["items"] == []
    calls = api.calls
    assert api.call(handler) == first
    assert api.calls == calls


@pytest.mark.parametrize("kind", ["map", "parallel"])
@pytest.mark.parametrize(
    "error_payload",
    [
        {
            "ErrorMessage": "original failure",
            "ErrorType": "CallableRuntimeError",
            "ErrorData": "opaque data",
            "StackTrace": ["frame one", "frame two"],
        },
        {"ErrorMessage": "original failure"},
        {
            "ErrorMessage": None,
            "ErrorType": "CallableRuntimeError",
            "ErrorData": None,
            "StackTrace": [],
        },
    ],
)
def test_flat_replay_preserves_valid_failure_details(kind, error_payload):
    api = LambdaHistory()
    effects = []

    async def branch(index):
        if index == 1:
            raise ValueError("original failure")

        async def value():
            effects.append("value")
            return "x" * 80000

        return "".join([await step(value, name=f"part-{i}") for i in range(4)])

    @durable_execution(boto3_client=api)
    async def handler(event):
        task = (
            durable_map(
                branch, range(2), nesting_type=NestingType.FLAT, max_concurrency=1
            )
            if kind == "map"
            else parallel(
                [lambda: branch(0), lambda: branch(1)],
                nesting_type=NestingType.FLAT,
                max_concurrency=1,
            )
        )
        batch = await task
        return {"error": batch.all[1].error.to_dict(), **describe(batch)}

    assert api.call(handler)["Status"] == "SUCCEEDED"
    parent = next(
        op
        for op in api.operations.values()
        if op["Type"] == "CONTEXT"
        and op.get("ContextDetails", {}).get("ReplayChildren")
    )
    metadata = json.loads(parent["ContextDetails"]["Result"])
    metadata["items"][1]["error"] = error_payload
    parent["ContextDetails"]["Result"] = json.dumps(metadata)
    calls = api.calls
    result = api.call(handler)
    assert result["Status"] == "SUCCEEDED", result
    assert json.loads(result["Result"])["error"] == {
        k: v for k, v in error_payload.items() if v is not None
    }
    assert effects == ["value"] * 4 and api.calls == calls
