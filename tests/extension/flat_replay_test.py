"""Completed flat aggregates replay through the public Lambda handler contract."""

import asyncio
import copy
from datetime import datetime, timezone
import json
from types import SimpleNamespace

import pytest

from async_durable_execution import (
    CompletionConfig,
    CompletionDecision,
    CompletionReason,
    NestingType,
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
            assert update["Type"] in ("STEP", "CONTEXT")
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
            details = record.setdefault(
                "StepDetails" if update["Type"] == "STEP" else "ContextDetails", {}
            )
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

    def legacy_summary(self):
        for operation in self.operations.values():
            if operation["Type"] == "CONTEXT" and operation.get(
                "ContextDetails", {}
            ).get("ReplayChildren"):
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
@pytest.mark.parametrize("legacy", [False, True])
def test_large_flat_result_replays_without_effects_or_checkpoints(kind, legacy):
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
    if legacy:
        api.legacy_summary()
    calls = api.calls
    second = api.call(handler)
    assert second == first
    assert api.calls == calls
    assert sorted(effects) == list(range(4))
    assert sorted(body_calls) == sorted(list(range(4)) * 2)


@pytest.mark.parametrize("policy", ["first_successful", "custom", "failure"])
def test_large_flat_terminal_decision_does_not_restart_other_branches(policy):
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
    assert entries.count("other") == 1 and "never" not in entries
    assert len(decisions) == old_decisions
    if policy != "failure":
        # Legacy summaries cannot prove the cancelled branch's outcome. Fail
        # explicitly before resuming its unfinished effect, without rewriting history.
        api.legacy_summary()
        failed = api.call(handler)
        assert failed["Status"] == "FAILED"
        assert "lacks terminal decision metadata" in str(failed)
        assert api.calls == calls and effects == before


@pytest.mark.parametrize("legacy", [False, True])
@pytest.mark.parametrize(
    "incomplete", ["missing", "started", "cancelled", "timed_out", "stopped"]
)
def test_completed_flat_replay_never_restarts_an_unfinished_step(legacy, incomplete):
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
    if legacy:
        api.legacy_summary()
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
