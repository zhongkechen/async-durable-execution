"""Tests for replay-safe helper operations."""

from typing import no_type_check

from typing import Any

import inspect
import json
import uuid as uuid_module
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from async_durable_execution import (
    InvocationStatus,
    create_local_runner,
    durable_execution,
    now,
    random,
    timestamp,
    uuid,
    wait,
)
from async_durable_execution._core.serdes import ExtendedTypeSerDes


def test_replay_safe_helper_signatures_use_keyword_only_names() -> None:
    """Helper operation names are explicit keyword-only options."""
    for helper in (random, now, timestamp, uuid):
        parameters = inspect.signature(helper).parameters

        assert list(parameters) == ["name"]
        assert parameters["name"].kind is inspect.Parameter.KEYWORD_ONLY


async def test_replay_safe_helpers_keep_default_step_retries(monkeypatch) -> None:
    """Replay-safe helpers retain the normal step retry policy."""
    monkeypatch.setenv("DURABLE_EXECUTION_TIME_SCALE", "0.01")
    calls = 0

    async def flaky_random() -> float:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("transient")
        return 0.5

    monkeypatch.setattr(
        "async_durable_execution._operation.replay_safe._random_value",
        flaky_random,
    )

    @durable_execution
    async def function_under_test(event) -> float:
        return await random()

    async with create_local_runner(
        handler=function_under_test,
        input={},
        poll_interval=0.01,
        timeout=10,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == 0.5
    assert calls == 2


@no_type_check
async def test_replay_safe_helpers_use_default_step_names(monkeypatch) -> None:
    """Default helper names make their checkpoints easy to inspect."""
    monkeypatch.setenv("DURABLE_EXECUTION_TIME_SCALE", "0")
    expected_now = datetime(2026, 7, 12, 1, 2, 3, tzinfo=timezone.utc)
    expected_uuid = uuid_module.UUID("12345678-1234-5678-1234-567812345678")

    @durable_execution
    async def function_under_test(event) -> Any:
        value_random = await random()
        value_now = await now()
        value_timestamp = await timestamp()
        value_uuid = await uuid()
        return {
            "random": value_random,
            "now": value_now.isoformat(),
            "timestamp": value_timestamp,
            "uuid": str(value_uuid),
        }

    with (
        patch(
            "async_durable_execution._operation.replay_safe._random.random",
            return_value=0.25,
        ),
        patch(
            "async_durable_execution._operation.replay_safe.datetime",
        ) as mock_datetime,
        patch(
            "async_durable_execution._operation.replay_safe._timestamp_value",
            return_value=1783821723.5,
        ),
        patch(
            "async_durable_execution._operation.replay_safe._uuid.uuid4",
            return_value=expected_uuid,
        ),
    ):
        mock_datetime.now.return_value = expected_now

        async with create_local_runner(
            handler=function_under_test,
            input={},
            timeout=10,
        ) as runner:
            result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert json.loads(result.result) == {
        "random": 0.25,
        "now": "2026-07-12T01:02:03+00:00",
        "timestamp": 1783821723.5,
        "uuid": "12345678-1234-5678-1234-567812345678",
    }

    serdes = ExtendedTypeSerDes()
    random_operation = result.get_step("random")
    now_operation = result.get_step("now")
    timestamp_operation = result.get_step("timestamp")
    uuid_operation = result.get_step("uuid")

    assert random_operation.step_details is not None
    assert now_operation.step_details is not None
    assert timestamp_operation.step_details is not None
    assert uuid_operation.step_details is not None
    assert await serdes.deserialize(random_operation.step_details.result) == 0.25
    assert await serdes.deserialize(now_operation.step_details.result) == expected_now
    assert (
        await serdes.deserialize(timestamp_operation.step_details.result)
        == 1783821723.5
    )
    assert await serdes.deserialize(uuid_operation.step_details.result) == expected_uuid


@no_type_check
async def test_replay_safe_helpers_reuse_checkpointed_values_after_replay(
    monkeypatch,
) -> None:
    """Values generated before a wait are replayed from checkpoints."""
    monkeypatch.setenv("DURABLE_EXECUTION_TIME_SCALE", "0")
    now_values = [
        datetime(2026, 7, 12, 1, 0, 0, tzinfo=timezone.utc),
        datetime(2026, 7, 12, 2, 0, 0, tzinfo=timezone.utc),
    ]
    uuid_values = [
        uuid_module.UUID("12345678-1234-5678-1234-567812345678"),
        uuid_module.UUID("87654321-4321-8765-4321-876543218765"),
    ]

    @durable_execution
    async def function_under_test(event) -> Any:
        first_random = await random(name="first-random")
        first_now = await now(name="first-now")
        first_timestamp = await timestamp(name="first-timestamp")
        first_uuid = await uuid(name="first-uuid")

        await wait(timedelta(seconds=1), name="force-replay")

        second_random = await random(name="second-random")
        second_now = await now(name="second-now")
        second_timestamp = await timestamp(name="second-timestamp")
        second_uuid = await uuid(name="second-uuid")

        return {
            "first": {
                "random": first_random,
                "now": first_now.isoformat(),
                "timestamp": first_timestamp,
                "uuid": str(first_uuid),
            },
            "second": {
                "random": second_random,
                "now": second_now.isoformat(),
                "timestamp": second_timestamp,
                "uuid": str(second_uuid),
            },
        }

    with (
        patch(
            "async_durable_execution._operation.replay_safe._random.random",
            side_effect=[0.125, 0.875],
        ) as mock_random,
        patch(
            "async_durable_execution._operation.replay_safe.datetime",
        ) as mock_datetime,
        patch(
            "async_durable_execution._operation.replay_safe._timestamp_value",
            side_effect=[1783818000.0, 1783821600.0],
        ) as mock_timestamp,
        patch(
            "async_durable_execution._operation.replay_safe._uuid.uuid4",
            side_effect=uuid_values,
        ) as mock_uuid4,
    ):
        mock_datetime.now.side_effect = now_values

        async with create_local_runner(
            handler=function_under_test,
            input={},
            timeout=10,
        ) as runner:
            result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert json.loads(result.result) == {
        "first": {
            "random": 0.125,
            "now": "2026-07-12T01:00:00+00:00",
            "timestamp": 1783818000.0,
            "uuid": "12345678-1234-5678-1234-567812345678",
        },
        "second": {
            "random": 0.875,
            "now": "2026-07-12T02:00:00+00:00",
            "timestamp": 1783821600.0,
            "uuid": "87654321-4321-8765-4321-876543218765",
        },
    }

    assert mock_random.call_count == 2
    assert mock_datetime.now.call_count == 2
    assert mock_timestamp.call_count == 2
    assert mock_uuid4.call_count == 2
