"""Cloud runner polling and failure contracts using the public client boundary."""

import asyncio
from unittest.mock import AsyncMock

import pytest
from botocore.exceptions import ClientError

from async_durable_execution import (
    ErrorObject,
    ExecutionError,
    InvocationStatus,
    create_cloud_runner,
)


@pytest.mark.parametrize("lookup", ["result", "callback"])
@pytest.mark.parametrize("missing_page", [0, 1])
async def test_cloud_history_retries_not_yet_visible_pages(lookup, missing_page):
    client = AsyncMock()
    client.get_durable_execution.return_value = {"Status": "SUCCEEDED", "Result": "42"}
    pages = [
        {
            "Events": [
                {
                    "EventId": 1,
                    "EventType": "StepStarted",
                    "Id": "step",
                    "Name": "prepare",
                }
            ],
            "NextMarker": "page2",
        },
        {
            "Events": [
                {
                    "EventId": 2,
                    "EventType": "CallbackStarted",
                    "Id": "callback",
                    "Name": "approval",
                    "CallbackStartedDetails": {"CallbackId": "callback-id"},
                }
            ],
        },
    ]
    responses: list[object] = list(pages)
    responses.insert(
        missing_page,
        ClientError(
            {
                "Error": {
                    "Code": "ResourceNotFoundException",
                    "Message": "not visible yet",
                }
            },
            "GetDurableExecutionHistory",
        ),
    )
    client.get_durable_execution_history.side_effect = responses

    async with create_cloud_runner(function_name="worker:1", poll_interval=0) as runner:
        runner.lambda_client = client
        if lookup == "callback":
            assert (
                await runner.wait_for_callback("execution", name="approval", timeout=1)
                == "callback-id"
            )
        else:
            result = await runner.wait_for_result("execution", timeout=1)
            assert result.get_deserialized_result() == 42
            assert [operation.name for operation in result.operations] == [
                "prepare",
                "approval",
            ]

    requests = client.get_durable_execution_history.await_args_list
    assert len(requests) == 3
    assert [call.kwargs.get("Marker") for call in requests] == (
        [None, None, "page2"] if missing_page == 0 else [None, "page2", "page2"]
    )


@pytest.mark.parametrize("lookup", ["result", "callback"])
async def test_cloud_history_permanent_error_is_not_retried(lookup):
    client = AsyncMock()
    client.get_durable_execution.return_value = {"Status": "SUCCEEDED", "Result": "42"}
    error = ClientError(
        {"Error": {"Code": "AccessDeniedException", "Message": "denied"}},
        "GetDurableExecutionHistory",
    )
    client.get_durable_execution_history.side_effect = error

    async with create_cloud_runner(function_name="worker:1", poll_interval=0) as runner:
        runner.lambda_client = client
        with pytest.raises(ClientError) as caught:
            await getattr(runner, f"wait_for_{lookup}")("execution", timeout=1)

    assert caught.value is error
    client.get_durable_execution_history.assert_awaited_once()


@pytest.mark.parametrize("lookup", ["result", "callback"])
async def test_cloud_history_not_found_retries_respect_polling_deadline(lookup):
    client = AsyncMock()
    client.get_durable_execution.return_value = {"Status": "SUCCEEDED", "Result": "42"}
    client.get_durable_execution_history.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}},
        "GetDurableExecutionHistory",
    )

    async with create_cloud_runner(
        function_name="worker:1", poll_interval=0.01
    ) as runner:
        runner.lambda_client = client
        with pytest.raises(asyncio.TimeoutError):
            await getattr(runner, f"wait_for_{lookup}")("execution", timeout=0.03)
        requests = client.get_durable_execution_history.await_count
        assert requests >= 1
        await asyncio.sleep(0.02)
        assert client.get_durable_execution_history.await_count == requests


async def test_cloud_polling_recovers_from_eventual_consistency():
    client = AsyncMock()
    client.get_durable_execution.side_effect = [
        ClientError(
            {
                "Error": {
                    "Code": "ResourceNotFoundException",
                    "Message": "not visible yet",
                }
            },
            "GetDurableExecution",
        ),
        {"Status": "RUNNING"},
        {"Status": "SUCCEEDED", "Result": "42"},
    ]
    client.get_durable_execution_history.return_value = {"Events": []}
    async with create_cloud_runner(function_name="worker:1", poll_interval=0) as runner:
        runner.lambda_client = client
        result = await runner.wait_for_result("execution", timeout=1)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == 42
    assert client.get_durable_execution.await_count == 3
    client.get_durable_execution_history.assert_awaited_once_with(
        DurableExecutionArn="execution", IncludeExecutionData=True
    )
    client.aclose.assert_awaited_once()


async def test_cloud_polling_propagates_permanent_client_error():
    client = AsyncMock()
    error = ClientError(
        {"Error": {"Code": "AccessDeniedException", "Message": "denied"}},
        "GetDurableExecution",
    )
    client.get_durable_execution.side_effect = error
    async with create_cloud_runner(function_name="worker:1", poll_interval=0) as runner:
        runner.lambda_client = client
        with pytest.raises(ClientError) as caught:
            await runner.wait_for_result("execution", timeout=1)

    assert caught.value is error
    client.get_durable_execution.assert_awaited_once()
    client.get_durable_execution_history.assert_not_awaited()
    client.aclose.assert_awaited_once()


async def test_cloud_history_repeated_marker_fails_instead_of_looping():
    client = AsyncMock()
    client.get_durable_execution.return_value = {"Status": "SUCCEEDED", "Result": "1"}
    client.get_durable_execution_history.return_value = {
        "Events": [],
        "NextMarker": "same",
    }
    async with create_cloud_runner(function_name="worker:1") as runner:
        runner.lambda_client = client
        with pytest.raises(ExecutionError, match="pagination repeated a marker"):
            await runner.wait_for_result("execution", timeout=1)

    assert client.get_durable_execution_history.await_count == 2
    assert (
        client.get_durable_execution_history.await_args_list[1].kwargs["Marker"]
        == "same"
    )


@pytest.mark.parametrize("mode", ["run", "run_async"])
async def test_cloud_invocation_requires_execution_arn(mode):
    client = AsyncMock()
    client.invoke.return_value = {"StatusCode": 200}
    async with create_cloud_runner(function_name="worker:1") as runner:
        runner.lambda_client = client
        with pytest.raises(
            ExecutionError, match="did not return a durable execution ARN"
        ):
            await getattr(runner, mode)()

    client.invoke.assert_awaited_once()
    client.get_durable_execution.assert_not_awaited()


@pytest.mark.parametrize("name", [None, "approval"])
async def test_cloud_callback_discovery_waits_for_a_pending_callback(name):
    client = AsyncMock()
    completed = {
        "EventId": 1,
        "EventType": "CallbackSucceeded",
        "Id": "old",
        "Name": "old-approval",
    }
    pending = {
        "EventId": 2,
        "EventType": "CallbackStarted",
        "Id": "new",
        "Name": "approval",
        "CallbackStartedDetails": {"CallbackId": "callback-id"},
    }
    client.get_durable_execution_history.side_effect = [
        {"Events": [completed]},
        {"Events": [completed, pending]},
    ]
    async with create_cloud_runner(function_name="worker:1", poll_interval=0) as runner:
        runner.lambda_client = client
        callback_id = await runner.wait_for_callback("execution", name=name, timeout=1)
        await runner.send_callback_success(callback_id)
        await runner.send_callback_heartbeat(callback_id)

    assert callback_id == "callback-id"
    assert client.get_durable_execution_history.await_count == 2
    client.send_durable_execution_callback_success.assert_awaited_once_with(
        CallbackId="callback-id"
    )
    client.send_durable_execution_callback_heartbeat.assert_awaited_once_with(
        CallbackId="callback-id"
    )


async def test_cloud_callback_discovery_rejects_completed_named_callback():
    client = AsyncMock()
    client.get_durable_execution_history.return_value = {
        "Events": [
            {
                "EventId": 1,
                "EventType": "CallbackSucceeded",
                "Id": "old",
                "Name": "approval",
            }
        ]
    }
    async with create_cloud_runner(function_name="worker:1") as runner:
        runner.lambda_client = client
        with pytest.raises(ValueError, match="approval has completed"):
            await runner.wait_for_callback("execution", name="approval", timeout=1)

    client.get_durable_execution_history.assert_awaited_once()


@pytest.mark.parametrize(
    "error", [None, ErrorObject("rejected", "Denied", "detail", ["trace"])]
)
async def test_cloud_callback_failure_preserves_supplied_error(error):
    client = AsyncMock()
    async with create_cloud_runner(function_name="worker:1") as runner:
        runner.lambda_client = client
        await runner.send_callback_failure("callback-id", error)

    expected = {"CallbackId": "callback-id"}
    if error is not None:
        expected["Error"] = error.to_dict()
    client.send_durable_execution_callback_failure.assert_awaited_once_with(**expected)


async def test_cloud_poll_timeout_cancels_inflight_request():
    started = asyncio.Event()
    stopped = asyncio.Event()

    async def block(**kwargs):
        started.set()
        try:
            await asyncio.Future()
        finally:
            stopped.set()

    client = AsyncMock()
    client.get_durable_execution.side_effect = block
    async with create_cloud_runner(function_name="worker:1") as runner:
        runner.lambda_client = client
        with pytest.raises(asyncio.TimeoutError):
            await runner.wait_for_result("execution", timeout=0.05)

    assert started.is_set() and stopped.is_set()
    client.get_durable_execution_history.assert_not_awaited()
    client.aclose.assert_awaited_once()
