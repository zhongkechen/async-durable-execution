"""Public service-client and cloud-runner boundaries, with no live AWS access."""

import asyncio
from types import SimpleNamespace
from datetime import datetime, timezone
from unittest.mock import AsyncMock
import pytest
from botocore.exceptions import ClientError
from async_durable_execution import *

ROOT = {
    "Id": "root",
    "Type": "EXECUTION",
    "Status": "STARTED",
    "StartTimestamp": datetime(2026, 9, 9, tzinfo=timezone.utc),
    "ExecutionDetails": {"InputPayload": '{"value":3}'},
}
EVENT = {
    "DurableExecutionArn": "execution/root",
    "CheckpointToken": "one",
    "InitialExecutionState": {"Operations": [ROOT]},
}


async def test_cancelled_invocation_does_not_leave_checkpoint_request_running():
    entered = asyncio.Event()
    cancelled = asyncio.Event()

    class Blocking:
        async def checkpoint(
            self, durable_execution_arn, checkpoint_token, updates, client_token
        ):
            entered.set()
            try:
                await asyncio.Future()
            finally:
                cancelled.set()

        async def get_execution_state(
            self, durable_execution_arn, checkpoint_token, next_marker, max_items=1000
        ):
            raise AssertionError("Unexpected pagination")

    @durable_execution(service_client=Blocking())
    async def handler(event):
        return await step(lambda: asyncio.sleep(0, result=1))

    task = asyncio.create_task(
        getattr(handler, "_async_handler")(EVENT, SimpleNamespace())
    )
    await asyncio.wait_for(entered.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 1)
    assert cancelled.is_set()


@pytest.mark.parametrize(
    "http,code,retries",
    [
        (429, "TooManyRequestsException", True),
        (503, "ServiceException", True),
        (400, "KMSAccessDeniedException", False),
        (403, "AccessDeniedException", False),
    ],
)
async def test_transport_failures_keep_retry_classification(http, code, retries):
    class Failing:
        async def checkpoint(
            self, durable_execution_arn, checkpoint_token, updates, client_token
        ):
            raise ClientError(
                {
                    "Error": {"Code": code, "Message": "failure"},
                    "ResponseMetadata": {
                        "HTTPStatusCode": http,
                        "RequestId": "test",
                        "HostId": "",
                        "HTTPHeaders": {},
                        "RetryAttempts": 0,
                    },
                },
                "CheckpointDurableExecution",
            )

        async def get_execution_state(
            self, durable_execution_arn, checkpoint_token, next_marker, max_items=1000
        ):
            raise AssertionError("Unexpected pagination")

    @durable_execution(service_client=Failing())
    async def handler(event):
        return await step(lambda: asyncio.sleep(0, result=1))

    if retries:
        with pytest.raises(InvocationError):
            await getattr(handler, "_async_handler")(EVENT, SimpleNamespace())
    else:
        result = await getattr(handler, "_async_handler")(EVENT, SimpleNamespace())
        assert result["Status"] == "FAILED"


async def test_service_replay_and_initial_pagination():
    effects = []
    requests = []

    class Service:
        def __init__(self):
            self.records = {}
            self.token = "one"

        async def checkpoint(
            self, durable_execution_arn, checkpoint_token, updates, client_token
        ):
            assert checkpoint_token == self.token and client_token
            for change in updates:
                request = change.to_dict()
                requests.append(request)
                self.records[request["Id"]] = {
                    "Id": request["Id"],
                    "Type": "STEP",
                    "Status": "SUCCEEDED"
                    if request["Action"] == "SUCCEED"
                    else "STARTED",
                    "Name": request["Name"],
                    "SubType": request["SubType"],
                    "StartTimestamp": ROOT["StartTimestamp"],
                    "StepDetails": {"Attempt": 1, "Result": request.get("Payload")},
                }
            self.token += "x"
            return {
                "CheckpointToken": self.token,
                "NewExecutionState": {"Operations": list(self.records.values())},
            }

        async def get_execution_state(
            self, durable_execution_arn, checkpoint_token, next_marker, max_items=1000
        ):
            assert next_marker == "page2"
            return {"Operations": [ROOT, *self.records.values()]}

    service = Service()

    async def work():
        effects.append(1)
        return 9

    @durable_execution(service_client=service)
    async def handler(event):
        assert event == {"value": 3}
        return await step(work, name="saved")

    first = await getattr(handler, "_async_handler")(EVENT, SimpleNamespace())
    replay = dict(
        EVENT,
        CheckpointToken=service.token,
        InitialExecutionState={"Operations": [], "NextMarker": "page2"},
    )
    second = await getattr(handler, "_async_handler")(replay, SimpleNamespace())
    assert first == second == {"Status": "SUCCEEDED", "Result": "9"}
    assert effects == [1] and len(requests) == 2


async def test_cloud_runner_collects_paginated_history_and_callbacks():
    date = datetime(2026, 9, 9, tzinfo=timezone.utc)
    client = AsyncMock()
    client.invoke.return_value = {"StatusCode": 202, "DurableExecutionArn": "arn"}
    client.get_durable_execution.return_value = {"Status": "SUCCEEDED", "Result": "42"}
    client.get_durable_execution_history.side_effect = [
        {
            "Events": [
                {
                    "EventType": "StepStarted",
                    "EventTimestamp": date,
                    "EventId": 1,
                    "Id": "step",
                    "Name": "work",
                    "SubType": "Step",
                }
            ],
            "NextMarker": "next",
        },
        {
            "Events": [
                {
                    "EventType": "StepSucceeded",
                    "EventTimestamp": date,
                    "EventId": 2,
                    "Id": "step",
                    "Name": "work",
                    "StepSucceededDetails": {
                        "Result": {"Payload": "42"},
                        "RetryDetails": {"CurrentAttempt": 1},
                    },
                }
            ]
        },
    ]
    async with create_cloud_runner(function_name="function:1") as runner:
        runner.lambda_client = client
        arn = await runner.run_async()
        result = await runner.wait_for_result(arn)
        await runner.send_callback_success("callback", b"ok")
    assert result.get_deserialized_result() == 42
    assert result.get_operation_deserialized_result(result.get_step("work")) == 42
    assert client.get_durable_execution_history.await_count == 2
    assert (
        client.get_durable_execution_history.await_args_list[1].kwargs["Marker"]
        == "next"
    )
    client.send_durable_execution_callback_success.assert_awaited_once_with(
        CallbackId="callback", Result=b"ok"
    )


async def test_httpx_transport_signs_model_generated_request(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing-secret")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    import httpx
    from async_durable_execution._remote import AsyncAws

    observed = []

    def endpoint(request):
        observed.append(request)
        return httpx.Response(
            200,
            json={"CheckpointToken": "next", "NewExecutionState": {"Operations": []}},
            headers={"x-amzn-requestid": "request"},
        )

    client = AsyncAws(region_name="us-west-2", endpoint_url="https://example.invalid")
    await client.http.aclose()
    client.http = httpx.AsyncClient(transport=httpx.MockTransport(endpoint))
    try:
        result = await client.checkpoint_durable_execution(
            DurableExecutionArn="arn:aws:lambda:us-west-2:123456789012:function:fn:1/durable-execution/job/run",
            CheckpointToken="token",
            Updates=[],
        )
    finally:
        await client.aclose()
    assert result["CheckpointToken"] == "next"
    assert observed[0].headers["authorization"].startswith("AWS4-HMAC-SHA256 ")
    assert b"ClientToken" in observed[0].content


@pytest.mark.parametrize("mode", ["Event", "RequestResponse"])
async def test_cloud_runner_reads_invoke_arn_from_httpx_response_headers(
    monkeypatch, mode
):
    import httpx

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing-secret")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    arn = (
        "arn:aws:lambda:us-west-2:123456789012:function:fn:1/durable-execution/job/run"
    )
    requests = []

    def endpoint(request):
        requests.append(request)
        if request.method == "POST":
            return httpx.Response(
                202 if mode == "Event" else 200,
                content=b"8",
                headers={"x-AmZ-Durable-Execution-Arn": arn},
            )
        if request.url.path.endswith("/history"):
            return httpx.Response(200, json={"Events": []})
        return httpx.Response(200, json={"Status": "SUCCEEDED", "Result": "8"})

    client_type = httpx.AsyncClient
    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda **kwargs: client_type(transport=httpx.MockTransport(endpoint)),
    )
    async with create_cloud_runner(
        function_name="fn:1", lambda_endpoint="https://example.invalid"
    ) as runner:
        if mode == "Event":
            assert await runner.run_async() == arn
        else:
            result = await runner.run()
            assert result.status is InvocationStatus.SUCCEEDED
            assert result.get_deserialized_result() == 8
    assert requests[0].headers["x-amz-invocation-type"] == mode
    assert len(requests) == (1 if mode == "Event" else 3)


async def test_httpx_transport_preserves_modeled_invoke_header_fields(monkeypatch):
    import httpx
    from async_durable_execution._remote import AsyncAws

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing-secret")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    payload = b'{"errorMessage":"failed"}'
    headers = {
        "x-amz-durable-execution-arn": "execution-arn",
        "x-amz-function-error": "Unhandled",
        "x-amz-executed-version": "1",
        "x-amz-log-result": "bG9ncw==",
        "x-amzn-requestid": "request-id",
    }
    client = AsyncAws(region_name="us-west-2", endpoint_url="https://example.invalid")
    await client.http.aclose()
    client.http = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, content=payload, headers=headers)
        )
    )
    try:
        result = await client.invoke(FunctionName="fn:1", Payload=b"{}")
    finally:
        await client.aclose()
    assert result["DurableExecutionArn"] == "execution-arn"
    assert result["FunctionError"] == "Unhandled"
    assert result["ExecutedVersion"] == "1"
    assert result["LogResult"] == "bG9ncw=="
    assert result["Payload"] == payload
    assert result["ResponseMetadata"]["RequestId"] == "request-id"
    assert isinstance(result["ResponseMetadata"]["HTTPHeaders"], dict)
