"""Tests for the model-free Lambda HTTP clients."""

from __future__ import annotations

import datetime
import json
from urllib.parse import parse_qs, urlsplit
from unittest.mock import AsyncMock, Mock, call, patch

import pytest
from botocore.awsrequest import AWSRequest
from botocore.config import Config
from botocore.exceptions import (
    ClientError,
    NoCredentialsError,
    NoRegionError,
    UnknownEndpointError,
)
from botocore.session import Session

from async_durable_execution.__about__ import __version__
from async_durable_execution._core.aws_http import (
    BotocoreHttpLambdaClient,
    HttpxLambdaClient,
    LambdaHttpRequestFactory,
    _resolve_max_attempts,
    create_botocore_http_client,
    create_httpx_client,
    parse_lambda_response,
)


def _session() -> Session:
    session = Session()
    session.set_config_variable("region", "us-west-2")
    session.set_credentials("access-key", "secret-key", "session-token")
    return session


def _request_factory() -> LambdaHttpRequestFactory:
    return LambdaHttpRequestFactory(
        session=_session(),
        endpoint_url="https://lambda.us-west-2.amazonaws.com",
    )


def test_checkpoint_request_is_model_free_and_extensible() -> None:
    session = _session()
    with patch.object(session, "create_client") as create_client:
        factory = LambdaHttpRequestFactory(
            session=session,
            endpoint_url="https://lambda.us-west-2.amazonaws.com",
        )
        request = factory.prepare(
            "CheckpointDurableExecution",
            {
                "DurableExecutionArn": (
                    "arn:aws:lambda:us-west-2:123456789012:function:test:1/execution/id"
                ),
                "CheckpointToken": "token",
                "Updates": [],
                "ClientToken": "client-token",
                "FutureExtension": {"Enabled": True},
            },
        )

    create_client.assert_not_called()
    assert request.method == "POST"
    assert (
        urlsplit(request.url).path == "/2025-12-01/durable-executions/"
        "arn%3Aaws%3Alambda%3Aus-west-2%3A123456789012%3A"
        "function%3Atest%3A1%2Fexecution%2Fid/checkpoint"
    )
    assert isinstance(request.body, bytes)
    assert json.loads(request.body) == {
        "CheckpointToken": "token",
        "Updates": [],
        "ClientToken": "client-token",
        "FutureExtension": {"Enabled": True},
    }
    assert request.headers["Authorization"].startswith("AWS4-HMAC-SHA256 ")
    assert request.headers["X-Amz-Security-Token"] == "session-token"


def test_state_request_serializes_query_parameters() -> None:
    request = _request_factory().prepare(
        "GetDurableExecutionState",
        {
            "DurableExecutionArn": "arn/test",
            "CheckpointToken": "token",
            "Marker": "next marker",
            "MaxItems": 1000,
        },
    )

    assert urlsplit(request.url).path.endswith("/durable-executions/arn%2Ftest/state")
    assert parse_qs(urlsplit(request.url).query) == {
        "CheckpointToken": ["token"],
        "Marker": ["next marker"],
        "MaxItems": ["1000"],
    }
    assert request.body is None


def test_get_execution_forwards_include_execution_data() -> None:
    request = _request_factory().prepare(
        "GetDurableExecution",
        {
            "DurableExecutionArn": "arn/test",
            "IncludeExecutionData": False,
        },
    )

    assert parse_qs(urlsplit(request.url).query) == {"IncludeExecutionData": ["false"]}


def test_region_only_resolves_lambda_endpoint() -> None:
    factory = LambdaHttpRequestFactory(session=_session())

    assert factory.endpoint_url == "https://lambda.us-west-2.amazonaws.com"


@patch.dict(
    "os.environ",
    {"AWS_ENDPOINT_URL_LAMBDA": "http://localhost:3000"},
)
def test_service_endpoint_environment_variable_is_used() -> None:
    factory = LambdaHttpRequestFactory(session=_session())

    assert factory.endpoint_url == "http://localhost:3000"


@patch.dict(
    "os.environ",
    {"AWS_ENDPOINT_URL_LAMBDA": "http://localhost:3000"},
)
def test_configured_endpoint_can_be_ignored() -> None:
    session = _session()
    session.set_config_variable("ignore_configured_endpoint_urls", True)

    factory = LambdaHttpRequestFactory(session=session)

    assert factory.endpoint_url == "https://lambda.us-west-2.amazonaws.com"


@patch.dict(
    "os.environ",
    {"AWS_DEFAULT_REGION": "", "AWS_REGION": ""},
)
def test_missing_region_is_rejected() -> None:
    session = Session()
    session.set_credentials("access-key", "secret-key")

    with pytest.raises(NoRegionError):
        LambdaHttpRequestFactory(session=session)


def test_unknown_endpoint_is_rejected() -> None:
    with patch(
        "async_durable_execution._core.aws_http.EndpointResolver.construct_endpoint",
        return_value=None,
    ):
        with pytest.raises(UnknownEndpointError):
            LambdaHttpRequestFactory(session=_session())


def test_missing_credentials_are_rejected_when_preparing_request() -> None:
    session = _session()
    factory = LambdaHttpRequestFactory(
        session=session,
        endpoint_url="https://lambda.us-west-2.amazonaws.com",
    )

    with patch.object(session, "get_credentials", return_value=None):
        with pytest.raises(NoCredentialsError):
            factory.prepare(
                "GetDurableExecution",
                {"DurableExecutionArn": "arn"},
            )


@pytest.mark.parametrize(
    ("config", "expected"),
    [
        (Config(), 5),
        (Config(retries={"mode": "standard"}), 3),
        (Config(retries={"max_attempts": 0}), 1),
        (Config(retries={"max_attempts": 2}), 3),
        (Config(retries={"total_max_attempts": 4}), 4),
    ],
)
def test_retry_attempt_configuration(config: Config, expected: int) -> None:
    assert _resolve_max_attempts(_session(), config) == expected


@pytest.mark.parametrize(
    ("operation_name", "params", "method", "path"),
    [
        (
            "Invoke",
            {
                "FunctionName": "function:prod",
                "InvocationType": "RequestResponse",
                "Payload": b"payload",
            },
            "POST",
            "/2015-03-31/functions/function%3Aprod/invocations",
        ),
        (
            "GetDurableExecution",
            {"DurableExecutionArn": "arn/execution"},
            "GET",
            "/2025-12-01/durable-executions/arn%2Fexecution",
        ),
        (
            "GetDurableExecutionHistory",
            {
                "DurableExecutionArn": "arn/execution",
                "IncludeExecutionData": True,
            },
            "GET",
            "/2025-12-01/durable-executions/arn%2Fexecution/history",
        ),
        (
            "SendDurableExecutionCallbackSuccess",
            {"CallbackId": "callback/id", "Result": b"result"},
            "POST",
            "/2025-12-01/durable-execution-callbacks/callback%2Fid/succeed",
        ),
        (
            "SendDurableExecutionCallbackFailure",
            {
                "CallbackId": "callback/id",
                "Error": {"ErrorType": "Example", "ErrorMessage": "failed"},
            },
            "POST",
            "/2025-12-01/durable-execution-callbacks/callback%2Fid/fail",
        ),
        (
            "SendDurableExecutionCallbackHeartbeat",
            {"CallbackId": "callback/id"},
            "POST",
            "/2025-12-01/durable-execution-callbacks/callback%2Fid/heartbeat",
        ),
    ],
)
def test_cloud_operation_wire_routes(
    operation_name: str,
    params: dict,
    method: str,
    path: str,
) -> None:
    request = _request_factory().prepare(operation_name, params)

    assert request.method == method
    assert urlsplit(request.url).path == path


def test_callback_failure_uses_error_structure_as_payload() -> None:
    request = _request_factory().prepare(
        "SendDurableExecutionCallbackFailure",
        {
            "CallbackId": "callback",
            "Error": {
                "ErrorType": "ExampleError",
                "ErrorMessage": "failed",
                "StackTrace": ["line"],
            },
        },
    )

    assert isinstance(request.body, bytes)
    assert json.loads(request.body) == {
        "ErrorType": "ExampleError",
        "ErrorMessage": "failed",
        "StackTrace": ["line"],
    }


def test_default_user_agent_is_not_duplicated() -> None:
    factory = LambdaHttpRequestFactory(
        session=_session(),
        endpoint_url="https://lambda.us-west-2.amazonaws.com",
        user_agent_extra=f"durable-execution-sdk-python/{__version__}-async",
    )

    assert factory.user_agent.count("durable-execution-sdk-python/") == 1


def test_request_serializes_datetime_as_unix_timestamp() -> None:
    timestamp = datetime.datetime(
        2026,
        1,
        2,
        3,
        4,
        5,
        tzinfo=datetime.timezone.utc,
    )
    request = _request_factory().prepare(
        "CheckpointDurableExecution",
        {
            "DurableExecutionArn": "arn",
            "CheckpointToken": "token",
            "Updates": [{"EndTimestamp": timestamp}],
        },
    )

    assert isinstance(request.body, bytes)
    assert (
        json.loads(request.body)["Updates"][0]["EndTimestamp"] == timestamp.timestamp()
    )


def test_parse_json_response_restores_timestamps_and_metadata() -> None:
    response = parse_lambda_response(
        operation_name="GetDurableExecution",
        status_code=200,
        headers={"x-amzn-requestid": "request-id"},
        content=b'{"Status":"SUCCEEDED","StartTimestamp":1770000000}',
    )

    assert response["Status"] == "SUCCEEDED"
    assert response["StartTimestamp"] == datetime.datetime.fromtimestamp(
        1770000000,
        tz=datetime.timezone.utc,
    )
    assert response["ResponseMetadata"]["RequestId"] == "request-id"


def test_parse_invoke_response_preserves_raw_payload_and_headers() -> None:
    response = parse_lambda_response(
        operation_name="Invoke",
        status_code=202,
        headers={
            "x-amz-durable-execution-arn": "execution-arn",
            "x-amzn-requestid": "request-id",
        },
        content=b'{"accepted":true}',
    )

    assert response["StatusCode"] == 202
    assert response["Payload"] == b'{"accepted":true}'
    assert response["DurableExecutionArn"] == "execution-arn"


def test_parse_error_response_raises_botocore_client_error() -> None:
    with pytest.raises(ClientError) as error:
        parse_lambda_response(
            operation_name="CheckpointDurableExecution",
            status_code=400,
            headers={
                "x-amzn-errortype": "InvalidParameterValueException:http",
                "x-amzn-requestid": "request-id",
            },
            content=b'{"message":"Invalid Checkpoint Token"}',
        )

    assert error.value.response["Error"]["Code"] == "InvalidParameterValueException"
    assert error.value.response["Error"]["Message"] == "Invalid Checkpoint Token"
    assert error.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400


def test_botocore_client_retries_and_reuses_checkpoint_client_token() -> None:
    request = AWSRequest(
        method="POST",
        url="https://example.com",
        data=b"body",
    ).prepare()
    request_factory = Mock()
    request_factory.prepare.return_value = request
    http_session = Mock()
    http_session.send.side_effect = [
        Mock(
            status_code=503,
            headers={"x-amzn-requestid": "first"},
            content=b'{"message":"busy"}',
        ),
        Mock(
            status_code=200,
            headers={"x-amzn-requestid": "second"},
            content=(
                b'{"CheckpointToken":"next","NewExecutionState":{"Operations":[]}}'
            ),
        ),
    ]
    client = BotocoreHttpLambdaClient(
        request_factory=request_factory,
        http_session=http_session,
        max_attempts=2,
    )

    with (
        patch(
            "async_durable_execution._core.aws_http.uuid4",
            return_value="generated-token",
        ),
        patch("async_durable_execution._core.aws_http.time.sleep") as sleep,
    ):
        result = client.checkpoint_durable_execution(
            DurableExecutionArn="arn",
            CheckpointToken="checkpoint",
            Updates=[],
        )

    expected_params = {
        "DurableExecutionArn": "arn",
        "CheckpointToken": "checkpoint",
        "Updates": [],
        "ClientToken": "generated-token",
    }
    assert request_factory.prepare.call_args_list == [
        call("CheckpointDurableExecution", expected_params),
        call("CheckpointDurableExecution", expected_params),
    ]
    sleep.assert_called_once()
    assert result["CheckpointToken"] == "next"
    assert result["ResponseMetadata"]["RetryAttempts"] == 1


def test_botocore_http_client_sends_prepared_request() -> None:
    request = AWSRequest(
        method="GET",
        url="https://example.com",
        data=b"",
    ).prepare()
    request_factory = Mock()
    request_factory.prepare.return_value = request
    response = Mock(
        status_code=200,
        headers={"x-amzn-requestid": "request-id"},
        content=b'{"Operations":[]}',
    )
    http_session = Mock()
    http_session.send.return_value = response
    client = BotocoreHttpLambdaClient(
        request_factory=request_factory,
        http_session=http_session,
    )

    result = client.get_durable_execution_state(CheckpointToken="token")

    request_factory.prepare.assert_called_once_with(
        "GetDurableExecutionState",
        {"CheckpointToken": "token"},
    )
    http_session.send.assert_called_once_with(request)
    assert result["Operations"] == []

    client.close()
    http_session.close.assert_called_once_with()


def test_botocore_factory_applies_configured_retries() -> None:
    client = create_botocore_http_client(
        session=_session(),
        endpoint_url="https://lambda.us-west-2.amazonaws.com",
        config=Config(retries={"max_attempts": 2}),
    )

    assert client._max_attempts == 3  # noqa: SLF001
    client.close()


def test_botocore_factory_uses_config_region() -> None:
    session = Session()
    session.set_credentials("access-key", "secret-key")
    client = create_botocore_http_client(
        session=session,
        config=Config(
            region_name="us-west-2",
            retries={"max_attempts": 0},
        ),
    )

    assert client._request_factory.region_name == "us-west-2"  # noqa: SLF001
    assert (  # noqa: SLF001
        client._request_factory.endpoint_url == "https://lambda.us-west-2.amazonaws.com"
    )
    client.close()


@pytest.mark.parametrize(
    ("config", "expected_endpoint"),
    [
        (
            Config(use_fips_endpoint=True, retries={"max_attempts": 0}),
            "https://lambda-fips.us-west-2.amazonaws.com",
        ),
        (
            Config(use_dualstack_endpoint=True, retries={"max_attempts": 0}),
            "https://lambda.us-west-2.api.aws",
        ),
    ],
)
def test_botocore_factory_uses_config_endpoint_variant(
    config: Config,
    expected_endpoint: str,
) -> None:
    client = create_botocore_http_client(
        session=_session(),
        config=config,
    )

    assert client._request_factory.endpoint_url == expected_endpoint  # noqa: SLF001
    client.close()


@patch.dict(
    "os.environ",
    {"AWS_ENDPOINT_URL_LAMBDA": "http://localhost:3000"},
)
def test_botocore_factory_uses_config_ignore_endpoint_override() -> None:
    config = Config(retries={"max_attempts": 0})
    setattr(config, "ignore_configured_endpoint_urls", True)
    client = create_botocore_http_client(
        session=_session(),
        config=config,
    )

    assert (  # noqa: SLF001
        client._request_factory.endpoint_url == "https://lambda.us-west-2.amazonaws.com"
    )
    client.close()


async def test_httpx_client_sends_prepared_request_asynchronously() -> None:
    request = AWSRequest(
        method="POST",
        url="https://example.com",
        data=b"body",
        headers={"X-Test": "value"},
    ).prepare()
    request_factory = Mock()
    request_factory.prepare.return_value = request
    response = Mock(
        status_code=200,
        headers={"x-amzn-requestid": "request-id"},
        content=b'{"CheckpointToken":"next","NewExecutionState":{"Operations":[]}}',
    )
    http_client = Mock()
    http_client.request = AsyncMock(return_value=response)
    http_client.aclose = AsyncMock()
    client = HttpxLambdaClient(
        request_factory=request_factory,
        http_client=http_client,
    )

    with patch(
        "async_durable_execution._core.aws_http.uuid4",
        return_value="generated-token",
    ):
        result = await client.checkpoint_durable_execution(CheckpointToken="token")

    request_factory.prepare.assert_called_once_with(
        "CheckpointDurableExecution",
        {
            "CheckpointToken": "token",
            "ClientToken": "generated-token",
        },
    )
    http_client.request.assert_awaited_once_with(
        "POST",
        "https://example.com",
        content=b"body",
        headers=request.headers,
    )
    assert result["CheckpointToken"] == "next"

    await client.aclose()
    http_client.aclose.assert_awaited_once_with()


async def test_httpx_client_retries_transport_errors() -> None:
    request = AWSRequest(
        method="GET",
        url="https://example.com",
        data=b"",
    ).prepare()
    request_factory = Mock()
    request_factory.prepare.return_value = request
    response = Mock(
        status_code=200,
        headers={"x-amzn-requestid": "request-id"},
        content=b'{"Operations":[]}',
    )
    http_client = Mock()
    http_client.request = AsyncMock(side_effect=[RuntimeError("temporary"), response])
    http_client.aclose = AsyncMock()
    client = HttpxLambdaClient(
        request_factory=request_factory,
        http_client=http_client,
        max_attempts=2,
    )

    with patch(
        "async_durable_execution._core.aws_http.asyncio.sleep",
        new=AsyncMock(),
    ) as sleep:
        result = await client.get_durable_execution_state(
            DurableExecutionArn="arn",
            CheckpointToken="token",
        )

    assert http_client.request.await_count == 2
    sleep.assert_awaited_once()
    assert result["Operations"] == []
    assert result["ResponseMetadata"]["RetryAttempts"] == 1


async def test_httpx_factory_applies_configured_retries() -> None:
    client = create_httpx_client(
        session=_session(),
        endpoint_url="https://lambda.us-west-2.amazonaws.com",
        config=Config(retries={"max_attempts": 0}),
    )

    assert client._max_attempts == 1  # noqa: SLF001
    await client.aclose()


async def test_httpx_factory_uses_config_region_and_fips() -> None:
    session = Session()
    session.set_credentials("access-key", "secret-key")
    client = create_httpx_client(
        session=session,
        config=Config(
            region_name="us-west-2",
            use_fips_endpoint=True,
            retries={"max_attempts": 0},
        ),
    )

    assert client._request_factory.region_name == "us-west-2"  # noqa: SLF001
    assert (  # noqa: SLF001
        client._request_factory.endpoint_url
        == "https://lambda-fips.us-west-2.amazonaws.com"
    )
    await client.aclose()
