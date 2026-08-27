"""Model-free AWS Lambda HTTP clients.

The clients in this module deliberately bypass botocore's generated Lambda
service model. Botocore remains responsible for credential discovery, endpoint
metadata, SigV4 signing, and the synchronous HTTP transport. The optional
``aioboto`` extra supplies HTTPX as the asynchronous transport.
"""

from __future__ import annotations

import asyncio
import datetime
import importlib
import json
import random
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol, cast
from urllib.parse import quote, urlencode
from uuid import uuid4

from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSPreparedRequest, AWSRequest
from botocore.config import Config
from botocore.configprovider import ConfiguredEndpointProvider
from botocore.exceptions import (
    ClientError,
    InvalidRetryConfigurationError,
    InvalidRetryModeError,
    NoCredentialsError,
    NoRegionError,
    UnknownEndpointError,
)
from botocore.httpsession import URLLib3Session
from botocore.regions import EndpointResolver
from botocore.session import Session, get_session
from botocore.utils import ensure_boolean
from botocore.utils import get_environ_proxies

from ..__about__ import __version__


_LAMBDA_SERVICE_NAME = "lambda"
_DURABLE_API_VERSION = "2025-12-01"
_INVOKE_API_VERSION = "2015-03-31"
_DEFAULT_CONNECT_TIMEOUT_SECONDS = 5
_DEFAULT_READ_TIMEOUT_SECONDS = 50
_DEFAULT_MAX_POOL_CONNECTIONS = 10
_DEFAULT_LEGACY_MAX_ATTEMPTS = 5
_DEFAULT_STANDARD_MAX_ATTEMPTS = 3
_MAX_RETRY_DELAY_SECONDS = 20.0
_RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504, 509})
_RETRYABLE_ERROR_CODES = frozenset(
    {
        "Throttling",
        "ThrottlingException",
        "ThrottledException",
        "RequestThrottledException",
        "ProvisionedThroughputExceededException",
    }
)


class _SyncHttpSession(Protocol):
    def send(self, request: AWSPreparedRequest) -> Any: ...

    def close(self) -> None: ...


class _AsyncHttpClient(Protocol):
    async def request(
        self,
        method: str,
        url: str,
        *,
        content: bytes,
        headers: Mapping[str, str],
    ) -> Any: ...

    async def aclose(self) -> None: ...


@dataclass(frozen=True)
class _RequestSpec:
    method: str
    path: str
    query: Mapping[str, Any]
    headers: Mapping[str, str]
    body: bytes


@dataclass(frozen=True)
class _EndpointResolution:
    endpoint_url: str
    signing_region: str
    signing_name: str


def _quote_path_value(value: Any) -> str:
    return quote(str(value), safe="-_.~")


def _query_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime.datetime):
        return value.timestamp()
    if isinstance(value, datetime.date):
        return datetime.datetime.combine(
            value,
            datetime.time(),
            tzinfo=datetime.timezone.utc,
        ).timestamp()
    if isinstance(value, bytes):
        return value.decode("utf-8")
    msg = f"Object of type {type(value).__name__} is not JSON serializable"
    raise TypeError(msg)


def _json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        default=_json_default,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")


def _body_params(
    params: Mapping[str, Any],
    *,
    excluded: frozenset[str],
) -> dict[str, Any]:
    return {
        key: value
        for key, value in params.items()
        if key not in excluded and value is not None
    }


def _request_spec(operation_name: str, params: Mapping[str, Any]) -> _RequestSpec:
    if operation_name == "CheckpointDurableExecution":
        execution_arn = _quote_path_value(params["DurableExecutionArn"])
        return _RequestSpec(
            method="POST",
            path=(
                f"/{_DURABLE_API_VERSION}/durable-executions/{execution_arn}/checkpoint"
            ),
            query={},
            headers={"Content-Type": "application/json"},
            body=_json_bytes(
                _body_params(
                    params,
                    excluded=frozenset({"DurableExecutionArn"}),
                )
            ),
        )

    if operation_name == "GetDurableExecutionState":
        execution_arn = _quote_path_value(params["DurableExecutionArn"])
        return _RequestSpec(
            method="GET",
            path=(f"/{_DURABLE_API_VERSION}/durable-executions/{execution_arn}/state"),
            query=_body_params(
                params,
                excluded=frozenset({"DurableExecutionArn"}),
            ),
            headers={},
            body=b"",
        )

    if operation_name == "Invoke":
        function_name = _quote_path_value(params["FunctionName"])
        headers = {}
        header_names = {
            "InvocationType": "X-Amz-Invocation-Type",
            "LogType": "X-Amz-Log-Type",
            "ClientContext": "X-Amz-Client-Context",
            "DurableExecutionName": "X-Amz-Durable-Execution-Name",
            "TenantId": "X-Amz-Tenant-Id",
        }
        for parameter_name, header_name in header_names.items():
            value = params.get(parameter_name)
            if value is not None:
                headers[header_name] = str(value)
        query = {}
        if params.get("Qualifier") is not None:
            query["Qualifier"] = params["Qualifier"]
        payload = params.get("Payload", b"")
        if isinstance(payload, str):
            payload = payload.encode("utf-8")
        return _RequestSpec(
            method="POST",
            path=f"/{_INVOKE_API_VERSION}/functions/{function_name}/invocations",
            query=query,
            headers=headers,
            body=cast(bytes, payload),
        )

    if operation_name == "GetDurableExecution":
        execution_arn = _quote_path_value(params["DurableExecutionArn"])
        return _RequestSpec(
            method="GET",
            path=f"/{_DURABLE_API_VERSION}/durable-executions/{execution_arn}",
            query=_body_params(
                params,
                excluded=frozenset({"DurableExecutionArn"}),
            ),
            headers={},
            body=b"",
        )

    if operation_name == "GetDurableExecutionHistory":
        execution_arn = _quote_path_value(params["DurableExecutionArn"])
        return _RequestSpec(
            method="GET",
            path=(
                f"/{_DURABLE_API_VERSION}/durable-executions/{execution_arn}/history"
            ),
            query=_body_params(
                params,
                excluded=frozenset({"DurableExecutionArn"}),
            ),
            headers={},
            body=b"",
        )

    if operation_name == "SendDurableExecutionCallbackSuccess":
        callback_id = _quote_path_value(params["CallbackId"])
        result = params.get("Result", b"")
        if result is None:
            result = b""
        if isinstance(result, str):
            result = result.encode("utf-8")
        return _RequestSpec(
            method="POST",
            path=(
                f"/{_DURABLE_API_VERSION}/durable-execution-callbacks/"
                f"{callback_id}/succeed"
            ),
            query={},
            headers={},
            body=cast(bytes, result),
        )

    if operation_name == "SendDurableExecutionCallbackFailure":
        callback_id = _quote_path_value(params["CallbackId"])
        error = params.get("Error") or {}
        return _RequestSpec(
            method="POST",
            path=(
                f"/{_DURABLE_API_VERSION}/durable-execution-callbacks/"
                f"{callback_id}/fail"
            ),
            query={},
            headers={"Content-Type": "application/json"},
            body=_json_bytes(error),
        )

    if operation_name == "SendDurableExecutionCallbackHeartbeat":
        callback_id = _quote_path_value(params["CallbackId"])
        return _RequestSpec(
            method="POST",
            path=(
                f"/{_DURABLE_API_VERSION}/durable-execution-callbacks/"
                f"{callback_id}/heartbeat"
            ),
            query={},
            headers={},
            body=b"",
        )

    msg = f"Unsupported Lambda operation: {operation_name}"
    raise ValueError(msg)


def _configured_endpoint_url(
    session: Session,
    *,
    ignore_configured_endpoint_urls: bool | None = None,
) -> str | None:
    ignore_configured = ignore_configured_endpoint_urls
    if ignore_configured is None:
        ignore_configured = ensure_boolean(
            session.get_config_variable("ignore_configured_endpoint_urls")
        )
    if ignore_configured:
        return None
    provider = ConfiguredEndpointProvider(
        full_config=session.full_config,
        scoped_config=session.get_scoped_config(),
        client_name=_LAMBDA_SERVICE_NAME,
    )
    return cast(str | None, provider.provide())


def _resolve_region(session: Session, region_name: str | None) -> str:
    resolved_region = region_name or session.get_config_variable("region")
    if not resolved_region:
        raise NoRegionError()
    return cast(str, resolved_region)


def _normalize_fips_region(region_name: str) -> tuple[str, bool]:
    if region_name.startswith("fips-"):
        return region_name.removeprefix("fips-"), True
    if region_name.endswith("-fips"):
        return region_name.removesuffix("-fips"), True
    return region_name, False


def _resolve_endpoint(
    session: Session,
    *,
    region_name: str,
    endpoint_url: str | None,
    use_dualstack_endpoint: bool | None = None,
    use_fips_endpoint: bool | None = None,
    ignore_configured_endpoint_urls: bool | None = None,
) -> _EndpointResolution:
    resolved_endpoint = endpoint_url or _configured_endpoint_url(
        session,
        ignore_configured_endpoint_urls=ignore_configured_endpoint_urls,
    )

    if use_dualstack_endpoint is None:
        use_dualstack_endpoint = ensure_boolean(
            session.get_config_variable("use_dualstack_endpoint")
        )
    if use_fips_endpoint is None:
        use_fips_endpoint = ensure_boolean(
            session.get_config_variable("use_fips_endpoint")
        )
    endpoint = EndpointResolver(session.get_data("endpoints")).construct_endpoint(
        _LAMBDA_SERVICE_NAME,
        region_name,
        use_dualstack_endpoint=use_dualstack_endpoint,
        use_fips_endpoint=use_fips_endpoint,
    )
    if endpoint is None and not resolved_endpoint:
        raise UnknownEndpointError(
            service_name=_LAMBDA_SERVICE_NAME,
            region_name=region_name,
        )

    credential_scope = endpoint.get("credentialScope", {}) if endpoint else {}
    signing_region = credential_scope.get("region", region_name)
    signing_name = credential_scope.get("service", _LAMBDA_SERVICE_NAME)
    if resolved_endpoint:
        resolved_url = resolved_endpoint.rstrip("/")
    else:
        assert endpoint is not None
        protocols = endpoint.get("protocols") or ["https"]
        resolved_url = f"{protocols[0]}://{endpoint['hostname']}"

    return _EndpointResolution(
        endpoint_url=resolved_url,
        signing_region=signing_region,
        signing_name=signing_name,
    )


def _resolve_max_attempts(session: Session, config: Config) -> int:
    config_values = cast("Any", config)
    configured_retries = config_values.retries or {}

    total_max_attempts = configured_retries.get("total_max_attempts")
    if total_max_attempts is not None:
        return max(1, int(total_max_attempts))

    max_attempts = configured_retries.get("max_attempts")
    if max_attempts is not None:
        return max(1, int(max_attempts) + 1)

    session_max_attempts = session.get_config_variable("max_attempts")
    if session_max_attempts is not None:
        return max(1, int(session_max_attempts))

    retry_mode = (
        configured_retries.get("mode")
        or session.get_config_variable("retry_mode")
        or "legacy"
    )
    if retry_mode not in {"legacy", "standard", "adaptive"}:
        raise InvalidRetryModeError(
            provided_retry_mode=retry_mode,
            valid_modes="legacy, standard, adaptive",
        )
    if retry_mode == "adaptive":
        raise InvalidRetryConfigurationError(
            retry_config_option="mode=adaptive",
            valid_options="mode=legacy, mode=standard",
        )
    if retry_mode == "standard":
        return _DEFAULT_STANDARD_MAX_ATTEMPTS
    return _DEFAULT_LEGACY_MAX_ATTEMPTS


def _retry_delay_seconds(retry_index: int) -> float:
    exponential_ceiling = min(2**retry_index, _MAX_RETRY_DELAY_SECONDS)
    return random.random() * exponential_ceiling  # noqa: S311


def _response_is_retryable(
    *,
    status_code: int,
    headers: Mapping[str, Any],
    content: bytes,
) -> bool:
    if status_code in _RETRYABLE_STATUS_CODES:
        return True
    if status_code < 400:
        return False

    normalized_headers = _normalized_headers(headers)
    try:
        body = _decode_json_body(content)
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError):
        body = {}
    return _error_code(normalized_headers, body) in _RETRYABLE_ERROR_CODES


def _checkpoint_params_with_client_token(
    params: Mapping[str, Any],
) -> dict[str, Any]:
    resolved = dict(params)
    if resolved.get("ClientToken") is None:
        resolved["ClientToken"] = str(uuid4())
    return resolved


class LambdaHttpRequestFactory:
    """Build signed Lambda requests without loading a botocore service model."""

    def __init__(
        self,
        *,
        session: Session | None = None,
        region_name: str | None = None,
        endpoint_url: str | None = None,
        user_agent_extra: str | None = None,
        use_dualstack_endpoint: bool | None = None,
        use_fips_endpoint: bool | None = None,
        ignore_configured_endpoint_urls: bool | None = None,
    ) -> None:
        self.session = session or get_session()
        configured_region = _resolve_region(self.session, region_name)
        configured_region, legacy_fips_region = _normalize_fips_region(
            configured_region
        )
        if legacy_fips_region:
            use_fips_endpoint = True
        endpoint = _resolve_endpoint(
            self.session,
            region_name=configured_region,
            endpoint_url=endpoint_url,
            use_dualstack_endpoint=use_dualstack_endpoint,
            use_fips_endpoint=use_fips_endpoint,
            ignore_configured_endpoint_urls=ignore_configured_endpoint_urls,
        )
        self.endpoint_url = endpoint.endpoint_url
        self.region_name = endpoint.signing_region
        self.signing_name = endpoint.signing_name
        self.user_agent = f"durable-execution-sdk-python/{__version__}-async"
        if user_agent_extra and self.user_agent not in user_agent_extra:
            self.user_agent = f"{self.user_agent} {user_agent_extra}"
        elif user_agent_extra:
            self.user_agent = user_agent_extra

    def prepare(
        self, operation_name: str, params: Mapping[str, Any]
    ) -> AWSPreparedRequest:
        spec = _request_spec(operation_name, params)
        query = {
            key: _query_value(value)
            for key, value in spec.query.items()
            if value is not None
        }
        query_string = urlencode(query, quote_via=quote, safe="-_.~")
        url = f"{self.endpoint_url}{spec.path}"
        if query_string:
            url = f"{url}?{query_string}"

        headers = {
            "User-Agent": self.user_agent,
            **spec.headers,
        }
        credentials = self.session.get_credentials()
        if credentials is None:
            raise NoCredentialsError()
        frozen_credentials = credentials.get_frozen_credentials()

        request = AWSRequest(
            method=spec.method,
            url=url,
            data=spec.body,
            headers=headers,
        )
        SigV4Auth(
            frozen_credentials,
            self.signing_name,
            self.region_name,
        ).add_auth(request)
        return request.prepare()


def _normalized_headers(headers: Mapping[str, Any]) -> dict[str, str]:
    return {str(key).lower(): str(value) for key, value in headers.items()}


def _response_metadata(
    status_code: int,
    headers: Mapping[str, Any],
) -> dict[str, Any]:
    normalized = _normalized_headers(headers)
    return {
        "RequestId": (
            normalized.get("x-amzn-requestid") or normalized.get("x-amzn-request-id")
        ),
        "HTTPStatusCode": status_code,
        "HTTPHeaders": normalized,
        "RetryAttempts": 0,
    }


def _decode_timestamps(value: Any, *, field_name: str | None = None) -> Any:
    if (
        field_name is not None
        and field_name.endswith("Timestamp")
        and isinstance(value, int | float)
    ):
        return datetime.datetime.fromtimestamp(value, tz=datetime.timezone.utc)
    if isinstance(value, list):
        return [_decode_timestamps(item) for item in value]
    if isinstance(value, dict):
        return {
            key: _decode_timestamps(item, field_name=key) for key, item in value.items()
        }
    return value


def _decode_json_body(content: bytes) -> dict[str, Any]:
    if not content:
        return {}
    decoded = json.loads(content)
    if not isinstance(decoded, dict):
        msg = "AWS Lambda returned a non-object JSON response"
        raise ValueError(msg)
    return cast(dict[str, Any], _decode_timestamps(decoded))


def _error_code(headers: Mapping[str, str], body: Mapping[str, Any]) -> str:
    header_code = headers.get("x-amzn-errortype")
    if header_code:
        return header_code.split(":", 1)[0].rsplit("#", 1)[-1]

    body_code = body.get("__type") or body.get("code") or body.get("Code")
    if body_code:
        return str(body_code).split(":", 1)[0].rsplit("#", 1)[-1]
    return "UnknownError"


def parse_lambda_response(
    *,
    operation_name: str,
    status_code: int,
    headers: Mapping[str, Any],
    content: bytes,
    retry_attempts: int = 0,
) -> dict[str, Any]:
    """Parse a Lambda REST response into the established AWS API mapping."""

    metadata = _response_metadata(status_code, headers)
    metadata["RetryAttempts"] = retry_attempts
    normalized_headers = cast(dict[str, str], metadata["HTTPHeaders"])

    if status_code < 200 or status_code >= 300:
        try:
            body = _decode_json_body(content)
        except (UnicodeDecodeError, ValueError, json.JSONDecodeError):
            body = {}
        message = (
            body.get("message")
            or body.get("Message")
            or content.decode("utf-8", errors="replace")
            or f"HTTP {status_code}"
        )
        error_response = {
            "Error": {
                "Code": _error_code(normalized_headers, body),
                "Message": str(message),
            },
            "ResponseMetadata": metadata,
        }
        raise ClientError(cast("Any", error_response), operation_name)

    if operation_name == "Invoke":
        response: dict[str, Any] = {
            "StatusCode": status_code,
            "Payload": content,
        }
        invoke_headers = {
            "FunctionError": "x-amz-function-error",
            "LogResult": "x-amz-log-result",
            "ExecutedVersion": "x-amz-executed-version",
            "DurableExecutionArn": "x-amz-durable-execution-arn",
        }
        for output_name, header_name in invoke_headers.items():
            value = normalized_headers.get(header_name)
            if value is not None:
                response[output_name] = value
    else:
        response = _decode_json_body(content)

    response["ResponseMetadata"] = metadata
    return response


class BotocoreHttpLambdaClient:
    """Synchronous model-free Lambda client using botocore's HTTP session."""

    def __init__(
        self,
        *,
        request_factory: LambdaHttpRequestFactory,
        http_session: _SyncHttpSession,
        max_attempts: int = 1,
    ) -> None:
        self._request_factory = request_factory
        self._http_session = http_session
        self._max_attempts = max_attempts

    def _call(self, operation_name: str, **kwargs: Any) -> dict[str, Any]:
        for attempt in range(self._max_attempts):
            request = self._request_factory.prepare(operation_name, kwargs)
            try:
                response = self._http_session.send(request)
            except Exception:
                if attempt + 1 >= self._max_attempts:
                    raise
                time.sleep(_retry_delay_seconds(attempt))
                continue

            if attempt + 1 < self._max_attempts and _response_is_retryable(
                status_code=response.status_code,
                headers=response.headers,
                content=response.content,
            ):
                time.sleep(_retry_delay_seconds(attempt))
                continue

            return parse_lambda_response(
                operation_name=operation_name,
                status_code=response.status_code,
                headers=response.headers,
                content=response.content,
                retry_attempts=attempt,
            )

        msg = "Lambda HTTP retry loop exited without a response"
        raise RuntimeError(msg)

    def checkpoint_durable_execution(self, **kwargs: Any) -> dict[str, Any]:
        return self._call(
            "CheckpointDurableExecution",
            **_checkpoint_params_with_client_token(kwargs),
        )

    def get_durable_execution_state(self, **kwargs: Any) -> dict[str, Any]:
        return self._call("GetDurableExecutionState", **kwargs)

    def invoke(self, **kwargs: Any) -> dict[str, Any]:
        return self._call("Invoke", **kwargs)

    def get_durable_execution(self, **kwargs: Any) -> dict[str, Any]:
        return self._call("GetDurableExecution", **kwargs)

    def get_durable_execution_history(self, **kwargs: Any) -> dict[str, Any]:
        return self._call("GetDurableExecutionHistory", **kwargs)

    def send_durable_execution_callback_success(self, **kwargs: Any) -> dict[str, Any]:
        return self._call("SendDurableExecutionCallbackSuccess", **kwargs)

    def send_durable_execution_callback_failure(self, **kwargs: Any) -> dict[str, Any]:
        return self._call("SendDurableExecutionCallbackFailure", **kwargs)

    def send_durable_execution_callback_heartbeat(
        self, **kwargs: Any
    ) -> dict[str, Any]:
        return self._call("SendDurableExecutionCallbackHeartbeat", **kwargs)

    def close(self) -> None:
        self._http_session.close()


class HttpxLambdaClient:
    """Asynchronous model-free Lambda client using HTTPX."""

    def __init__(
        self,
        *,
        request_factory: LambdaHttpRequestFactory,
        http_client: _AsyncHttpClient,
        max_attempts: int = 1,
    ) -> None:
        self._request_factory = request_factory
        self._http_client = http_client
        self._max_attempts = max_attempts

    async def _call(self, operation_name: str, **kwargs: Any) -> dict[str, Any]:
        for attempt in range(self._max_attempts):
            request = await asyncio.to_thread(
                self._request_factory.prepare,
                operation_name,
                kwargs,
            )
            try:
                response = await self._http_client.request(
                    request.method,
                    request.url,
                    content=cast(bytes, request.body or b""),
                    headers=cast(Mapping[str, str], request.headers),
                )
            except Exception:
                if attempt + 1 >= self._max_attempts:
                    raise
                await asyncio.sleep(_retry_delay_seconds(attempt))
                continue

            if attempt + 1 < self._max_attempts and _response_is_retryable(
                status_code=response.status_code,
                headers=response.headers,
                content=response.content,
            ):
                await asyncio.sleep(_retry_delay_seconds(attempt))
                continue

            return parse_lambda_response(
                operation_name=operation_name,
                status_code=response.status_code,
                headers=response.headers,
                content=response.content,
                retry_attempts=attempt,
            )

        msg = "Lambda HTTP retry loop exited without a response"
        raise RuntimeError(msg)

    async def checkpoint_durable_execution(self, **kwargs: Any) -> dict[str, Any]:
        return await self._call(
            "CheckpointDurableExecution",
            **_checkpoint_params_with_client_token(kwargs),
        )

    async def get_durable_execution_state(self, **kwargs: Any) -> dict[str, Any]:
        return await self._call("GetDurableExecutionState", **kwargs)

    async def invoke(self, **kwargs: Any) -> dict[str, Any]:
        return await self._call("Invoke", **kwargs)

    async def get_durable_execution(self, **kwargs: Any) -> dict[str, Any]:
        return await self._call("GetDurableExecution", **kwargs)

    async def get_durable_execution_history(self, **kwargs: Any) -> dict[str, Any]:
        return await self._call("GetDurableExecutionHistory", **kwargs)

    async def send_durable_execution_callback_success(
        self, **kwargs: Any
    ) -> dict[str, Any]:
        return await self._call("SendDurableExecutionCallbackSuccess", **kwargs)

    async def send_durable_execution_callback_failure(
        self, **kwargs: Any
    ) -> dict[str, Any]:
        return await self._call("SendDurableExecutionCallbackFailure", **kwargs)

    async def send_durable_execution_callback_heartbeat(
        self, **kwargs: Any
    ) -> dict[str, Any]:
        return await self._call("SendDurableExecutionCallbackHeartbeat", **kwargs)

    async def aclose(self) -> None:
        await self._http_client.aclose()


def create_botocore_http_client(
    *,
    session: Session | None = None,
    region_name: str | None = None,
    endpoint_url: str | None = None,
    config: Config | None = None,
) -> BotocoreHttpLambdaClient:
    """Create the default synchronous model-free Lambda HTTP client."""

    resolved_session = session or get_session()
    resolved_config = config or Config(
        connect_timeout=_DEFAULT_CONNECT_TIMEOUT_SECONDS,
        read_timeout=_DEFAULT_READ_TIMEOUT_SECONDS,
    )
    config_values = cast("Any", resolved_config)
    request_factory = LambdaHttpRequestFactory(
        session=resolved_session,
        region_name=region_name or config_values.region_name,
        endpoint_url=endpoint_url,
        user_agent_extra=config_values.user_agent_extra,
        use_dualstack_endpoint=config_values.use_dualstack_endpoint,
        use_fips_endpoint=config_values.use_fips_endpoint,
        ignore_configured_endpoint_urls=(config_values.ignore_configured_endpoint_urls),
    )
    ca_bundle = resolved_session.get_config_variable("ca_bundle")
    verify: bool | str = ca_bundle if isinstance(ca_bundle, str) else True
    http_session = cast("Any", URLLib3Session)(
        verify=verify,
        proxies=config_values.proxies
        or get_environ_proxies(request_factory.endpoint_url),
        timeout=(
            config_values.connect_timeout,
            config_values.read_timeout,
        ),
        max_pool_connections=(
            config_values.max_pool_connections or _DEFAULT_MAX_POOL_CONNECTIONS
        ),
        client_cert=config_values.client_cert,
        proxies_config=config_values.proxies_config,
    )
    return BotocoreHttpLambdaClient(
        request_factory=request_factory,
        http_session=http_session,
        max_attempts=_resolve_max_attempts(resolved_session, resolved_config),
    )


def create_httpx_client(
    *,
    session: Session | None = None,
    region_name: str | None = None,
    endpoint_url: str | None = None,
    config: Config | None = None,
) -> HttpxLambdaClient:
    """Create the default asynchronous model-free Lambda HTTP client."""

    httpx = importlib.import_module("httpx")
    resolved_session = session or get_session()
    resolved_config = config or Config(
        connect_timeout=_DEFAULT_CONNECT_TIMEOUT_SECONDS,
        read_timeout=_DEFAULT_READ_TIMEOUT_SECONDS,
    )
    config_values = cast("Any", resolved_config)
    request_factory = LambdaHttpRequestFactory(
        session=resolved_session,
        region_name=region_name or config_values.region_name,
        endpoint_url=endpoint_url,
        user_agent_extra=config_values.user_agent_extra,
        use_dualstack_endpoint=config_values.use_dualstack_endpoint,
        use_fips_endpoint=config_values.use_fips_endpoint,
        ignore_configured_endpoint_urls=(config_values.ignore_configured_endpoint_urls),
    )
    ca_bundle = resolved_session.get_config_variable("ca_bundle")
    verify: Any = True
    if ca_bundle:
        import ssl

        verify = ssl.create_default_context(cafile=ca_bundle)
    http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(
            config_values.read_timeout,
            connect=config_values.connect_timeout,
        ),
        limits=httpx.Limits(
            max_connections=(
                config_values.max_pool_connections or _DEFAULT_MAX_POOL_CONNECTIONS
            ),
            max_keepalive_connections=(
                config_values.max_pool_connections or _DEFAULT_MAX_POOL_CONNECTIONS
            ),
        ),
        verify=verify,
        follow_redirects=False,
    )
    return HttpxLambdaClient(
        request_factory=request_factory,
        http_client=http_client,
        max_attempts=_resolve_max_attempts(resolved_session, resolved_config),
    )


__all__ = [
    "BotocoreHttpLambdaClient",
    "HttpxLambdaClient",
    "LambdaHttpRequestFactory",
    "create_botocore_http_client",
    "create_httpx_client",
    "parse_lambda_response",
]
