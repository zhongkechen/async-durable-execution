"""AWS protocol adapters built from botocore's published Lambda service model."""

from __future__ import annotations

import asyncio
import importlib.util
import inspect
import json
import random
import time
import uuid
from typing import Any, cast
from botocore.awsrequest import AWSRequest, prepare_request_dict
from botocore.config import Config
from botocore.exceptions import ClientError
from botocore.parsers import create_parser
from botocore.serialize import create_serializer
from botocore.session import Session

from ._journal import Change, mapping
from ._types import ErrorObject, ExecutionError, InvocationError
from ._views import DurableFunctionTestResult, history_entries


def create_default_sync_client(
    *, session=None, endpoint_url=None, region_name=None, config=None
):
    """Create a standard signed Lambda client with refreshable AWS credentials."""
    return (session or Session()).create_client(
        "lambda",
        region_name=region_name,
        endpoint_url=endpoint_url,
        config=config or Config(connect_timeout=5, read_timeout=50),
    )


async def call(client, name, **kwargs):
    method = getattr(client, name)
    output = (
        await method(**kwargs)
        if inspect.iscoroutinefunction(method)
        else await asyncio.to_thread(method, **kwargs)
    )
    return await output if inspect.isawaitable(output) else output


class AsyncAws:
    """Model-driven HTTPX transport, sharing botocore endpoint and signing rules."""

    def __init__(self, *, region_name=None, endpoint_url=None, config=None):
        import httpx
        import ssl

        self.session = Session()
        self.config = cast(Any, config or Config(connect_timeout=5, read_timeout=50))
        self.client = create_default_sync_client(
            session=self.session,
            region_name=region_name,
            endpoint_url=endpoint_url,
            config=self.config,
        )
        options: dict[str, Any] = {"trust_env": self.config.proxies is None}
        if self.config.proxies:
            from urllib.parse import urlsplit

            proxy = self.config.proxies.get(
                urlsplit(self.client.meta.endpoint_url).scheme
            )
            if proxy:
                options["proxy"] = proxy
        bundle = self.session.get_config_variable("ca_bundle")
        verify = ssl.create_default_context(cafile=bundle) if bundle else True
        self.http = httpx.AsyncClient(
            timeout=httpx.Timeout(
                self.config.read_timeout, connect=self.config.connect_timeout
            ),
            verify=verify,
            follow_redirects=False,
            **options,
        )

    def __getattr__(self, method):
        operation = self.client.meta.method_to_api_mapping.get(method)
        if operation is None:
            raise AttributeError(method)

        async def invoke(**params):
            return await self.request(operation, params)

        return invoke

    def prepare(self, operation, params):
        model = self.client.meta.service_model.operation_model(operation)
        request = create_serializer(
            self.client.meta.service_model.protocol
        ).serialize_to_request(params, model)
        prepare_request_dict(request, self.client.meta.endpoint_url)
        message = AWSRequest(
            method=request["method"],
            url=request["url"],
            data=request["body"],
            headers=request["headers"],
        )
        self.client._request_signer.sign(operation, message)
        return message.prepare(), model

    async def request(self, operation, params):
        if operation == "CheckpointDurableExecution":
            params = dict(params)
            params.setdefault("ClientToken", uuid.uuid4().hex)
        retries = self.config.retries or {}
        attempts = retries.get("total_max_attempts", retries.get("max_attempts", 2) + 1)
        for index in range(max(1, attempts)):
            prepared, model = await asyncio.to_thread(self.prepare, operation, params)
            try:
                response = await self.http.request(
                    prepared.method,
                    prepared.url,
                    content=prepared.body,
                    headers={
                        key: value.decode() if isinstance(value, bytes) else value
                        for key, value in prepared.headers.items()
                    },
                )
            except Exception:
                if index + 1 >= attempts:
                    raise
            else:
                # Modeled header names may use different casing from HTTPX's keys.
                headers = response.headers
                parsed = create_parser(self.client.meta.service_model.protocol).parse(
                    {
                        "status_code": response.status_code,
                        "headers": headers,
                        "body": response.content,
                    },
                    model.output_shape,
                )
                parsed["ResponseMetadata"]["HTTPHeaders"] = dict(headers)
                if response.status_code < 300:
                    if operation == "Invoke":
                        parsed["Payload"] = response.content
                    return parsed
                if (
                    response.status_code not in (429, 500, 502, 503, 504)
                    or index + 1 >= attempts
                ):
                    raise ClientError(parsed, operation)
            await asyncio.sleep(random.random() * min(20, 2**index))

    async def aclose(self):
        await self.http.aclose()
        self.client.close()


def default_api(**kwargs):
    return (
        AsyncAws(**kwargs)
        if importlib.util.find_spec("httpx")
        else create_default_sync_client(**kwargs)
    )


def api_error(error):
    response = getattr(error, "response", {})
    detail = response.get("Error", {})
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode", 500)
    code = detail.get("Code", "")
    stale = (
        str(detail.get("Message", "")).lower().startswith("invalid checkpoint token")
    )
    retryable = (status >= 500 or status == 429 or stale) and not code.startswith("KMS")
    return InvocationError(str(error)) if retryable else ExecutionError(str(error))


class ServiceBackend:
    def __init__(self, arn, *, service_client=None, api_client=None):
        self.arn, self.service, self.api = arn, service_client, api_client
        self.owned = service_client is None and api_client is None

    async def _client(self):
        if self.api is None:
            self.api = await asyncio.to_thread(default_api)
        return self.api

    async def transact(self, token, updates):
        try:
            if self.service is not None:
                value = await call(
                    self.service,
                    "checkpoint",
                    durable_execution_arn=self.arn,
                    checkpoint_token=token,
                    updates=[Change(update) for update in updates],
                    client_token=uuid.uuid4().hex,
                )
            else:
                value = await call(
                    await self._client(),
                    "checkpoint_durable_execution",
                    DurableExecutionArn=self.arn,
                    CheckpointToken=token,
                    Updates=updates,
                    ClientToken=uuid.uuid4().hex,
                )
            value = mapping(value)
            if not value.get("CheckpointToken") and not any(
                item["Type"] == "EXECUTION" and item["Action"] in ("SUCCEED", "FAIL")
                for item in updates
            ):
                raise InvocationError("Backend omitted the next checkpoint lease")
            return value
        except (InvocationError, ExecutionError):
            raise
        except Exception as error:
            raise api_error(error) from error

    async def page(self, token, marker):
        try:
            if self.service is not None:
                return mapping(
                    await call(
                        self.service,
                        "get_execution_state",
                        durable_execution_arn=self.arn,
                        checkpoint_token=token,
                        next_marker=marker,
                        max_items=1000,
                    )
                )
            return await call(
                await self._client(),
                "get_durable_execution_state",
                DurableExecutionArn=self.arn,
                CheckpointToken=token,
                Marker=marker,
                MaxItems=1000,
            )
        except Exception as error:
            raise api_error(error) from error

    async def close(self):
        if self.owned and self.api is not None:
            close = getattr(self.api, "aclose", None)
            if close:
                await close()
            else:
                await asyncio.to_thread(self.api.close)


class DurableFunctionCloudTestRunner:
    """Invoke a deployed durable Lambda function and inspect its execution history.

    The client is created lazily. Use an async context manager to close either
    transport. run() waits for completion; run_async() returns an ARN for later
    polling or callback interaction. This runner makes real AWS API calls.
    """

    def __init__(
        self,
        function_name,
        region="us-west-2",
        lambda_endpoint=None,
        poll_interval=1.0,
        input=None,
        timeout=60,
    ):
        """Configure the target function and completion-polling defaults.

        Args:
            function_name (str): Function name or ARN qualified by version, alias, or
                $LATEST.
            region (str): AWS region; defaults to us-west-2.
            lambda_endpoint (str | None): Optional Lambda endpoint URL override.
            poll_interval (float): Interval between status/history polls in seconds.
            input (Any): JSON-serializable application input.
            timeout (int): Default completion-polling timeout in seconds.
        """
        self.mode = "cloud"
        self.function_name, self.region, self.lambda_endpoint = (
            function_name,
            region,
            lambda_endpoint,
        )
        self.poll_interval, self.input, self.timeout = poll_interval, input, timeout
        self.lambda_client = None

    async def _request(self, method, **params):
        if self.lambda_client is None:
            self.lambda_client = await asyncio.to_thread(
                default_api,
                region_name=self.region,
                endpoint_url=self.lambda_endpoint,
                config=Config(read_timeout=960, retries={"max_attempts": 0}),
            )
        return await call(self.lambda_client, method, **params)

    async def __aenter__(self):
        """Return this runner without eagerly creating its AWS client."""
        return self

    async def __aexit__(self, *args):
        """Close the runner transport when leaving its async context."""
        await self.aclose()

    def close(self):
        """Close a synchronous client when present; use aclose() for async transports."""
        if self.lambda_client is not None and hasattr(self.lambda_client, "close"):
            self.lambda_client.close()

    async def aclose(self):
        """Close the configured asynchronous or synchronous Lambda client."""
        if self.lambda_client is not None and hasattr(self.lambda_client, "aclose"):
            await self.lambda_client.aclose()
        else:
            self.close()

    async def _invoke(self, mode):
        value = await self._request(
            "invoke",
            FunctionName=self.function_name,
            InvocationType=mode,
            Payload=json.dumps(self.input),
        )
        if not value.get("DurableExecutionArn"):
            raise ExecutionError("Lambda did not return a durable execution ARN")
        return value["DurableExecutionArn"]

    async def run_async(self):
        """Invoke the function with Event mode and return its durable execution ARN.

        Returns:
            (str): ARN used for result polling and callback discovery.

        Raises:
            ExecutionError: Lambda does not include a durable execution ARN in its
                response.
        """
        return await self._invoke("Event")

    async def run(self):
        """Invoke with RequestResponse mode, then poll until the execution completes.

        Returns:
            (DurableFunctionTestResult): Final status, payload, failure details, and
                operation views.

        Raises:
            TimeoutError: Completion polling exceeds the configured timeout.
        """
        return await self.wait_for_result(
            await self._invoke("RequestResponse"), self.timeout
        )

    async def _history(self, arn):
        events, marker, visited = [], None, set()
        while True:
            params = {"DurableExecutionArn": arn, "IncludeExecutionData": True}
            if marker:
                params["Marker"] = marker
            page = await self._request("get_durable_execution_history", **params)
            events.extend(page.get("Events", ()))
            marker = page.get("NextMarker")
            if not marker:
                return {"Events": events}
            if marker in visited:
                raise ExecutionError("History pagination repeated a marker")
            visited.add(marker)

    async def wait_for_result(self, execution_arn, timeout=60):
        """Poll for terminal execution status, then collect all history pages.

        Args:
            execution_arn (str): Durable execution ARN returned by Lambda.
            timeout (float): Completion-polling deadline in seconds.

        Returns:
            (DurableFunctionTestResult): Final outcome and operations reconstructed from
                history.

        Raises:
            TimeoutError: Polling does not complete before the deadline.
        """

        async def poll():
            while True:
                try:
                    execution = await self._request(
                        "get_durable_execution", DurableExecutionArn=execution_arn
                    )
                except ClientError as error:
                    if (
                        error.response.get("Error", {}).get("Code")
                        != "ResourceNotFoundException"
                    ):
                        raise
                else:
                    if execution.get("Status") in (
                        "SUCCEEDED",
                        "FAILED",
                        "TIMED_OUT",
                        "STOPPED",
                        "ABORTED",
                    ):
                        return DurableFunctionTestResult.from_execution_history(
                            execution, await self._history(execution_arn)
                        )
                await asyncio.sleep(self.poll_interval)

        return await asyncio.wait_for(poll(), timeout)

    async def wait_for_callback(self, execution_arn, name=None, timeout=60):
        """Poll execution history for a matching callback that is still active.

        Args:
            execution_arn (str): Durable execution whose history should be searched.
            name (str | None): Optional callback operation name.
            timeout (float): Maximum discovery wait in seconds.

        Returns:
            (str): Backend callback identifier for success, failure, or heartbeat calls.

        Raises:
            ValueError: Matching named callbacks have already completed.
            TimeoutError: No active matching callback appears before the deadline.
        """

        async def poll():
            while True:
                history = await self._history(execution_arn)
                entries = history_entries(history["Events"])
                matches = [
                    e
                    for e in entries.values()
                    if e.kind == "CALLBACK" and (name is None or e.name == name)
                ]
                for entry in reversed(matches):
                    if entry.status == "STARTED" and entry.callback:
                        return entry.callback
                if name and matches:
                    raise ValueError(f"Callback {name} has completed")
                await asyncio.sleep(self.poll_interval)

        return await asyncio.wait_for(poll(), timeout)

    async def send_callback_success(self, callback_id, result=None):
        """Send external callback success to AWS Lambda.

        Args:
            callback_id (str): Backend callback identifier.
            result (bytes | None): Optional serialized result payload.
        """
        params = {"CallbackId": callback_id}
        if result is not None:
            params["Result"] = result
        await self._request("send_durable_execution_callback_success", **params)

    async def send_callback_failure(self, callback_id, error=None):
        """Send external callback failure to AWS Lambda.

        Args:
            callback_id (str): Backend callback identifier.
            error (ErrorObject | None): Optional structured failure details.
        """
        params = {"CallbackId": callback_id}
        if error is not None:
            params["Error"] = error.to_dict()
        await self._request("send_durable_execution_callback_failure", **params)

    async def send_callback_heartbeat(self, callback_id):
        """Send a heartbeat for the given callback identifier to AWS Lambda."""
        await self._request(
            "send_durable_execution_callback_heartbeat", CallbackId=callback_id
        )


def create_cloud_runner(
    *,
    function_name: str,
    region: str = "us-west-2",
    lambda_endpoint: str | None = None,
    poll_interval: float = 1.0,
    input: Any = None,
    timeout: int = 60,
) -> DurableFunctionCloudTestRunner:
    """Create a runner for a deployed durable Lambda function.

    No invocation starts until run() or run_async() is awaited. Use an async
    context manager to release the transport after testing.

    Args:
        function_name: Qualified target Lambda function name or ARN.
        region: AWS region; defaults to us-west-2.
        lambda_endpoint: Optional endpoint URL override.
        poll_interval: Delay between completion/history polls in seconds.
        input: JSON-serializable application input.
        timeout: Default completion-polling timeout in seconds.

    Returns:
        (DurableFunctionCloudTestRunner): A configured cloud runner.
    """
    return DurableFunctionCloudTestRunner(
        function_name, region, lambda_endpoint, poll_interval, input, timeout
    )
