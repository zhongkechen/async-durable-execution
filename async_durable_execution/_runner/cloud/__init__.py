from __future__ import annotations

import asyncio
import inspect
import json
import logging
import time
from typing import Any, cast
from uuid import uuid4

from botocore.config import Config
from botocore.session import get_session

from ..._core import (
    DurableExecutionInvocationInput,
    DurableExecutionInvocationOutput,
    ErrorObject,
    aioboto_is_installed,
    create_default_async_client,
    create_default_sync_client,
)
from ..exceptions import (
    DurableFunctionsTestError,
    InvalidParameterValueException,
    ResourceNotFoundException,
)
from ..model import (
    DurableFunctionTestResult,
    GetDurableExecutionResponse,
    GetDurableExecutionHistoryResponse,
    InvokeResponse,
    _get_callback_id_from_events,
)


logger = logging.getLogger(__name__)


class ThreadedSyncCloudLambdaClient:
    """Adapt a sync Lambda client to the async cloud runner interface."""

    def __init__(self, client: Any) -> None:
        self.client = client

    @property
    def exceptions(self) -> Any:
        return self.client.exceptions

    async def invoke(self, **kwargs: Any) -> dict[str, Any]:
        return cast(
            dict[str, Any], await asyncio.to_thread(self.client.invoke, **kwargs)
        )

    async def get_durable_execution(self, **kwargs: Any) -> dict[str, Any]:
        return cast(
            dict[str, Any],
            await asyncio.to_thread(self.client.get_durable_execution, **kwargs),
        )

    async def get_durable_execution_history(self, **kwargs: Any) -> dict[str, Any]:
        return cast(
            dict[str, Any],
            await asyncio.to_thread(
                self.client.get_durable_execution_history, **kwargs
            ),
        )

    async def send_durable_execution_callback_success(
        self, **kwargs: Any
    ) -> dict[str, Any]:
        return cast(
            dict[str, Any],
            await asyncio.to_thread(
                self.client.send_durable_execution_callback_success, **kwargs
            ),
        )

    async def send_durable_execution_callback_failure(
        self, **kwargs: Any
    ) -> dict[str, Any]:
        return cast(
            dict[str, Any],
            await asyncio.to_thread(
                self.client.send_durable_execution_callback_failure, **kwargs
            ),
        )

    async def send_durable_execution_callback_heartbeat(
        self, **kwargs: Any
    ) -> dict[str, Any]:
        return cast(
            dict[str, Any],
            await asyncio.to_thread(
                self.client.send_durable_execution_callback_heartbeat, **kwargs
            ),
        )

    def close(self) -> None:
        close = getattr(self.client, "close", None)
        if callable(close):
            close()


class AsyncCloudLambdaClient:
    """Adapt an async Lambda client to the cloud runner interface."""

    def __init__(self, client: Any) -> None:
        self._client_context = client if hasattr(client, "__aenter__") else None
        self._client = None if self._client_context is not None else client
        self._entered_client: Any | None = None

    @property
    def exceptions(self) -> Any:
        client = self._entered_client or self._client
        if client is None:
            msg = "Async Lambda client has not been initialized"
            raise AttributeError(msg)
        return client.exceptions

    async def _get_client(self) -> Any:
        if self._client is not None:
            return self._client
        if self._entered_client is None:
            assert self._client_context is not None
            self._entered_client = await self._client_context.__aenter__()
        return self._entered_client

    async def _call(self, method_name: str, **kwargs: Any) -> dict[str, Any]:
        client = await self._get_client()
        result = getattr(client, method_name)(**kwargs)
        if inspect.isawaitable(result):
            result = await result
        return cast(dict[str, Any], result)

    async def invoke(self, **kwargs: Any) -> dict[str, Any]:
        return await self._call("invoke", **kwargs)

    async def get_durable_execution(self, **kwargs: Any) -> dict[str, Any]:
        return await self._call("get_durable_execution", **kwargs)

    async def get_durable_execution_history(self, **kwargs: Any) -> dict[str, Any]:
        return await self._call("get_durable_execution_history", **kwargs)

    async def send_durable_execution_callback_success(
        self, **kwargs: Any
    ) -> dict[str, Any]:
        return await self._call("send_durable_execution_callback_success", **kwargs)

    async def send_durable_execution_callback_failure(
        self, **kwargs: Any
    ) -> dict[str, Any]:
        return await self._call("send_durable_execution_callback_failure", **kwargs)

    async def send_durable_execution_callback_heartbeat(
        self, **kwargs: Any
    ) -> dict[str, Any]:
        return await self._call("send_durable_execution_callback_heartbeat", **kwargs)

    async def aclose(self) -> None:
        if self._entered_client is not None:
            assert self._client_context is not None
            await self._client_context.__aexit__(None, None, None)
            self._entered_client = None
            return

        close = getattr(self._client, "aclose", None)
        if callable(close):
            await close()


async def _read_payload(payload: Any) -> str:
    read = getattr(payload, "read", None)
    if callable(read):
        if inspect.iscoroutinefunction(read):
            data = await read()
        else:
            data = await asyncio.to_thread(read)
            if inspect.isawaitable(data):
                data = await data
    else:
        data = payload

    if isinstance(data, bytes):
        return data.decode("utf-8")
    return str(data)


def _cloud_lambda_client_is_async(client: Any) -> bool:
    return inspect.iscoroutinefunction(getattr(client, "invoke", None))


def adapt_lambda_client(client: Any) -> Any:
    """Adapt a raw Lambda client to the async cloud runner interface."""
    if isinstance(client, ThreadedSyncCloudLambdaClient | AsyncCloudLambdaClient):
        return client
    if _cloud_lambda_client_is_async(client):
        return AsyncCloudLambdaClient(client)
    return ThreadedSyncCloudLambdaClient(client)


_KNOWN_LAMBDA_ERROR_CODES = (
    "ResourceNotFoundException",
    "InvalidParameterValueException",
    "TooManyRequestsException",
    "ServiceException",
    "ResourceConflictException",
    "InvalidRequestContentException",
    "RequestTooLargeException",
    "UnsupportedMediaTypeException",
    "InvalidRuntimeException",
    "InvalidZipFileException",
    "ResourceNotReadyException",
    "SnapStartTimeoutException",
    "SnapStartNotReadyException",
    "SnapStartException",
    "RecursiveInvocationException",
    "InvalidSecurityGroupIDException",
    "EC2ThrottledException",
    "EFSMountConnectivityException",
    "SubnetIPAddressLimitReachedException",
    "EC2UnexpectedException",
    "InvalidSubnetIDException",
    "EC2AccessDeniedException",
    "EFSIOException",
    "ENILimitReachedException",
    "EFSMountTimeoutException",
    "EFSMountFailureException",
    "KMSAccessDeniedException",
    "KMSDisabledException",
    "KMSNotFoundException",
    "KMSInvalidStateException",
    "DurableExecutionAlreadyStartedException",
)


def _aws_error_code(error: Exception, client: Any = None) -> str:
    response = getattr(error, "response", None)
    if isinstance(response, dict):
        aws_error = response.get("Error")
        if isinstance(aws_error, dict) and aws_error.get("Code"):
            return str(aws_error["Code"])

    exceptions = getattr(client, "exceptions", None)
    if exceptions is not None:
        for error_code in _KNOWN_LAMBDA_ERROR_CODES:
            exception_type = getattr(exceptions, error_code, None)
            if isinstance(exception_type, type) and isinstance(error, exception_type):
                return error_code
    return type(error).__name__


def create_cloud_runner(
    *,
    function_name: str,
    region: str = "us-west-2",
    lambda_endpoint: str | None = None,
    poll_interval: float = 1.0,
    input: Any = None,  # noqa: A002
    timeout: int = 60,
) -> DurableFunctionCloudTestRunner:
    """Create a configured cloud durable function runner."""
    return DurableFunctionCloudTestRunner(
        function_name=function_name,
        region=region,
        lambda_endpoint=lambda_endpoint,
        poll_interval=poll_interval,
        input=input,
        timeout=timeout,
    )


class DurableFunctionCloudTestRunner:
    """Test runner that executes durable functions against actual AWS Lambda backend.

    This runner invokes deployed Lambda functions and polls for execution completion,
    providing the same interface as DurableFunctionLocalTestRunner for seamless test
    compatibility between local and cloud modes.
    """

    def __init__(
        self,
        function_name: str,
        region: str = "us-west-2",
        lambda_endpoint: str | None = None,
        poll_interval: float = 1.0,
        input: Any = None,  # noqa: A002
        timeout: int = 60,
    ) -> None:
        """Initialize cloud test runner."""
        self.mode = "cloud"
        self.function_name = function_name
        self.region = region
        self.lambda_endpoint = lambda_endpoint
        self.poll_interval = poll_interval
        self._default_input = input
        self._default_timeout = timeout

        self.lambda_client: Any = create_lambda_client(lambda_endpoint, region)

    async def __aenter__(self) -> DurableFunctionCloudTestRunner:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.aclose()

    def close(self) -> None:
        """Close the underlying sync client when supported."""
        close = getattr(self.lambda_client, "close", None)
        if callable(close):
            close()

    async def aclose(self) -> None:
        """Close the underlying client when supported."""
        aclose = getattr(self.lambda_client, "aclose", None)
        if callable(aclose):
            await aclose()
            return
        self.close()

    async def run(
        self,
    ) -> DurableFunctionTestResult:
        """Execute function on AWS Lambda and wait for completion."""
        execution_arn = await self._invoke_for_execution(
            invocation_type="RequestResponse",
            expected_status_code=200,
        )
        return await self.wait_for_result(
            execution_arn=execution_arn, timeout=self._default_timeout
        )

    async def run_async(
        self,
    ) -> str:
        """Execute function on AWS Lambda asynchronously"""
        return await self._invoke_for_execution(
            invocation_type="Event",
            expected_status_code=202,
        )

    async def _invoke_for_execution(
        self,
        *,
        invocation_type: str,
        expected_status_code: int,
    ) -> str:
        logger.info(
            "Invoking Lambda function: %s (timeout: %ds)",
            self.function_name,
            self._default_timeout,
        )
        payload = json.dumps(self._default_input)
        try:
            response = cast(
                dict[str, Any],
                await self.lambda_client.invoke(
                    FunctionName=self.function_name,
                    InvocationType=invocation_type,
                    Payload=payload,
                ),
            )
        except Exception as e:
            msg = f"Failed to invoke Lambda function {self.function_name}: {e}"
            raise DurableFunctionsTestError(msg) from e

        status_code = response.get("StatusCode")
        if status_code != expected_status_code:
            error_payload = await _read_payload(response["Payload"])
            msg = f"Lambda invocation failed with status {status_code}: {error_payload}"
            raise DurableFunctionsTestError(msg)

        if "FunctionError" in response:
            error_payload = await _read_payload(response["Payload"])
            logger.warning("Lambda function failed: %s", error_payload)

        execution_arn = cast(str | None, response.get("DurableExecutionArn"))
        if execution_arn is None:
            msg = (
                f"No DurableExecutionArn in response for function {self.function_name}"
            )
            raise DurableFunctionsTestError(msg)
        return execution_arn

    async def send_callback_success(
        self, callback_id: str, result: bytes | None = None
    ) -> None:
        try:
            await self.lambda_client.send_durable_execution_callback_success(
                CallbackId=callback_id,
                Result=cast(Any, result),
            )
        except Exception as e:
            msg = f"Failed to send callback success for {self.function_name}, callback_id {callback_id}: {e}"
            raise DurableFunctionsTestError(msg) from e

    async def send_callback_failure(
        self, callback_id: str, error: ErrorObject | None = None
    ) -> None:
        try:
            await self.lambda_client.send_durable_execution_callback_failure(
                CallbackId=callback_id,
                Error=cast(Any, error.to_dict() if error else None),
            )
        except Exception as e:
            msg = f"Failed to send callback failure for {self.function_name}, callback_id {callback_id}: {e}"
            raise DurableFunctionsTestError(msg) from e

    async def send_callback_heartbeat(self, callback_id: str) -> None:
        try:
            await self.lambda_client.send_durable_execution_callback_heartbeat(
                CallbackId=callback_id,
            )
        except Exception as e:
            msg = f"Failed to send callback heartbeat for {self.function_name}, callback_id {callback_id}: {e}"
            raise DurableFunctionsTestError(msg) from e

    async def _wait_for_completion(
        self, execution_arn: str, timeout: int
    ) -> GetDurableExecutionResponse:
        """Poll execution status until completion or timeout.

        Args:
            execution_arn: ARN of the durable execution
            timeout: Maximum seconds to wait

        Returns:
            GetDurableExecutionResponse with typed execution details

        Raises:
            TimeoutError: If execution doesn't complete within timeout
            DurableFunctionsTestError: If status check fails
        """
        start_time = time.time()
        last_status = None

        while time.time() - start_time < timeout:
            try:
                execution_dict = await self.lambda_client.get_durable_execution(
                    DurableExecutionArn=execution_arn,
                    IncludeExecutionData=True,
                )
                execution = GetDurableExecutionResponse.from_dict(execution_dict)
            except Exception as e:
                if (
                    _aws_error_code(e, self.lambda_client)
                    == "ResourceNotFoundException"
                ):
                    logger.info(
                        "Execution status not available yet for %s; retrying",
                        execution_arn,
                    )
                else:
                    msg = f"Failed to get execution status: {e}"
                    raise DurableFunctionsTestError(msg) from e
            else:
                # Log status changes
                if execution.status != last_status:
                    logger.info("Execution status: %s", execution.status)
                    last_status = execution.status

                # Check if execution completed
                if execution.status == "SUCCEEDED":
                    logger.info("Execution succeeded")
                    return execution
                if execution.status == "FAILED":
                    logger.warning("Execution failed")
                    return execution
                if execution.status in ["TIMED_OUT", "ABORTED"]:
                    logger.warning("Execution terminated: %s", execution.status)
                    return execution

            await asyncio.sleep(self.poll_interval)

        # Timeout reached
        elapsed = time.time() - start_time
        msg = (
            f"Execution did not complete within {timeout}s "
            f"(elapsed: {elapsed:.1f}s, last status: {last_status})"
        )
        raise TimeoutError(msg)

    async def wait_for_result(
        self, execution_arn: str, timeout: int = 60
    ) -> DurableFunctionTestResult:
        execution_result = self._wait_for_completion(execution_arn, timeout)
        execution_response = (
            await execution_result
            if inspect.isawaitable(execution_result)
            else execution_result
        )

        try:
            history_result = self._fetch_execution_history(execution_arn)
            history_response = (
                await history_result
                if inspect.isawaitable(history_result)
                else history_result
            )
        except Exception as e:
            msg = f"Failed to fetch execution history: {e}"
            raise DurableFunctionsTestError(msg) from e

        # Build test result from execution history
        return DurableFunctionTestResult.from_execution_history(
            execution_response, history_response
        )

    async def wait_for_callback(
        self, execution_arn: str, name: str | None = None, timeout: int = 60
    ) -> str:
        """
        Wait for and retrieve a callback ID from a durable execution.

        Polls the execution history at regular intervals until a callback ID is found
        or the timeout is reached.

        Args:
            execution_arn: Execution Arn
            name: Specific callback name, default to None
            timeout: Maximum time in seconds to wait for callback. Defaults to 60.

        Returns:
            str: The callback ID/token retrieved from the execution history

        Raises:
            TimeoutError: If callback is not found within the specified timeout period
            DurableFunctionsTestError: If there's an error fetching execution history
                (excluding retryable errors)
        """
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                history_response = await self._fetch_execution_history(execution_arn)
                callback_id = _get_callback_id_from_events(
                    events=history_response.events, name=name
                )
                if callback_id:
                    return callback_id
            except DurableFunctionsTestError:
                raise
            except Exception as e:
                # Retry while an asynchronously invoked execution is not yet visible.
                if (
                    _aws_error_code(e, self.lambda_client)
                    != "ResourceNotFoundException"
                ):
                    msg = f"Failed to fetch execution history: {e}"
                    raise DurableFunctionsTestError(msg) from e

            await asyncio.sleep(self.poll_interval)

        # Timeout reached
        elapsed = time.time() - start_time
        msg = f"Callback was not available within {timeout}s (elapsed: {elapsed:.1f}s)."
        raise TimeoutError(msg)

    async def _fetch_execution_history(
        self, execution_arn: str
    ) -> GetDurableExecutionHistoryResponse:
        """Retrieve the complete execution history from Lambda service.

        Args:
            execution_arn: ARN of the durable execution

        Returns:
            GetDurableExecutionHistoryResponse with typed Event objects

        Raises:
            Exception: If the Lambda API client encounters an error
        """
        events = []
        next_marker: str | None = None
        seen_markers: set[str] = set()
        page_count = 0

        while True:
            request: dict[str, Any] = {
                "DurableExecutionArn": execution_arn,
                "IncludeExecutionData": True,
            }
            if next_marker:
                request["Marker"] = next_marker

            history_dict = await self.lambda_client.get_durable_execution_history(
                **request
            )
            history_response = GetDurableExecutionHistoryResponse.from_dict(
                history_dict
            )
            page_count += 1
            events.extend(history_response.events)

            next_marker = history_response.next_marker
            if not next_marker:
                break
            if next_marker in seen_markers:
                msg = (
                    "Execution history pagination returned a repeated marker: "
                    f"{next_marker}"
                )
                raise DurableFunctionsTestError(msg)
            seen_markers.add(next_marker)

        logger.info(
            "Retrieved %d events from history across %d page(s)",
            len(events),
            page_count,
        )

        return GetDurableExecutionHistoryResponse(events=events)


class LambdaInvoker:
    def __init__(self, lambda_client: Any) -> None:
        self.lambda_client = adapt_lambda_client(lambda_client)
        # Maps execution_arn -> endpoint for that execution
        # Maps endpoint -> client to reuse clients across executions
        self._execution_endpoints: dict[str, str] = {}
        self._endpoint_clients: dict[str, Any] = {}
        self._current_endpoint: str = ""  # Track current endpoint for new executions

    def _get_client_for_execution(
        self,
        durable_execution_arn: str,
        lambda_endpoint: str | None = None,
        region_name: str | None = None,
    ) -> Any:
        """Get the appropriate client for this execution."""
        # Use provided endpoint or fall back to cached endpoint for this execution
        if lambda_endpoint:
            if lambda_endpoint not in self._endpoint_clients:
                self._endpoint_clients[lambda_endpoint] = adapt_lambda_client(
                    create_lambda_client(lambda_endpoint, region_name or "us-east-1")
                )
            return self._endpoint_clients[lambda_endpoint]

        # Fallback to cached endpoint
        if durable_execution_arn not in self._execution_endpoints:
            self._execution_endpoints[durable_execution_arn] = self._current_endpoint

        endpoint = self._execution_endpoints[durable_execution_arn]

        # If no endpoint configured, fall back to default client
        if not endpoint:
            return self.lambda_client

        return self._endpoint_clients[endpoint]

    async def invoke(
        self,
        function_name: str,
        input: DurableExecutionInvocationInput,
        endpoint_url: str | None = None,
    ) -> InvokeResponse:
        """Invoke AWS Lambda function and return durable execution result.

        Args:
            function_name: Name of the Lambda function to invoke
            input: Durable execution invocation input
            endpoint_url: Lambda endpoint url

        Returns:
            InvokeResponse: Response containing invocation output and request ID

        Raises:
            ResourceNotFoundException: If function does not exist
            InvalidParameterValueException: If parameters are invalid
            DurableFunctionsTestError: For other invocation failures
        """

        # Parameter validation
        if not function_name or not function_name.strip():
            msg = "Function name is required"
            raise InvalidParameterValueException(msg)

        # Get the client for this execution
        client = self._get_client_for_execution(
            input.durable_execution_arn, endpoint_url
        )

        try:
            # Invoke AWS Lambda function using standard invoke method
            response = await client.invoke(
                FunctionName=function_name,
                InvocationType="RequestResponse",  # Synchronous invocation
                Payload=json.dumps(input.to_dict()),
            )

            # Check HTTP status code
            status_code = response.get("StatusCode")
            if status_code not in (200, 202, 204):
                msg = f"Lambda invocation failed with status code: {status_code}"
                raise DurableFunctionsTestError(msg)

            # Check for function errors
            if "FunctionError" in response:
                error_payload = await _read_payload(response["Payload"])
                msg = f"Lambda invocation failed with status {status_code}: {error_payload}"
                raise DurableFunctionsTestError(msg)

            # Parse response payload
            response_payload = await _read_payload(response["Payload"])
            response_dict = json.loads(response_payload)

            # Extract request ID from response headers (x-amzn-RequestId or x-amzn-request-id)
            headers = response.get("ResponseMetadata", {}).get("HTTPHeaders", {})
            request_id = (
                headers.get("x-amzn-RequestId")
                or headers.get("x-amzn-request-id")
                or headers.get("x-amzn-requestid")
                or f"local-{uuid4()}"
            )

            # Convert to DurableExecutionInvocationOutput
            output = DurableExecutionInvocationOutput.from_dict(response_dict)
            return InvokeResponse(invocation_output=output, request_id=request_id)

        except Exception as e:
            error_code = _aws_error_code(e, client)
            if error_code == "ResourceNotFoundException":
                msg = f"Function not found: {function_name}"
                raise ResourceNotFoundException(msg) from e
            if error_code == "InvalidParameterValueException":
                msg = f"Invalid parameter: {e}"
                raise InvalidParameterValueException(msg) from e
            if error_code in {
                "TooManyRequestsException",
                "ServiceException",
                "ResourceConflictException",
                "InvalidRequestContentException",
                "RequestTooLargeException",
                "UnsupportedMediaTypeException",
                "InvalidRuntimeException",
                "InvalidZipFileException",
                "ResourceNotReadyException",
                "SnapStartTimeoutException",
                "SnapStartNotReadyException",
                "SnapStartException",
                "RecursiveInvocationException",
            }:
                msg = f"Lambda invocation failed: {e}"
                raise DurableFunctionsTestError(msg) from e
            if error_code in {
                "InvalidSecurityGroupIDException",
                "EC2ThrottledException",
                "EFSMountConnectivityException",
                "SubnetIPAddressLimitReachedException",
                "EC2UnexpectedException",
                "InvalidSubnetIDException",
                "EC2AccessDeniedException",
                "EFSIOException",
                "ENILimitReachedException",
                "EFSMountTimeoutException",
                "EFSMountFailureException",
            }:
                msg = f"Lambda infrastructure error: {e}"
                raise DurableFunctionsTestError(msg) from e
            if error_code in {
                "KMSAccessDeniedException",
                "KMSDisabledException",
                "KMSNotFoundException",
                "KMSInvalidStateException",
            }:
                msg = f"Lambda KMS error: {e}"
                raise DurableFunctionsTestError(msg) from e
            if error_code == "DurableExecutionAlreadyStartedException":
                msg = f"Durable execution already started: {e}"
                raise DurableFunctionsTestError(msg) from e
            msg = f"Unexpected error during Lambda invocation: {e}"
            raise DurableFunctionsTestError(msg) from e


def create_sync_lambda_client(endpoint_url: str | None, region_name: str) -> Any:
    """Create a sync Lambda client adapted for cloud runner calls."""
    return ThreadedSyncCloudLambdaClient(
        create_default_sync_client(
            session=get_session(),
            endpoint_url=endpoint_url,
            region_name=region_name,
            config=_LAMBDA_CLIENT_CONFIG,
        )
    )


def create_async_lambda_client(endpoint_url: str | None, region_name: str) -> Any:
    """Create an async Lambda client adapted for cloud runner calls."""
    return AsyncCloudLambdaClient(
        create_default_async_client(
            session=get_session(),
            endpoint_url=endpoint_url,
            region_name=region_name,
            config=_LAMBDA_CLIENT_CONFIG,
        )
    )


def create_lambda_client(endpoint_url: str | None, region_name: str) -> Any:
    """Create a Lambda client, preferring aioboto when installed."""
    if aioboto_is_installed():
        return create_async_lambda_client(endpoint_url, region_name)
    return create_sync_lambda_client(endpoint_url, region_name)


_LAMBDA_READ_TIMEOUT_SECONDS = 960
_LAMBDA_CLIENT_CONFIG = Config(
    parameter_validation=False,
    read_timeout=_LAMBDA_READ_TIMEOUT_SECONDS,
    retries={"max_attempts": 0},
)
