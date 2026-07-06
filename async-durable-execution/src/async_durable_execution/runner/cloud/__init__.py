from __future__ import annotations

import asyncio
import json
import logging
import time
from threading import Lock
from typing import Any, cast
from uuid import uuid4

from botocore.config import Config
from botocore.exceptions import ClientError
from botocore.session import get_session

from ...execution import (
    DurableExecutionInvocationInput,
)
from ...models import DurableExecutionInvocationOutput, ErrorObject
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
    ):
        """Initialize cloud test runner."""
        self.mode = "cloud"
        self.function_name = function_name
        self.region = region
        self.lambda_endpoint = lambda_endpoint
        self.poll_interval = poll_interval
        self._default_input = input
        self._default_timeout = timeout

        client_config = Config(parameter_validation=False)
        session = get_session()
        self.lambda_client: Any = session.create_client(
            "lambda",
            endpoint_url=lambda_endpoint,
            region_name=region,
            config=client_config,
        )

    def __enter__(self) -> DurableFunctionCloudTestRunner:
        """Return self for context manager compatibility with local runner."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Close underlying resources when leaving a context manager block."""
        self.close()

    async def __aenter__(self) -> DurableFunctionCloudTestRunner:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def close(self) -> None:
        """Close the underlying botocore client when supported."""
        close = getattr(self.lambda_client, "close", None)
        if callable(close):
            close()

    async def run(
        self,
    ) -> DurableFunctionTestResult:
        """Execute function on AWS Lambda and wait for completion."""
        logger.info(
            "Invoking Lambda function: %s (timeout: %ds)",
            self.function_name,
            self._default_timeout,
        )

        # JSON encode input
        payload = json.dumps(self._default_input)

        # Invoke Lambda function
        try:
            response: dict[str, Any] = await asyncio.to_thread(
                lambda: cast(
                    dict[str, Any],
                    self.lambda_client.invoke(
                        FunctionName=self.function_name,
                        InvocationType="RequestResponse",
                        Payload=payload,
                    ),
                )
            )
        except Exception as e:
            msg = f"Failed to invoke Lambda function {self.function_name}: {e}"
            raise DurableFunctionsTestError(msg) from e

        # Check HTTP status code, 200 for RequestResponse
        status_code = response.get("StatusCode")
        if status_code != 200:
            error_payload = response["Payload"].read().decode("utf-8")
            msg = f"Lambda invocation failed with status {status_code}: {error_payload}"
            raise DurableFunctionsTestError(msg)

        # Check for function errors, we want to return function error for testing purpose
        if "FunctionError" in response:
            error_payload = response["Payload"].read().decode("utf-8")
            logger.warning("Lambda function failed: %s", error_payload)

        result_payload = response["Payload"].read().decode("utf-8")
        logger.info(
            "Lambda invocation completed, response: %s",
            result_payload,
        )

        # Extract durable execution ARN from response headers
        # The InvocationResponse includes X-Amz-Durable-Execution-Arn header
        execution_arn = response.get("DurableExecutionArn")
        if not execution_arn:
            msg = (
                f"No DurableExecutionArn in response for function {self.function_name}"
            )
            raise DurableFunctionsTestError(msg)

        return await self.wait_for_result(
            execution_arn=execution_arn, timeout=self._default_timeout
        )

    async def run_async(
        self,
    ) -> str:
        """Execute function on AWS Lambda asynchronously"""
        logger.info(
            "Invoking Lambda function: %s (timeout: %ds)",
            self.function_name,
            self._default_timeout,
        )
        payload = json.dumps(self._default_input)
        try:
            response: dict[str, Any] = await asyncio.to_thread(
                lambda: cast(
                    dict[str, Any],
                    self.lambda_client.invoke(
                        FunctionName=self.function_name,
                        InvocationType="Event",
                        Payload=payload,
                    ),
                )
            )
        except Exception as e:
            msg = f"Failed to invoke Lambda function {self.function_name}: {e}"
            raise DurableFunctionsTestError(msg) from e

        # Check HTTP status code, 202 for Event
        status_code = response.get("StatusCode")
        if status_code != 202:
            error_payload = response["Payload"].read().decode("utf-8")
            msg = f"Lambda invocation failed with status {status_code}: {error_payload}"
            raise DurableFunctionsTestError(msg)

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
            await asyncio.to_thread(
                self.lambda_client.send_durable_execution_callback_success,
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
            await asyncio.to_thread(
                self.lambda_client.send_durable_execution_callback_failure,
                CallbackId=callback_id,
                Error=cast(Any, error.to_dict() if error else None),
            )
        except Exception as e:
            msg = f"Failed to send callback failure for {self.function_name}, callback_id {callback_id}: {e}"
            raise DurableFunctionsTestError(msg) from e

    async def send_callback_heartbeat(self, callback_id: str) -> None:
        try:
            await asyncio.to_thread(
                self.lambda_client.send_durable_execution_callback_heartbeat,
                CallbackId=callback_id,
            )
        except Exception as e:
            msg = f"Failed to send callback heartbeat for {self.function_name}, callback_id {callback_id}: {e}"
            raise DurableFunctionsTestError(msg) from e

    def _wait_for_completion(
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
                execution_dict = self.lambda_client.get_durable_execution(
                    DurableExecutionArn=execution_arn
                )
                execution = GetDurableExecutionResponse.from_dict(execution_dict)
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code")
                if error_code == "ResourceNotFoundException":
                    logger.info(
                        "Execution status not available yet for %s; retrying",
                        execution_arn,
                    )
                else:
                    msg = f"Failed to get execution status: {e}"
                    raise DurableFunctionsTestError(msg) from e
            except Exception as e:
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

            time.sleep(self.poll_interval)

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
        execution_response = await asyncio.to_thread(
            self._wait_for_completion, execution_arn, timeout
        )

        try:
            history_response = await asyncio.to_thread(
                self._fetch_execution_history, execution_arn
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
                history_response = await asyncio.to_thread(
                    self._fetch_execution_history, execution_arn
                )
                callback_id = _get_callback_id_from_events(
                    events=history_response.events, name=name
                )
                if callback_id:
                    return callback_id
            except ClientError as e:
                error_code = e.response["Error"]["Code"]
                # retryable error, the execution may not start yet in async invoke situation
                if error_code in ["ResourceNotFoundException"]:
                    pass
                else:
                    msg = f"Failed to fetch execution history: {e}"
                    raise DurableFunctionsTestError(msg) from e
            except DurableFunctionsTestError:
                raise
            except Exception as e:
                msg = f"Failed to fetch execution history: {e}"
                raise DurableFunctionsTestError(msg) from e

            await asyncio.sleep(self.poll_interval)

        # Timeout reached
        elapsed = time.time() - start_time
        msg = f"Callback was not available within {timeout}s (elapsed: {elapsed:.1f}s)."
        raise TimeoutError(msg)

    def _fetch_execution_history(
        self, execution_arn: str
    ) -> GetDurableExecutionHistoryResponse:
        """Retrieve execution history from Lambda service.

        Args:
            execution_arn: ARN of the durable execution

        Returns:
            GetDurableExecutionHistoryResponse with typed Event objects

        Raises:
            ClientError: If lambda client encounter error
        """
        history_dict = self.lambda_client.get_durable_execution_history(
            DurableExecutionArn=execution_arn,
            IncludeExecutionData=True,
        )
        history_response = GetDurableExecutionHistoryResponse.from_dict(history_dict)

        logger.info("Retrieved %d events from history", len(history_response.events))

        return history_response


class LambdaInvoker:
    def __init__(self, lambda_client: Any) -> None:
        self.lambda_client = lambda_client
        # Maps execution_arn -> endpoint for that execution
        # Maps endpoint -> client to reuse clients across executions
        self._execution_endpoints: dict[str, str] = {}
        self._endpoint_clients: dict[str, Any] = {}
        self._current_endpoint: str = ""  # Track current endpoint for new executions
        self._lock = Lock()

    @staticmethod
    def create(endpoint_url: str, region_name: str) -> LambdaInvoker:
        """Create with the boto lambda client."""
        invoker = LambdaInvoker(create_lambda_client(endpoint_url, region_name))
        invoker._current_endpoint = endpoint_url
        invoker._endpoint_clients[endpoint_url] = invoker.lambda_client
        return invoker

    def update_endpoint(self, endpoint_url: str, region_name: str) -> None:
        """Update the Lambda client endpoint."""
        # Cache client by endpoint to reuse across executions
        with self._lock:
            if endpoint_url not in self._endpoint_clients:
                self._endpoint_clients[endpoint_url] = create_lambda_client(
                    endpoint_url, region_name
                )
            self.lambda_client = self._endpoint_clients[endpoint_url]
        self._current_endpoint = endpoint_url

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
                self._endpoint_clients[lambda_endpoint] = create_lambda_client(
                    lambda_endpoint, region_name or "us-east-1"
                )
            return self._endpoint_clients[lambda_endpoint]

        # Fallback to cached endpoint
        if durable_execution_arn not in self._execution_endpoints:
            with self._lock:
                if durable_execution_arn not in self._execution_endpoints:
                    self._execution_endpoints[durable_execution_arn] = (
                        self._current_endpoint
                    )

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
            response: dict[str, Any] = await asyncio.to_thread(
                client.invoke,
                FunctionName=function_name,
                InvocationType="RequestResponse",  # Synchronous invocation
                Payload=json.dumps(input.to_json_dict()),
            )

            # Check HTTP status code
            status_code = response.get("StatusCode")
            if status_code not in (200, 202, 204):
                msg = f"Lambda invocation failed with status code: {status_code}"
                raise DurableFunctionsTestError(msg)

            # Check for function errors
            if "FunctionError" in response:
                error_payload = response["Payload"].read().decode("utf-8")
                msg = f"Lambda invocation failed with status {status_code}: {error_payload}"
                raise DurableFunctionsTestError(msg)

            # Parse response payload
            response_payload = response["Payload"].read().decode("utf-8")
            response_dict = json.loads(response_payload)

            # Extract request ID from response headers (x-amzn-RequestId or x-amzn-request-id)
            headers = response.get("ResponseMetadata", {}).get("HTTPHeaders", {})
            request_id = (
                headers.get("x-amzn-RequestId")
                or headers.get("x-amzn-request-id")
                or f"local-{uuid4()}"
            )

            # Convert to DurableExecutionInvocationOutput
            output = DurableExecutionInvocationOutput.from_dict(response_dict)
            return InvokeResponse(invocation_output=output, request_id=request_id)

        except client.exceptions.ResourceNotFoundException as e:
            msg = f"Function not found: {function_name}"
            raise ResourceNotFoundException(msg) from e
        except client.exceptions.InvalidParameterValueException as e:
            msg = f"Invalid parameter: {e}"
            raise InvalidParameterValueException(msg) from e
        except (
            client.exceptions.TooManyRequestsException,
            client.exceptions.ServiceException,
            client.exceptions.ResourceConflictException,
            client.exceptions.InvalidRequestContentException,
            client.exceptions.RequestTooLargeException,
            client.exceptions.UnsupportedMediaTypeException,
            client.exceptions.InvalidRuntimeException,
            client.exceptions.InvalidZipFileException,
            client.exceptions.ResourceNotReadyException,
            client.exceptions.SnapStartTimeoutException,
            client.exceptions.SnapStartNotReadyException,
            client.exceptions.SnapStartException,
            client.exceptions.RecursiveInvocationException,
        ) as e:
            msg = f"Lambda invocation failed: {e}"
            raise DurableFunctionsTestError(msg) from e
        except (
            client.exceptions.InvalidSecurityGroupIDException,
            client.exceptions.EC2ThrottledException,
            client.exceptions.EFSMountConnectivityException,
            client.exceptions.SubnetIPAddressLimitReachedException,
            client.exceptions.EC2UnexpectedException,
            client.exceptions.InvalidSubnetIDException,
            client.exceptions.EC2AccessDeniedException,
            client.exceptions.EFSIOException,
            client.exceptions.ENILimitReachedException,
            client.exceptions.EFSMountTimeoutException,
            client.exceptions.EFSMountFailureException,
        ) as e:
            msg = f"Lambda infrastructure error: {e}"
            raise DurableFunctionsTestError(msg) from e
        except (
            client.exceptions.KMSAccessDeniedException,
            client.exceptions.KMSDisabledException,
            client.exceptions.KMSNotFoundException,
            client.exceptions.KMSInvalidStateException,
        ) as e:
            msg = f"Lambda KMS error: {e}"
            raise DurableFunctionsTestError(msg) from e
        except Exception as e:
            # Handle any remaining exceptions, including custom ones like DurableExecutionAlreadyStartedException
            if "DurableExecutionAlreadyStartedException" in str(type(e)):
                msg = f"Durable execution already started: {e}"
                raise DurableFunctionsTestError(msg) from e
            msg = f"Unexpected error during Lambda invocation: {e}"
            raise DurableFunctionsTestError(msg) from e


def create_lambda_client(endpoint_url: str | None, region_name: str) -> Any:
    """Create a botocore Lambda client configured for durable function invocations."""

    session = get_session()
    return session.create_client(
        "lambda",
        endpoint_url=endpoint_url,
        region_name=region_name,
        config=_LAMBDA_CLIENT_CONFIG,
    )


_LAMBDA_READ_TIMEOUT_SECONDS = 960
_LAMBDA_CLIENT_CONFIG = Config(
    read_timeout=_LAMBDA_READ_TIMEOUT_SECONDS,
    retries={"max_attempts": 0},
)
