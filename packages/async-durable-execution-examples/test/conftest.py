"""Pytest configuration and fixtures for durable execution tests."""

import contextlib
import inspect
import json
import logging
import os
import sys
from enum import StrEnum
from pathlib import Path
from typing import Any

import pytest
from async_durable_execution.lambda_service import (
    ErrorObject,
    OperationPayload,
)
from async_durable_execution.serdes import ExtendedTypeSerDes

from async_durable_execution_runner.runner import (
    DurableFunctionCloudTestRunner,
    DurableFunctionTestResult,
    DurableFunctionTestRunner,
)


# Add the source root so package imports resolve in source checkouts.
examples_src = Path(__file__).parent.parent / "src"
if str(examples_src) not in sys.path:
    sys.path.insert(0, str(examples_src))

EXAMPLES_PACKAGE_PREFIX = "async_durable_execution_examples"

logger = logging.getLogger(__name__)


def deserialize_operation_payload(
    payload: OperationPayload | None, serdes: ExtendedTypeSerDes | None = None
) -> Any:
    """Deserialize an operation payload using the provided or default serializer.

    This utility function helps test code deserialize operation results that are
    returned as raw strings. It supports both the default ExtendedTypeSerDes and
    custom serializers.

    Args:
        payload: The operation payload string to deserialize, or None.
        serdes: Optional custom serializer. If None, uses ExtendedTypeSerDes.

    Returns:
        Deserialized result object, or None if payload is None.
    """
    if not payload:
        return None

    if serdes is None:
        serdes = ExtendedTypeSerDes()

    try:
        return serdes.deserialize(payload)
    except Exception:
        # Fallback to plain JSON for backwards compatibility
        return json.loads(payload)


class RunnerMode(StrEnum):
    """Runner mode for local or cloud execution."""

    LOCAL = "local"
    CLOUD = "cloud"


def pytest_addoption(parser):
    """Add custom command line options for test execution."""
    parser.addoption(
        "--runner-mode",
        action="store",
        default=RunnerMode.LOCAL,
        choices=[RunnerMode.LOCAL, RunnerMode.CLOUD],
        help="Test runner mode: local (in-memory) or cloud (deployed Lambda)",
    )


class TestRunnerAdapter:
    """Adapter that provides consistent interface for both local and cloud runners.

    This adapter encapsulates the differences between local and cloud test runners:
    - Local runner: Requires context manager for resource cleanup (scheduler thread)
    - Cloud runner: No resource cleanup needed (stateless boto3 client)

    The adapter ensures proper resource management while providing a unified interface.
    """

    def __init__(
        self,
        runner: DurableFunctionTestRunner | DurableFunctionCloudTestRunner,
        mode: str,
    ):
        """Initialize the adapter."""
        self._runner: DurableFunctionTestRunner | DurableFunctionCloudTestRunner = (
            runner
        )
        self._mode: str = mode

    def run(
        self,
        input: str | None = None,  # noqa: A002
        timeout: int = 60,
    ) -> DurableFunctionTestResult:
        """Execute the durable function and return results."""
        return self._runner.run(input=input, timeout=timeout)

    def run_async(
        self,
        input: str | None = None,  # noqa: A002
        timeout: int = 60,
    ) -> str:
        return self._runner.run_async(input=input, timeout=timeout)

    def send_callback_success(
        self, callback_id: str, result: bytes | None = None
    ) -> None:
        self._runner.send_callback_success(callback_id=callback_id, result=result)

    def send_callback_failure(
        self, callback_id: str, error: ErrorObject | None = None
    ) -> None:
        self._runner.send_callback_failure(callback_id=callback_id, error=error)

    def send_callback_heartbeat(self, callback_id: str) -> None:
        self._runner.send_callback_heartbeat(callback_id=callback_id)

    def wait_for_result(
        self, execution_arn: str, timeout: int = 60
    ) -> DurableFunctionTestResult:
        return self._runner.wait_for_result(
            execution_arn=execution_arn, timeout=timeout
        )

    def wait_for_callback(
        self, execution_arn: str, name: str | None = None, timeout: int = 60
    ) -> str:
        return self._runner.wait_for_callback(
            execution_arn=execution_arn, name=name, timeout=timeout
        )

    @property
    def mode(self) -> str:
        """Get the runner mode (local or cloud)."""
        return self._mode

    def __enter__(self):
        """Context manager entry - only calls runner's __enter__ if it's a context manager."""
        if isinstance(self._runner, contextlib.AbstractContextManager):
            self._runner.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - only calls runner's __exit__ if it's a context manager."""
        if isinstance(self._runner, contextlib.AbstractContextManager):
            return self._runner.__exit__(exc_type, exc_val, exc_tb)
        return None


@pytest.fixture
def durable_runner(request):
    """Pytest fixture that provides a test runner based on configuration.

    Configuration for cloud mode:
        Environment variables (required):
            AWS_REGION: AWS region for Lambda invocation (default: us-west-2)
            LAMBDA_ENDPOINT: Optional Lambda endpoint URL
            PYTEST_FUNCTION_NAME_MAP: JSON mapping of handler identifiers to deployed function names
        
        CLI option:
            --runner-mode=cloud (or local, default: local)
        
        Example:
            AWS_REGION=us-west-2 \
            LAMBDA_ENDPOINT=https://lambda.us-west-2.amazonaws.com \
            PYTEST_FUNCTION_NAME_MAP='{"async_durable_execution_examples.hello_world.handler":"HelloWorld:$LATEST"}' \
            pytest --runner-mode=cloud -k test_hello_world

    Usage in tests:
        @pytest.mark.durable_execution(
            handler=hello_world.handler,
        )
        def test_hello_world(durable_runner):
            with durable_runner:
                result = durable_runner.run(input="test", timeout=10)
            assert result.status == InvocationStatus.SUCCEEDED
    """
    # Get marker with test configuration
    marker = request.node.get_closest_marker("durable_execution")
    if not marker:
        pytest.fail("Test must be marked with @pytest.mark.durable_execution")

    handler: Any = marker.kwargs.get("handler")
    if not handler:
        pytest.fail("handler is required for durable_execution tests")

    handler_identifier = _get_handler_identifier(handler)

    # Get runner mode from CLI option
    runner_mode: str = request.config.getoption("--runner-mode")

    logger.info("Running test in %s mode", runner_mode.upper())

    # Create appropriate runner
    if runner_mode == RunnerMode.CLOUD:
        # Get deployed function name and AWS config from environment
        deployed_name = _get_deployed_function_name(handler_identifier)
        region = os.environ.get("AWS_REGION", "us-west-2")
        lambda_endpoint = os.environ.get("LAMBDA_ENDPOINT")

        logger.info("Using AWS region: %s", region)

        # Create cloud runner (no cleanup needed)
        runner = DurableFunctionCloudTestRunner(
            function_name=deployed_name,
            region=region,
            lambda_endpoint=lambda_endpoint,
        )
    else:
        # Create local runner (needs cleanup via context manager)
        runner = DurableFunctionTestRunner(handler=handler)

    # Wrap in adapter and use context manager for proper cleanup
    with TestRunnerAdapter(runner, runner_mode) as adapter:
        yield adapter


def _get_handler_identifier(handler: Any) -> str:
    """Return the catalog handler identifier for a durable handler function."""
    original_handler = _find_original_handler(handler)

    module_name = getattr(original_handler, "__module__", None)
    function_name = getattr(original_handler, "__name__", None)
    if not module_name or not function_name:
        pytest.fail("handler must expose __module__ and __name__")

    return f"{module_name}.{function_name}"


def _find_original_handler(handler: Any) -> Any:
    """Walk wrapped functions and closures to find the user-defined handler."""
    seen: set[int] = set()
    stack = [handler]

    while stack:
        candidate = stack.pop()
        candidate_id = id(candidate)
        if candidate_id in seen:
            continue
        seen.add(candidate_id)

        module_name = getattr(candidate, "__module__", None)
        function_name = getattr(candidate, "__name__", None)
        if (
            module_name
            and function_name
            and module_name.startswith(EXAMPLES_PACKAGE_PREFIX)
            and function_name == "handler"
        ):
            return candidate

        wrapped = getattr(candidate, "__wrapped__", None)
        if wrapped is not None:
            stack.append(wrapped)

        try:
            closure_vars = inspect.getclosurevars(candidate)
        except TypeError:
            continue

        for value in closure_vars.nonlocals.values():
            if callable(value):
                stack.append(value)

    return inspect.unwrap(handler)


def _get_deployed_function_name(handler_identifier: str) -> str:
    """Get the deployed function name from environment variables.

    Preferred environment variable:
    - PYTEST_FUNCTION_NAME_MAP: JSON mapping of handler identifiers to qualified
      function names

    Fallback environment variable:
    - QUALIFIED_FUNCTION_NAME: The qualified function ARN for single-function runs
    """
    function_map_json = os.environ.get("PYTEST_FUNCTION_NAME_MAP")
    if function_map_json:
        try:
            function_map = json.loads(function_map_json)
        except json.JSONDecodeError as exc:
            pytest.fail(f"Invalid PYTEST_FUNCTION_NAME_MAP JSON: {exc}")

        configured_function = function_map.get(handler_identifier)
        if configured_function:
            logger.info(
                "Using function ARN: %s for handler: %s",
                configured_function,
                handler_identifier,
            )
            return configured_function

        pytest.skip(
            f"Handler '{handler_identifier}' is not present in PYTEST_FUNCTION_NAME_MAP"
        )

    function_arn = os.environ.get("QUALIFIED_FUNCTION_NAME")
    if function_arn:
        logger.info(
            "Using function ARN: %s for handler: %s",
            function_arn,
            handler_identifier,
        )
        return function_arn

    pytest.fail(
        "Cloud mode requires PYTEST_FUNCTION_NAME_MAP or QUALIFIED_FUNCTION_NAME\n"
        'Example: PYTEST_FUNCTION_NAME_MAP=\'{"async_durable_execution_examples.hello_world.handler":"HelloWorld:$LATEST"}\' pytest --runner-mode=cloud'
    )
