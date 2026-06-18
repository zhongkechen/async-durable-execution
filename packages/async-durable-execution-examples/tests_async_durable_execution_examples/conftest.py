"""Pytest configuration and fixtures for durable execution tests."""

import inspect
import logging
import os
import sys
from enum import Enum
from pathlib import Path
from typing import Any

import pytest
from async_durable_execution_runner import create_runner


# Add the source root so package imports resolve in source checkouts.
examples_src = Path(__file__).parent.parent / "src"
if str(examples_src) not in sys.path:
    sys.path.insert(0, str(examples_src))
examples_scripts = Path(__file__).parent.parent / "scripts"
if str(examples_scripts) not in sys.path:
    sys.path.insert(0, str(examples_scripts))

EXAMPLES_PACKAGE_PREFIX = "async_durable_execution_examples"

logger = logging.getLogger(__name__)

from function_naming import to_function_name_suffix

DEFAULT_CLOUD_REGION = "eu-south-1"


class RunnerMode(str, Enum):
    """Runner mode for local or cloud execution."""

    LOCAL = "local"
    CLOUD = "cloud"


class AsyncRunnerAdapter:
    """Expose an async test-friendly facade over the runner API."""

    def __init__(self, runner: Any) -> None:
        self._runner = runner

    def __enter__(self) -> "AsyncRunnerAdapter":
        self._runner.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self._runner.__exit__(exc_type, exc_val, exc_tb)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._runner, name)

    async def run(self):
        return await self._runner.run()

    async def run_async(self):
        return await self._runner.run_async()

    async def wait_for_result(self, execution_arn: str, timeout: int = 60):
        return await self._runner.wait_for_result(execution_arn, timeout)

    async def wait_for_callback(
        self, execution_arn: str, name: str | None = None, timeout: int = 60
    ):
        return await self._runner.wait_for_callback(
            execution_arn,
            name=name,
            timeout=timeout,
        )

    async def send_callback_success(
        self, callback_id: str, result: bytes | None = None
    ) -> None:
        await self._runner.send_callback_success(callback_id, result)

    async def send_callback_failure(
        self, callback_id: str, error: Any | None = None
    ) -> None:
        await self._runner.send_callback_failure(callback_id, error)

    async def send_callback_heartbeat(self, callback_id: str) -> None:
        await self._runner.send_callback_heartbeat(callback_id)


def pytest_addoption(parser):
    """Add custom command line options for test execution."""
    parser.addoption(
        "--runner-mode",
        action="store",
        default=RunnerMode.LOCAL,
        choices=[RunnerMode.LOCAL, RunnerMode.CLOUD],
        help="Test runner mode: local (in-memory) or cloud (deployed Lambda)",
    )


@pytest.fixture
def durable_runner(request):
    """Pytest fixture that provides a test runner based on configuration.

    Configuration for cloud mode:
        Environment variables (required):
            AWS_REGION: AWS region for Lambda invocation (default: eu-south-1)
            LAMBDA_ENDPOINT: Optional Lambda endpoint URL
            PYTEST_FUNCTION_NAME_PREFIX: Prefix used when examples are deployed
                with generated function names
        
        CLI option:
            --runner-mode=cloud (or local, default: local)
        
        Example:
            AWS_REGION=eu-south-1 \
            LAMBDA_ENDPOINT=https://lambda.eu-south-1.amazonaws.com \
            PYTEST_FUNCTION_NAME_PREFIX="py313-" \
            pytest --runner-mode=cloud -k test_hello_world

    Usage in tests:
        def test_hello_world(durable_runner):
            with durable_runner(
                handler=hello_world.handler,
                input="test",
                timeout=10,
            ) as runner:
                result = runner.run()
            assert result.status == InvocationStatus.SUCCEEDED
    """
    # Get runner mode from CLI option
    runner_mode: str = request.config.getoption("--runner-mode")

    def build_runner(
        *,
        handler: Any,
        input: Any = None,  # noqa: A002
        timeout: int = 60,
    ):
        """Create a configured runner for a durable handler."""
        handler_identifier = _get_handler_identifier(handler)

        logger.info("Running test in %s mode", runner_mode.upper())

        if runner_mode == RunnerMode.CLOUD:
            deployed_name = _get_deployed_function_name(handler_identifier)
            region = os.environ.get("AWS_REGION", DEFAULT_CLOUD_REGION)
            lambda_endpoint = os.environ.get("LAMBDA_ENDPOINT")

            logger.info("Using AWS region: %s", region)

            return AsyncRunnerAdapter(
                create_runner(
                    mode=runner_mode,
                    handler=handler,
                    function_name=deployed_name,
                    region=region,
                    lambda_endpoint=lambda_endpoint,
                    input=input,
                    timeout=timeout,
                )
            )
        return AsyncRunnerAdapter(
            create_runner(
                mode=runner_mode,
                handler=handler,
                input=input,
                timeout=timeout,
            )
        )

    return build_runner


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
    - PYTEST_FUNCTION_NAME_PREFIX: Prefix used with deterministic function naming

    Fallback environment variable:
    - QUALIFIED_FUNCTION_NAME: The qualified function ARN for single-function runs
    """
    function_name_prefix = os.environ.get("PYTEST_FUNCTION_NAME_PREFIX")
    if function_name_prefix:
        configured_function = f"{function_name_prefix}{to_function_name_suffix(handler_identifier)}:$LATEST"
        logger.info(
            "Using derived function ARN: %s for handler: %s",
            configured_function,
            handler_identifier,
        )
        return configured_function

    function_arn = os.environ.get("QUALIFIED_FUNCTION_NAME")
    if function_arn:
        logger.info(
            "Using function ARN: %s for handler: %s",
            function_arn,
            handler_identifier,
        )
        return function_arn

    pytest.fail(
        "Cloud mode requires PYTEST_FUNCTION_NAME_PREFIX or QUALIFIED_FUNCTION_NAME\n"
        'Example: PYTEST_FUNCTION_NAME_PREFIX="py313-" pytest --runner-mode=cloud'
    )
