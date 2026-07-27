"""Pytest configuration and fixtures for durable execution tests."""

import inspect
import logging
import os
from collections.abc import Callable
from enum import Enum
from typing import Any

import pytest
from async_durable_execution import (
    DurableFunctionCloudTestRunner,
    DurableFunctionLocalTestRunner,
    create_cloud_runner,
    create_local_runner,
)
from examples.function_naming import to_function_name_suffix


EXAMPLES_PACKAGE_PREFIX = "examples"

logger = logging.getLogger(__name__)

DEFAULT_CLOUD_REGION = "eu-south-1"


class RunnerMode(str, Enum):
    """Runner mode for local or cloud execution."""

    LOCAL = "local"
    CLOUD = "cloud"


def pytest_addoption(parser) -> None:
    """Add custom command line options for test execution."""
    parser.addoption(
        "--runner-mode",
        action="store",
        default=RunnerMode.LOCAL,
        choices=[RunnerMode.LOCAL, RunnerMode.CLOUD],
        help="Test runner mode: local (in-memory) or cloud (deployed Lambda)",
    )


@pytest.fixture
def durable_runner(
    request, monkeypatch
) -> Callable[
    ...,
    DurableFunctionCloudTestRunner | DurableFunctionLocalTestRunner,
]:
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
        async def test_hello_world(durable_runner):
            async with durable_runner(
                handler=hello_world.handler,
                input="test",
                timeout=10,
            ) as runner:
                result = await runner.run()
            assert result.status == InvocationStatus.SUCCEEDED
    """
    # Get runner mode from CLI option
    runner_mode: str = request.config.getoption("--runner-mode")

    def build_runner(
        *,
        handler: Any,
        input: Any = None,  # noqa: A002
        timeout: int = 60,
        time_scale: str | None = None,
    ) -> DurableFunctionCloudTestRunner | DurableFunctionLocalTestRunner:
        """Create a configured runner for a durable handler."""
        handler_identifier = _get_handler_identifier(handler)

        logger.info("Running test in %s mode", runner_mode.upper())

        if runner_mode == RunnerMode.CLOUD:
            deployed_name = _get_deployed_function_name(handler_identifier)
            region = os.environ.get("AWS_REGION", DEFAULT_CLOUD_REGION)
            lambda_endpoint = os.environ.get("LAMBDA_ENDPOINT")

            logger.info("Using AWS region: %s", region)

            return create_cloud_runner(
                function_name=deployed_name,
                region=region,
                lambda_endpoint=lambda_endpoint,
                input=input,
                timeout=timeout,
            )
        configured_time_scale = time_scale or os.environ.get(
            "DURABLE_EXECUTION_TIME_SCALE", "0.05"
        )
        monkeypatch.setenv("DURABLE_EXECUTION_TIME_SCALE", configured_time_scale)
        try:
            poll_interval = min(0.05, max(0.001, float(configured_time_scale)))
        except ValueError:
            poll_interval = 0.05

        return create_local_runner(
            handler=handler,
            input=input,
            timeout=timeout,
            poll_interval=poll_interval,
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
