"""Public runner selection for application examples."""

import os
import pytest
from async_durable_execution import create_cloud_runner, create_local_runner
from examples.function_naming import to_function_name_suffix


def pytest_addoption(parser):
    parser.addoption(
        "--runner-mode",
        choices=("local", "cloud"),
        default="local",
        help="Execute examples locally or against deployed Lambda functions",
    )


@pytest.fixture
def durable_runner(request):
    def factory(*, handler, input=None, timeout=60):
        if request.config.getoption("--runner-mode") == "local":
            return create_local_runner(handler=handler, input=input, timeout=timeout)
        prefix = os.environ.get("PYTEST_FUNCTION_NAME_PREFIX")
        name = os.environ.get("QUALIFIED_FUNCTION_NAME")
        if prefix:
            suffix = to_function_name_suffix(f"{handler.__module__}.{handler.__name__}")
            name = f"{prefix}{suffix}:$LATEST"
        if not name:
            pytest.skip(
                "Cloud examples require QUALIFIED_FUNCTION_NAME or PYTEST_FUNCTION_NAME_PREFIX"
            )
        return create_cloud_runner(
            function_name=name,
            region=os.environ.get("AWS_REGION", "eu-south-1"),
            lambda_endpoint=os.environ.get("LAMBDA_ENDPOINT"),
            input=input,
            timeout=timeout,
        )

    return factory
