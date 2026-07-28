"""Tests for exceptions used by the durable execution runners."""

from collections.abc import Callable

import pytest

from async_durable_execution._runner import exceptions


def test_durable_functions_test_error_base_exception() -> None:
    error = exceptions.DurableFunctionsTestError("Base error message")

    assert str(error) == "Base error message"
    assert isinstance(error, Exception)


def test_durable_functions_local_runner_error_base_exception() -> None:
    error = exceptions.DurableFunctionsLocalRunnerError("Local runner error")

    assert str(error) == "Local runner error"
    assert isinstance(error, Exception)


def test_serialization_error() -> None:
    error = exceptions.SerializationError("Failed to serialize data")

    assert str(error) == "Failed to serialize data"
    assert isinstance(error, exceptions.DurableFunctionsLocalRunnerError)


def test_unknown_route_error() -> None:
    error = exceptions.UnknownRouteError("POST", "/unknown/path")

    assert str(error) == "Unknown path pattern: POST /unknown/path"
    assert error.method == "POST"
    assert error.path == "/unknown/path"
    assert isinstance(error, exceptions.DurableFunctionsLocalRunnerError)


@pytest.mark.parametrize(
    ("factory", "status_code", "message_field"),
    [
        (exceptions.InvalidParameterValueException, 400, "message"),
        (exceptions.ResourceNotFoundException, 404, "Message"),
        (exceptions.ServiceException, 500, "Message"),
        (exceptions.ExecutionConflictException, 409, "message"),
        (exceptions.CallbackTimeoutException, 408, "message"),
        (exceptions.TooManyRequestsException, 429, "message"),
        (exceptions.IllegalStateException, 500, "message"),
        (exceptions.RuntimeException, 500, "message"),
        (exceptions.IllegalArgumentException, 400, "message"),
    ],
)
def test_aws_api_exception_properties(
    factory: Callable[[str], exceptions.AwsApiException],
    status_code: int,
    message_field: str,
) -> None:
    error = factory("test message")

    assert isinstance(error, exceptions.AwsApiException)
    assert isinstance(error, exceptions.DurableFunctionsLocalRunnerError)
    assert str(error) == "test message"
    assert error.http_status_code == status_code
    assert getattr(error, message_field) == "test message"


def test_execution_already_started_exception_properties() -> None:
    error = exceptions.ExecutionAlreadyStartedException(
        "Execution already started",
        "arn:aws:lambda:us-east-1:123456789012:function:test",
    )

    assert isinstance(error, exceptions.AwsApiException)
    assert error.http_status_code == 409
    assert error.message == "Execution already started"
    assert (
        error.DurableExecutionArn
        == "arn:aws:lambda:us-east-1:123456789012:function:test"
    )
    assert str(error) == "Execution already started"


def test_invalid_parameter_value_exception_accepts_none_message() -> None:
    error = exceptions.InvalidParameterValueException(None)

    assert error.message is None
    assert str(error) == "None"


def test_aws_api_exceptions_do_not_expose_serialization() -> None:
    aws_exceptions = [
        exceptions.AwsApiException("test"),
        exceptions.InvalidParameterValueException("test"),
        exceptions.ResourceNotFoundException("test"),
        exceptions.ServiceException("test"),
        exceptions.ExecutionAlreadyStartedException("test", "arn"),
        exceptions.ExecutionConflictException("test"),
        exceptions.CallbackTimeoutException("test"),
        exceptions.TooManyRequestsException("test"),
        exceptions.IllegalStateException("test"),
        exceptions.RuntimeException("test"),
        exceptions.IllegalArgumentException("test"),
    ]

    for error in aws_exceptions:
        assert not hasattr(error, "to_boto")
        assert not hasattr(error, "from_boto")
        assert not hasattr(error, "to_dict")
        assert not hasattr(error, "from_dict")
