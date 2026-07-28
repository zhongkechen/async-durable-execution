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


@pytest.mark.parametrize(
    ("factory", "status_code", "message_field"),
    [
        (exceptions.InvalidParameterValueException, 400, "message"),
        (exceptions.ResourceNotFoundException, 404, "Message"),
        (exceptions.IllegalStateException, 500, "message"),
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


def test_invalid_parameter_value_exception_accepts_none_message() -> None:
    error = exceptions.InvalidParameterValueException(None)

    assert error.message is None
    assert str(error) == "None"


def test_aws_api_exceptions_do_not_expose_serialization() -> None:
    aws_exceptions = [
        exceptions.AwsApiException("test"),
        exceptions.InvalidParameterValueException("test"),
        exceptions.ResourceNotFoundException("test"),
        exceptions.IllegalStateException("test"),
    ]

    for error in aws_exceptions:
        assert not hasattr(error, "to_boto")
        assert not hasattr(error, "from_boto")
        assert not hasattr(error, "to_dict")
        assert not hasattr(error, "from_dict")
