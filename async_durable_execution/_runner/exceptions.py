"""Exceptions raised by the Durable Executions Testing Library."""

from __future__ import annotations


class DurableFunctionsLocalRunnerError(Exception):
    """Base class for Durable Executions exceptions"""


class DurableFunctionsTestError(Exception):
    """Base class for testing errors."""


class AwsApiException(DurableFunctionsLocalRunnerError):  # noqa: N818
    """Base class for local errors corresponding to AWS API failures."""

    http_status_code: int = 500  # Default to server error


# Smithy-Mapped Exceptions (defined in Smithy models)
class InvalidParameterValueException(AwsApiException):
    """Exception for invalid parameter values."""

    http_status_code = 400

    def __init__(self, message: str | None) -> None:
        """Initialize with message field (lowercase per Smithy definition)."""
        self.message = message
        super().__init__(message)


class ResourceNotFoundException(AwsApiException):
    """Exception for resource not found errors."""

    http_status_code = 404

    def __init__(
        self,
        Message: str,  # noqa: N803
    ) -> None:  # Capital M per Smithy definition
        """Initialize with Message field (capital M per Smithy definition)."""
        self.Message = Message
        super().__init__(Message)


# Unmapped Exceptions (thrown by services but not in Smithy)
class IllegalStateException(AwsApiException):
    """IllegalStateException."""

    http_status_code = 500

    def __init__(self, message: str) -> None:
        """Initialize with message field."""
        self.message = message
        super().__init__(message)
