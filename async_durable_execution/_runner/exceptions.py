"""Exceptions raised by the Durable Executions Testing Library."""

from __future__ import annotations


class DurableFunctionsLocalRunnerError(Exception):
    """Base class for Durable Executions exceptions"""


class UnknownRouteError(DurableFunctionsLocalRunnerError):
    """No route matches the requested path pattern."""

    def __init__(self, method: str, path: str) -> None:
        """Initialize UnknownRouteError with method and path.

        Args:
            method: HTTP method (GET, POST, etc.)
            path: Request path that couldn't be matched
        """
        self.method = method
        self.path = path
        message = f"Unknown path pattern: {method} {path}"
        super().__init__(message)


class SerializationError(DurableFunctionsLocalRunnerError):
    """Exception for serialization errors."""


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


class ServiceException(AwsApiException):
    """Exception for general service errors."""

    http_status_code = 500

    def __init__(
        self,
        Message: str,  # noqa: N803
    ) -> None:  # Capital M per Smithy definition
        """Initialize with Message field (capital M per Smithy definition)."""
        self.Message = Message
        super().__init__(Message)


class ExecutionAlreadyStartedException(AwsApiException):
    """Exception for execution already started errors."""

    http_status_code = 409

    def __init__(self, message: str, DurableExecutionArn: str) -> None:  # noqa: N803
        """Initialize with message and DurableExecutionArn fields."""
        self.message = message
        self.DurableExecutionArn = DurableExecutionArn
        super().__init__(message)


class ExecutionConflictException(AwsApiException):
    """Exception for execution conflict errors."""

    http_status_code = 409

    def __init__(self, message: str) -> None:
        """Initialize with message field."""
        self.message = message
        super().__init__(message)


class CallbackTimeoutException(AwsApiException):
    """Exception for callback timeout errors."""

    http_status_code = 408

    def __init__(self, message: str) -> None:
        """Initialize with message field (lowercase per Smithy definition)."""
        self.message = message
        super().__init__(message)


class TooManyRequestsException(AwsApiException):
    """Exception for too many requests errors."""

    http_status_code = 429

    def __init__(self, message: str) -> None:
        """Initialize with message field (lowercase per Smithy definition)."""
        self.message = message
        super().__init__(message)


# Unmapped Exceptions (thrown by services but not in Smithy)
class IllegalStateException(AwsApiException):
    """IllegalStateException."""

    http_status_code = 500

    def __init__(self, message: str) -> None:
        """Initialize with message field."""
        self.message = message
        super().__init__(message)


class RuntimeException(AwsApiException):
    """RuntimeException."""

    http_status_code = 500

    def __init__(self, message: str) -> None:
        """Initialize with message field."""
        self.message = message
        super().__init__(message)


class IllegalArgumentException(AwsApiException):
    """IllegalArgumentException."""

    http_status_code = 400

    def __init__(self, message: str) -> None:
        """Initialize with message field."""
        self.message = message
        super().__init__(message)
