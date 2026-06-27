"""Types and Protocols. Don't import anything other than config here - the reason it exists is to avoid circular references."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Awaitable, Protocol, TypeVar

if TYPE_CHECKING:
    from collections.abc import Mapping


T = TypeVar("T")
U = TypeVar("U")
C_co = TypeVar("C_co", covariant=True)
C_contra = TypeVar("C_contra", contravariant=True)


class LambdaContext(Protocol):  # pragma: no cover
    """Minimal AWS Lambda context surface used by the SDK."""

    aws_request_id: str
    log_group_name: str | None = None
    log_stream_name: str | None = None
    function_name: str | None = None
    memory_limit_in_mb: str | None = None
    function_version: str | None = None
    invoked_function_arn: str | None = None
    tenant_id: str | None = None
    client_context: Any | None = None
    identity: Any | None = None

    def get_remaining_time_in_millis(self) -> int: ...
    def log(self, msg) -> None: ...


class SummaryGenerator(Protocol[C_contra]):
    """Create a compact JSON summary for oversized checkpoint payloads."""

    def __call__(self, result: C_contra) -> str: ...  # pragma: no cover


class LambdaApiClient(Protocol):
    """Minimal Lambda client surface needed by durable execution."""

    def checkpoint_durable_execution(
        self, **kwargs: Any
    ) -> Mapping[str, Any]: ...  # pragma: no cover

    def get_durable_execution_state(
        self, **kwargs: Any
    ) -> Mapping[str, Any]: ...  # pragma: no cover


class AsyncLambdaApiClient(Protocol):
    """Minimal async Lambda client surface needed by durable execution."""

    def checkpoint_durable_execution(
        self, **kwargs: Any
    ) -> Awaitable[Mapping[str, Any]]: ...  # pragma: no cover

    def get_durable_execution_state(
        self, **kwargs: Any
    ) -> Awaitable[Mapping[str, Any]]: ...  # pragma: no cover
