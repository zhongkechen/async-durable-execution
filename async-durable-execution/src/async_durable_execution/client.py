from __future__ import annotations

import asyncio
import inspect
import importlib
import importlib.util
import logging
from collections.abc import Awaitable, Mapping
from typing import Any, Protocol, cast

from botocore.config import Config
from botocore.session import get_session

from .__about__ import __version__
from .exceptions import CheckpointError, GetExecutionStateError
from .models import (
    CheckpointOutput,
    OperationUpdate,
    StateOutput,
)

logger = logging.getLogger(__name__)


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


class DurableServiceClient(Protocol):
    """Durable Service clients must implement this interface."""

    async def checkpoint(
        self,
        durable_execution_arn: str,
        checkpoint_token: str | None,
        updates: list[OperationUpdate],
        client_token: str | None,
    ) -> CheckpointOutput: ...  # pragma: no cover

    async def get_execution_state(
        self,
        durable_execution_arn: str,
        checkpoint_token: str | None,
        next_marker: str,
        max_items: int = 1000,
    ) -> StateOutput: ...  # pragma: no cover


def _create_client_config() -> Config:
    user_agent = f"durable-execution-sdk-python/{__version__}-async"
    return Config(
        connect_timeout=5,
        read_timeout=50,
        user_agent_extra=user_agent,
    )


def aioboto_is_installed() -> bool:
    """Return whether the optional aioboto dependency is available."""
    return importlib.util.find_spec("aiobotocore") is not None


def create_default_sync_client() -> LambdaApiClient:
    """Create the default botocore Lambda client used for durable API calls."""
    session = get_session()
    return cast(
        "LambdaApiClient",
        session.create_client("lambda", config=_create_client_config()),
    )


def create_default_async_client() -> AsyncLambdaApiClient:
    """Create the default aioboto Lambda client used for durable API calls."""
    aiobotocore_session = importlib.import_module("aiobotocore.session")
    session = aiobotocore_session.get_session()
    return _AiobotocoreLambdaApiClient(
        session.create_client("lambda", config=_create_client_config())
    )


def create_default_client() -> LambdaApiClient | AsyncLambdaApiClient:
    """Create the default Lambda client, preferring async when aioboto is installed."""
    if aioboto_is_installed():
        return create_default_async_client()
    return create_default_sync_client()


def lambda_api_client_is_async(
    client: LambdaApiClient | AsyncLambdaApiClient,
) -> bool:
    """Return whether a Lambda API client exposes async durable methods."""
    return inspect.iscoroutinefunction(client.checkpoint_durable_execution)


def create_default_service_client(
    client: LambdaApiClient | AsyncLambdaApiClient | None = None,
) -> DurableServiceClient:
    """Create the default durable service client."""
    lambda_client = client or create_default_client()
    if lambda_api_client_is_async(lambda_client):
        return AsyncLambdaClient(cast("AsyncLambdaApiClient", lambda_client))
    return ThreadedSyncLambdaClient(cast("LambdaApiClient", lambda_client))


class ThreadedSyncLambdaClient(DurableServiceClient):
    """Adapt the sync botocore Lambda client to the async service interface."""

    _cached_boto_client: LambdaApiClient | None = None

    def __init__(self, client: LambdaApiClient | None) -> None:
        self.client = client or create_default_sync_client()

    async def checkpoint(
        self,
        durable_execution_arn: str,
        checkpoint_token: str | None,
        updates: list[OperationUpdate],
        client_token: str | None,
    ) -> CheckpointOutput:
        if not checkpoint_token:
            msg = "Cannot checkpoint without a checkpoint token."
            raise CheckpointError(msg)

        try:
            optional_params: dict[str, str] = {}
            if client_token is not None:
                optional_params["ClientToken"] = client_token

            result = await asyncio.to_thread(
                self.client.checkpoint_durable_execution,
                DurableExecutionArn=durable_execution_arn,
                CheckpointToken=checkpoint_token,
                Updates=cast("Any", [o.to_dict() for o in updates]),
                **optional_params,
            )

            return CheckpointOutput.from_dict(result)
        except Exception as e:
            checkpoint_error = CheckpointError.from_exception(e)
            logger.exception(
                "Failed to checkpoint.", extra=checkpoint_error.build_logger_extras()
            )
            raise checkpoint_error from None

    async def get_execution_state(
        self,
        durable_execution_arn: str,
        checkpoint_token: str | None,
        next_marker: str,
        max_items: int = 1000,
    ) -> StateOutput:
        if not checkpoint_token:
            msg = "Cannot get execution state without a checkpoint token."
            raise GetExecutionStateError(msg)

        try:
            result = await asyncio.to_thread(
                self.client.get_durable_execution_state,
                DurableExecutionArn=durable_execution_arn,
                CheckpointToken=checkpoint_token,
                Marker=next_marker,
                MaxItems=max_items,
            )
            return StateOutput.from_dict(result)
        except Exception as e:
            error = GetExecutionStateError.from_exception(e)
            logger.exception(
                "Failed to get execution state.", extra=error.build_logger_extras()
            )
            raise error from None


class _AiobotocoreLambdaApiClient:
    """Lazily enter an aiobotocore Lambda client context for durable API calls."""

    def __init__(self, client_context: Any) -> None:
        self._client_context = client_context
        self._client: Any | None = None

    async def _get_client(self) -> Any:
        if self._client is None:
            self._client = await self._client_context.__aenter__()
        return self._client

    async def checkpoint_durable_execution(self, **kwargs: Any) -> Any:
        client = await self._get_client()
        return await client.checkpoint_durable_execution(**kwargs)

    async def get_durable_execution_state(self, **kwargs: Any) -> Any:
        client = await self._get_client()
        return await client.get_durable_execution_state(**kwargs)

    async def aclose(self) -> None:
        if self._client is None:
            return
        await self._client_context.__aexit__(None, None, None)
        self._client = None


class AsyncLambdaClient(DurableServiceClient):
    """Adapt an async aioboto Lambda client to the durable service interface."""

    def __init__(self, client: AsyncLambdaApiClient) -> None:
        self.client = client

    async def checkpoint(
        self,
        durable_execution_arn: str,
        checkpoint_token: str | None,
        updates: list[OperationUpdate],
        client_token: str | None,
    ) -> CheckpointOutput:
        if not checkpoint_token:
            msg = "Cannot checkpoint without a checkpoint token."
            raise CheckpointError(msg)

        try:
            optional_params: dict[str, str] = {}
            if client_token is not None:
                optional_params["ClientToken"] = client_token

            result = await self.client.checkpoint_durable_execution(
                DurableExecutionArn=durable_execution_arn,
                CheckpointToken=checkpoint_token,
                Updates=cast("Any", [o.to_dict() for o in updates]),
                **optional_params,
            )

            return CheckpointOutput.from_dict(result)
        except Exception as e:
            checkpoint_error = CheckpointError.from_exception(e)
            logger.exception(
                "Failed to checkpoint.", extra=checkpoint_error.build_logger_extras()
            )
            raise checkpoint_error from None

    async def get_execution_state(
        self,
        durable_execution_arn: str,
        checkpoint_token: str | None,
        next_marker: str,
        max_items: int = 1000,
    ) -> StateOutput:
        if not checkpoint_token:
            msg = "Cannot get execution state without a checkpoint token."
            raise GetExecutionStateError(msg)

        try:
            result = await self.client.get_durable_execution_state(
                DurableExecutionArn=durable_execution_arn,
                CheckpointToken=checkpoint_token,
                Marker=next_marker,
                MaxItems=max_items,
            )
            return StateOutput.from_dict(result)
        except Exception as e:
            error = GetExecutionStateError.from_exception(e)
            logger.exception(
                "Failed to get execution state.", extra=error.build_logger_extras()
            )
            raise error from None

    async def aclose(self) -> None:
        close = getattr(self.client, "aclose", None)
        if close is None:
            return
        await close()


__all__ = [
    "AsyncLambdaClient",
    "DurableServiceClient",
    "ThreadedSyncLambdaClient",
    "create_default_async_client",
    "create_default_client",
    "create_default_service_client",
    "create_default_sync_client",
    "lambda_api_client_is_async",
]
