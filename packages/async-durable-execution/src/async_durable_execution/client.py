from __future__ import annotations

import asyncio
import logging
from typing import Any, cast

import boto3
from botocore.config import Config

from .__about__ import __version__
from .exceptions import CheckpointError, GetExecutionStateError
from .models import (
    CheckpointOutput,
    OperationUpdate,
    StateOutput,
)
from .types import DurableServiceClient, LambdaApiClient

logger = logging.getLogger(__name__)


def create_default_client():
    user_agent = f"async-durable-execution/{__version__}-async"
    return boto3.client(
        "lambda",
        config=Config(
            connect_timeout=5,
            read_timeout=50,
            user_agent_extra=user_agent,
        ),
    )


class ThreadedSyncLambdaClient(DurableServiceClient):
    """Adapt the sync boto3 Lambda client to the async service interface."""

    _cached_boto_client: LambdaApiClient | None = None

    def __init__(self, client: LambdaApiClient | None) -> None:
        self.client = client or create_default_client()

    async def checkpoint(
        self,
        durable_execution_arn: str,
        checkpoint_token: str,
        updates: list[OperationUpdate],
        client_token: str | None,
    ) -> CheckpointOutput:
        try:
            optional_params: dict[str, str] = {}
            if client_token is not None:
                optional_params["ClientToken"] = client_token

            result = await asyncio.to_thread(
                self.client.checkpoint_durable_execution,
                DurableExecutionArn=durable_execution_arn,
                CheckpointToken=checkpoint_token,
                Updates=cast("Any", [o.to_dict() for o in updates]),
                **optional_params,  # type: ignore[arg-type]
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
        checkpoint_token: str,
        next_marker: str,
        max_items: int = 1000,
    ) -> StateOutput:
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


__all__ = ["ThreadedSyncLambdaClient"]
