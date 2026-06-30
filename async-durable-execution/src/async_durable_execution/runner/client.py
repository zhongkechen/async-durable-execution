"""An in-memory service client, that can replace the boto lambda service client."""

from async_durable_execution.models import (
    CheckpointOutput,
    OperationUpdate,
    StateOutput,
)
from async_durable_execution.client import DurableServiceClient
from .processor import (
    CheckpointProcessor,
)


class InMemoryServiceClient(DurableServiceClient):
    """An in-memory service client, that can replace the boto lambda service client."""

    def __init__(self, checkpoint_processor: CheckpointProcessor):
        self._checkpoint_processor: CheckpointProcessor = checkpoint_processor

    async def checkpoint(
        self,
        durable_execution_arn: str,  # noqa: ARG002
        checkpoint_token: str,
        updates: list[OperationUpdate],
        client_token: str | None,
    ) -> CheckpointOutput:
        # durable_execution_arn is not used in in-memory testing
        return self._checkpoint_processor.process_checkpoint(
            checkpoint_token, updates, client_token
        )

    async def get_execution_state(
        self,
        durable_execution_arn: str,  # noqa: ARG002
        checkpoint_token: str,
        next_marker: str,
        max_items: int = 1000,
    ) -> StateOutput:
        # durable_execution_arn is not used in in-memory testing
        return self._checkpoint_processor.get_execution_state(
            checkpoint_token, next_marker, max_items
        )
