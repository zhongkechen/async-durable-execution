"""Tests for synchronous composition functions."""

from async_durable_execution import InvocationStatus, OperationStatus
from examples.sync_functions import sync_composition


async def test_sync_composition(durable_runner):
    """Execute synchronous map items and parallel branches."""
    async with durable_runner(
        handler=sync_composition.handler,
        input={"items": [" alpha ", "beta", "Gamma"]},
        timeout=10,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == {
        "items": ["0:ALPHA", "1:BETA", "2:GAMMA"],
        "summaries": ["3 items", "0:ALPHA"],
    }

    map_operation = result.get_context("normalize-items")
    assert map_operation is not None
    assert map_operation.status is OperationStatus.SUCCEEDED

    parallel_operation = result.get_context("summarize-items")
    assert parallel_operation is not None
    assert parallel_operation.status is OperationStatus.SUCCEEDED
