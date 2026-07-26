"""Tests for run_in_child_context_large_data."""

from async_durable_execution import InvocationStatus
from examples.run_in_child_context import (
    run_in_child_context_large_data,
)


async def test_handle_large_data_exceeding_256k_limit_using_run_in_child_context(
    durable_runner,
):
    """Test handling large data exceeding 256k limit using runInChildContext."""
    async with durable_runner(
        handler=run_in_child_context_large_data.handler, input=None, timeout=30
    ) as runner:
        result = await runner.run()

    result_data = result.get_deserialized_result()

    # Verify the execution succeeded
    assert result.status is InvocationStatus.SUCCEEDED
    assert result_data["success"] is True

    # Verify large data was processed
    assert result_data["summary"]["totalDataSize"] > 240  # Should be ~250KB
    assert result_data["summary"]["stepsExecuted"] == 5
    assert result_data["summary"]["childContextUsed"] is True
    assert result_data["summary"]["waitExecuted"] is True
    assert result_data["summary"]["dataPreservedAcrossWait"] is True

    # Verify data integrity across wait
    assert result_data["dataIntegrityCheck"] is True
