"""Tests for map_large_scale."""

from async_durable_execution.execution import InvocationStatus
from async_durable_execution_examples.map import map_with_large_scale


def test_handle_50_items_with_100kb_each_using_map(durable_runner):
    """Test handling 50 items with 100KB each using map."""
    with durable_runner(
        handler=map_with_large_scale.handler, input=None, timeout=60
    ) as runner:
        result = runner.run()

    result_data = result.get_deserialized_result()

    # Verify the execution succeeded
    assert result.status is InvocationStatus.SUCCEEDED
    assert result_data["success"] is True

    # Verify the expected number of items were processed (50 items)
    assert result_data["summary"]["itemsProcessed"] == 50
    assert result_data["summary"]["allItemsProcessed"] is True

    # Verify data size expectations (~5MB total from 50 items × 100KB each)
    assert result_data["summary"]["totalDataSizeMB"] > 4  # Should be ~5MB
    assert result_data["summary"]["totalDataSizeMB"] < 6
    assert result_data["summary"]["totalDataSizeBytes"] > 5000000  # ~5MB
    assert result_data["summary"]["averageItemSize"] > 100000  # ~100KB per item
    assert result_data["summary"]["maxConcurrency"] == 10
