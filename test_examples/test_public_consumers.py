"""Existing application examples execute against the replacement public facade."""

import importlib
import pytest
from async_durable_execution import InvocationStatus, create_local_runner


@pytest.mark.parametrize(
    "module,payload,expected",
    [
        ("examples.step.step", None, 8),
        ("examples.map.map_operations", None, [2, 4, 6, 8, 10]),
        ("examples.map.map_operations_flat", None, [2, 4, 6, 8, 10]),
        (
            "examples.parallel.parallel_with_batch_serdes",
            None,
            {"success_count": 3, "results": [100, 200, 300], "total": 600},
        ),
        (
            "examples.parallel.parallel_with_custom_serdes",
            None,
            {
                "success_count": 3,
                "results": [
                    {"task": "task1", "value": 100},
                    {"task": "task2", "value": 200},
                    {"task": "task3", "value": 300},
                ],
                "total_value": 600,
            },
        ),
        (
            "examples.flow.flow_fanout_fanin",
            {"orderId": "A", "quantity": 2, "unitPrice": 7.5},
            {"orderId": "A", "valid": True, "total": 15.0},
        ),
    ],
)
async def test_consumer_example(module, payload, expected, durable_runner):
    handler = importlib.import_module(module).handler
    async with durable_runner(handler=handler, input=payload, timeout=5) as runner:
        result = await runner.run()
    assert result.status is InvocationStatus.SUCCEEDED, result.error
    assert result.get_deserialized_result() == expected
