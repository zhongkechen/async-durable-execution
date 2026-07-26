"""Tests for conditional success and failure flow branches."""

import pytest

from async_durable_execution import InvocationStatus
from examples.flow import flow_failure_recovery


@pytest.mark.parametrize(
    ("approved", "expected_statuses", "expected_outcomes"),
    [
        (
            True,
            {
                "charge-payment": "SUCCEEDED",
                "fulfill-order": "SUCCEEDED",
                "record-payment-failure": "SKIPPED",
            },
            {
                "fulfill-order": "fulfilled:order-123:payment accepted",
                "record-payment-failure": None,
            },
        ),
        (
            False,
            {
                "charge-payment": "FAILED",
                "fulfill-order": "SKIPPED",
                "record-payment-failure": "SUCCEEDED",
            },
            {
                "fulfill-order": None,
                "record-payment-failure": "recovered:payment declined",
            },
        ),
    ],
)
async def test_flow_failure_recovery(
    durable_runner,
    approved,
    expected_statuses,
    expected_outcomes,
):
    """Verify success and failure conditions select the expected branch."""
    async with durable_runner(
        handler=flow_failure_recovery.handler,
        input={"orderId": "order-123", "approved": approved},
        timeout=10,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    payload = result.get_deserialized_result()
    assert {
        name: node_result["status"] for name, node_result in payload["results"].items()
    } == expected_statuses
    assert {
        name: payload["results"][name]["outcome"] for name in expected_outcomes
    } == expected_outcomes
    assert payload["unhandledFailures"] == []
