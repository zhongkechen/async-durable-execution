"""Tests for nested conditional flow dependencies."""

import pytest

from async_durable_execution import InvocationStatus
from async_durable_execution_examples.flow import flow_complex_dependencies


@pytest.mark.parametrize(
    ("overrides", "expected_decision", "expected_risk_status"),
    [
        (
            {},
            ("fulfilled:order-123:payment accepted:inventory reserved:risk clear"),
            "SUCCEEDED",
        ),
        (
            {"paymentApproved": False},
            "recovered:charge-payment:payment declined",
            "SUCCEEDED",
        ),
        (
            {"inventoryAvailable": False},
            "recovered:reserve-inventory:inventory unavailable",
            "SUCCEEDED",
        ),
        (
            {"riskClear": False},
            "recovered:screen-risk:risk review required",
            "FAILED",
        ),
    ],
)
async def test_flow_complex_dependencies(
    durable_runner,
    overrides,
    expected_decision,
    expected_risk_status,
):
    event = {
        "orderId": "order-123",
        "paymentApproved": True,
        "inventoryAvailable": True,
        "riskClear": True,
        **overrides,
    }
    async with durable_runner(
        handler=flow_complex_dependencies.handler,
        input=event,
        timeout=10,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == {
        "decision": expected_decision,
        "riskStatus": expected_risk_status,
    }
