"""Tests for wait decision models."""

from datetime import timedelta

from async_durable_execution.composite.wait_for_condition import (
    WaitForConditionDecision,
)


class TestWaitForConditionDecision:
    """Test WaitForConditionDecision factory methods."""

    def test_continue_waiting_factory(self):
        """Test continue_waiting factory method."""
        decision = WaitForConditionDecision.continue_waiting(timedelta(seconds=45))
        assert decision.should_continue is True
        assert decision.delay_seconds == 45

    def test_continue_waiting_factory_accepts_int_seconds(self):
        """Test continue_waiting accepts integer seconds."""
        decision = WaitForConditionDecision.continue_waiting(45)
        assert decision.should_continue is True
        assert decision.delay == 45
        assert decision.delay_seconds == 45

    def test_stop_polling_factory(self):
        """Test stop_polling factory method."""
        decision = WaitForConditionDecision.stop_polling()
        assert decision.should_continue is False
        assert decision.delay_seconds == 0
