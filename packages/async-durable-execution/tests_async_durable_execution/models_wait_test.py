"""Tests for wait decision models."""

from datetime import timedelta

from async_durable_execution.models import WaitDecision, WaitForConditionDecision


class TestWaitDecision:
    """Test WaitDecision factory methods."""

    def test_wait_factory(self):
        """Test wait factory method."""
        decision = WaitDecision.wait(timedelta(seconds=30))
        assert decision.should_wait is True
        assert decision.delay_seconds == 30

    def test_no_wait_factory(self):
        """Test no_wait factory method."""
        decision = WaitDecision.no_wait()
        assert decision.should_wait is False
        assert decision.delay_seconds == 0


class TestWaitForConditionDecision:
    """Test WaitForConditionDecision factory methods."""

    def test_continue_waiting_factory(self):
        """Test continue_waiting factory method."""
        decision = WaitForConditionDecision.continue_waiting(timedelta(seconds=45))
        assert decision.should_continue is True
        assert decision.delay_seconds == 45

    def test_stop_polling_factory(self):
        """Test stop_polling factory method."""
        decision = WaitForConditionDecision.stop_polling()
        assert decision.should_continue is False
        assert decision.delay_seconds == 0
