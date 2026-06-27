"""Tests for retry decision models."""

from datetime import timedelta

from async_durable_execution.models import RetryDecision


def test_retry_factory():
    """Test retry factory method."""
    decision = RetryDecision.retry(timedelta(seconds=30))
    assert decision.should_retry is True
    assert decision.delay_seconds == 30


def test_retry_factory_accepts_int_seconds():
    """Test retry factory accepts integer seconds."""
    decision = RetryDecision.retry(30)
    assert decision.should_retry is True
    assert decision.delay == 30
    assert decision.delay_seconds == 30


def test_no_retry_factory():
    """Test no_retry factory method."""
    decision = RetryDecision.no_retry()
    assert decision.should_retry is False
    assert decision.delay_seconds == 0
