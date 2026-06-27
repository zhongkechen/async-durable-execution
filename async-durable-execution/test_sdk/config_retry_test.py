"""Tests for retry strategy configuration and presets."""

import re
from datetime import timedelta
from unittest.mock import patch

import pytest

from async_durable_execution.config import (
    JitterStrategy,
    RetryPresets,
    RetryStrategyBuilder,
)


def test_none_jitter_returns_delay():
    """Test NONE jitter returns the original delay unchanged."""
    strategy = JitterStrategy.NONE
    assert strategy.apply_jitter(10) == 10
    assert strategy.apply_jitter(100) == 100


@patch("random.random")
def test_full_jitter_range(mock_random):
    """Test FULL jitter returns value between 0 and delay."""
    mock_random.return_value = 0.5
    strategy = JitterStrategy.FULL
    delay = 10
    result = strategy.apply_jitter(delay)
    assert result == 5.0


@patch("random.random")
def test_half_jitter_range(mock_random):
    """Test HALF jitter returns value between delay/2 and delay."""
    mock_random.return_value = 0.5
    strategy = JitterStrategy.HALF
    result = strategy.apply_jitter(10)
    assert result == 7.5


@patch("random.random")
def test_half_jitter_boundary_values(mock_random):
    """Test HALF jitter boundary values."""
    strategy = JitterStrategy.HALF

    mock_random.return_value = 0.0
    result = strategy.apply_jitter(100)
    assert result == 50

    mock_random.return_value = 1.0
    result = strategy.apply_jitter(100)
    assert result == 100


def test_invalid_jitter_strategy():
    """Test behavior with invalid jitter strategy."""
    with pytest.raises((ValueError, AttributeError)):
        JitterStrategy("INVALID").apply_jitter(10)


def test_default_config():
    """Test default configuration values."""
    config = RetryStrategyBuilder()
    assert config.max_attempts == 3
    assert config.initial_delay == 5
    assert config.max_delay == 300
    assert config.initial_delay_seconds == 5
    assert config.max_delay_seconds == 300
    assert config.backoff_rate == 2.0
    assert config.jitter_strategy == JitterStrategy.FULL
    assert config.retryable_errors is None
    assert config.retryable_error_types is None


def test_config_accepts_int_seconds():
    """Test duration fields accept integer seconds."""
    config = RetryStrategyBuilder(initial_delay=2, max_delay=50)

    assert config.initial_delay == 2
    assert config.max_delay == 50
    assert config.initial_delay_seconds == 2
    assert config.max_delay_seconds == 50


def test_max_attempts_exceeded():
    """Test strategy returns no_retry when max attempts exceeded."""
    config = RetryStrategyBuilder(max_attempts=2)
    strategy = config.build()

    error = Exception("test error")
    decision = strategy(error, 2)
    assert decision.should_retry is False


def test_retryable_error_message_string():
    """Test retry based on error message string match."""
    config = RetryStrategyBuilder(retryable_errors=["timeout"])
    strategy = config.build()

    error = Exception("connection timeout")
    decision = strategy(error, 1)
    assert decision.should_retry is True


def test_retryable_error_message_regex():
    """Test retry based on error message regex match."""
    config = RetryStrategyBuilder(retryable_errors=[re.compile(r"timeout|error")])
    strategy = config.build()

    error = Exception("network timeout occurred")
    decision = strategy(error, 1)
    assert decision.should_retry is True


def test_retryable_error_type():
    """Test retry based on error type."""
    config = RetryStrategyBuilder(retryable_error_types=[ValueError])
    strategy = config.build()

    error = ValueError("invalid value")
    decision = strategy(error, 1)
    assert decision.should_retry is True


def test_non_retryable_error():
    """Test no retry for non-retryable error."""
    config = RetryStrategyBuilder(retryable_errors=["timeout"])
    strategy = config.build()

    error = Exception("permission denied")
    decision = strategy(error, 1)
    assert decision.should_retry is False


@patch("random.random")
def test_exponential_backoff_calculation(mock_random):
    """Test exponential backoff delay calculation with jitter."""
    mock_random.return_value = 0.5
    config = RetryStrategyBuilder(
        initial_delay=timedelta(seconds=2),
        backoff_rate=2.0,
        jitter_strategy=JitterStrategy.FULL,
    )
    strategy = config.build()

    error = Exception("test error")

    decision = strategy(error, 1)
    assert decision.delay_seconds == 1

    decision = strategy(error, 2)
    assert decision.delay_seconds == 2


def test_max_delay_cap():
    """Test delay is capped at max_delay_seconds."""
    config = RetryStrategyBuilder(
        initial_delay=timedelta(seconds=100),
        max_delay=timedelta(seconds=50),
        backoff_rate=2.0,
        jitter_strategy=JitterStrategy.NONE,
    )
    strategy = config.build()

    error = Exception("test error")
    decision = strategy(error, 2)
    assert decision.delay_seconds == 50


def test_minimum_delay_one_second():
    """Test delay is at least 1 second."""
    config = RetryStrategyBuilder(
        initial_delay=timedelta(seconds=0), jitter_strategy=JitterStrategy.NONE
    )
    strategy = config.build()

    error = Exception("test error")
    decision = strategy(error, 1)
    assert decision.delay_seconds == 1


def test_delay_ceiling_applied():
    """Test delay is rounded up using math.ceil."""
    with patch("random.random", return_value=0.3):
        config = RetryStrategyBuilder(
            initial_delay=timedelta(seconds=3),
            jitter_strategy=JitterStrategy.FULL,
        )
        strategy = config.build()

        error = Exception("test error")
        decision = strategy(error, 1)
        assert decision.delay_seconds == 1


def test_none_preset():
    """Test none preset allows no retries."""
    strategy = RetryPresets.none()
    error = Exception("test error")

    decision = strategy(error, 1)
    assert decision.should_retry is False


def test_default_preset_config():
    """Test default preset configuration."""
    strategy = RetryPresets.default()
    error = Exception("test error")

    decision = strategy(error, 1)
    assert decision.should_retry is True

    decision = strategy(error, 6)
    assert decision.should_retry is False


def test_transient_preset_config():
    """Test transient preset configuration."""
    strategy = RetryPresets.transient()
    error = Exception("test error")

    decision = strategy(error, 1)
    assert decision.should_retry is True

    decision = strategy(error, 3)
    assert decision.should_retry is False


def test_resource_availability_preset():
    """Test resource availability preset allows longer retries."""
    strategy = RetryPresets.resource_availability()
    error = Exception("test error")

    decision = strategy(error, 1)
    assert decision.should_retry is True

    decision = strategy(error, 5)
    assert decision.should_retry is False


def test_critical_preset_config():
    """Test critical preset allows many retries."""
    strategy = RetryPresets.critical()
    error = Exception("test error")

    decision = strategy(error, 5)
    assert decision.should_retry is True

    decision = strategy(error, 10)
    assert decision.should_retry is False


@patch("random.random")
def test_critical_preset_no_jitter(mock_random):
    """Test critical preset uses no jitter."""
    mock_random.return_value = 0.5
    strategy = RetryPresets.critical()
    error = Exception("test error")

    decision = strategy(error, 1)
    assert decision.delay_seconds == 1


@patch("random.random")
def test_full_jitter_integration(mock_random):
    """Test full jitter integration in retry strategy."""
    mock_random.return_value = 0.8
    config = RetryStrategyBuilder(
        initial_delay=timedelta(seconds=10), jitter_strategy=JitterStrategy.FULL
    )
    strategy = config.build()

    error = Exception("test error")
    decision = strategy(error, 1)
    assert decision.delay_seconds == 8


@patch("random.random")
def test_half_jitter_integration(mock_random):
    """Test half jitter integration in retry strategy."""
    mock_random.return_value = 0.6
    config = RetryStrategyBuilder(
        initial_delay=timedelta(seconds=10), jitter_strategy=JitterStrategy.HALF
    )
    strategy = config.build()

    error = Exception("test error")
    decision = strategy(error, 1)
    assert decision.delay_seconds == 8


@patch("random.random")
def test_half_jitter_integration_corrected(mock_random):
    """Test half jitter with minimum random value."""
    mock_random.return_value = 0.0
    config = RetryStrategyBuilder(
        initial_delay=timedelta(seconds=10), jitter_strategy=JitterStrategy.HALF
    )
    strategy = config.build()

    error = Exception("test error")
    decision = strategy(error, 1)
    assert decision.delay_seconds == 5


def test_none_jitter_integration():
    """Test no jitter integration in retry strategy."""
    config = RetryStrategyBuilder(
        initial_delay=timedelta(seconds=10), jitter_strategy=JitterStrategy.NONE
    )
    strategy = config.build()

    error = Exception("test error")
    decision = strategy(error, 1)
    assert decision.delay_seconds == 10


def test_no_filters_retries_all_errors():
    """Test that when neither filter is specified, all errors are retried."""
    config = RetryStrategyBuilder()
    strategy = config.build()

    error1 = Exception("any error message")
    decision1 = strategy(error1, 1)
    assert decision1.should_retry is True

    error2 = ValueError("different error type")
    decision2 = strategy(error2, 1)
    assert decision2.should_retry is True


def test_only_retryable_errors_specified():
    """Test that when only retryable_errors is specified, only matching messages are retried."""
    config = RetryStrategyBuilder(retryable_errors=["timeout"])
    strategy = config.build()

    error1 = Exception("connection timeout")
    decision1 = strategy(error1, 1)
    assert decision1.should_retry is True

    error2 = Exception("permission denied")
    decision2 = strategy(error2, 1)
    assert decision2.should_retry is False


def test_only_retryable_error_types_specified():
    """Test that when only retryable_error_types is specified, only matching types are retried."""
    config = RetryStrategyBuilder(retryable_error_types=[ValueError, TypeError])
    strategy = config.build()

    error1 = ValueError("invalid value")
    decision1 = strategy(error1, 1)
    assert decision1.should_retry is True

    error2 = TypeError("type error")
    decision2 = strategy(error2, 1)
    assert decision2.should_retry is True

    error3 = Exception("some error")
    decision3 = strategy(error3, 1)
    assert decision3.should_retry is False


def test_both_filters_specified_or_logic():
    """Test that when both filters are specified, errors matching either are retried (OR logic)."""
    config = RetryStrategyBuilder(
        retryable_errors=["timeout"], retryable_error_types=[ValueError]
    )
    strategy = config.build()

    error1 = Exception("connection timeout")
    decision1 = strategy(error1, 1)
    assert decision1.should_retry is True

    error2 = ValueError("some value error")
    decision2 = strategy(error2, 1)
    assert decision2.should_retry is True

    error3 = RuntimeError("runtime error")
    decision3 = strategy(error3, 1)
    assert decision3.should_retry is False


def test_empty_retryable_errors_with_types():
    """Test that empty retryable_errors list with types specified only retries matching types."""
    config = RetryStrategyBuilder(
        retryable_errors=[], retryable_error_types=[ValueError]
    )
    strategy = config.build()

    error1 = ValueError("value error")
    decision1 = strategy(error1, 1)
    assert decision1.should_retry is True

    error2 = Exception("some error")
    decision2 = strategy(error2, 1)
    assert decision2.should_retry is False


def test_empty_retryable_error_types_with_errors():
    """Test that empty retryable_error_types list with errors specified only retries matching messages."""
    config = RetryStrategyBuilder(
        retryable_errors=["timeout"], retryable_error_types=[]
    )
    strategy = config.build()

    error1 = Exception("connection timeout")
    decision1 = strategy(error1, 1)
    assert decision1.should_retry is True

    error2 = Exception("permission denied")
    decision2 = strategy(error2, 1)
    assert decision2.should_retry is False


def test_none_config():
    """Test behavior when config is None."""
    strategy = RetryStrategyBuilder().build()
    error = Exception("test error")
    decision = strategy(error, 1)
    assert decision.should_retry is True
    assert decision.delay_seconds >= 1


def test_zero_backoff_rate():
    """Test behavior with zero backoff rate."""
    config = RetryStrategyBuilder(
        initial_delay=timedelta(seconds=5),
        backoff_rate=0,
        jitter_strategy=JitterStrategy.NONE,
    )
    strategy = config.build()

    error = Exception("test error")
    decision = strategy(error, 1)
    assert decision.delay_seconds == 5


def test_fractional_backoff_rate():
    """Test behavior with fractional backoff rate."""
    config = RetryStrategyBuilder(
        initial_delay=timedelta(seconds=8),
        backoff_rate=0.5,
        jitter_strategy=JitterStrategy.NONE,
    )
    strategy = config.build()

    error = Exception("test error")
    decision = strategy(error, 2)
    assert decision.delay_seconds == 4


def test_empty_retryable_errors_list():
    """Test behavior with empty retryable errors list."""
    config = RetryStrategyBuilder(retryable_errors=[])
    strategy = config.build()

    error = Exception("test error")
    decision = strategy(error, 1)
    assert decision.should_retry is False


def test_multiple_error_patterns():
    """Test multiple error patterns matching."""
    config = RetryStrategyBuilder(
        retryable_errors=["timeout", re.compile(r"network.*error")]
    )
    strategy = config.build()

    error1 = Exception("connection timeout")
    decision1 = strategy(error1, 1)
    assert decision1.should_retry is True

    error2 = Exception("network connection error")
    decision2 = strategy(error2, 1)
    assert decision2.should_retry is True


def test_mixed_error_types_and_patterns():
    """Test combination of error types and patterns."""
    config = RetryStrategyBuilder(
        retryable_errors=["timeout"], retryable_error_types=[ValueError]
    )
    strategy = config.build()

    error = ValueError("some value error")
    decision = strategy(error, 1)
    assert decision.should_retry is True
