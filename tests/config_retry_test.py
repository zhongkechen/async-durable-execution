"""Tests for retry strategy configuration and presets."""

import re
from datetime import timedelta
from unittest.mock import patch

import pytest

from async_durable_execution.core.config import (
    JitterStrategy,
    RetryStrategy,
    duration_to_seconds,
)
from async_durable_execution.core.exceptions import ValidationError


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


@pytest.mark.parametrize("duration", [True, "5", 1.2, None])
def test_duration_to_seconds_rejects_non_duration_values(duration):
    with pytest.raises(ValidationError, match="wait_time must be an int"):
        duration_to_seconds(duration, field_name="wait_time")


def test_default_config():
    """RetryStrategy() uses the same values as the default preset."""
    config = RetryStrategy()
    default_config = RetryStrategy.default()

    assert config == default_config
    assert config is not default_config
    assert config.max_attempts == 6
    assert config.initial_delay == 5
    assert config.max_delay == 60
    assert config.initial_delay_seconds == 5
    assert config.max_delay_seconds == 60
    assert config.backoff_rate == 2
    assert config.jitter_strategy == JitterStrategy.FULL
    assert config.retryable_errors is None
    assert config.retryable_error_types is None
    assert config.increment is None
    assert config.increment_seconds is None


def test_custom_config():
    """Explicit configuration overrides the default preset values."""
    config = RetryStrategy(max_attempts=3, max_delay=300, backoff_rate=2.0)

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
    config = RetryStrategy(initial_delay=2, max_delay=50)

    assert config.initial_delay == 2
    assert config.max_delay == 50
    assert config.initial_delay_seconds == 2
    assert config.max_delay_seconds == 50


def test_retry_strategy_is_directly_callable():
    """Test retry strategy instances implement the strategy callable contract."""
    strategy = RetryStrategy()

    assert strategy(Exception("test error"), 1) >= 1


def test_retry_strategy_has_no_build_method():
    """RetryStrategy is the callable strategy, not a builder."""
    assert not hasattr(RetryStrategy(), "build")


def test_max_attempts_exceeded():
    """Test strategy stops retrying when max attempts are exceeded."""
    config = RetryStrategy(max_attempts=2)
    strategy = config

    error = Exception("test error")
    assert strategy(error, 2) is None


def test_retryable_error_message_string():
    """Test retry based on error message string match."""
    config = RetryStrategy(retryable_errors=["timeout"])
    strategy = config

    error = Exception("connection timeout")
    assert strategy(error, 1) >= 1


def test_retryable_error_message_regex():
    """Test retry based on error message regex match."""
    config = RetryStrategy(retryable_errors=[re.compile(r"timeout|error")])
    strategy = config

    error = Exception("network timeout occurred")
    assert strategy(error, 1) >= 1


def test_retryable_error_type():
    """Test retry based on error type."""
    config = RetryStrategy(retryable_error_types=[ValueError])
    strategy = config

    error = ValueError("invalid value")
    assert strategy(error, 1) >= 1


def test_non_retryable_error():
    """Test no retry for non-retryable error."""
    config = RetryStrategy(retryable_errors=["timeout"])
    strategy = config

    error = Exception("permission denied")
    assert strategy(error, 1) is None


@patch("random.random")
def test_exponential_backoff_calculation(mock_random):
    """Test exponential backoff delay calculation with jitter."""
    mock_random.return_value = 0.5
    config = RetryStrategy(
        initial_delay=timedelta(seconds=2),
        backoff_rate=2.0,
        jitter_strategy=JitterStrategy.FULL,
    )
    strategy = config

    error = Exception("test error")

    assert strategy(error, 1) == 1

    assert strategy(error, 2) == 2


def test_max_delay_cap():
    """Test delay is capped at max_delay_seconds."""
    config = RetryStrategy(
        initial_delay=timedelta(seconds=100),
        max_delay=timedelta(seconds=50),
        backoff_rate=2.0,
        jitter_strategy=JitterStrategy.NONE,
    )
    strategy = config

    error = Exception("test error")
    assert strategy(error, 2) == 50


def test_minimum_delay_one_second():
    """Test delay is at least 1 second."""
    config = RetryStrategy(
        initial_delay=timedelta(seconds=0), jitter_strategy=JitterStrategy.NONE
    )
    strategy = config

    error = Exception("test error")
    assert strategy(error, 1) == 1


def test_delay_ceiling_applied():
    """Test delay is rounded up using math.ceil."""
    with patch("random.random", return_value=0.3):
        config = RetryStrategy(
            initial_delay=timedelta(seconds=3),
            jitter_strategy=JitterStrategy.FULL,
        )
        strategy = config

        error = Exception("test error")
        assert strategy(error, 1) == 1


def test_none_preset():
    """Test none preset allows no retries."""
    strategy = RetryStrategy.none()
    error = Exception("test error")

    assert strategy(error, 1) is None


def test_default_preset_config():
    """Test default preset configuration."""
    strategy = RetryStrategy.default()
    error = Exception("test error")

    assert strategy == RetryStrategy()
    assert strategy(error, 1) >= 1

    assert strategy(error, 6) is None


def test_transient_preset_config():
    """Test transient preset configuration."""
    strategy = RetryStrategy.transient()
    error = Exception("test error")

    assert strategy(error, 1) >= 1

    assert strategy(error, 3) is None


def test_resource_availability_preset():
    """Test resource availability preset allows longer retries."""
    strategy = RetryStrategy.resource_availability()
    error = Exception("test error")

    assert strategy(error, 1) >= 1

    assert strategy(error, 5) is None


def test_critical_preset_config():
    """Test critical preset allows many retries."""
    strategy = RetryStrategy.critical()
    error = Exception("test error")

    assert strategy(error, 5) >= 1

    assert strategy(error, 10) is None


@patch("random.random")
def test_critical_preset_no_jitter(mock_random):
    """Test critical preset uses no jitter."""
    mock_random.return_value = 0.5
    strategy = RetryStrategy.critical()
    error = Exception("test error")

    assert strategy(error, 1) == 1


def test_linear_preset_config():
    """Test linear preset uses additive delays without jitter."""
    strategy = RetryStrategy.linear()
    error = Exception("test error")

    assert strategy.max_attempts == 6
    assert strategy.initial_delay == 1
    assert strategy.increment == 1
    assert strategy.increment_seconds == 1
    assert strategy.max_delay == 300
    assert strategy.jitter_strategy == JitterStrategy.NONE

    assert [strategy(error, attempt) for attempt in range(1, 6)] == [1, 2, 3, 4, 5]

    assert strategy(error, 6) is None


def test_linear_strategy_caps_at_max_delay():
    """Linear retry delay is capped at max_delay before jitter."""
    strategy = RetryStrategy(
        max_attempts=10,
        initial_delay=10,
        increment=10,
        max_delay=25,
        jitter_strategy=JitterStrategy.NONE,
    )
    error = Exception("test error")

    assert [strategy(error, attempt) for attempt in range(1, 4)] == [10, 20, 25]


@patch("random.random")
def test_linear_strategy_applies_jitter(mock_random):
    """Linear retry uses the configured jitter strategy."""
    mock_random.return_value = 0.5
    strategy = RetryStrategy(
        initial_delay=4,
        increment=4,
        jitter_strategy=JitterStrategy.FULL,
    )
    error = Exception("test error")

    assert strategy(error, 2) == 4


@patch("random.random")
def test_full_jitter_integration(mock_random):
    """Test full jitter integration in retry strategy."""
    mock_random.return_value = 0.8
    config = RetryStrategy(
        initial_delay=timedelta(seconds=10), jitter_strategy=JitterStrategy.FULL
    )
    strategy = config

    error = Exception("test error")
    assert strategy(error, 1) == 8


@patch("random.random")
def test_half_jitter_integration(mock_random):
    """Test half jitter integration in retry strategy."""
    mock_random.return_value = 0.6
    config = RetryStrategy(
        initial_delay=timedelta(seconds=10), jitter_strategy=JitterStrategy.HALF
    )
    strategy = config

    error = Exception("test error")
    assert strategy(error, 1) == 8


@patch("random.random")
def test_half_jitter_integration_corrected(mock_random):
    """Test half jitter with minimum random value."""
    mock_random.return_value = 0.0
    config = RetryStrategy(
        initial_delay=timedelta(seconds=10), jitter_strategy=JitterStrategy.HALF
    )
    strategy = config

    error = Exception("test error")
    assert strategy(error, 1) == 5


def test_none_jitter_integration():
    """Test no jitter integration in retry strategy."""
    config = RetryStrategy(
        initial_delay=timedelta(seconds=10), jitter_strategy=JitterStrategy.NONE
    )
    strategy = config

    error = Exception("test error")
    assert strategy(error, 1) == 10


def test_no_filters_retries_all_errors():
    """Test that when neither filter is specified, all errors are retried."""
    config = RetryStrategy()
    strategy = config

    error1 = Exception("any error message")
    assert strategy(error1, 1) >= 1

    error2 = ValueError("different error type")
    assert strategy(error2, 1) >= 1


def test_only_retryable_errors_specified():
    """Test that when only retryable_errors is specified, only matching messages are retried."""
    config = RetryStrategy(retryable_errors=["timeout"])
    strategy = config

    error1 = Exception("connection timeout")
    assert strategy(error1, 1) >= 1

    error2 = Exception("permission denied")
    assert strategy(error2, 1) is None


def test_only_retryable_error_types_specified():
    """Test that when only retryable_error_types is specified, only matching types are retried."""
    config = RetryStrategy(retryable_error_types=[ValueError, TypeError])
    strategy = config

    error1 = ValueError("invalid value")
    assert strategy(error1, 1) >= 1

    error2 = TypeError("type error")
    assert strategy(error2, 1) >= 1

    error3 = Exception("some error")
    assert strategy(error3, 1) is None


def test_both_filters_specified_or_logic():
    """Test that when both filters are specified, errors matching either are retried (OR logic)."""
    config = RetryStrategy(
        retryable_errors=["timeout"], retryable_error_types=[ValueError]
    )
    strategy = config

    error1 = Exception("connection timeout")
    assert strategy(error1, 1) >= 1

    error2 = ValueError("some value error")
    assert strategy(error2, 1) >= 1

    error3 = RuntimeError("runtime error")
    assert strategy(error3, 1) is None


def test_empty_retryable_errors_with_types():
    """Test that empty retryable_errors list with types specified only retries matching types."""
    config = RetryStrategy(retryable_errors=[], retryable_error_types=[ValueError])
    strategy = config

    error1 = ValueError("value error")
    assert strategy(error1, 1) >= 1

    error2 = Exception("some error")
    assert strategy(error2, 1) is None


def test_empty_retryable_error_types_with_errors():
    """Test that empty retryable_error_types list with errors specified only retries matching messages."""
    config = RetryStrategy(retryable_errors=["timeout"], retryable_error_types=[])
    strategy = config

    error1 = Exception("connection timeout")
    assert strategy(error1, 1) >= 1

    error2 = Exception("permission denied")
    assert strategy(error2, 1) is None


def test_none_config():
    """Test behavior when config is None."""
    strategy = RetryStrategy()
    error = Exception("test error")
    assert strategy(error, 1) >= 1


def test_zero_backoff_rate():
    """Test behavior with zero backoff rate."""
    config = RetryStrategy(
        initial_delay=timedelta(seconds=5),
        backoff_rate=0,
        jitter_strategy=JitterStrategy.NONE,
    )
    strategy = config

    error = Exception("test error")
    assert strategy(error, 1) == 5


def test_fractional_backoff_rate():
    """Test behavior with fractional backoff rate."""
    config = RetryStrategy(
        initial_delay=timedelta(seconds=8),
        backoff_rate=0.5,
        jitter_strategy=JitterStrategy.NONE,
    )
    strategy = config

    error = Exception("test error")
    assert strategy(error, 2) == 4


def test_empty_retryable_errors_list():
    """Test behavior with empty retryable errors list."""
    config = RetryStrategy(retryable_errors=[])
    strategy = config

    error = Exception("test error")
    assert strategy(error, 1) is None


def test_multiple_error_patterns():
    """Test multiple error patterns matching."""
    config = RetryStrategy(retryable_errors=["timeout", re.compile(r"network.*error")])
    strategy = config

    error1 = Exception("connection timeout")
    assert strategy(error1, 1) >= 1

    error2 = Exception("network connection error")
    assert strategy(error2, 1) >= 1


def test_mixed_error_types_and_patterns():
    """Test combination of error types and patterns."""
    config = RetryStrategy(
        retryable_errors=["timeout"], retryable_error_types=[ValueError]
    )
    strategy = config

    error = ValueError("some value error")
    assert strategy(error, 1) >= 1
