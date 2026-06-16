"""Tests for wait strategy configuration."""

from datetime import timedelta
from unittest.mock import patch

from async_durable_execution.config import (
    JitterStrategy,
    WaitForConditionConfig,
    WaitStrategyBuilder,
)
from async_durable_execution.models import WaitForConditionDecision
from async_durable_execution.serdes import JsonSerDes


class TestWaitStrategyBuilder:
    """Test WaitStrategyBuilder defaults and behavior."""

    def test_default_config(self):
        """Test default configuration values."""
        config = WaitStrategyBuilder(should_continue_polling=lambda x: True)
        assert config.max_attempts == 60
        assert config.initial_delay_seconds == 5
        assert config.max_delay_seconds == 300
        assert config.backoff_rate == 1.5
        assert config.jitter_strategy == JitterStrategy.FULL
        assert config.timeout_seconds is None


class TestCreateWaitStrategy:
    """Test wait strategy creation and behavior."""

    def test_condition_met_returns_no_wait(self):
        """Test strategy returns no_wait when condition is met."""
        config = WaitStrategyBuilder(should_continue_polling=lambda x: False)
        strategy = config.build()

        result = "completed"
        decision = strategy(result, 1)
        assert decision.should_wait is False

    def test_max_attempts_exceeded(self):
        """Test strategy returns no_wait when max attempts exceeded."""
        config = WaitStrategyBuilder(
            should_continue_polling=lambda x: True, max_attempts=5
        )
        strategy = config.build()

        result = "pending"
        decision = strategy(result, 5)
        assert decision.should_wait is False

    def test_should_continue_polling(self):
        """Test strategy continues when condition not met."""
        config = WaitStrategyBuilder(should_continue_polling=lambda x: x == "pending")
        strategy = config.build()

        result = "pending"
        decision = strategy(result, 1)
        assert decision.should_wait is True

    @patch("random.random")
    def test_exponential_backoff_calculation(self, mock_random):
        """Test exponential backoff delay calculation."""
        mock_random.return_value = 0.5
        config = WaitStrategyBuilder(
            should_continue_polling=lambda x: True,
            initial_delay=timedelta(seconds=2),
            backoff_rate=2.0,
            jitter_strategy=JitterStrategy.FULL,
        )
        strategy = config.build()

        result = "pending"

        decision = strategy(result, 1)
        assert decision.delay_seconds == 1

        decision = strategy(result, 2)
        assert decision.delay_seconds == 2

    def test_max_delay_cap(self):
        """Test delay is capped at max_delay_seconds."""
        config = WaitStrategyBuilder(
            should_continue_polling=lambda x: True,
            initial_delay=timedelta(seconds=100),
            max_delay=timedelta(seconds=50),
            backoff_rate=2.0,
            jitter_strategy=JitterStrategy.NONE,
        )
        strategy = config.build()

        result = "pending"
        decision = strategy(result, 2)
        assert decision.delay_seconds == 50

    def test_minimum_delay_one_second(self):
        """Test delay is at least 1 second."""
        config = WaitStrategyBuilder(
            should_continue_polling=lambda x: True,
            initial_delay=timedelta(seconds=0),
            jitter_strategy=JitterStrategy.NONE,
        )
        strategy = config.build()

        result = "pending"
        decision = strategy(result, 1)
        assert decision.delay_seconds == 1

    @patch("random.random")
    def test_full_jitter_integration(self, mock_random):
        """Test full jitter integration in wait strategy."""
        mock_random.return_value = 0.8
        config = WaitStrategyBuilder(
            should_continue_polling=lambda x: True,
            initial_delay=timedelta(seconds=10),
            jitter_strategy=JitterStrategy.FULL,
        )
        strategy = config.build()

        result = "pending"
        decision = strategy(result, 1)
        assert decision.delay_seconds == 8

    @patch("random.random")
    def test_half_jitter_integration(self, mock_random):
        """Test half jitter integration in wait strategy."""
        mock_random.return_value = 0.0
        config = WaitStrategyBuilder(
            should_continue_polling=lambda x: True,
            initial_delay=timedelta(seconds=10),
            jitter_strategy=JitterStrategy.HALF,
        )
        strategy = config.build()

        result = "pending"
        decision = strategy(result, 1)
        assert decision.delay_seconds == 5

    def test_none_jitter_integration(self):
        """Test no jitter integration in wait strategy."""
        config = WaitStrategyBuilder(
            should_continue_polling=lambda x: True,
            initial_delay=timedelta(seconds=10),
            jitter_strategy=JitterStrategy.NONE,
        )
        strategy = config.build()

        result = "pending"
        decision = strategy(result, 1)
        assert decision.delay_seconds == 10


class TestWaitStrategyWithStatefulConditions:
    """Test wait strategy with stateful condition checks."""

    def test_stateful_condition_check(self):
        """Test condition check with stateful result."""

        class State:
            def __init__(self, count):
                self.count = count

        config = WaitStrategyBuilder(
            should_continue_polling=lambda s: s.count < 3, max_attempts=10
        )
        strategy = config.build()

        state1 = State(1)
        decision1 = strategy(state1, 1)
        assert decision1.should_wait is True

        state2 = State(3)
        decision2 = strategy(state2, 1)
        assert decision2.should_wait is False

    def test_complex_condition_logic(self):
        """Test complex condition logic."""

        def complex_condition(result):
            return result.get("status") == "pending" and result.get("retries", 0) < 5

        config = WaitStrategyBuilder(should_continue_polling=complex_condition)
        strategy = config.build()

        result1 = {"status": "pending", "retries": 2}
        decision1 = strategy(result1, 1)
        assert decision1.should_wait is True

        result2 = {"status": "completed", "retries": 2}
        decision2 = strategy(result2, 1)
        assert decision2.should_wait is False

        result3 = {"status": "pending", "retries": 5}
        decision3 = strategy(result3, 1)
        assert decision3.should_wait is False


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_zero_backoff_rate(self):
        """Test behavior with zero backoff rate."""
        config = WaitStrategyBuilder(
            should_continue_polling=lambda x: True,
            initial_delay=timedelta(seconds=5),
            backoff_rate=0,
            jitter_strategy=JitterStrategy.NONE,
        )
        strategy = config.build()

        result = "pending"
        decision = strategy(result, 1)
        assert decision.delay_seconds == 5

    def test_fractional_backoff_rate(self):
        """Test behavior with fractional backoff rate."""
        config = WaitStrategyBuilder(
            should_continue_polling=lambda x: True,
            initial_delay=timedelta(seconds=8),
            backoff_rate=0.5,
            jitter_strategy=JitterStrategy.NONE,
        )
        strategy = config.build()

        result = "pending"
        decision = strategy(result, 2)
        assert decision.delay_seconds == 4

    def test_large_backoff_rate(self):
        """Test behavior with large backoff rate hits max delay."""
        config = WaitStrategyBuilder(
            should_continue_polling=lambda x: True,
            initial_delay=timedelta(seconds=10),
            max_delay=timedelta(seconds=100),
            backoff_rate=10.0,
            jitter_strategy=JitterStrategy.NONE,
        )
        strategy = config.build()

        result = "pending"
        decision = strategy(result, 3)
        assert decision.delay_seconds == 100

    def test_attempt_at_boundary(self):
        """Test behavior at max_attempts boundary."""
        config = WaitStrategyBuilder(
            should_continue_polling=lambda x: True, max_attempts=3
        )
        strategy = config.build()

        result = "pending"

        decision = strategy(result, 3)
        assert decision.should_wait is False

        decision = strategy(result, 2)
        assert decision.should_wait is True

    def test_negative_delay_clamped_to_one(self):
        """Test negative delay is clamped to 1."""
        config = WaitStrategyBuilder(
            should_continue_polling=lambda x: True,
            initial_delay=timedelta(seconds=0),
            backoff_rate=0,
            jitter_strategy=JitterStrategy.NONE,
        )
        strategy = config.build()

        result = "pending"
        decision = strategy(result, 1)
        assert decision.delay_seconds == 1

    @patch("random.random")
    def test_rounding_behavior(self, mock_random):
        """Test delay rounding behavior."""
        mock_random.return_value = 0.3
        config = WaitStrategyBuilder(
            should_continue_polling=lambda x: True,
            initial_delay=timedelta(seconds=3),
            jitter_strategy=JitterStrategy.FULL,
        )
        strategy = config.build()

        result = "pending"
        decision = strategy(result, 1)
        assert decision.delay_seconds == 1


class TestWaitForConditionConfig:
    """Test WaitForConditionConfig."""

    def test_config_creation(self):
        """Test creating WaitForConditionConfig."""

        def wait_strategy(state, attempt):
            return WaitForConditionDecision.continue_waiting(timedelta(seconds=10))

        config = WaitForConditionConfig(
            wait_strategy=wait_strategy, initial_state={"count": 0}
        )

        assert config.wait_strategy is wait_strategy
        assert config.initial_state == {"count": 0}
        assert config.serdes is None

    def test_config_with_serdes(self):
        """Test WaitForConditionConfig with custom serdes."""

        def wait_strategy(state, attempt):
            return WaitForConditionDecision.stop_polling()

        serdes = JsonSerDes()
        config = WaitForConditionConfig(
            wait_strategy=wait_strategy, initial_state=0, serdes=serdes
        )

        assert config.serdes is serdes


class TestWaitStrategyCallableConditions:
    """Test wait strategy with various callable conditions."""

    def test_lambda_condition(self):
        """Test with lambda condition."""
        config = WaitStrategyBuilder(should_continue_polling=lambda x: x < 10)
        strategy = config.build()

        decision1 = strategy(5, 1)
        assert decision1.should_wait is True

        decision2 = strategy(10, 1)
        assert decision2.should_wait is False

    def test_function_condition(self):
        """Test with function condition."""

        def is_pending(status):
            return status == "pending"

        config = WaitStrategyBuilder(should_continue_polling=is_pending)
        strategy = config.build()

        decision1 = strategy("pending", 1)
        assert decision1.should_wait is True

        decision2 = strategy("completed", 1)
        assert decision2.should_wait is False

    def test_method_condition(self):
        """Test with method condition."""

        class Checker:
            def __init__(self, threshold):
                self.threshold = threshold

            def should_continue(self, value):
                return value < self.threshold

        checker = Checker(100)
        config = WaitStrategyBuilder(should_continue_polling=checker.should_continue)
        strategy = config.build()

        decision1 = strategy(50, 1)
        assert decision1.should_wait is True

        decision2 = strategy(100, 1)
        assert decision2.should_wait is False
