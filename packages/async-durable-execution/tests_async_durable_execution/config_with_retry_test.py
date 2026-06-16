"""Tests for with_retry configuration."""

from unittest.mock import MagicMock

from async_durable_execution.config import WithRetryConfig


def test_with_retry_config_defaults():
    """WithRetryConfig defaults align with the public API."""
    config = WithRetryConfig()

    assert config.retry_strategy is None
    assert config.wrap_with_run_in_child_context is True
    assert config.child_context_config is None


def test_with_retry_config_custom_values():
    """WithRetryConfig stores explicit values."""
    retry_strategy = MagicMock()
    child_context_config = MagicMock()

    config = WithRetryConfig(
        retry_strategy=retry_strategy,
        wrap_with_run_in_child_context=False,
        child_context_config=child_context_config,
    )

    assert config.retry_strategy is retry_strategy
    assert config.wrap_with_run_in_child_context is False
    assert config.child_context_config is child_context_config


def test_with_retry_config_importable_from_package():
    """WithRetryConfig is re-exported from the package root."""
    from async_durable_execution import WithRetryConfig as ImportedConfig

    assert ImportedConfig is WithRetryConfig
