"""Shared adapters for SDK-owned operations implemented through the public SPI."""

from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar

from .._core import Duration, RetryStrategy
from ..extension import ExtensionStepResult, ExtensionStepRetryStrategy

T = TypeVar("T")


def adapt_retry_strategy(
    retry_strategy: Callable[[Exception, int], Duration | None] | None,
) -> ExtensionStepRetryStrategy[T]:
    """Adapt the public step retry contract to the stateful SPI contract."""
    retry = retry_strategy or RetryStrategy.default()

    def decide(
        error: Exception,
        state: T | None,
        attempt: int,
    ) -> ExtensionStepResult[T] | None:
        delay = retry(error, attempt)
        if delay is None:
            return None
        return ExtensionStepResult.retry(state, delay)

    return decide
