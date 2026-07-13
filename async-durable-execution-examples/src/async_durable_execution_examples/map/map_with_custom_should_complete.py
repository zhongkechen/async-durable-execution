"""Example demonstrating custom map completion decisions."""

import asyncio
from typing import Any

from async_durable_execution import (
    CompletionConfig,
    CompletionDecision,
    CompletionReason,
    CompletionStatus,
    RetryStrategy,
    durable_callable,
    durable_execution,
    get_current_context,
    map,
    step,
)


@durable_execution
async def handler(event: dict[str, Any] | None) -> dict[str, Any]:
    """Query providers until enough succeed or too many fail."""
    event = event or {}
    providers = event.get(
        "providers",
        ["primary", "secondary", "slow-tertiary"],
    )
    required_successes = int(event.get("required_successes", 2))
    failure_limit = int(event.get("failure_limit", 2))
    retry_strategy = RetryStrategy(max_attempts=1)

    def should_complete(status: CompletionStatus) -> CompletionDecision:
        if status.success_count >= required_successes:
            return CompletionDecision.complete(
                CompletionReason.CUSTOM_COMPLETION_SUCCEEDED
            )
        if status.failure_count >= failure_limit:
            return CompletionDecision.complete(
                CompletionReason.CUSTOM_COMPLETION_FAILED
            )
        return CompletionDecision.continue_execution()

    async def query_provider(provider: str) -> str:
        map_context = get_current_context()

        @durable_callable
        async def run() -> str:
            return await _query_provider(provider)

        return await step(
            run(),
            name=f"query-{map_context.index}",
            retry_strategy=retry_strategy,
        )

    results = await map(
        func=query_provider,
        items=providers,
        name="custom_should_complete_map",
        max_concurrency=1,
        completion_config=CompletionConfig.custom(should_complete),
    )

    return {
        "completion_reason": results.completion_reason.value,
        "completion_succeeded": results.completion_reason.is_succeeded,
        "responses": results.get_results(),
        "success_count": results.success_count,
        "failure_count": results.failure_count,
        "started_count": results.started_count,
        "total_count": results.total_count,
    }


async def _query_provider(provider: str) -> str:
    """Query one provider."""
    if provider.startswith("bad-"):
        raise ValueError(f"Provider unavailable: {provider}")
    if provider.startswith("slow-"):
        await asyncio.sleep(2)
    return f"response:{provider}"
