"""Reproduces issue where map with minSuccessful loses failure count."""

import logging
from datetime import timedelta
from typing import Any

from async_durable_execution import (
    CompletionConfig,
    durable_callable,
    get_current_context,
    step,
    durable_execution,
    RetryStrategy,
    map,
)

logger = logging.getLogger(__name__)


@durable_execution
async def handler(_event: Any) -> dict[str, Any]:
    """Handler demonstrating map with threshold completion config."""
    # Test data: Items 2 and 4 will fail (40% failure rate)
    items = [
        {"id": 1, "shouldFail": False},
        {"id": 2, "shouldFail": True},  # Will fail
        {"id": 3, "shouldFail": False},
        {"id": 4, "shouldFail": True},  # Will fail
        {"id": 5, "shouldFail": False},
    ]

    completion_config = CompletionConfig.thresholds(
        min_successful=2,
        tolerated_failure_count=2,
    )

    logger.info("Starting map with config: min_successful=2, tolerated_failure_count=2")
    logger.info(
        f"Items pattern: {', '.join(['FAIL' if i['shouldFail'] else 'SUCCESS' for i in items])}"
    )

    async def process_item(item: dict[str, Any]) -> dict[str, Any]:
        """Process each item in the map."""
        map_context = get_current_context()
        logger.info(
            f"Processing item {item['id']} (index {map_context.index}), shouldFail: {item['shouldFail']}"
        )

        retry_strategy = RetryStrategy(
            max_attempts=2,
            initial_delay=timedelta(seconds=1),
            max_delay=timedelta(seconds=1),
        )

        @durable_callable
        async def step_function() -> dict[str, Any]:
            """Step that processes or fails based on item."""
            if item["shouldFail"]:
                raise Exception(f"Processing failed for item {item['id']}")
            return {
                "itemId": item["id"],
                "processed": True,
                "result": f"Item {item['id']} processed successfully",
            }

        return await step(
            step_function(),
            name=f"process-item-{map_context.index}",
            retry_strategy=retry_strategy,
        )

    results = await map(
        func=process_item,
        items=items,
        name="completion-config-items",
        max_concurrency=3,
        completion_config=completion_config,
    )

    logger.info("Map completed with results:")
    logger.info(f"Total items processed: {results.total_count}")
    logger.info(f"Successful items: {results.success_count}")
    logger.info(f"Failed items: {results.failure_count}")
    logger.info(f"Has failures: {results.has_failure}")
    logger.info(f"Batch status: {results.status}")
    logger.info(f"Completion reason: {results.completion_reason}")

    return {
        "totalItems": results.total_count,
        "successfulCount": results.success_count,
        "failedCount": results.failure_count,
        "hasFailures": results.has_failure,
        "batchStatus": str(results.status),
        "completionReason": str(results.completion_reason),
        "successfulItems": [
            {
                "index": item.index,
                "itemId": items[item.index]["id"],
            }
            for item in results.succeeded()
        ],
        "failedItems": [
            {
                "index": item.index,
                "itemId": items[item.index]["id"],
                "error": str(item.error),
            }
            for item in results.failed()
        ],
    }
