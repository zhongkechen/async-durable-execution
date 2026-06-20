"""Reproduces issue where map with minSuccessful loses failure count."""

import logging
from datetime import timedelta
from typing import Any

from async_durable_execution import (
    LambdaContext,
    CompletionConfig,
    MapConfig,
    StepConfig,
    durable_callable,
    step,
    durable_execution,
    RetryStrategyBuilder,
    map,
)

logger = logging.getLogger(__name__)


@durable_execution
async def handler(_event: Any, context: LambdaContext) -> dict[str, Any]:
    """Handler demonstrating map with completion config issue."""
    # Test data: Items 2 and 4 will fail (40% failure rate)
    items = [
        {"id": 1, "shouldFail": False},
        {"id": 2, "shouldFail": True},  # Will fail
        {"id": 3, "shouldFail": False},
        {"id": 4, "shouldFail": True},  # Will fail
        {"id": 5, "shouldFail": False},
    ]

    # Fixed completion config that causes the issue
    completion_config = CompletionConfig(
        min_successful=2,
        tolerated_failure_percentage=50,
    )

    logger.info(
        "Starting map with config: min_successful=2, tolerated_failure_percentage=50"
    )
    logger.info(
        f"Items pattern: {', '.join(['FAIL' if i['shouldFail'] else 'SUCCESS' for i in items])}"
    )

    async def process_item(
        item: dict[str, Any],
        index: int,
        _items: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Process each item in the map."""
        logger.info(
            f"Processing item {item['id']} (index {index}), shouldFail: {item['shouldFail']}"
        )

        retry_config = RetryStrategyBuilder(
            max_attempts=2,
            initial_delay=timedelta(seconds=1),
            max_delay=timedelta(seconds=1),
        )
        step_config = StepConfig(retry_strategy=retry_config.build())

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
            name=f"process-item-{index}",
            config=step_config,
        )

    config = MapConfig(
        max_concurrency=3,
        completion_config=completion_config,
    )

    results = await map(
        inputs=items,
        func=process_item,
        name="completion-config-items",
        config=config,
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
