"""Example demonstrating parallel with batch-level serdes."""

import json
from typing import Any

from async_durable_execution.concurrency.models import (
    BatchItem,
    BatchItemStatus,
    BatchResult,
    CompletionReason,
)
from async_durable_execution.config import ParallelConfig
from async_durable_execution.context import DurableContext
from async_durable_execution.execution import durable_execution
from async_durable_execution.models import ErrorObject
from async_durable_execution.serdes import JsonSerDes, SerDes, SerDesContext


class CustomBatchSerDes(SerDes[BatchResult]):
    """Custom serializer for the entire BatchResult."""

    def serialize(self, value: BatchResult, _: SerDesContext) -> str:
        wrapped = {
            "batch_metadata": {
                "serializer": "CustomBatchSerDes",
                "version": "2.0",
                "total_branches": len(value.get_results()),
            },
            "success_count": value.success_count,
            "failure_count": value.failure_count,
            "results": value.get_results(),
            "errors": [e.to_dict() if e else None for e in value.get_errors()],
        }
        return json.dumps(wrapped)

    def deserialize(self, payload: str, _: SerDesContext) -> BatchResult:
        wrapped = json.loads(payload)
        # Reconstruct BatchResult from wrapped data
        # Need to rebuild BatchItem list from results and errors

        batch_items = []
        results = wrapped["results"]
        errors = wrapped["errors"]

        for i, result in enumerate(results):
            error = errors[i] if i < len(errors) else None
            if error:
                batch_items.append(
                    BatchItem(
                        index=i,
                        status=BatchItemStatus.FAILED,
                        result=None,
                        error=ErrorObject.from_dict(error) if error else None,
                    )
                )
            else:
                batch_items.append(
                    BatchItem(
                        index=i,
                        status=BatchItemStatus.SUCCEEDED,
                        result=result,
                        error=None,
                    )
                )

        # Infer completion reason (assume ALL_COMPLETED if all succeeded)
        completion_reason = (
            CompletionReason.ALL_COMPLETED
            if wrapped["failure_count"] == 0
            else CompletionReason.FAILURE_TOLERANCE_EXCEEDED
        )

        return BatchResult(all=batch_items, completion_reason=completion_reason)


@durable_execution
async def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    """Execute parallel tasks with custom batch-level serialization."""

    # Use custom serdes for the entire BatchResult, default JSON for individual functions
    config = ParallelConfig(serdes=CustomBatchSerDes(), item_serdes=JsonSerDes())

    async def branch1(ctx: DurableContext) -> int:
        async def run() -> int:
            return 100

        return await ctx.step(run, name="branch1")

    async def branch2(ctx: DurableContext) -> int:
        async def run() -> int:
            return 200

        return await ctx.step(run, name="branch2")

    async def branch3(ctx: DurableContext) -> int:
        async def run() -> int:
            return 300

        return await ctx.step(run, name="branch3")

    results = await context.parallel(
        functions=[branch1, branch2, branch3],
        name="parallel_with_batch_serdes",
        config=config,
    )

    return {
        "success_count": results.success_count,
        "results": results.get_results(),
        "total": sum(results.get_results()),
    }
