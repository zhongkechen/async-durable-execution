from async_durable_execution import BatchResult


async def fail_on(item: str, failed_value: str = "fail") -> str:
    if item == failed_value:
        raise RuntimeError("item failed")
    return item


def summary(result: BatchResult) -> dict:
    return {
        "completionReason": result.completion_reason.value,
        "successCount": result.success_count,
        "failureCount": result.failure_count,
        "totalCount": result.total_count,
    }
