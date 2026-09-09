from async_durable_execution import BatchResult


async def fail() -> str:
    raise RuntimeError("branch failed")


def summary(result: BatchResult) -> dict:
    return {
        "completionReason": result.completion_reason.value,
        "successCount": result.success_count,
        "failureCount": result.failure_count,
        "totalCount": result.total_count,
    }
