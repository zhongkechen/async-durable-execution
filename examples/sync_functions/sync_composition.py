"""Example using synchronous functions with map and parallel."""

from typing import Any

from async_durable_execution import (
    durable_execution,
    get_map_item_context,
    map,
    parallel,
)


@durable_execution
async def handler(event: dict[str, Any]) -> dict[str, list[str]]:
    """Compose synchronous map items and parallel branches."""
    items = [str(item) for item in event.get("items", ["alpha", "beta"])]

    def normalize_item(item: str) -> str:
        item_context = get_map_item_context()
        return f"{item_context.index}:{item.strip().upper()}"

    normalized = (
        await map(
            func=normalize_item,
            items=items,
            name="normalize-items",
            max_concurrency=2,
        )
    ).get_results()

    def summarize_count() -> str:
        return f"{len(normalized)} items"

    def select_first() -> str:
        return normalized[0] if normalized else "none"

    summaries = (
        await parallel(
            branches=[summarize_count, select_first],
            name="summarize-items",
        )
    ).get_results()
    return {"items": normalized, "summaries": summaries}
