"""Recursively quicksort values while exercising Lambda recursion protection."""

from typing import Any

from async_durable_execution import durable_execution, get_current_context, recurse


@durable_execution
async def handler(event: dict[str, Any]) -> dict[str, Any]:
    values = [int(value) for value in event["values"]]
    recursive_level = get_current_context().recursive_level

    if len(values) <= 1:
        return {
            "sorted": values,
            "count": len(values),
            "recursive_level": recursive_level,
        }

    pivot_index = len(values) // 2
    pivot = values[pivot_index]
    rest = [*values[:pivot_index], *values[pivot_index + 1 :]]
    left = [value for value in rest if value < pivot]
    right = [value for value in rest if value >= pivot]

    sorted_left: list[int] = left
    sorted_right: list[int] = right
    max_recursive_level = recursive_level

    if len(left) > 1:
        left_result: dict[str, Any] = await recurse(
            {
                "values": left,
            },
            name=f"sort-left-{recursive_level + 1}",
            with_recursive_level=True,
        )
        sorted_left = list(left_result["sorted"])
        max_recursive_level = max(
            max_recursive_level,
            int(left_result["recursive_level"]),
        )

    if len(right) > 1:
        right_result: dict[str, Any] = await recurse(
            {
                "values": right,
            },
            name=f"sort-right-{recursive_level + 1}",
            with_recursive_level=True,
        )
        sorted_right = list(right_result["sorted"])
        max_recursive_level = max(
            max_recursive_level,
            int(right_result["recursive_level"]),
        )

    return {
        "sorted": [*sorted_left, pivot, *sorted_right],
        "count": len(values),
        "recursive_level": max_recursive_level,
    }
