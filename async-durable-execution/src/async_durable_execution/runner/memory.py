"""In-memory execution store implementation."""

from __future__ import annotations

from datetime import timezone
from threading import Lock
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from async_durable_execution.models import Operation

    from .execution import Execution


class InMemoryExecutionStore:
    """Dict-based storage for testing."""

    def __init__(self) -> None:
        self._store: dict[str, Execution] = {}
        self._lock: Lock = Lock()

    def save(self, execution: Execution) -> None:
        with self._lock:
            self._store[execution.durable_execution_arn] = execution

    def load(self, execution_arn: str) -> Execution:
        with self._lock:
            return self._store[execution_arn]

    def update(self, execution: Execution) -> None:
        with self._lock:
            self._store[execution.durable_execution_arn] = execution

    def list_all(self) -> list[Execution]:
        with self._lock:
            return list(self._store.values())

    def query(
        self,
        function_name: str | None = None,
        execution_name: str | None = None,
        status_filter: str | None = None,
        started_after: str | None = None,
        started_before: str | None = None,
        limit: int | None = None,
        offset: int = 0,
        reverse_order: bool = False,  # noqa: FBT001, FBT002
    ) -> tuple[list[Execution], str | None]:
        """Apply filtering, sorting, and pagination to executions."""
        executions: list[Execution] = self.list_all()
        return self.process_query(
            executions,
            function_name=function_name,
            execution_name=execution_name,
            status_filter=status_filter,
            started_after=started_after,
            started_before=started_before,
            limit=limit,
            offset=offset,
            reverse_order=reverse_order,
        )

    @staticmethod
    def process_query(
        executions: list[Execution],
        function_name: str | None = None,
        execution_name: str | None = None,
        status_filter: str | None = None,
        started_after: str | None = None,
        started_before: str | None = None,
        limit: int | None = None,
        offset: int = 0,
        reverse_order: bool = False,  # noqa: FBT001, FBT002
    ) -> tuple[list[Execution], str | None]:
        """Apply filtering, sorting, and pagination to executions."""
        filtered: list[Execution] = []
        for execution in executions:
            if function_name and execution.start_input.function_name != function_name:
                continue
            if (
                execution_name
                and execution.start_input.execution_name != execution_name
            ):
                continue

            if status_filter and execution.current_status().value != status_filter:
                continue

            if started_after or started_before:
                try:
                    operation: Operation = execution.get_operation_execution_started()
                    if operation.start_timestamp:
                        timestamp: float = (
                            operation.start_timestamp.timestamp()
                            if hasattr(operation.start_timestamp, "timestamp")
                            else operation.start_timestamp.replace(
                                tzinfo=timezone.utc
                            ).timestamp()
                        )
                        if started_after and timestamp < float(started_after):
                            continue
                        if started_before and timestamp > float(started_before):
                            continue
                except (ValueError, AttributeError):
                    continue

            filtered.append(execution)

        def get_sort_key(exe: Execution):
            try:
                op: Operation = exe.get_operation_execution_started()
                if op.start_timestamp:
                    return (
                        op.start_timestamp.timestamp()
                        if hasattr(op.start_timestamp, "timestamp")
                        else op.start_timestamp.replace(tzinfo=timezone.utc).timestamp()
                    )
            except Exception:  # noqa: BLE001, S110
                pass
            return 0

        filtered.sort(key=get_sort_key, reverse=reverse_order)

        if limit is not None and limit > 0:
            end_idx: int = offset + limit
            paginated: list[Execution] = filtered[offset:end_idx]
            has_more: bool = end_idx < len(filtered)
            next_marker: str | None = str(end_idx) if has_more else None
            return paginated, next_marker
        return filtered[offset:], None
