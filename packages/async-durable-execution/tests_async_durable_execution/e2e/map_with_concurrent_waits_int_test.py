"""Integration test: empty checkpoint coalescing with concurrent map + wait.

Python equivalent of the Java MapWithConditionAndCallbackExample referenced in
issue #325. Verifies that when many concurrent map branches resume from timed
wait operations simultaneously, the empty checkpoints produced by the
resubmitter (executor.py) are coalesced into minimal API calls instead of
being split across multiple batches.

Background
----------
When a map branch suspends via TimedSuspendExecution and later resumes, the
ConcurrentExecutor resubmitter calls::

    execution_state.create_checkpoint()  # empty checkpoint

before resubmitting the branch. In high-concurrency scenarios (300+ branches)
all resuming at the same time, 300+ empty checkpoints flood the checkpoint
queue.

Without the coalescing optimization (issue #325), the 250-operation batch limit
causes these to be split across multiple batches → multiple API calls.
With the optimization, all subsequent empty checkpoints beyond the first do
NOT count toward the batch limit, so they are coalesced into a single batch
and a single API call.

These tests directly simulate that concurrent-checkpoint pattern by launching
many threads that each call ``create_checkpoint()`` simultaneously, mirroring
what the map resubmitter does when all branches resume at once.
"""

from __future__ import annotations

import asyncio

from unittest.mock import Mock

from async_durable_execution.lambda_service import (
    CheckpointOutput,
    CheckpointUpdatedExecutionState,
    LambdaClient,
    OperationAction,
    OperationType,
    OperationUpdate,
)
from async_durable_execution.plugin import PluginExecutor
from async_durable_execution.state import (
    CheckpointBatcherConfig,
    ExecutionState,
    QueuedOperation,
)


def _make_state(
    mock_client: Mock,
    batch_time: float = 5.0,
    max_ops: int = 250,
) -> ExecutionState:
    config = CheckpointBatcherConfig(
        max_batch_size_bytes=10 * 1024 * 1024,
        max_batch_time_seconds=batch_time,
        max_batch_operations=max_ops,
    )
    return ExecutionState(
        durable_execution_arn="test-arn",
        initial_checkpoint_token="token-0",  # noqa: S106
        operations={},
        service_client=mock_client,
        batcher_config=config,
        plugin_executor=PluginExecutor([]),
    )


def _make_tracking_client() -> tuple[Mock, list]:
    """Return a (mock LambdaClient, checkpoint_calls list) pair."""
    calls: list[list] = []
    mock_client = Mock(spec=LambdaClient)

    def _checkpoint(
        durable_execution_arn, checkpoint_token, updates, client_token=None
    ):
        calls.append(list(updates))
        return CheckpointOutput(
            checkpoint_token=f"token_{len(calls)}",
            new_execution_state=CheckpointUpdatedExecutionState(),
        )

    mock_client.checkpoint = _checkpoint
    return mock_client, calls


async def _drain_checkpoint_burst(
    state: ExecutionState,
    queued_operations: list[QueuedOperation],
) -> None:
    for queued_operation in queued_operations:
        await state._checkpoint_queue.put(queued_operation)

    state.start_checkpointing()
    await asyncio.gather(
        *(
            queued_operation.completion_future
            for queued_operation in queued_operations
            if queued_operation.completion_future is not None
        )
    )


def test_map_with_concurrent_waits_coalesces_empty_checkpoints():
    """300 concurrent empty checkpoints are drained as one coalesced API call.

    The test pre-queues the full burst before starting the batcher so it verifies
    the checkpoint coalescing rules directly instead of depending on scheduler
    timing differences between Python versions.
    """
    mock_client, calls = _make_tracking_client()
    state = _make_state(mock_client, batch_time=5.0, max_ops=250)

    async def run_test():
        batcher_task: asyncio.Task[None] | None = None
        branch_count = 300
        completion_futures = [
            asyncio.get_running_loop().create_future() for _ in range(branch_count)
        ]
        queued_operations = [
            QueuedOperation(None, completion_future)
            for completion_future in completion_futures
        ]
        try:
            await _drain_checkpoint_burst(state, queued_operations)
            batcher_task = state._checkpointing_task

            assert len(calls) == 1, (
                f"Expected 1 coalesced API call for {branch_count} concurrent empty "
                f"checkpoints, got {len(calls)}. The 250-op limit must not split empties."
            )
            assert calls[0] == [], (
                "Empty checkpoints should produce an empty updates list"
            )
        finally:
            state.stop_checkpointing()
            if batcher_task is not None:
                await batcher_task

    asyncio.run(run_test())


def test_map_with_concurrent_waits_api_call_count_scales_with_real_ops_not_empties():
    """400 empty checkpoints + 10 real ops → 1 API call with limit=11.

    Demonstrates that the effective batch count is driven by real operations
    (and only the *first* empty), not the total number of empties.

    With limit=11: the first empty counts as effective_op 1, and each of the
    10 real ops increments the count (effective_ops 2–11). The limit is hit
    exactly when the last real op is collected. All 399 remaining empties are
    coalesced in without incrementing the count.

    Result: 1 batch (410 operations, 10 real) → 1 API call.
    """
    mock_client, calls = _make_tracking_client()
    # limit = 1 (first empty) + 10 (real ops) = 11, so all fit in one batch
    state = _make_state(mock_client, batch_time=5.0, max_ops=11)

    async def run_test():
        batcher_task: asyncio.Task[None] | None = None
        try:
            completion_futures = [
                asyncio.get_running_loop().create_future() for _ in range(410)
            ]
            queued_operations = [
                QueuedOperation(None, completion_futures[i]) for i in range(400)
            ]
            queued_operations.extend(
                QueuedOperation(
                    OperationUpdate(
                        operation_id=f"op_{i}",
                        operation_type=OperationType.STEP,
                        action=OperationAction.START,
                    ),
                    completion_futures[400 + i],
                )
                for i in range(10)
            )

            await _drain_checkpoint_burst(state, queued_operations)
            batcher_task = state._checkpointing_task

            # 1 empty (effective=1) + 10 real ops (effective=11) exhaust the batch
            # limit exactly. The 399 remaining empties coalesce in -> still 1 API call.
            assert len(calls) == 1, (
                f"Expected 1 API call with 400 empty + 10 real ops (limit=11), "
                f"got {len(calls)}."
            )
            # Only the 10 real ops appear in the updates list; empties are excluded.
            real_op_ids = {u.operation_id for batch in calls for u in batch}
            assert real_op_ids == {f"op_{i}" for i in range(10)}
        finally:
            state.stop_checkpointing()
            if batcher_task is not None:
                await batcher_task

    asyncio.run(run_test())
