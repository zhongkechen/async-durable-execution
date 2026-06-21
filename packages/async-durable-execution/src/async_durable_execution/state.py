"""Model for execution state."""

from __future__ import annotations

import asyncio
import functools
import json
import logging
from collections import deque
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any

from .async_tools import invoke_callable
from .exceptions import (
    DurableExecutionsError,
    GetExecutionStateError,
    OrphanedChildException,
    SuspendExecution,
)
from .models import (
    ErrorObject,
    Operation,
    OperationIdentifier,
    OperationAction,
    OperationStatus,
    OperationType,
    OperationUpdate,
)
from .types import DurableServiceClient
from .plugin import PluginExecutor


if TYPE_CHECKING:
    from collections.abc import MutableMapping

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CheckpointBatcherConfig:
    """Configuration for checkpoint batching behavior.

    Attributes:
        max_batch_size_bytes: Maximum batch size in bytes (default: 750KB)
        max_batch_time_seconds: Maximum time to wait before flushing batch (default: 1.0 second)
        max_batch_operations: Maximum number of operations per batch (default: 250)
    """

    max_batch_size_bytes: int = 750 * 1024  # 750KB
    max_batch_time_seconds: float = 1.0
    max_batch_operations: int = 250


@dataclass(frozen=True)
class QueuedOperation:
    """Wrapper for operations in the checkpoint queue.

    Attributes:
        operation_update: The operation update to be checkpointed, or None for empty checkpoints
        completion_future: Completion future for synchronous operations, or None for async operations
    """

    operation_update: OperationUpdate | None
    completion_future: asyncio.Future[None] | None = None


def _completion_done(completion) -> bool:
    return completion is None or completion.done()


def _completion_set_result(completion) -> None:
    if completion is None:
        return
    completion.set_result(None)


def _completion_set_exception(completion, error: Exception) -> None:
    if completion is None:
        return
    completion.set_exception(error)


class ReplayStatus(Enum):
    """Status indicating whether execution is replaying or executing new operations."""

    REPLAY = "replay"
    NEW = "new"


class ExecutionState:
    """Get, set and maintain execution state. This is mutable. Create and check checkpoints."""

    @property
    def operations(self) -> MutableMapping[str, Operation]:
        return self._operations

    @operations.setter
    def operations(self, value: MutableMapping[str, Operation]) -> None:
        self._operations = value

    def __init__(
        self,
        durable_execution_arn: str,
        initial_checkpoint_token: str,
        service_client: DurableServiceClient,
        plugin_executor: PluginExecutor,
        batcher_config: CheckpointBatcherConfig | None = None,
    ):
        self.operations: MutableMapping[str, Operation] = {}
        self.durable_execution_arn: str = durable_execution_arn
        self._current_checkpoint_token: str = initial_checkpoint_token
        self._service_client: DurableServiceClient = service_client
        self._plugin_executor: PluginExecutor = plugin_executor

        # Checkpoint batching configuration
        self._batcher_config = batcher_config or CheckpointBatcherConfig()

        # Checkpoint batching components
        self._checkpoint_queue: asyncio.Queue[QueuedOperation | None] = asyncio.Queue()
        self._overflow_queue: deque[QueuedOperation] = deque()
        self._checkpointing_stopped = asyncio.Event()
        self._checkpointing_failed = asyncio.Event()
        self._checkpointing_failure: Exception | None = None
        self._checkpointing_task: asyncio.Task[None] | None = None

        # Concurrency management for parallel operations: parent_id -> {child_operation_ids}
        self._parent_to_children: dict[str, set[str]] = {}

        # Operations whose parent has completed
        self._parent_done: set[str] = set()

        self._replay_status: ReplayStatus = ReplayStatus.NEW
        self._visited_operations: set[str] = set()

    async def initialize(self, invocation_input):
        await self.fetch_paginated_operations(
            invocation_input.initial_execution_state.operations,
            invocation_input.checkpoint_token,
            invocation_input.initial_execution_state.next_marker,
        )

        self.mark_replaying_if_prior_operations_exist()

    async def fetch_paginated_operations(
        self,
        initial_operations: list[Operation],
        checkpoint_token: str,
        next_marker: str | None,
    ) -> list[Operation]:
        """Add initial operations and fetch all paginated operations from the Durable Functions API.

        The checkpoint_token is passed explicitly as a parameter rather than using the
        instance variable so pagination continues from the correct checkpoint state.

        Args:
            initial_operations: initial operations to be added to ExecutionState
            checkpoint_token: checkpoint token used to call Durable Functions API.
            next_marker: a marker indicates that there are paginated operations.
        Returns:
            List of all operations fetched from the Durable Functions API

        Raises:
            GetExecutionStateError: If the API call fails. The error is logged
                with structured extras before re-raising. Callers are responsible
                for deciding whether to fail the execution or allow Lambda retry
                based on is_retryable().
        """
        all_operations: list[Operation] = (
            initial_operations.copy() if initial_operations else []
        )
        try:
            while next_marker:
                output = await self._service_client.get_execution_state(
                    durable_execution_arn=self.durable_execution_arn,
                    checkpoint_token=checkpoint_token,
                    next_marker=next_marker,
                )
                all_operations.extend(output.operations)
                next_marker = output.next_marker
        except GetExecutionStateError as e:
            logger.exception(
                "Durable API error during state fetch.",
                extra=e.build_logger_extras(),
            )
            raise
        finally:
            # Always store whatever operations we successfully fetched
            if all_operations:
                self.operations.update({op.operation_id: op for op in all_operations})
        return all_operations

    def get_raw_input_payload(self) -> str | None:
        # It is possible that backend will not provide an execution operation
        # for the initial page of results.
        if not (operations := self.get_execution_operation()):
            return None
        if not (execution_details := operations.execution_details):
            return None
        return execution_details.input_payload

    def get_input_event(self):
        # Python RIC LambdaMarshaller just uses standard json deserialization for event
        # https://github.com/aws/aws-lambda-python-runtime-interface-client/blob/main/awslambdaric/lambda_runtime_marshaller.py#L46
        raw_input_payload: str | None = self.get_raw_input_payload()
        input_event: Any = {}
        if raw_input_payload and raw_input_payload.strip():
            try:
                input_event = json.loads(raw_input_payload)
            except json.JSONDecodeError:
                logger.exception(
                    "Failed to parse input payload as JSON: payload: %r",
                    raw_input_payload,
                )
                raise
        return input_event

    def get_execution_operation(self) -> Operation | None:
        # invocation id is id of execution operation
        invocation_id = self.durable_execution_arn.split("/")[-1]
        candidate = self.operations.get(invocation_id)
        if not candidate:
            # Due to payload size limitations we may have an empty operations list.
            # This will only happen when loading the initial page of results and is
            # expected behaviour. We don't fail, but instead return None
            # as the execution operation does not exist
            msg: str = "No durable operations found in execution state."
            logger.debug(msg)
            return None
        if candidate.operation_type is not OperationType.EXECUTION:
            msg = f"The execution operation in execution state does not have EXECUTION type: {candidate.operation_type}"
            raise DurableExecutionsError(msg)

        return candidate

    def track_replay(self, operation_id: str) -> None:
        """Check if operation exists with completed status; if not, transition to NEW status.

        This method is called before each operation (step, wait, invoke, etc.) to determine
        if we've reached the replay boundary. Once we encounter an operation that doesn't
        exist or isn't completed, we transition from REPLAY to NEW status, which enables
        logging for all subsequent code.

        Args:
            operation_id: The operation ID to check
        """
        if self._replay_status == ReplayStatus.REPLAY:
            self._visited_operations.add(operation_id)
            completed_ops = {
                op_id
                for op_id, op in self.operations.items()
                if op.operation_type != OperationType.EXECUTION
                and op.status
                in {
                    OperationStatus.SUCCEEDED,
                    OperationStatus.FAILED,
                    OperationStatus.CANCELLED,
                    OperationStatus.STOPPED,
                    OperationStatus.TIMED_OUT,
                }
            }
            if completed_ops.issubset(self._visited_operations):
                logger.debug(
                    "Transitioning from REPLAY to NEW status at operation %s",
                    operation_id,
                )
                self._replay_status = ReplayStatus.NEW

    def is_replaying(self) -> bool:
        """Check if execution is currently in replay mode.

        Returns:
            True if in REPLAY status, False if in NEW status
        """
        return self._replay_status is ReplayStatus.REPLAY

    def mark_replaying_if_prior_operations_exist(self) -> None:
        """Mark execution state as replaying when non-execution operations exist."""
        has_prior_operations: bool = any(
            op.operation_type is not OperationType.EXECUTION
            for op in self.operations.values()
        )

        if has_prior_operations:
            self._replay_status = ReplayStatus.REPLAY
        else:
            self._replay_status = ReplayStatus.NEW

    async def create_checkpoint(
        self,
        operation_update: OperationUpdate | None = None,
        is_sync: bool = True,  # noqa: FBT001, FBT002
    ):
        """Create a checkpoint with optional synchronous behavior.

        This method enqueues a checkpoint operation for processing by the background
        batching thread. By default, the operation is synchronous (blocking) to ensure
        the checkpoint is persisted before continuing. For performance-critical paths
        where immediate confirmation is not required, set is_sync=False.

        Synchronous checkpoints (is_sync=True, default):
        - Block the caller until the checkpoint is processed by the background thread
        - Ensure the checkpoint is persisted before continuing
        - Safe default for correctness
        - Use cases: Most operations requiring confirmation before proceeding

        Asynchronous checkpoints (is_sync=False, opt-in):
        - Return immediately without waiting for the checkpoint to complete
        - Performance optimization for specific use cases
        - Use cases: observability checkpoints, fire-and-forget operations

        When to use synchronous checkpoints (is_sync=True, default):
        1. Step START with AtMostOncePerRetry semantics - prevents duplicate execution
        2. Operation completion (SUCCEED/FAIL) - ensures state persisted before returning
        3. Retry operations - ensures retry state recorded before continuing
        4. Callback START - must wait for API to generate callback ID
        5. Invoke START - ensures chained invoke recorded before proceeding
        6. Child context results - ensures results persisted before returning
        7. Large results - ensures results saved before returning to caller
        8. Wait for condition completion - ensures state recorded before proceeding
        9. Most operations - safe default

        When to use asynchronous checkpoints (is_sync=False, opt-in):
        1. Step START with AtLeastOncePerRetry semantics - performance optimization
        2. Child context START - fire-and-forget for performance
        3. Wait for condition START - observability only, no blocking needed
        4. Any checkpoint where immediate confirmation is not required AND performance matters

        Args:
            operation_update: The checkpoint to create. If None, creates an empty
                            checkpoint to get a fresh checkpoint token and updated
                            operations list.
            is_sync: If True (default), blocks until the checkpoint is processed.
                    If False, returns immediately without blocking for performance.

        Raises:
            Any exception from checkpoint processing will propagate back to the
            awaiting coroutine, terminating the Lambda invocation.

        Examples:
            # Synchronous checkpoint (default, safe)
            execution_state.create_checkpoint(operation_update)

            # Explicit synchronous checkpoint
            execution_state.create_checkpoint(operation_update, is_sync=True)

            # Asynchronous checkpoint (opt-in for performance)
            execution_state.create_checkpoint(operation_update, is_sync=False)

            # Empty checkpoint (sync by default)
            execution_state.create_checkpoint()

            # Empty checkpoint (async for performance)
            execution_state.create_checkpoint(is_sync=False)
        """
        # if this is CONTEXT complete, mark incomplete descendants as orphans so the children can't complete after the parent
        if operation_update is not None:
            if operation_update.parent_id:
                if operation_update.parent_id not in self._parent_to_children:
                    self._parent_to_children[operation_update.parent_id] = set()
                self._parent_to_children[operation_update.parent_id].add(
                    operation_update.operation_id
                )

            if (
                operation_update.operation_type == OperationType.CONTEXT
                and operation_update.action
                in {OperationAction.SUCCEED, OperationAction.FAIL}
            ):
                self._mark_orphans(operation_update.operation_id)

            if operation_update.operation_id in self._parent_done:
                logger.debug(
                    "Rejecting checkpoint for operation %s - parent is done",
                    operation_update.operation_id,
                )
                error_msg = (
                    "Parent context completed, child operation cannot checkpoint"
                )
                raise OrphanedChildException(
                    error_msg,
                    operation_id=operation_update.operation_id,
                )

        if self._checkpointing_failure is not None:
            raise self._checkpointing_failure

        if self._checkpointing_task is None or self._checkpointing_task.done():
            self.start_checkpointing()

        completion_future: asyncio.Future[None] | None = None
        if is_sync:
            completion_future = asyncio.get_running_loop().create_future()

        # Create wrapper object for queue
        queued_op = QueuedOperation(operation_update, completion_future)

        # Enqueue the wrapper object (operation_update can be None for empty checkpoints)
        await self._checkpoint_queue.put(queued_op)

        # Conditionally wait for completion based on is_sync parameter
        if is_sync:
            logger.debug("Enqueued checkpoint operation for synchronous processing")
            if completion_future is None:  # pragma: no cover
                # this shouldn't ever be possible
                msg: str = "completion_future must be set for synchronous execution"
                raise DurableExecutionsError(msg)
            await completion_future
        else:
            logger.debug("Enqueued checkpoint operation for asynchronous processing")

    def _mark_orphans(self, context_id: str) -> None:
        """Mark all descendants (direct and transitive) as orphaned.

        This method uses BFS (Breadth-First Search) to recursively collect all
        descendants of the given context operation and marks them as orphaned.
        Once marked, these operations will be rejected if they attempt to checkpoint.

        Args:
            context_id: The operation ID of the CONTEXT that has completed
        """
        # Collect all descendants recursively using BFS
        all_descendants = set()
        # Start with root
        to_process: set[str] = {context_id}

        while to_process:
            current_id = to_process.pop()

            # Skip if already processed (avoid cycles, though shouldn't happen)
            if current_id in all_descendants:
                continue

            all_descendants.add(current_id)

            # Add all direct children to processing queue
            direct_children = self._parent_to_children.get(current_id, set())
            to_process.update(direct_children)

        # Remove the root itself (we only want descendants)
        all_descendants.discard(context_id)

        # Mark all descendants as orphaned
        self._parent_done.update(all_descendants)
        logger.debug(
            "Marked %d descendants as parent-done for context %s",
            len(all_descendants),
            context_id,
        )

    def start_checkpointing(self) -> None:
        """Start the checkpoint processor on the current event loop."""
        if self._checkpointing_task is None or self._checkpointing_task.done():
            self._checkpointing_stopped.clear()
            self._checkpointing_failed.clear()
            self._checkpointing_task = asyncio.create_task(
                self.checkpoint_batches_forever()
            )

    async def checkpoint_batches_forever(self):
        """Background coroutine that batches operations and processes results.

        Runs until shutdown is signaled. This method processes checkpoint operations
        in batches, makes API calls to persist them, and updates the execution state
        with the results.

        The method maintains the checkpoint token locally and updates it after each
        successful batch processing. It continues running until stop_checkpointing()
        is called.

        Note: When shutdown is signaled, only non-essential async checkpoints may remain
        in the queue. All critical synchronous checkpoints (SUCCEED, FAIL, etc.) will
        have already completed because the main thread blocks on them. Therefore, we
        don't need to drain the queue - the Lambda timeout will handle cleanup.

        Raises:
            Any exception from the service client checkpoint call will propagate naturally,
            terminating the background thread and signaling an error to the main thread.
        """
        await asyncio.sleep(0)

        # Keep checkpoint token as local variable in the loop
        current_checkpoint_token: str = self._current_checkpoint_token

        while not self._checkpointing_stopped.is_set():
            # Collect operations into a batch
            batch: list[QueuedOperation] = await self._collect_checkpoint_batch()

            if batch:
                # Extract OperationUpdates, excluding empty checkpoints from API call
                updates: list[OperationUpdate] = []
                empty_count = 0

                for q in batch:
                    if q.operation_update is not None:
                        updates.append(q.operation_update)
                    else:
                        empty_count += 1

                logger.debug(
                    "Sending %d OperationUpdates out of %d operations, excluding %d empty checkpoints",
                    len(updates),
                    len(batch),
                    empty_count,
                )

                try:
                    # Make API call with batched operations
                    output = await self._service_client.checkpoint(
                        durable_execution_arn=self.durable_execution_arn,
                        checkpoint_token=current_checkpoint_token,
                        updates=updates,
                        client_token=None,
                    )

                    logger.debug("Checkpoint batch processed successfully")

                    # Update local token for next iteration
                    current_checkpoint_token = output.checkpoint_token

                    # Fetch new operations from the API before unblocking sync waiters
                    updated_operations = await self.fetch_paginated_operations(
                        output.new_execution_state.operations,
                        output.checkpoint_token,
                        output.new_execution_state.next_marker,
                    )

                    for update in updates:
                        await self._plugin_executor.on_operation_action(update)

                    for operation in updated_operations:
                        await self._plugin_executor.on_operation_update(operation)

                    # Signal completion for any synchronous operations
                    for queued_op in batch:
                        if not _completion_done(queued_op.completion_future):
                            _completion_set_result(queued_op.completion_future)
                except Exception as e:
                    # Checkpoint failed - wake blocked coroutines so they can raise error
                    logger.exception("Checkpoint batch processing failed")
                    self._checkpointing_failure = e
                    self._checkpointing_failed.set()

                    # Signal completion futures for the failed batch
                    for queued_op in batch:
                        if not _completion_done(queued_op.completion_future):
                            _completion_set_exception(queued_op.completion_future, e)

                    while self._overflow_queue:
                        overflow_item = self._overflow_queue.popleft()
                        if not _completion_done(overflow_item.completion_future):
                            _completion_set_exception(
                                overflow_item.completion_future, e
                            )

                    while not self._checkpoint_queue.empty():
                        queued_item: QueuedOperation | None = (
                            self._checkpoint_queue.get_nowait()
                        )
                        if queued_item is not None and not _completion_done(
                            queued_item.completion_future
                        ):
                            _completion_set_exception(queued_item.completion_future, e)
                    break

        logger.debug("Background checkpoint processing stopped")

    def stop_checkpointing(self) -> None:
        """Signal the checkpoint processor to stop.

        This method sets the checkpointing stopped event, which signals the background
        thread to exit. Any remaining async checkpoints in the queue are non-essential
        (observability only) and will be abandoned. All critical synchronous checkpoints
        will have already completed before this is called.
        """
        logger.debug("Signaling checkpoint processor to stop")
        self._checkpointing_stopped.set()
        if self._checkpointing_task is not None and not self._checkpoint_queue.full():
            self._checkpoint_queue.put_nowait(None)

    async def _collect_checkpoint_batch(self):
        """Collect multiple checkpoint operations into a batch for API efficiency.

        Processes overflow queue first to maintain FIFO order, then collects from main queue.
        Respects configured size, time, and operation count limits. Blocks for the first
        operation if queues are empty, then collects additional operations within the time
        window.

        Empty checkpoints (operation_update=None) are coalesced: the first empty checkpoint
        counts toward the batch operation limit, but subsequent empty checkpoints do not.
        All empty checkpoints remain in the batch so their completion events are signaled.
        This avoids unnecessary batches when many concurrent map/parallel branches resume
        simultaneously and each queues an empty checkpoint.

        Returns:
            List of QueuedOperation objects ready for batch processing. Returns empty list
            if no operations are available.
        """
        batch: list[QueuedOperation] = []
        has_empty_checkpoint = False
        total_size = 0
        effective_operation_count = 0  # Operations that count toward batch limit

        # First, drain overflow queue (FIFO order preserved)
        while (
            self._overflow_queue
            and effective_operation_count < self._batcher_config.max_batch_operations
        ):
            overflow_op = self._overflow_queue.popleft()

            if overflow_op.operation_update is None:  # Empty checkpoint
                batch.append(overflow_op)
                if not has_empty_checkpoint:
                    effective_operation_count += 1
                    has_empty_checkpoint = True
            else:
                op_size = self._calculate_operation_size(overflow_op)
                if total_size + op_size > self._batcher_config.max_batch_size_bytes:
                    self._overflow_queue.appendleft(overflow_op)
                    break
                batch.append(overflow_op)
                total_size += op_size
                effective_operation_count += 1

        # If batch is empty, get first operation from main queue
        if not batch:
            while not self._checkpointing_stopped.is_set():
                try:
                    first_op = await asyncio.wait_for(
                        self._checkpoint_queue.get(), timeout=0.1
                    )
                    if first_op is None:
                        continue
                    batch.append(first_op)

                    if first_op.operation_update is None:
                        has_empty_checkpoint = True
                    else:
                        total_size += self._calculate_operation_size(first_op)

                    effective_operation_count = 1
                    break
                except asyncio.TimeoutError:
                    continue

            # If stopped and no operation retrieved, return empty batch
            if not batch:
                return batch

        # Start batching window using configured time
        batch_deadline = time.time() + self._batcher_config.max_batch_time_seconds

        # Collect additional operations within the time window. Once the batch
        # reaches the real-operation limit, keep accepting empty checkpoints so
        # concurrent resubmits can coalesce instead of spilling into a new API call.
        while time.time() < batch_deadline and not self._checkpointing_stopped.is_set():
            remaining_time = min(
                batch_deadline - time.time(),
                0.1,  # Check stop signal every 100ms
            )

            if remaining_time <= 0:
                break

            try:
                additional_op = await asyncio.wait_for(
                    self._checkpoint_queue.get(), timeout=remaining_time
                )
                if additional_op is None:
                    continue

                if additional_op.operation_update is None:  # Empty checkpoint
                    batch.append(additional_op)
                    if not has_empty_checkpoint:
                        effective_operation_count += (
                            1  # First empty counts toward limit
                        )
                        has_empty_checkpoint = True
                    # Subsequent empties don't count toward limit
                else:
                    if (
                        effective_operation_count
                        >= self._batcher_config.max_batch_operations
                    ):
                        self._overflow_queue.append(additional_op)
                        logger.debug(
                            "Batch operation limit reached, moving operation to overflow queue"
                        )
                        break

                    op_size = self._calculate_operation_size(additional_op)
                    # Check if adding this operation would exceed size limit
                    if total_size + op_size > self._batcher_config.max_batch_size_bytes:
                        # Put in overflow queue for next batch
                        self._overflow_queue.append(additional_op)
                        logger.debug(
                            "Batch size limit reached, moving operation to overflow queue"
                        )
                        break
                    batch.append(additional_op)
                    total_size += op_size
                    effective_operation_count += 1

            except asyncio.TimeoutError:
                break

        empty_count = sum(1 for q in batch if q.operation_update is None)
        logger.debug(
            "Collected batch of %d operations (%d effective, %d non-empty, %d empty), total size: %d bytes",
            len(batch),
            effective_operation_count,
            len(batch) - empty_count,
            empty_count,
            total_size,
        )
        return batch

    @staticmethod
    def _calculate_operation_size(queued_op: QueuedOperation) -> int:
        """Calculate the serialized size of a queued operation for batching limits.

        Uses JSON serialization to estimate the size of the operation update. Empty
        checkpoints (None operation_update) have zero size.

        Args:
            queued_op: The queued operation to calculate size for

        Returns:
            Size in bytes of the serialized operation, or 0 for empty checkpoints
        """
        # Empty checkpoints have no size
        if queued_op.operation_update is None:
            return 0

        # Use JSON serialization to estimate size
        serialized = json.dumps(queued_op.operation_update.to_dict()).encode("utf-8")
        return len(serialized)

    async def aclose(self) -> None:
        self.stop_checkpointing()
        if self._checkpointing_task is not None:
            await self._checkpointing_task

    def close(self):
        self.stop_checkpointing()

    def wrap_user_function(
        self,
        user_function: Callable,
        operation_identifier: OperationIdentifier,
        is_replay_children: bool = False,
        attempt: int | None = None,
    ):
        @functools.wraps(user_function)
        async def wrapper(*args, **kwargs):
            start_info = await self._plugin_executor.on_user_function_start(
                operation_identifier, is_replay_children, attempt
            )
            try:
                result = await invoke_callable(user_function, *args, **kwargs)
                await self._plugin_executor.on_user_function_end(start_info, None)
                return result
            except SuspendExecution as e:
                await self._plugin_executor.on_user_function_end(
                    start_info,
                    ErrorObject(
                        type=type(e).__name__, message=None, data=None, stack_trace=None
                    ),
                )
                raise
            except Exception as e:
                await self._plugin_executor.on_user_function_end(
                    start_info, ErrorObject.from_exception(e)
                )
                raise

        return wrapper
