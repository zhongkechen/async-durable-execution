"""Execution life-cycle logic."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from ...core.execution import (
    DurableExecutionInvocationInput,
    DurableExecutionInvocationOutput,
    InvocationStatus,
)
from ...core.models import (
    CallbackOptions,
    CallbackTimeoutType,
    CheckpointUpdatedExecutionState,
    ErrorObject,
    Operation,
    OperationStatus,
    OperationType,
    OperationUpdate,
)
from ..exceptions import (
    IllegalStateException,
    InvalidParameterValueException,
    ResourceNotFoundException,
)
from ..model import (
    TERMINAL_STATUSES,
    EventCreationContext,
    GetDurableExecutionHistoryResponse,
)
from ..model import (
    Event as HistoryEvent,
)
from .model import (
    CallbackToken,
    CheckpointDurableExecutionResponse,
    GetDurableExecutionStateResponse,
    Invoker,
    SendDurableExecutionCallbackFailureResponse,
    SendDurableExecutionCallbackHeartbeatResponse,
    SendDurableExecutionCallbackSuccessResponse,
    StartDurableExecutionInput,
    StartDurableExecutionOutput,
)
from .time_scale import scale_delay
from .execution import Execution

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from asyncio import Future

    from . import InMemoryServiceClient
    from .scheduler import Event, Scheduler

logger = logging.getLogger(__name__)

_CALLBACK_TIMEOUT_MINIMUM_DELAY_SECONDS = 5.0


class Executor:
    MAX_CONSECUTIVE_FAILED_ATTEMPTS: int = 5
    RETRY_BACKOFF_SECONDS: int = 5

    def __init__(
        self,
        scheduler: Scheduler,
        invoker: Invoker,
        service_client: InMemoryServiceClient,
    ):
        self._scheduler = scheduler
        self._invoker = invoker
        self._service_client = service_client
        self._execution: Execution | None = None
        self._execution_arn: str | None = None
        self._completion_event: Event | None = None
        self._callback_timeouts: dict[str, Future] = {}
        self._callback_heartbeats: dict[str, Future] = {}
        self._execution_timeout: Future | None = None
        self._active_invocation: bool = False
        self._scheduled_resume: bool = False
        self._pending_resume: bool = False
        self._deferred_wait_resumes: set[str] = set()
        self._deferred_retry_resumes: set[str] = set()
        self._deferred_callback_timeouts: set[tuple[str, CallbackTimeoutType]] = set()

    def start_execution(
        self,
        input: StartDurableExecutionInput,  # noqa: A002
    ) -> StartDurableExecutionOutput:
        self._ensure_can_start_execution()

        # Generate invocation_id if not provided
        if input.invocation_id is None:
            input = StartDurableExecutionInput(
                account_id=input.account_id,
                function_name=input.function_name,
                function_qualifier=input.function_qualifier,
                execution_name=input.execution_name,
                execution_timeout_seconds=input.execution_timeout_seconds,
                execution_retention_period_days=input.execution_retention_period_days,
                invocation_id=str(uuid.uuid4()),
                trace_fields=input.trace_fields,
                tenant_id=input.tenant_id,
                input=input.input,
                lambda_endpoint=input.lambda_endpoint,
            )

        execution = Execution.new(input=input)
        execution.start()
        self._save_execution(execution)
        logger.debug("Created execution with ARN: %s", execution.durable_execution_arn)

        completion_event = self._scheduler.create_event()
        self._execution_arn = execution.durable_execution_arn
        self._completion_event = completion_event
        self._execution_timeout = None
        self._active_invocation = False
        self._scheduled_resume = False
        self._pending_resume = False
        self._deferred_wait_resumes.clear()
        self._deferred_retry_resumes.clear()
        self._deferred_callback_timeouts.clear()

        # Schedule execution timeout
        if input.execution_timeout_seconds > 0:

            async def timeout_handler() -> None:
                error = ErrorObject.from_message(
                    f"Execution timed out after {input.execution_timeout_seconds} seconds."
                )
                self.timeout_execution(execution.durable_execution_arn, error)

            self._execution_timeout = self._scheduler.call_later(
                timeout_handler,
                delay=input.execution_timeout_seconds,
                completion_event=completion_event,
            )

        # Schedule initial invocation to run immediately
        self._invoke_execution(execution.durable_execution_arn)

        return StartDurableExecutionOutput(
            execution_arn=execution.durable_execution_arn
        )

    def _ensure_can_start_execution(self) -> None:
        """Reject overlapping executions in the local runner."""
        if self._execution is None:
            return

        if not self._execution.is_complete:
            msg = "Local runner supports only one execution at a time."
            raise IllegalStateException(msg)

    def _save_execution(self, execution: Execution) -> None:
        self._execution = execution
        self._execution_arn = execution.durable_execution_arn

    def _update_execution(self, execution: Execution) -> None:
        self._execution = execution
        self._execution_arn = execution.durable_execution_arn

    def set_execution(self, execution: Execution) -> None:
        """Replace the current local execution object."""
        self._update_execution(execution)

    def _validate_current_execution(self, execution_arn: str) -> None:
        if self._execution_arn is None:
            return

        if self._execution_arn != execution_arn:
            msg = f"Execution {execution_arn} is not the active local execution."
            raise ResourceNotFoundException(msg)

    def get_execution(self, execution_arn: str) -> Execution:
        """Get execution by ARN.

        Args:
            execution_arn: The execution ARN to retrieve

        Returns:
            Execution: The execution object

        Raises:
            ResourceNotFoundException: If execution does not exist
        """
        if (
            self._execution is None
            or self._execution.durable_execution_arn != execution_arn
        ):
            msg: str = f"Execution {execution_arn} not found"
            raise ResourceNotFoundException(msg)
        return self._execution

    def get_execution_state(
        self,
        execution_arn: str,
        checkpoint_token: str | None = None,
        marker: str | None = None,
        max_items: int | None = None,
    ) -> GetDurableExecutionStateResponse:
        """Get execution state with operations.

        Args:
            execution_arn: The execution ARN
            checkpoint_token: Checkpoint token for state consistency
            marker: Pagination marker
            max_items: Maximum items to return

        Returns:
            GetDurableExecutionStateResponse: Execution state with operations

        Raises:
            ResourceNotFoundException: If execution does not exist
            InvalidParameterValueException: If checkpoint token is invalid
        """
        execution = self.get_execution(execution_arn)

        # TODO: Validate checkpoint token if provided
        if checkpoint_token and checkpoint_token not in execution.used_tokens:
            msg: str = f"Invalid checkpoint token: {checkpoint_token}"
            raise InvalidParameterValueException(msg)

        # Get operations (excluding the initial EXECUTION operation for state)
        operations = execution.get_assertable_operations()

        # Apply pagination
        if max_items is None:
            max_items = 100

        # Simple pagination - in real implementation would need proper marker handling
        start_index = 0
        if marker:
            try:
                start_index = int(marker)
            except ValueError:
                start_index = 0

        end_index = start_index + max_items
        paginated_operations = operations[start_index:end_index]

        next_marker = None
        if end_index < len(operations):
            next_marker = str(end_index)

        return GetDurableExecutionStateResponse(
            operations=paginated_operations, next_marker=next_marker
        )

    def get_execution_history(
        self,
        execution_arn: str,
        include_execution_data: bool = False,  # noqa: FBT001, FBT002
        reverse_order: bool = False,  # noqa: FBT001, FBT002
        marker: str | None = None,
        max_items: int | None = None,
    ) -> GetDurableExecutionHistoryResponse:
        """Get execution history with events.

        Args:
            execution_arn: The execution ARN
            include_execution_data: Whether to include execution data in events
            reverse_order: Return events in reverse chronological order
            marker: Pagination marker (event_id)
            max_items: Maximum items to return

        Returns:
            GetDurableExecutionHistoryResponse: Execution history with events

        Raises:
            ResourceNotFoundException: If execution does not exist
        """
        execution: Execution = self.get_execution(execution_arn)

        # Generate events
        all_events: list[HistoryEvent] = []
        ops: list[Operation] = execution.operations
        updates: list[OperationUpdate] = execution.updates
        updates_dict: dict[str, OperationUpdate] = {u.operation_id: u for u in updates}
        durable_execution_arn: str = execution.durable_execution_arn

        # Add InvocationCompleted events
        for completion in execution.invocation_completions:
            invocation_event = HistoryEvent.create_invocation_completed(
                event_id=0,  # Temporary, will be reassigned
                event_timestamp=completion.end_timestamp,
                start_timestamp=completion.start_timestamp,
                end_timestamp=completion.end_timestamp,
                request_id=completion.request_id,
            )
            all_events.append(invocation_event)

        # Generate all events first (without final event IDs)
        for op in ops:
            operation_update: OperationUpdate | None = updates_dict.get(op.operation_id)

            if op.status is OperationStatus.PENDING:
                if (
                    op.operation_type is not OperationType.CHAINED_INVOKE
                    or op.start_timestamp is None
                ):
                    continue
                context: EventCreationContext = EventCreationContext(
                    op,
                    0,  # Temporary event_id, will be reassigned after sorting
                    durable_execution_arn,
                    execution.start_input,
                    execution.result,
                    operation_update,
                    include_execution_data,
                )
                pending = HistoryEvent.create_chained_invoke_event_pending(context)
                all_events.append(pending)
            if op.start_timestamp is not None:
                context = EventCreationContext(
                    op,
                    0,  # Temporary event_id, will be reassigned after sorting
                    durable_execution_arn,
                    execution.start_input,
                    execution.result,
                    operation_update,
                    include_execution_data,
                )
                started = HistoryEvent.create_event_started(context)
                all_events.append(started)
            if op.end_timestamp is not None and op.status in TERMINAL_STATUSES:
                context = EventCreationContext(
                    op,
                    0,  # Temporary event_id, will be reassigned after sorting
                    durable_execution_arn,
                    execution.start_input,
                    execution.result,
                    operation_update,
                    include_execution_data,
                )
                finished = HistoryEvent.create_event_terminated(context)
                all_events.append(finished)

        # Sort events by timestamp to get correct chronological order
        all_events.sort(key=lambda event: event.event_timestamp)

        # Reassign event IDs based on chronological order
        all_events = [
            HistoryEvent.from_event_with_id(event, i)
            for i, event in enumerate(all_events, 1)
        ]

        # Apply cursor-based pagination
        if max_items is None:
            max_items = 100

        # Handle pagination marker
        if reverse_order:
            all_events.reverse()
        start_index: int = 0
        if marker:
            try:
                marker_event_id: int = int(marker)
                # Find the index of the first event with event_id >= marker
                start_index = len(all_events)
                for i, e in enumerate(all_events):
                    is_valid_page_start: bool = (
                        e.event_id < marker_event_id
                        if reverse_order
                        else e.event_id >= marker_event_id
                    )
                    if is_valid_page_start:
                        start_index = i
                        break
            except ValueError:
                start_index = 0

        # Get paginated events
        end_index: int = start_index + max_items
        paginated_events: list[HistoryEvent] = all_events[start_index:end_index]

        # Generate next marker
        next_marker: str | None = None
        if end_index < len(all_events):
            if reverse_order:
                # Next marker is the event_id of the last returned event
                next_marker = (
                    str(paginated_events[-1].event_id) if paginated_events else None
                )
            else:
                # Next marker is the event_id of the next event after the last returned
                next_marker = (
                    str(all_events[end_index].event_id)
                    if end_index < len(all_events)
                    else None
                )

        return GetDurableExecutionHistoryResponse(
            events=paginated_events, next_marker=next_marker
        )

    def checkpoint_execution(
        self,
        execution_arn: str,
        checkpoint_token: str,
        updates: list[OperationUpdate] | None = None,
        client_token: str | None = None,
    ) -> CheckpointDurableExecutionResponse:
        """Process checkpoint for an execution.

        Args:
            execution_arn: The execution ARN
            checkpoint_token: Current checkpoint token
            updates: List of operation updates to process
            client_token: Client token for idempotency

        Returns:
            CheckpointDurableExecutionResponse: Updated checkpoint token and state

        Raises:
            ResourceNotFoundException: If execution does not exist
            InvalidParameterValueException: If checkpoint token is invalid
        """
        execution = self.get_execution(execution_arn)

        # Validate checkpoint token
        if checkpoint_token not in execution.used_tokens:
            msg: str = f"Invalid checkpoint token: {checkpoint_token}"
            raise InvalidParameterValueException(msg)

        if updates:
            checkpoint_output = self._service_client.process_checkpoint(
                checkpoint_token=checkpoint_token,
                updates=updates,
                client_token=client_token,
            )

            new_execution_state = None
            if checkpoint_output.new_execution_state:
                new_execution_state = CheckpointUpdatedExecutionState(
                    operations=checkpoint_output.new_execution_state.operations,
                    next_marker=checkpoint_output.new_execution_state.next_marker,
                )

            return CheckpointDurableExecutionResponse(
                checkpoint_token=checkpoint_output.checkpoint_token,
                new_execution_state=new_execution_state,
            )

        # Save execution state after generating new token
        new_checkpoint_token = execution.get_new_checkpoint_token()
        self._update_execution(execution)

        return CheckpointDurableExecutionResponse(
            checkpoint_token=new_checkpoint_token,
            new_execution_state=None,
        )

    def send_callback_success(
        self,
        callback_id: str,
        result: bytes | None = None,
    ) -> SendDurableExecutionCallbackSuccessResponse:
        """Send callback success response.

        Args:
            callback_id: The callback ID to respond to
            result: Optional result data for the callback

        Returns:
            SendDurableExecutionCallbackSuccessResponse: Empty response

        Raises:
            InvalidParameterValueException: If callback_id is invalid
            ResourceNotFoundException: If callback does not exist
        """
        if not callback_id:
            msg: str = "callback_id is required"
            raise InvalidParameterValueException(msg)

        try:
            callback_token = CallbackToken.from_str(callback_id)
            execution = self.get_execution(callback_token.execution_arn)
            execution.complete_callback_success(callback_id, result)
            self._update_execution(execution)
            self._cleanup_callback_timeouts(callback_id)
            self._schedule_resume(callback_token.execution_arn)
            logger.info("Callback success completed for callback_id: %s", callback_id)
        except Exception as e:
            msg = f"Failed to process callback success: {e}"
            raise ResourceNotFoundException(msg) from e

        return SendDurableExecutionCallbackSuccessResponse()

    def send_callback_failure(
        self,
        callback_id: str,
        error: ErrorObject | None = None,
    ) -> SendDurableExecutionCallbackFailureResponse:
        """Send callback failure response.

        Args:
            callback_id: The callback ID to respond to
            error: Optional error object for the callback failure

        Returns:
            SendDurableExecutionCallbackFailureResponse: Empty response

        Raises:
            InvalidParameterValueException: If callback_id is invalid
            ResourceNotFoundException: If callback does not exist
        """
        if not callback_id:
            msg: str = "callback_id is required"
            raise InvalidParameterValueException(msg)

        callback_error: ErrorObject = error or ErrorObject.from_message("")

        try:
            callback_token: CallbackToken = CallbackToken.from_str(callback_id)
            execution: Execution = self.get_execution(callback_token.execution_arn)
            execution.complete_callback_failure(callback_id, callback_error)
            self._update_execution(execution)
            self._cleanup_callback_timeouts(callback_id)
            self._schedule_resume(callback_token.execution_arn)
            logger.info("Callback failure completed for callback_id: %s", callback_id)
        except Exception as e:
            msg = f"Failed to process callback failure: {e}"
            raise ResourceNotFoundException(msg) from e

        return SendDurableExecutionCallbackFailureResponse()

    def send_callback_heartbeat(
        self, callback_id: str
    ) -> SendDurableExecutionCallbackHeartbeatResponse:
        """Send callback heartbeat to keep callback alive.

        Args:
            callback_id: The callback ID to send heartbeat for

        Returns:
            SendDurableExecutionCallbackHeartbeatResponse: Empty response

        Raises:
            InvalidParameterValueException: If callback_id is invalid
            ResourceNotFoundException: If callback does not exist
        """
        if not callback_id:
            msg: str = "callback_id is required"
            raise InvalidParameterValueException(msg)

        try:
            callback_token: CallbackToken = CallbackToken.from_str(callback_id)
            execution: Execution = self.get_execution(callback_token.execution_arn)

            # Find callback operation to verify it exists and is active
            _, operation = execution.find_callback_operation(callback_id)
            if operation.status != OperationStatus.STARTED:
                msg = f"Callback {callback_id} is not active"
                raise ResourceNotFoundException(msg)

            # Reset heartbeat timeout if configured
            self._reset_callback_heartbeat_timeout(
                callback_id, execution.durable_execution_arn
            )
            logger.info("Callback heartbeat processed for callback_id: %s", callback_id)
        except Exception as e:
            msg = f"Failed to process callback heartbeat: {e}"
            raise ResourceNotFoundException(msg) from e

        return SendDurableExecutionCallbackHeartbeatResponse()

    def _validate_invocation_response(
        self,
        execution_arn: str,
        response: DurableExecutionInvocationOutput,
        execution: Execution,
    ):
        """Validate response status and apply the resulting execution changes.

        Raises:
            InvalidParameterValueException: If the response status is invalid.
            IllegalStateException: If the response status is valid but the execution is already completed.
        """
        if execution.is_complete:
            msg_already_complete: str = "Execution already completed, ignoring result"

            raise IllegalStateException(msg_already_complete)

        if response.status is None:
            msg_status_required: str = "Response status is required"

            raise InvalidParameterValueException(msg_status_required)

        match response.status:
            case InvocationStatus.FAILED:
                if response.result is not None:
                    msg_failed_result: str = (
                        "Cannot provide a Result for FAILED status."
                    )
                    raise InvalidParameterValueException(msg_failed_result)
                logger.info("[%s] Execution failed", execution_arn)
                self._complete_workflow(
                    execution_arn, result=None, error=response.error
                )

            case InvocationStatus.SUCCEEDED:
                if response.error is not None:
                    msg_success_error: str = (
                        "Cannot provide an Error for SUCCEEDED status."
                    )
                    raise InvalidParameterValueException(msg_success_error)
                logger.info("[%s] Execution succeeded", execution_arn)
                self._complete_workflow(
                    execution_arn, result=response.result, error=None
                )

            case InvocationStatus.PENDING:
                if not execution.has_pending_operations():
                    msg_pending_ops: str = (
                        "Cannot return PENDING status with no pending operations."
                    )
                    raise InvalidParameterValueException(msg_pending_ops)
                logger.info("[%s] Execution pending async work", execution_arn)

            case _:
                msg_unexpected_status: str = (
                    f"Unexpected invocation status: {response.status}"
                )
                raise IllegalStateException(msg_unexpected_status)

    def _invoke_handler(self, execution_arn: str) -> Callable[[], Awaitable[None]]:
        """Create a parameterless callable that captures execution arn for the scheduler."""

        async def invoke() -> None:
            self._mark_invocation_started(execution_arn)
            execution: Execution = self.get_execution(execution_arn)

            # Early exit if execution is already completed - like Java's COMPLETED check
            if execution.is_complete:
                logger.info(
                    "[%s] Execution already completed, ignoring result", execution_arn
                )
                return

            try:
                checkpoint_token = execution.get_new_checkpoint_token()
                invocation_input: DurableExecutionInvocationInput = (
                    self._invoker.create_invocation_input(
                        start_input=execution.start_input,
                        durable_execution_arn=execution.durable_execution_arn,
                        checkpoint_token=checkpoint_token,
                        operations=execution.operations,
                    )
                )

                self._save_execution(execution)

                invocation_start = datetime.now(timezone.utc)
                invoke_response = await self._invoker.invoke(
                    execution.start_input.function_name,
                    invocation_input,
                    execution.start_input.lambda_endpoint,
                )
                invocation_end = datetime.now(timezone.utc)

                # Reload execution after invocation in case it was completed via checkpoint
                execution = self.get_execution(execution_arn)

                # Record invocation completion and save immediately
                execution.record_invocation_completion(
                    invocation_start, invocation_end, invoke_response.request_id
                )
                self._save_execution(execution)

                if execution.is_complete:
                    logger.info(
                        "[%s] Execution completed during invocation, ignoring result",
                        execution_arn,
                    )
                    return

                # Process successful received response - validate status and handle accordingly
                response = invoke_response.invocation_output
                try:
                    self._validate_invocation_response(
                        execution_arn, response, execution
                    )
                except (InvalidParameterValueException, IllegalStateException) as e:
                    logger.warning(
                        "[%s] Lambda output validation failure: %s", execution_arn, e
                    )
                    error_obj = ErrorObject.from_exception(e)
                    self._retry_invocation(execution, error_obj)

            except ResourceNotFoundException:
                logger.warning(
                    "[%s] Function No longer exists: %s",
                    execution_arn,
                    execution.start_input.function_name,
                )
                error_obj = ErrorObject.from_message(
                    message=f"Function not found: {execution.start_input.function_name}"
                )
                self._fail_workflow(execution_arn, error_obj)

            except Exception as e:  # noqa: BLE001
                # Handle invocation errors (network, function not found, etc.)
                logger.warning("[%s] Invocation failed: %s", execution_arn, e)
                error_obj = ErrorObject.from_exception(e)
                self._retry_invocation(execution, error_obj)
            finally:
                self._mark_invocation_finished(execution_arn)

        return invoke

    def _schedule_callback_resume(self, execution_arn: str) -> None:
        """Schedule a callback-triggered resume.

        Kept as a named wrapper for tests and callers that exercise callback behavior.
        """
        self._schedule_resume(execution_arn)

    def _schedule_resume(self, execution_arn: str) -> None:
        """Coalesce external resumes to avoid overlapping replays."""
        self._validate_current_execution(execution_arn)
        if self._active_invocation:
            self._pending_resume = True
            return

        if self._scheduled_resume:
            return

        self._scheduled_resume = True
        self._invoke_execution(execution_arn)

    def _mark_invocation_started(self, execution_arn: str) -> None:
        self._validate_current_execution(execution_arn)
        self._scheduled_resume = False
        self._active_invocation = True

    def _mark_invocation_finished(self, execution_arn: str) -> None:
        self._validate_current_execution(execution_arn)
        self._active_invocation = False
        should_resume = self._apply_deferred_resume_events(execution_arn)
        if self._pending_resume:
            self._pending_resume = False
            should_resume = True

        if should_resume:
            execution = self.get_execution(execution_arn)
            if not execution.is_complete:
                self._scheduled_resume = True
                self._invoke_execution(execution_arn)

    def _invoke_execution(self, execution_arn: str, delay: float = 0) -> None:
        """Invoke execution after delay in seconds."""
        self._validate_current_execution(execution_arn)
        self._scheduler.call_later(
            self._invoke_handler(execution_arn),
            delay=delay,
            completion_event=self._completion_event,
        )

    def _complete_workflow(
        self, execution_arn: str, result: str | None, error: ErrorObject | None
    ):
        """Complete workflow - handles both success and failure with terminal state validation."""
        execution = self.get_execution(execution_arn)

        if execution.is_complete:
            msg: str = "Cannot make multiple close workflow decisions."

            raise IllegalStateException(msg)

        if error is not None:
            self.fail_execution(execution_arn, error)
        else:
            self.complete_execution(execution_arn, result)

    def _fail_workflow(self, execution_arn: str, error: ErrorObject):
        """Fail workflow with terminal state validation."""
        execution = self.get_execution(execution_arn)

        if execution.is_complete:
            msg: str = "Cannot make multiple close workflow decisions."

            raise IllegalStateException(msg)

        self.fail_execution(execution_arn, error)

    def _retry_invocation(self, execution: Execution, error: ErrorObject):
        """Handle retry logic or fail execution if retries exhausted."""
        if (
            execution.consecutive_failed_invocation_attempts
            > self.MAX_CONSECUTIVE_FAILED_ATTEMPTS
        ):
            # Exhausted retries - fail the execution
            self._fail_workflow(
                execution_arn=execution.durable_execution_arn, error=error
            )
        else:
            # Schedule retry with backoff
            execution.consecutive_failed_invocation_attempts += 1
            self._save_execution(execution)
            self._invoke_execution(
                execution_arn=execution.durable_execution_arn,
                delay=self.RETRY_BACKOFF_SECONDS,
            )

    def _complete_events(self, execution_arn: str):
        self._validate_current_execution(execution_arn)
        # complete doesn't actually checkpoint explicitly
        if self._completion_event:
            self._completion_event.set()
        if execution_timeout := self._execution_timeout:
            execution_timeout.cancel()
            self._execution_timeout = None

    async def wait_until_complete(
        self, execution_arn: str, timeout: float | None = None
    ) -> bool:
        """Wait until execution completion.

        Args
            timeout (int|float|None): Wait for event to set until this timeout.

        Returns:
            True when set. False if the event timed out without being set.
        """
        self._validate_current_execution(execution_arn)
        if event := self._completion_event:
            return await event.wait_async(timeout)

        # this really shouldn't happen - implies execution timed out?
        msg: str = "execution does not exist."

        raise ResourceNotFoundException(msg)

    def complete_execution(self, execution_arn: str, result: str | None = None) -> None:
        """Complete execution successfully (COMPLETE_WORKFLOW_EXECUTION decision)."""
        logger.debug("[%s] Completing execution with result: %s", execution_arn, result)
        execution: Execution = self.get_execution(execution_arn)
        execution.complete_success(result=result)  # Sets CloseStatus.COMPLETED
        self._update_execution(execution)
        if execution.result is None:
            msg: str = "Execution result is required"
            raise IllegalStateException(msg)
        self._complete_events(execution_arn=execution_arn)

    def fail_execution(self, execution_arn: str, error: ErrorObject) -> None:
        """Fail execution with error (FAIL_WORKFLOW_EXECUTION decision)."""
        logger.error("[%s] Completing execution with error: %s", execution_arn, error)
        execution: Execution = self.get_execution(execution_arn)
        execution.complete_fail(error=error)  # Sets CloseStatus.FAILED
        self._update_execution(execution)
        # set by complete_fail
        if execution.result is None:
            msg: str = "Execution result is required"
            raise IllegalStateException(msg)
        self._complete_events(execution_arn=execution_arn)

    def _on_wait_succeeded(self, execution_arn: str, operation_id: str) -> bool:
        """Private method - called when a wait operation completes successfully."""
        execution = self.get_execution(execution_arn)

        if execution.is_complete:
            logger.info(
                "[%s] Execution already completed, ignoring wait succeeded event",
                execution_arn,
            )
            return False

        try:
            execution.complete_wait(operation_id=operation_id)
            self._update_execution(execution)
            logger.debug(
                "[%s] Wait succeeded for operation %s", execution_arn, operation_id
            )
            return True
        except Exception:
            logger.exception("[%s] Error processing wait succeeded.", execution_arn)
            return False

    def _on_retry_ready(self, execution_arn: str, operation_id: str) -> bool:
        """Private method - called when a retry delay has elapsed and retry is ready."""
        execution = self.get_execution(execution_arn)

        if execution.is_complete:
            logger.info(
                "[%s] Execution already completed, ignoring retry", execution_arn
            )
            return False

        try:
            execution.complete_retry(operation_id=operation_id)
            self._update_execution(execution)
            logger.debug(
                "[%s] Retry ready for operation %s", execution_arn, operation_id
            )
            return True
        except Exception:
            logger.exception("[%s] Error processing retry ready.", execution_arn)
            return False

    def _defer_resume_event_if_active(
        self,
        execution_arn: str,
        operation_id: str,
        deferred_events: set[str],
    ) -> bool:
        self._validate_current_execution(execution_arn)
        if not self._active_invocation:
            return False

        deferred_events.add(operation_id)
        return True

    def _defer_callback_timeout_if_active(
        self,
        execution_arn: str,
        callback_id: str,
        timeout_type: CallbackTimeoutType,
    ) -> bool:
        self._validate_current_execution(execution_arn)
        if not self._active_invocation:
            return False

        self._deferred_callback_timeouts.add((callback_id, timeout_type))
        return True

    def _apply_deferred_resume_events(self, execution_arn: str) -> bool:
        self._validate_current_execution(execution_arn)
        should_resume = False

        for operation_id in list(self._deferred_wait_resumes):
            self._deferred_wait_resumes.discard(operation_id)
            should_resume = (
                self._on_wait_succeeded(execution_arn, operation_id) or should_resume
            )

        for operation_id in list(self._deferred_retry_resumes):
            self._deferred_retry_resumes.discard(operation_id)
            should_resume = (
                self._on_retry_ready(execution_arn, operation_id) or should_resume
            )

        for callback_event in list(self._deferred_callback_timeouts):
            callback_id, timeout_type = callback_event
            self._deferred_callback_timeouts.discard(callback_event)
            should_resume = (
                self._complete_callback_timeout(
                    execution_arn, callback_id, timeout_type
                )
                or should_resume
            )

        return should_resume

    def timeout_execution(self, execution_arn: str, error: ErrorObject) -> None:
        """Handle execution timeout."""
        logger.exception("[%s] Execution timed out.", execution_arn)
        execution: Execution = self.get_execution(execution_arn)
        execution.complete_timeout(error=error)  # Sets CloseStatus.TIMED_OUT
        self._update_execution(execution)
        self._complete_events(execution_arn=execution_arn)

    def stop_execution(self, execution_arn: str, error: ErrorObject) -> None:
        """Handle execution stop."""
        self.fail_execution(execution_arn, error)

    def schedule_wait_timer(
        self, execution_arn: str, operation_id: str, delay: float
    ) -> None:
        """Schedule a wait operation."""
        logger.debug("[%s] scheduling wait with delay: %d", execution_arn, delay)

        async def wait_handler() -> None:
            if self._defer_resume_event_if_active(
                execution_arn, operation_id, self._deferred_wait_resumes
            ):
                return
            if self._on_wait_succeeded(execution_arn, operation_id):
                self._schedule_resume(execution_arn)

        self._scheduler.call_later(
            wait_handler, delay=delay, completion_event=self._completion_event
        )

    def schedule_step_retry(
        self, execution_arn: str, operation_id: str, delay: float
    ) -> None:
        """Schedule a step retry."""
        logger.debug(
            "[%s] scheduling retry for %s with delay: %d",
            execution_arn,
            operation_id,
            delay,
        )

        async def retry_handler() -> None:
            if self._defer_resume_event_if_active(
                execution_arn, operation_id, self._deferred_retry_resumes
            ):
                return
            if self._on_retry_ready(execution_arn, operation_id):
                self._schedule_resume(execution_arn)

        self._scheduler.call_later(
            retry_handler, delay=delay, completion_event=self._completion_event
        )

    def schedule_callback_timeouts(
        self,
        execution_arn: str,
        callback_options: CallbackOptions | None,
        callback_id: str,
    ) -> None:
        """Schedule callback timeout and heartbeat timeout if configured."""
        self._schedule_callback_timeouts(execution_arn, callback_options, callback_id)

    def _schedule_callback_timeouts(
        self,
        execution_arn: str,
        callback_options: CallbackOptions | None,
        callback_id: str,
    ) -> None:
        """Schedule callback timeout and heartbeat timeout if configured."""
        try:
            if not callback_options:
                return

            # Schedule main timeout if configured
            if callback_options.timeout_seconds > 0:
                timeout_delay = scale_delay(
                    callback_options.timeout_seconds,
                    minimum=_CALLBACK_TIMEOUT_MINIMUM_DELAY_SECONDS,
                )

                async def timeout_handler() -> None:
                    self._on_callback_timeout(execution_arn, callback_id)

                timeout_future = self._scheduler.call_later(
                    timeout_handler,
                    delay=timeout_delay,
                    completion_event=self._completion_event,
                )
                self._callback_timeouts[callback_id] = timeout_future

            # Schedule heartbeat timeout if configured
            if callback_options.heartbeat_timeout_seconds > 0:
                heartbeat_delay = scale_delay(
                    callback_options.heartbeat_timeout_seconds,
                    minimum=_CALLBACK_TIMEOUT_MINIMUM_DELAY_SECONDS,
                )

                async def heartbeat_timeout_handler() -> None:
                    self._on_callback_heartbeat_timeout(execution_arn, callback_id)

                heartbeat_future = self._scheduler.call_later(
                    heartbeat_timeout_handler,
                    delay=heartbeat_delay,
                    completion_event=self._completion_event,
                )
                self._callback_heartbeats[callback_id] = heartbeat_future

        except Exception:
            logger.exception(
                "[%s] Error scheduling callback timeouts for %s",
                execution_arn,
                callback_id,
            )

    def _reset_callback_heartbeat_timeout(
        self, callback_id: str, execution_arn: str
    ) -> None:
        """Reset the heartbeat timeout for a callback."""
        # Cancel existing heartbeat timeout
        if heartbeat_future := self._callback_heartbeats.pop(callback_id, None):
            heartbeat_future.cancel()

        # Find callback options to reschedule heartbeat timeout
        try:
            callback_token = CallbackToken.from_str(callback_id)
            execution = self.get_execution(callback_token.execution_arn)

            callback_options = None
            for update in execution.updates:
                if (
                    update.operation_id == callback_token.operation_id
                    and update.callback_options
                    and update.action.value == "START"
                ):
                    callback_options = update.callback_options
                    break

            if callback_options and callback_options.heartbeat_timeout_seconds > 0:
                heartbeat_delay = scale_delay(
                    callback_options.heartbeat_timeout_seconds,
                    minimum=_CALLBACK_TIMEOUT_MINIMUM_DELAY_SECONDS,
                )

                async def heartbeat_timeout_handler() -> None:
                    self._on_callback_heartbeat_timeout(execution_arn, callback_id)

                heartbeat_future = self._scheduler.call_later(
                    heartbeat_timeout_handler,
                    delay=heartbeat_delay,
                    completion_event=self._completion_event,
                )
                self._callback_heartbeats[callback_id] = heartbeat_future

        except Exception:
            logger.exception(
                "[%s] Error resetting callback heartbeat timeout for %s",
                execution_arn,
                callback_id,
            )

    def _cleanup_callback_timeouts(self, callback_id: str) -> None:
        """Clean up timeout events for a completed callback."""
        # Clean up main timeout
        if timeout_future := self._callback_timeouts.pop(callback_id, None):
            timeout_future.cancel()

        # Clean up heartbeat timeout
        if heartbeat_future := self._callback_heartbeats.pop(callback_id, None):
            heartbeat_future.cancel()

    def _complete_callback_timeout(
        self,
        execution_arn: str,
        callback_id: str,
        timeout_type: CallbackTimeoutType,
    ) -> bool:
        """Complete a callback with a timeout if the execution is still active."""
        try:
            callback_token = CallbackToken.from_str(callback_id)
            execution = self.get_execution(callback_token.execution_arn)

            if execution.is_complete:
                return False

            timeout_label = (
                "Callback heartbeat timed out"
                if timeout_type is CallbackTimeoutType.HEARTBEAT
                else "Callback timed out"
            )
            timeout_error = ErrorObject.from_message(
                f"{timeout_label}: {timeout_type.value}"
            )
            execution.complete_callback_timeout(callback_id, timeout_error)
            self._update_execution(execution)
            logger.warning("[%s] %s %s", execution_arn, timeout_label, callback_id)
            return True
        except Exception:
            logger.exception(
                "[%s] Error processing callback timeout for %s",
                execution_arn,
                callback_id,
            )
            return False

    def _on_callback_timeout(self, execution_arn: str, callback_id: str) -> None:
        """Handle callback timeout."""
        if self._defer_callback_timeout_if_active(
            execution_arn, callback_id, CallbackTimeoutType.TIMEOUT
        ):
            return
        if self._complete_callback_timeout(
            execution_arn, callback_id, CallbackTimeoutType.TIMEOUT
        ):
            self._schedule_resume(execution_arn)

    def _on_callback_heartbeat_timeout(
        self, execution_arn: str, callback_id: str
    ) -> None:
        """Handle callback heartbeat timeout."""
        if self._defer_callback_timeout_if_active(
            execution_arn, callback_id, CallbackTimeoutType.HEARTBEAT
        ):
            return
        if self._complete_callback_timeout(
            execution_arn, callback_id, CallbackTimeoutType.HEARTBEAT
        ):
            self._schedule_resume(execution_arn)
