"""Unit tests for logger module."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from unittest.mock import Mock

from async_durable_execution.context import DurableContext, ExecutionContext
from async_durable_execution.logger import (
    DurableContextFilter,
    LogInfo,
    build_context_log_extra,
    configure_durable_logger,
    reset_current_context,
    set_current_context,
)
from async_durable_execution.models import OperationIdentifier
from async_durable_execution.models import (
    Operation,
    OperationStatus,
    OperationSubType,
    OperationType,
)
from async_durable_execution.plugin import PluginExecutor
from async_durable_execution.state import ExecutionState, ReplayStatus
from async_durable_execution.types import LoggerInterface, StepContext


class PowertoolsLoggerStub:
    """Stub implementation of AWS Powertools Logger with exact method signatures."""

    filters: list[logging.Filter]
    handlers: list[logging.Handler]

    def __init__(self) -> None:
        self.filters = []
        self.handlers = []

    def addFilter(self, filter: logging.Filter) -> None:  # noqa: N802
        self.filters.append(filter)

    def debug(
        self,
        msg: object,
        *args: object,
        exc_info=None,
        stack_info: bool = False,
        stacklevel: int = 2,
        extra: Mapping[str, object] | None = None,
        **kwargs: object,
    ) -> None:
        pass

    def info(
        self,
        msg: object,
        *args: object,
        exc_info=None,
        stack_info: bool = False,
        stacklevel: int = 2,
        extra: Mapping[str, object] | None = None,
        **kwargs: object,
    ) -> None:
        pass

    def warning(
        self,
        msg: object,
        *args: object,
        exc_info=None,
        stack_info: bool = False,
        stacklevel: int = 2,
        extra: Mapping[str, object] | None = None,
        **kwargs: object,
    ) -> None:
        pass

    def error(
        self,
        msg: object,
        *args: object,
        exc_info=None,
        stack_info: bool = False,
        stacklevel: int = 2,
        extra: Mapping[str, object] | None = None,
        **kwargs: object,
    ) -> None:
        pass

    def exception(
        self,
        msg: object,
        *args: object,
        exc_info=True,
        stack_info: bool = False,
        stacklevel: int = 2,
        extra: Mapping[str, object] | None = None,
        **kwargs: object,
    ) -> None:
        pass


EXECUTION_STATE = ExecutionState(
    durable_execution_arn="arn:aws:test",
    initial_checkpoint_token="test_token",  # noqa: S106
    operations={},
    service_client=Mock(),
    plugin_executor=PluginExecutor(plugins=None),
)


def create_durable_context(parent_id: str | None = None) -> DurableContext:
    return DurableContext(
        state=EXECUTION_STATE,
        execution_context=ExecutionContext(
            durable_execution_arn=EXECUTION_STATE.durable_execution_arn
        ),
        parent_id=parent_id,
    )


def test_powertools_logger_compatibility():
    """The public logger protocol should still accept Powertools-style loggers."""
    powertools_logger = PowertoolsLoggerStub()

    def accepts_logger_interface(logger: LoggerInterface) -> None:
        logger.debug("test")
        logger.info("test")
        logger.warning("test")
        logger.error("test")
        logger.exception("test")

    accepts_logger_interface(powertools_logger)
    configure_durable_logger(powertools_logger)
    assert any(
        isinstance(item, DurableContextFilter) for item in powertools_logger.filters
    )


def test_log_info_creation_and_helpers():
    log_info = LogInfo(EXECUTION_STATE, "parent123", "operation123", "test_name", 5)
    assert log_info.execution_state.durable_execution_arn == "arn:aws:test"
    assert log_info.parent_id == "parent123"
    assert log_info.operation_id == "operation123"
    assert log_info.name == "test_name"
    assert log_info.attempt == 5

    op_id = OperationIdentifier("op123", OperationSubType.STEP, "parent456", "op_name")
    from_operation = LogInfo.from_operation_identifier(EXECUTION_STATE, op_id, 3)
    assert from_operation.parent_id == "parent456"
    assert from_operation.operation_id == "op123"
    assert from_operation.name == "op_name"
    assert from_operation.attempt == 3

    assert log_info.with_parent_id("new_parent").parent_id == "new_parent"


def test_build_context_log_extra_for_durable_context():
    context = create_durable_context(parent_id="parent-1")
    context.operation_id = "context-op"
    context.operation_name = "child-context"

    assert build_context_log_extra(context) == {
        "executionArn": "arn:aws:test",
        "parentId": "parent-1",
        "operationId": "context-op",
        "operationName": "child-context",
    }


def test_build_context_log_extra_for_step_context():
    step_context = StepContext(
        attempt=2,
        execution_state=EXECUTION_STATE,
        execution_arn="arn:aws:test",
        parent_id="parent-1",
        operation_id="step-1",
        operation_name="process",
    )

    assert build_context_log_extra(step_context) == {
        "executionArn": "arn:aws:test",
        "parentId": "parent-1",
        "operationId": "step-1",
        "operationName": "process",
        "attempt": 2,
    }


def test_filter_adds_fields_from_active_context():
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="hello",
        args=(),
        exc_info=None,
    )
    context = create_durable_context(parent_id="parent-1")
    context.operation_id = "context-op"

    token = set_current_context(context)
    try:
        allowed = DurableContextFilter().filter(record)
    finally:
        reset_current_context(token)

    assert allowed is True
    assert record.executionArn == "arn:aws:test"
    assert record.parentId == "parent-1"
    assert record.operationId == "context-op"


def test_filter_preserves_existing_extra_fields():
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="hello",
        args=(),
        exc_info=None,
    )
    record.executionArn = "preexisting"

    step_context = StepContext(
        attempt=4,
        execution_state=EXECUTION_STATE,
        execution_arn="arn:aws:test",
        parent_id="parent-1",
        operation_id="step-1",
        operation_name="process",
    )

    token = set_current_context(step_context)
    try:
        DurableContextFilter().filter(record)
    finally:
        reset_current_context(token)

    assert record.executionArn == "preexisting"
    assert record.parentId == "parent-1"
    assert record.operationId == "step-1"
    assert record.operationName == "process"
    assert record.attempt == 4


def test_filter_suppresses_logs_during_replay():
    operation = Operation(
        operation_id="op1",
        operation_type=OperationType.STEP,
        status=OperationStatus.SUCCEEDED,
    )
    replay_state = ExecutionState(
        durable_execution_arn="arn:aws:test",
        initial_checkpoint_token="test_token",  # noqa: S106
        operations={"op1": operation},
        service_client=Mock(),
        replay_status=ReplayStatus.REPLAY,
        plugin_executor=PluginExecutor([]),
    )
    step_context = StepContext(
        attempt=1,
        execution_state=replay_state,
        execution_arn="arn:aws:test",
        operation_id="op1",
    )

    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="hello",
        args=(),
        exc_info=None,
    )

    token = set_current_context(step_context)
    try:
        allowed = DurableContextFilter().filter(record)
    finally:
        reset_current_context(token)

    assert allowed is False


def test_configure_durable_logger_is_idempotent_for_logger_and_handlers():
    logger = logging.getLogger("async_durable_execution.tests.logger")
    logger.handlers = []
    handler = logging.StreamHandler()
    logger.addHandler(handler)

    try:
        configure_durable_logger(logger)
        configure_durable_logger(logger)
    finally:
        logger.removeHandler(handler)
        handler.close()

    assert sum(isinstance(item, DurableContextFilter) for item in logger.filters) == 1
    assert sum(isinstance(item, DurableContextFilter) for item in handler.filters) == 1
