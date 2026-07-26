"""Checkpoint processors module."""

from __future__ import annotations

from ....models import (
    OperationType,
)
from .base import (
    OperationProcessor,
)
from .callback import (
    CallbackProcessor,
)
from .context import (
    ContextProcessor,
)
from .execution import (
    ExecutionProcessor,
)
from .invoke import (
    ChainedInvokeProcessor,
)
from .step import (
    StepProcessor,
)
from .wait import (
    WaitProcessor,
)


def create_default_processors() -> dict[OperationType, OperationProcessor]:
    """Create the default operation processors keyed by operation type."""
    return {
        OperationType.STEP: StepProcessor(),
        OperationType.WAIT: WaitProcessor(),
        OperationType.CONTEXT: ContextProcessor(),
        OperationType.CALLBACK: CallbackProcessor(),
        OperationType.EXECUTION: ExecutionProcessor(),
        OperationType.CHAINED_INVOKE: ChainedInvokeProcessor(),
    }
