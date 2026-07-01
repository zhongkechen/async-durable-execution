from __future__ import annotations

import logging
from typing import (
    TYPE_CHECKING,
    Any,
)

from .cloud import DurableFunctionCloudTestRunner
from .exceptions import (
    InvalidParameterValueException,
)
from .local import DurableFunctionLocalTestRunner

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)


def create_runner(
    *,
    mode: str,
    handler: Callable | None = None,
    function_name: str | None = None,
    region: str = "us-west-2",
    lambda_endpoint: str | None = None,
    poll_interval: float = 1.0,
    input: Any = None,  # noqa: A002
    timeout: int = 60,
) -> DurableFunctionLocalTestRunner | DurableFunctionCloudTestRunner:
    """Create a configured local or cloud durable function runner.

    Args:
        mode: Runner mode, either ``local`` or ``cloud``.
        handler: Durable handler to run locally. Required when ``mode='local'``.
        function_name: Qualified Lambda function name. Required when ``mode='cloud'``.
        region: AWS region for cloud mode.
        lambda_endpoint: Optional Lambda endpoint for cloud mode.
        poll_interval: Poll interval used by the underlying runner.
        input: Default input for ``await run()`` and ``await run_async()``.
        timeout: Default timeout for ``await run()`` and ``await run_async()``.

    Returns:
        A configured runner that can be used as a context manager.
    """
    runner: DurableFunctionLocalTestRunner | DurableFunctionCloudTestRunner
    if mode == "local":
        if handler is None:
            msg = "handler is required when mode='local'"
            raise InvalidParameterValueException(msg)
        runner = DurableFunctionLocalTestRunner(
            handler=handler,
            poll_interval=poll_interval,
            input=input,
            timeout=timeout,
        )
    elif mode == "cloud":
        if function_name is None:
            msg = "function_name is required when mode='cloud'"
            raise InvalidParameterValueException(msg)
        runner = DurableFunctionCloudTestRunner(
            function_name=function_name,
            region=region,
            lambda_endpoint=lambda_endpoint,
            poll_interval=poll_interval,
            input=input,
            timeout=timeout,
        )
    else:
        msg = f"Unsupported runner mode: {mode}"
        raise InvalidParameterValueException(msg)

    return runner
