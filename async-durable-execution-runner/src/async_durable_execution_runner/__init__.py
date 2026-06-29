"""Compatibility package for durable execution runner helpers.

The runner implementation now lives in :mod:`async_durable_execution.runner`.
This package remains only to keep existing imports working while users migrate.
"""

from __future__ import annotations

import importlib
import pkgutil
import sys
import warnings

import async_durable_execution.runner as _runner


warnings.warn(
    "async_durable_execution_runner is deprecated; install async-durable-execution "
    "and import runner helpers from async_durable_execution.runner.",
    DeprecationWarning,
    stacklevel=2,
)

_OLD_PACKAGE = __name__
_NEW_PACKAGE = _runner.__name__


def _install_module_aliases() -> None:
    for module_info in pkgutil.walk_packages(
        _runner.__path__,
        prefix=f"{_NEW_PACKAGE}.",
    ):
        new_name = module_info.name
        old_name = f"{_OLD_PACKAGE}{new_name[len(_NEW_PACKAGE) :]}"
        sys.modules[old_name] = importlib.import_module(new_name)


_install_module_aliases()

from async_durable_execution.runner import (  # noqa: E402
    ContextOperation,
    DurableFunctionCloudTestRunner,
    DurableFunctionLocalTestRunner,
    DurableFunctionTestResult,
    StepOperation,
    __version__,
    create_runner,
)


__all__ = [
    "ContextOperation",
    "DurableFunctionCloudTestRunner",
    "DurableFunctionLocalTestRunner",
    "DurableFunctionTestResult",
    "StepOperation",
    "__version__",
    "create_runner",
]


def __getattr__(name: str):
    return getattr(_runner, name)
