"""Compatibility aliases for the former private operation package."""

from importlib import import_module
import sys


_OPERATION_MODULES = (
    "flow",
    "map",
    "parallel",
    "recurse",
    "replay_safe",
    "wait_for_callback",
    "wait_for_condition",
    "with_retry",
)

for _module_name in _OPERATION_MODULES:
    _module = import_module(f"async_durable_execution._operation.{_module_name}")
    sys.modules[f"{__name__}.{_module_name}"] = _module
    globals()[_module_name] = _module

del _module, _module_name
