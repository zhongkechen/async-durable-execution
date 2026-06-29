"""Compatibility tests for the deprecated runner distribution package."""

import importlib
import sys
import warnings
from pathlib import Path


def test_deprecated_runner_package_aliases_sdk_runner_modules() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    runner_src = repo_root / "async-durable-execution-runner" / "src"
    sys.path.insert(0, str(runner_src))
    try:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            compat = importlib.import_module("async_durable_execution_runner")

        from async_durable_execution import runner
        from async_durable_execution.runner import model as sdk_model
        from async_durable_execution.runner.checkpoint.validators.operations import (
            step as sdk_step_validator,
        )

        compat_model = importlib.import_module("async_durable_execution_runner.model")
        compat_runner = importlib.import_module("async_durable_execution_runner.runner")
        compat_step_validator = importlib.import_module(
            "async_durable_execution_runner.checkpoint.validators.operations.step"
        )

        assert compat.create_runner is runner.create_runner
        assert compat_model is sdk_model
        assert compat_runner is importlib.import_module(
            "async_durable_execution.runner.runner"
        )
        assert compat_step_validator is sdk_step_validator
        assert any(
            "async_durable_execution_runner is deprecated" in str(w.message)
            for w in caught
        )
    finally:
        sys.path.remove(str(runner_src))
