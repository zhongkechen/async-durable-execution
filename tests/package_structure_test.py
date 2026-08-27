"""Package dependency rules."""

from __future__ import annotations

import ast
import importlib.util
from pathlib import Path


PACKAGE_ROOT = Path(__file__).parents[1] / "async_durable_execution"


def test_non_core_packages_import_core_through_package_facade() -> None:
    """Sibling packages must depend on the core package interface."""
    violations: list[str] = []

    package_paths = (
        path
        for path in PACKAGE_ROOT.iterdir()
        if path.name != "_core" and (path / "__init__.py").is_file()
    )
    for package_path in package_paths:
        for path in package_path.rglob("*.py"):
            tree = ast.parse(path.read_text(), filename=str(path))
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom):
                    module = node.module or ""
                    if module.startswith(("_core.", "async_durable_execution._core.")):
                        violations.append(
                            f"{path.relative_to(PACKAGE_ROOT)}:{node.lineno}"
                        )
                elif isinstance(node, ast.Import):
                    if any(
                        alias.name.startswith("async_durable_execution._core.")
                        for alias in node.names
                    ):
                        violations.append(
                            f"{path.relative_to(PACKAGE_ROOT)}:{node.lineno}"
                        )

    assert not violations, (
        "Import core symbols from async_durable_execution._core.__init__: "
        + ", ".join(violations)
    )


def test_implementation_packages_are_not_public_import_paths() -> None:
    """Only underscore-prefixed implementation package names are available."""
    for package_name in ("core", "operation", "runner", "primitive"):
        assert (
            importlib.util.find_spec(f"async_durable_execution.{package_name}") is None
        )


def test_extension_author_module_is_a_public_import_path():
    assert importlib.util.find_spec("async_durable_execution.extension") is not None


def test_extension_step_executor_is_internal_to_primitive_layer() -> None:
    """The stable SPI delegates stateful STEP execution to an internal executor."""
    import async_durable_execution.extension as extension
    from async_durable_execution._primitive.step import (
        StatefulStepOperationExecutor,
    )

    assert not hasattr(extension, "_ExtensionStepOperationExecutor")
    assert (
        StatefulStepOperationExecutor.__module__
        == "async_durable_execution._primitive.step"
    )


def test_httpx_extra_is_canonical_and_aioboto_is_compatible_alias() -> None:
    pyproject = (PACKAGE_ROOT.parent / "pyproject.toml").read_text(encoding="utf-8")
    optional_dependencies = pyproject.split(
        "[project.optional-dependencies]",
        maxsplit=1,
    )[1].split("\n[", maxsplit=1)[0]

    dependency = '["httpx>=0.28.1,<1"]'
    assert f"httpx = {dependency}" in optional_dependencies
    assert f"aioboto = {dependency}" in optional_dependencies
