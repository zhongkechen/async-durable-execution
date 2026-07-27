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
    for package_name in ("core", "runner", "primitive", "extension"):
        assert (
            importlib.util.find_spec(f"async_durable_execution.{package_name}") is None
        )
