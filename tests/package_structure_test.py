"""Package dependency rules."""

from __future__ import annotations

import ast
from pathlib import Path


PACKAGE_ROOT = Path(__file__).parents[1] / "async_durable_execution"


def test_non_core_packages_import_core_through_package_facade():
    """Sibling packages must depend on the core package interface."""
    violations: list[str] = []

    package_paths = (
        path
        for path in PACKAGE_ROOT.iterdir()
        if path.name != "core" and (path / "__init__.py").is_file()
    )
    for package_path in package_paths:
        for path in package_path.rglob("*.py"):
            tree = ast.parse(path.read_text(), filename=str(path))
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom):
                    module = node.module or ""
                    if module.startswith(("core.", "async_durable_execution.core.")):
                        violations.append(
                            f"{path.relative_to(PACKAGE_ROOT)}:{node.lineno}"
                        )
                elif isinstance(node, ast.Import):
                    if any(
                        alias.name.startswith("async_durable_execution.core.")
                        for alias in node.names
                    ):
                        violations.append(
                            f"{path.relative_to(PACKAGE_ROOT)}:{node.lineno}"
                        )

    assert not violations, (
        "Import core symbols from async_durable_execution.core.__init__: "
        + ", ".join(violations)
    )
