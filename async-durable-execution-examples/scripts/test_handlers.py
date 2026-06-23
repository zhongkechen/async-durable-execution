#!/usr/bin/env python3

import ast
from pathlib import Path


EXAMPLES_IMPORT_PREFIX = "async_durable_execution_examples"
HANDLER_SUFFIX = ".handler"


def load_test_handlers(test_root: Path) -> set[str]:
    """Load handler identifiers referenced by the example pytest suite."""
    handlers: set[str] = set()
    for path in sorted(test_root.rglob("test_*.py")):
        handlers.update(load_test_handlers_from_file(path))
    return handlers


def load_test_handlers_from_file(path: Path) -> set[str]:
    """Load handler identifiers from a single pytest file."""
    tree = ast.parse(path.read_text(), filename=str(path))
    imported_modules: dict[str, str] = {}

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            if not node.module.startswith(EXAMPLES_IMPORT_PREFIX):
                continue
            for alias in node.names:
                # Example: from async_durable_execution_examples.step import step
                imported_modules[alias.asname or alias.name] = (
                    f"{node.module}.{alias.name}{HANDLER_SUFFIX}"
                )

    handlers: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if not _is_durable_execution_marker(node.func):
            continue

        handler_name = _get_handler_name(node)
        if not handler_name:
            continue

        handler = imported_modules.get(handler_name)
        if handler:
            handlers.add(handler)

    return handlers


def _is_durable_execution_marker(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.Attribute)
        and node.attr == "durable_execution"
        and isinstance(node.value, ast.Attribute)
        and node.value.attr == "mark"
        and isinstance(node.value.value, ast.Name)
        and node.value.value.id == "pytest"
    )


def _get_handler_name(node: ast.Call) -> str | None:
    for keyword in node.keywords:
        if keyword.arg != "handler":
            continue
        value = keyword.value
        if (
            isinstance(value, ast.Attribute)
            and value.attr == "handler"
            and isinstance(value.value, ast.Name)
        ):
            return value.value.id
    return None
