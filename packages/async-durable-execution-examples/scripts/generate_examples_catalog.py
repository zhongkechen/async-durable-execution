#!/usr/bin/env python3

import argparse
import ast
import json
from pathlib import Path
from typing import Any

PACKAGE_NAME = "DurableExecutionsPythonExamples-1.0"
PACKAGE_PREFIX = "async_durable_execution_examples"
DEFAULT_DURABLE_CONFIG = {
    "RetentionPeriodInDays": 7,
    "ExecutionTimeout": 300,
}
SPECIAL_LOGGING_CONFIG = {
    "callback/callback_concurrency.py": {
        "ApplicationLogLevel": "DEBUG",
        "LogFormat": "JSON",
    },
    "logger_example/logger_example.py": {
        "ApplicationLogLevel": "INFO",
        "LogFormat": "JSON",
    },
}


def build_examples_catalog() -> dict[str, Any]:
    """Build the examples catalog by scanning example handlers."""
    source_root = Path(__file__).resolve().parent.parent / "src" / PACKAGE_PREFIX
    examples = []
    for path in sorted(source_root.rglob("*.py")):
        if path.name in {"__init__.py", "__about__.py"}:
            continue

        example = build_example_entry(path, source_root)
        if example is not None:
            examples.append(example)

    return {
        "packageName": PACKAGE_NAME,
        "examples": examples,
    }


def build_example_entry(path: Path, source_root: Path) -> dict[str, Any] | None:
    """Build a catalog entry for a module if it exports a handler."""
    tree = ast.parse(path.read_text(), filename=str(path))
    handler_node = find_handler_node(tree)
    if handler_node is None:
        return None

    relative_path = path.relative_to(source_root)
    handler_module = ".".join([PACKAGE_PREFIX, *relative_path.with_suffix("").parts])
    description = get_description(tree, handler_node, relative_path)

    example: dict[str, Any] = {
        "name": to_example_name(relative_path),
        "description": description,
        "handler": f"{handler_module}.handler",
        "integration": True,
        "durableConfig": DEFAULT_DURABLE_CONFIG.copy(),
        "path": f"./src/{PACKAGE_PREFIX}/{relative_path.as_posix()}",
    }

    logging_config = SPECIAL_LOGGING_CONFIG.get(relative_path.as_posix())
    if logging_config is not None:
        example["loggingConfig"] = logging_config.copy()

    return example


def find_handler_node(
    tree: ast.Module,
) -> ast.FunctionDef | ast.AsyncFunctionDef | None:
    """Return the top-level handler function node if present."""
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.name == "handler":
                return node
    return None


def get_description(
    tree: ast.Module,
    handler_node: ast.FunctionDef | ast.AsyncFunctionDef,
    relative_path: Path,
) -> str:
    """Extract an example description from the handler or module docstring."""
    handler_docstring = ast.get_docstring(handler_node)
    if handler_docstring:
        return first_line(handler_docstring)

    module_docstring = ast.get_docstring(tree)
    if module_docstring:
        return first_line(module_docstring)

    return f"Example for {to_example_name(relative_path)}."


def first_line(docstring: str) -> str:
    """Return the first non-empty line from a docstring."""
    for line in docstring.splitlines():
        stripped = line.strip()
        if stripped:
            return stripped
    return ""


def to_example_name(relative_path: Path) -> str:
    """Convert a module path to a human-readable example name."""
    parts = list(relative_path.with_suffix("").parts)
    words: list[str] = []
    previous_part_words: list[str] = []
    for part in parts:
        part_words = [word for word in part.split("_") if word]
        if part_words[: len(previous_part_words)] == previous_part_words:
            part_words = part_words[len(previous_part_words) :]
        words.extend(part_words)
        previous_part_words = [word for word in part.split("_") if word]

    return " ".join(word.capitalize() for word in words if word)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate the examples catalog by scanning example handlers"
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Write the generated catalog JSON to this path instead of stdout",
    )
    args = parser.parse_args()

    catalog = build_examples_catalog()
    payload = json.dumps(catalog, sort_keys=False, indent=2) + "\n"

    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload)
    else:
        print(payload, end="")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
