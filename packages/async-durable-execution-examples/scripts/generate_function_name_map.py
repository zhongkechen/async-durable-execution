#!/usr/bin/env python3

import argparse
import json
from pathlib import Path
from typing import Any

from function_naming import to_function_name_suffix
from generate_examples_catalog import build_examples_catalog
from test_handlers import load_test_handlers


def load_catalog(catalog_path: Path | None = None) -> dict[str, Any]:
    """Load the examples catalog from disk or generate it from source."""
    if catalog_path is None:
        return build_examples_catalog()

    with catalog_path.open() as file:
        return json.load(file)


def build_function_name_map(catalog: dict[str, Any], prefix: str) -> dict[str, str]:
    """Build a pytest function-name map from the examples catalog."""
    function_name_by_handler = {
        example[
            "handler"
        ]: f"{prefix}{to_function_name_suffix(example['handler'])}:$LATEST"
        for example in catalog["examples"]
    }

    test_root = Path(__file__).resolve().parent.parent / "test"
    test_handlers = load_test_handlers(test_root)
    missing_handlers = sorted(
        handler for handler in test_handlers if handler not in function_name_by_handler
    )

    if missing_handlers:
        missing_details = ", ".join(missing_handlers)
        msg = (
            "Example tests reference handlers missing from the generated examples catalog: "
            f"{missing_details}"
        )
        raise SystemExit(msg)

    return {
        handler: function_name_by_handler[handler] for handler in sorted(test_handlers)
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate a JSON map of example handlers to qualified function names"
    )
    parser.add_argument(
        "--prefix",
        required=True,
        help="Function name prefix used in the deployed SAM stack",
    )
    parser.add_argument(
        "--catalog",
        type=Path,
        help="Optional path to a prebuilt catalog JSON",
    )
    args = parser.parse_args()

    catalog = load_catalog(args.catalog)
    function_name_map = build_function_name_map(catalog, args.prefix)
    print(json.dumps(function_name_map, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
