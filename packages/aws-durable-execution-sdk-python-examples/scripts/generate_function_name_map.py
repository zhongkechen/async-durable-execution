#!/usr/bin/env python3

import argparse
import json
from pathlib import Path
from typing import Any


def load_catalog(catalog_path: Path) -> dict[str, Any]:
    """Load the examples catalog from disk."""
    with catalog_path.open() as file:
        return json.load(file)


def to_logical_id(handler_name: str) -> str:
    """Convert a handler module name to a CloudFormation logical id."""
    handler_base = handler_name.replace(".handler", "")
    return "".join(word.capitalize() for word in handler_base.split("_"))


def build_function_name_map(catalog: dict[str, Any], prefix: str) -> dict[str, str]:
    """Build a pytest function-name map from the examples catalog."""
    return {
        example["name"]: f"{prefix}{to_logical_id(example['handler'])}:$LATEST"
        for example in catalog["examples"]
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate a JSON map of example names to qualified function names"
    )
    parser.add_argument(
        "--prefix",
        required=True,
        help="Function name prefix used in the deployed SAM stack",
    )
    parser.add_argument(
        "--catalog",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "examples-catalog.json",
        help="Path to examples-catalog.json",
    )
    args = parser.parse_args()

    catalog = load_catalog(args.catalog)
    function_name_map = build_function_name_map(catalog, args.prefix)
    print(json.dumps(function_name_map, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
