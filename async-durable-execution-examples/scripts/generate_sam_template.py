#!/usr/bin/env python3

import argparse
import ast
import json
from pathlib import Path
from typing import Any

from function_naming import to_function_name_suffix, to_logical_id
from test_handlers import load_test_handlers

PACKAGE_NAME = "DurableExecutionsPythonExamples-1.0"
PACKAGE_PREFIX = "async_durable_execution_examples"
DEFAULT_AWS_REGION = "eu-south-1"
DEFAULT_LAMBDA_ENDPOINT = f"https://lambda.{DEFAULT_AWS_REGION}.amazonaws.com"
DEFAULT_RUNTIME = "python3.13"
SDK_LAYER_LOGICAL_ID = "AsyncDurableExecutionSdkLayer"
SDK_LAYER_CONTENT_URI = (
    "../async-durable-execution-lambda-layer/dist/async-durable-execution-layer.zip"
)
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


def load_catalog() -> dict[str, Any]:
    """Generate the examples catalog from source."""
    return build_examples_catalog()


def build_template(
    examples: list[dict[str, Any]],
    *,
    runtime: str = DEFAULT_RUNTIME,
) -> dict[str, Any]:
    """Build a SAM template for all examples."""
    parameters: dict[str, Any] = {
        "LambdaEndpoint": {
            "Type": "String",
            "Default": DEFAULT_LAMBDA_ENDPOINT,
        },
        "FunctionNamePrefix": {
            "Type": "String",
            "Default": "",
        },
    }

    template: dict[str, Any] = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Transform": "AWS::Serverless-2016-10-31",
        "Globals": {
            "Function": {
                "Runtime": runtime,
                "Timeout": 60,
                "MemorySize": 128,
                "Environment": {
                    "Variables": {"AWS_ENDPOINT_URL_LAMBDA": {"Ref": "LambdaEndpoint"}}
                },
            }
        },
        "Parameters": parameters,
        "Resources": {
            SDK_LAYER_LOGICAL_ID: {
                "Type": "AWS::Serverless::LayerVersion",
                "Properties": {
                    "LayerName": {"Fn::Sub": "${FunctionNamePrefix}sdk"},
                    "Description": "async-durable-execution SDK for e2e functions",
                    "ContentUri": SDK_LAYER_CONTENT_URI,
                    "CompatibleRuntimes": [runtime],
                    "CompatibleArchitectures": ["x86_64"],
                },
            },
            "DurableFunctionRole": {
                "Type": "AWS::IAM::Role",
                "Properties": {
                    "AssumeRolePolicyDocument": {
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Principal": {"Service": "lambda.amazonaws.com"},
                                "Action": "sts:AssumeRole",
                            }
                        ],
                    },
                    "ManagedPolicyArns": [
                        "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
                    ],
                    "Policies": [
                        {
                            "PolicyName": "DurableExecutionPolicy",
                            "PolicyDocument": {
                                "Version": "2012-10-17",
                                "Statement": [
                                    {
                                        "Effect": "Allow",
                                        "Action": [
                                            "lambda:CheckpointDurableExecution",
                                            "lambda:GetDurableExecutionState",
                                            "lambda:InvokeFunction",
                                        ],
                                        "Resource": "*",
                                    }
                                ],
                            },
                        }
                    ],
                },
            }
        },
    }

    for example in examples:
        logical_id = to_logical_id(example["handler"])
        function_name_suffix = to_function_name_suffix(example["handler"])
        properties: dict[str, Any] = {
            "CodeUri": "build/",
            "Handler": example["handler"],
            "Description": example["description"],
            "Role": {"Fn::GetAtt": ["DurableFunctionRole", "Arn"]},
            "Layers": [{"Ref": SDK_LAYER_LOGICAL_ID}],
            "FunctionName": {
                "Fn::Sub": f"${{FunctionNamePrefix}}{function_name_suffix}"
            },
        }

        if "durableConfig" in example:
            properties["DurableConfig"] = example["durableConfig"]

        template["Resources"][logical_id] = {
            "Type": "AWS::Serverless::Function",
            "Properties": properties,
        }

    return template


def validate_catalog_test_coverage(catalog: dict[str, Any]) -> None:
    """Ensure every example test handler is represented in the examples catalog."""
    catalog_handlers = {example["handler"] for example in catalog["examples"]}
    test_root = Path(__file__).resolve().parent.parent / "test"
    missing_handlers = sorted(
        handler
        for handler in load_test_handlers(test_root)
        if handler not in catalog_handlers
    )
    if not missing_handlers:
        return

    missing_details = ", ".join(missing_handlers)
    msg = (
        "Example tests reference handlers missing from the generated examples catalog: "
        f"{missing_details}"
    )
    raise SystemExit(msg)


def generate_sam_template(
    *,
    output_path: Path | None = None,
    runtime: str = DEFAULT_RUNTIME,
) -> Path:
    """Generate a SAM template for the full examples stack."""
    catalog = load_catalog()
    validate_catalog_test_coverage(catalog)

    template = build_template(
        catalog["examples"],
        runtime=runtime,
    )

    template_path = output_path or (
        Path(__file__).resolve().parent.parent / "template.generated.json"
    )
    template_path.parent.mkdir(parents=True, exist_ok=True)
    with template_path.open("w") as file:
        json.dump(template, file, sort_keys=False, indent=2)
        file.write("\n")

    return template_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate a SAM template for examples")
    parser.add_argument(
        "--output",
        type=Path,
        help="Write the generated template to this path",
    )
    parser.add_argument(
        "--runtime",
        default=DEFAULT_RUNTIME,
        help=f"SAM Lambda runtime to use for generated functions (default: {DEFAULT_RUNTIME})",
    )
    args = parser.parse_args()

    template_path = generate_sam_template(
        output_path=args.output,
        runtime=args.runtime,
    )
    print(f"Generated SAM template at {template_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
