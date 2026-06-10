#!/usr/bin/env python3

import argparse
import json
from pathlib import Path
from typing import Any


def load_catalog() -> dict[str, Any]:
    """Load the examples catalog."""
    catalog_path = Path(__file__).resolve().parent.parent / "examples-catalog.json"
    with catalog_path.open() as file:
        return json.load(file)


def to_logical_id(handler_name: str) -> str:
    """Convert a handler module name to a CloudFormation logical id."""
    handler_base = handler_name.replace(".handler", "")
    return "".join(word.capitalize() for word in handler_base.split("_"))


def build_template(
    examples: list[dict[str, Any]], *, include_function_name_parameter: bool
) -> dict[str, Any]:
    """Build a SAM template for one or more examples."""
    parameters: dict[str, Any] = {
        "LambdaEndpoint": {
            "Type": "String",
            "Default": "https://lambda.us-west-2.amazonaws.com",
        }
    }
    if include_function_name_parameter:
        parameters["FunctionName"] = {"Type": "String"}
    else:
        parameters["FunctionNamePrefix"] = {
            "Type": "String",
            "Default": "",
        }

    template: dict[str, Any] = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Transform": "AWS::Serverless-2016-10-31",
        "Globals": {
            "Function": {
                "Runtime": "python3.13",
                "Timeout": 60,
                "MemorySize": 128,
                "Environment": {
                    "Variables": {"AWS_ENDPOINT_URL_LAMBDA": {"Ref": "LambdaEndpoint"}}
                },
            }
        },
        "Parameters": parameters,
        "Resources": {
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
        properties: dict[str, Any] = {
            "CodeUri": "build/",
            "Handler": example["handler"],
            "Description": example["description"],
            "Role": {"Fn::GetAtt": ["DurableFunctionRole", "Arn"]},
        }

        if include_function_name_parameter:
            properties["FunctionName"] = {"Ref": "FunctionName"}
        else:
            properties["FunctionName"] = {
                "Fn::Sub": f"${{FunctionNamePrefix}}{logical_id}"
            }
        if "durableConfig" in example:
            properties["DurableConfig"] = example["durableConfig"]

        template["Resources"][logical_id] = {
            "Type": "AWS::Serverless::Function",
            "Properties": properties,
        }

    return template


def generate_sam_template(
    *, example_name: str | None = None, output_path: Path | None = None
) -> Path:
    """Generate a SAM template for either the full catalog or one example."""
    catalog = load_catalog()
    selected_examples = catalog["examples"]

    if example_name is not None:
        selected_examples = [
            example
            for example in catalog["examples"]
            if example["name"].lower() == example_name.lower()
        ]
        if not selected_examples:
            msg = f"Example not found in catalog: {example_name}"
            raise SystemExit(msg)
        selected_examples = [selected_examples[0]]

    template = build_template(
        selected_examples,
        include_function_name_parameter=example_name is not None,
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
        "--example-name",
        help="Generate a template for a single catalog example",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Write the generated template to this path",
    )
    args = parser.parse_args()

    template_path = generate_sam_template(
        example_name=args.example_name,
        output_path=args.output,
    )
    print(f"Generated SAM template at {template_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
