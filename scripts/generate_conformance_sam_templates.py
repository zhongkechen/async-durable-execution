#!/usr/bin/env python3

import argparse
import ast
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_RUNTIME = "python3.13"
DEFAULT_DURABLE_CONFIG = {
    "RetentionPeriodInDays": 7,
    "ExecutionTimeout": 300,
}
SUITE_PREFIXES = {
    "step": "1",
    "wait": "2",
    "child": "3",
    "callback": "4",
    "invoke": "5",
    "wait_for_condition": "6",
    "wait_for_callback": "7",
    "parallel": "8",
    "map": "9",
}
UNMAPPED_MODULES = {
    # The upstream suite does not have requirements 5-17 and 5-18 yet.
    "invoke.invoke_timeout",
    "invoke.invoke_timeout_caught",
}
INVOKE_TARGETS = {
    "invoke_basic": {"TARGET_FUNCTION_NAME": "TargetEcho"},
    "invoke_with_name": {"TARGET_FUNCTION_NAME": "TargetEcho"},
    "invoke_complex_object": {"TARGET_FUNCTION_NAME": "TargetEcho"},
    "invoke_null_result": {"TARGET_FUNCTION_NAME": "TargetEcho"},
    "invoke_target_fails": {"TARGET_FUNCTION_NAME": "TargetError"},
    "invoke_target_fails_caught": {"TARGET_FUNCTION_NAME": "TargetError"},
    "invoke_timeout": {"TARGET_FUNCTION_NAME": "TargetSlow"},
    "invoke_timeout_caught": {"TARGET_FUNCTION_NAME": "TargetSlow"},
    "invoke_replay_skips": {"TARGET_FUNCTION_NAME": "TargetEcho"},
    "invoke_replay_rethrows": {"TARGET_FUNCTION_NAME": "TargetError"},
    "step_then_invoke": {"TARGET_FUNCTION_NAME": "TargetEcho"},
    "invoke_then_step": {"TARGET_FUNCTION_NAME": "TargetEcho"},
    "invoke_in_child_context": {"TARGET_FUNCTION_NAME": "TargetEcho"},
    "invoke_multiple_sequential": {
        "TARGET_FUNCTION_NAME_1": "TargetEcho",
        "TARGET_FUNCTION_NAME_2": "TargetEcho",
    },
    "invoke_custom_payload_serdes": {"TARGET_FUNCTION_NAME": "TargetEcho"},
    "invoke_custom_result_serdes": {"TARGET_FUNCTION_NAME": "TargetEcho"},
    "invoke_large_payload": {"TARGET_FUNCTION_NAME": "TargetEcho"},
    "invoke_with_tenant_id": {"TARGET_FUNCTION_NAME": "TargetEchoTenant"},
}
INVOKE_HELPERS = {
    "TargetEcho": {
        "handler": "invoke.target_echo.handler",
        "description": "Echo target function",
        "durable": True,
    },
    "TargetEchoTenant": {
        "handler": "invoke.target_echo.handler",
        "description": "Tenant-isolated echo target function",
        "durable": True,
        "tenancy": True,
    },
    "TargetError": {
        "handler": "invoke.target_error.handler",
        "description": "Target function that raises an exception",
        "durable": True,
    },
    "TargetSlow": {
        "handler": "invoke.target_slow.handler",
        "description": "Target function that takes too long",
        "durable": True,
    },
    "TargetNonDurable": {
        "handler": "invoke.target_non_durable.handler",
        "description": "Plain Lambda target function",
        "durable": False,
    },
}
EXECUTION_TIMEOUT_OVERRIDES = {
    "wait.wait_duration_units": 3700,
    "wait.wait_long_duration": 7200,
}
ATTEMPTS_ENVIRONMENT_VARIABLE = "ATTEMPTS_PARAMETER_PREFIX"
ATTEMPTS_PARAMETER_PREFIX = "/durable-execution-conformance/attempts"
TEST_ID_PATTERN = re.compile(r"(?<!\d)([1-9]-\d+)(?!\d)")


@dataclass(frozen=True)
class ConformanceFunction:
    module: str
    logical_id: str
    handler: str
    description: str
    test_id: str | None
    uses_attempts_store: bool


def find_handler_node(
    tree: ast.Module,
) -> ast.FunctionDef | ast.AsyncFunctionDef | None:
    """Return a module's top-level Lambda handler."""
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.name == "handler":
                return node
    return None


def to_logical_id(value: str) -> str:
    """Convert a snake-case module name to a CloudFormation logical ID."""
    words = [word for word in re.split(r"[^A-Za-z0-9]+", value) if word]
    return "".join(word[:1].upper() + word[1:] for word in words)


def to_description(module_name: str, source: str, tree: ast.Module) -> str:
    """Build a short function description from source metadata."""
    module_docstring = ast.get_docstring(tree)
    candidates = []
    if module_docstring:
        candidates.extend(module_docstring.splitlines())
    candidates.extend(source.splitlines()[:12])

    for candidate in candidates:
        match = re.search(r"[1-9]-\d+\s*:\s*(.+)", candidate)
        if match:
            return match.group(1).strip().rstrip(".")[:256]

    return " ".join(word.capitalize() for word in module_name.split("_"))[:256]


def discover_suite(
    suite: str,
    *,
    conformance_dir: Path,
) -> list[ConformanceFunction]:
    """Discover the test functions belonging to one conformance suite."""
    functions = []
    for path in sorted((conformance_dir / suite).glob("*.py")):
        if path.name.startswith("_") or path.stem.startswith("target_"):
            continue

        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))
        if find_handler_node(tree) is None:
            continue

        module = f"{suite}.{path.stem}"
        if module in UNMAPPED_MODULES:
            test_id = None
        else:
            match = TEST_ID_PATTERN.search(ast.get_docstring(tree) or source)
            if match is None:
                raise ValueError(f"{path} does not declare a conformance test ID")
            test_id = match.group(1)

        functions.append(
            ConformanceFunction(
                module=module,
                logical_id=to_logical_id(path.stem),
                handler=f"{module}.handler",
                description=to_description(path.stem, source, tree),
                test_id=test_id,
                uses_attempts_store=(
                    ATTEMPTS_ENVIRONMENT_VARIABLE in source
                    or "support.attempts" in source
                ),
            )
        )

    validate_suite(suite, functions)
    return sorted(
        functions,
        key=lambda function: (
            function.test_id is None,
            int(function.test_id.split("-")[1]) if function.test_id else 0,
        ),
    )


def validate_suite(suite: str, functions: list[ConformanceFunction]) -> None:
    """Validate discovered requirement IDs before generating a template."""
    expected_prefix = SUITE_PREFIXES[suite]
    seen: set[str] = set()
    for function in functions:
        if function.test_id is None:
            continue
        if not function.test_id.startswith(f"{expected_prefix}-"):
            raise ValueError(
                f"{function.module} declares test {function.test_id}, "
                f"which does not belong to suite {suite}"
            )
        if function.test_id in seen:
            raise ValueError(f"duplicate test ID {function.test_id} in suite {suite}")
        seen.add(function.test_id)


def build_role(*, suite: str, uses_attempts_store: bool) -> dict[str, Any]:
    """Build the shared execution role for a suite stack."""
    policies = []
    additional_actions = []
    if suite == "invoke":
        additional_actions.append("lambda:InvokeFunction")
    if suite == "callback":
        additional_actions.extend(
            [
                "lambda:SendDurableExecutionCallbackSuccess",
                "lambda:SendDurableExecutionCallbackFailure",
                "lambda:SendDurableExecutionCallbackHeartbeat",
            ]
        )
    if additional_actions:
        policies.append(
            {
                "PolicyName": "ConformanceLambdaPolicy",
                "PolicyDocument": {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": additional_actions,
                            "Resource": "*",
                        }
                    ],
                },
            }
        )
    if uses_attempts_store:
        policies.append(
            {
                "PolicyName": "ConformanceAttemptsParameterPolicy",
                "PolicyDocument": {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": [
                                "ssm:GetParameter",
                                "ssm:PutParameter",
                            ],
                            "Resource": {
                                "Fn::Sub": (
                                    "arn:${AWS::Partition}:ssm:${AWS::Region}:"
                                    "${AWS::AccountId}:parameter"
                                    f"{ATTEMPTS_PARAMETER_PREFIX}/*"
                                )
                            },
                        }
                    ],
                },
            }
        )

    properties: dict[str, Any] = {
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
            "arn:aws:iam::aws:policy/service-role/"
            "AWSLambdaBasicDurableExecutionRolePolicy"
        ],
    }
    if policies:
        properties["Policies"] = policies

    return {
        "Type": "AWS::IAM::Role",
        "Properties": properties,
    }


def build_function_properties(
    function: ConformanceFunction,
) -> dict[str, Any]:
    """Build SAM properties for a discovered conformance function."""
    properties: dict[str, Any] = {
        "CodeUri": "../build/",
        "Handler": function.handler,
        "Description": function.description,
        "Role": {"Fn::GetAtt": ["DurableFunctionRole", "Arn"]},
        "DurableConfig": {
            **DEFAULT_DURABLE_CONFIG,
            "ExecutionTimeout": EXECUTION_TIMEOUT_OVERRIDES.get(
                function.module,
                DEFAULT_DURABLE_CONFIG["ExecutionTimeout"],
            ),
        },
    }
    environment: dict[str, Any] = {}
    if function.uses_attempts_store:
        environment[ATTEMPTS_ENVIRONMENT_VARIABLE] = ATTEMPTS_PARAMETER_PREFIX
    for variable, target in INVOKE_TARGETS.get(
        function.module.split(".")[-1], {}
    ).items():
        environment[variable] = {"Fn::Sub": f"${{{target}.Arn}}:$LATEST"}
    if environment:
        properties["Environment"] = {"Variables": environment}
    return properties


def build_template(
    suite: str,
    functions: list[ConformanceFunction],
    *,
    runtime: str = DEFAULT_RUNTIME,
) -> dict[str, Any]:
    """Build a deployable SAM template for one conformance suite."""
    uses_attempts_store = any(function.uses_attempts_store for function in functions)
    resources: dict[str, Any] = {
        "DurableFunctionRole": build_role(
            suite=suite,
            uses_attempts_store=uses_attempts_store,
        )
    }

    if suite == "invoke":
        for logical_id, helper in INVOKE_HELPERS.items():
            properties: dict[str, Any] = {
                "CodeUri": "../build/",
                "Handler": helper["handler"],
                "Description": helper["description"],
                "Role": {"Fn::GetAtt": ["DurableFunctionRole", "Arn"]},
            }
            if helper["durable"]:
                properties["DurableConfig"] = DEFAULT_DURABLE_CONFIG.copy()
            if helper.get("tenancy"):
                properties["TenancyConfig"] = {"TenantIsolationMode": "PER_TENANT"}
            resources[logical_id] = {
                "Type": "AWS::Serverless::Function",
                "Properties": properties,
            }

    for function in functions:
        resource: dict[str, Any] = {
            "Type": "AWS::Serverless::Function",
            "Properties": build_function_properties(function),
        }
        if function.test_id is not None:
            resource["TestingMetadata"] = {"TestDescription": [function.test_id]}
        resources[function.logical_id] = resource

    return {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Transform": "AWS::Serverless-2016-10-31",
        "Globals": {
            "Function": {
                "Runtime": runtime,
                "Timeout": 60,
                "MemorySize": 256 if suite == "callback" else 128,
            }
        },
        "Resources": resources,
    }


def generate_templates(
    *,
    conformance_dir: Path,
    output_dir: Path,
    runtime: str = DEFAULT_RUNTIME,
) -> list[Path]:
    """Discover handlers and write one generated template per suite."""
    output_dir.mkdir(parents=True, exist_ok=True)
    generated = []
    for suite in SUITE_PREFIXES:
        functions = discover_suite(suite, conformance_dir=conformance_dir)
        template = build_template(suite, functions, runtime=runtime)
        output_path = output_dir / f"template_{suite}.json"
        with output_path.open("w", encoding="utf-8") as file:
            json.dump(template, file, indent=2)
            file.write("\n")
        generated.append(output_path)
    return generated


def main() -> int:
    repo_dir = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(
        description="Generate SAM templates for Python conformance handlers"
    )
    parser.add_argument(
        "--conformance-dir",
        type=Path,
        default=repo_dir / "conformance",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=repo_dir / "conformance" / "generated",
    )
    parser.add_argument("--runtime", default=DEFAULT_RUNTIME)
    args = parser.parse_args()

    generated = generate_templates(
        conformance_dir=args.conformance_dir,
        output_dir=args.output_dir,
        runtime=args.runtime,
    )
    print(f"Generated {len(generated)} conformance SAM templates")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
