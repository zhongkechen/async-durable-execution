import json
from pathlib import Path

import pytest

from scripts.generate_conformance_sam_templates import DEFAULT_DURABLE_CONFIG
from scripts.generate_conformance_sam_templates import ConformanceFunction
from scripts.generate_conformance_sam_templates import build_template
from scripts.generate_conformance_sam_templates import discover_suite
from scripts.generate_conformance_sam_templates import generate_templates
from scripts.generate_conformance_sam_templates import to_logical_id
from scripts.generate_conformance_sam_templates import validate_suite


REPO_DIR = Path(__file__).resolve().parents[2]


def write_handler(path: Path, source: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(source, encoding="utf-8")


def test_to_logical_id_converts_module_name() -> None:
    assert to_logical_id("wait_for_callback_basic") == "WaitForCallbackBasic"


def test_discover_suite_reads_ids_and_attempts_store_usage(tmp_path: Path) -> None:
    write_handler(
        tmp_path / "step" / "custom_case.py",
        '''
"""1-21: Custom step case."""
import os

async def handler(event):
    return os.environ["ATTEMPTS_PARAMETER_PREFIX"], event
''',
    )

    functions = discover_suite("step", conformance_dir=tmp_path)

    assert functions == [
        ConformanceFunction(
            module="step.custom_case",
            logical_id="CustomCase",
            handler="step.custom_case.handler",
            description="Custom step case",
            test_id="1-21",
            uses_attempts_store=True,
        )
    ]


def test_discover_suite_rejects_missing_test_id(tmp_path: Path) -> None:
    write_handler(
        tmp_path / "step" / "custom_case.py",
        "async def handler(event):\n    return event\n",
    )

    with pytest.raises(ValueError, match="does not declare"):
        discover_suite("step", conformance_dir=tmp_path)


def test_validate_suite_rejects_duplicate_ids() -> None:
    function = ConformanceFunction(
        module="step.one",
        logical_id="One",
        handler="step.one.handler",
        description="One",
        test_id="1-1",
        uses_attempts_store=False,
    )

    with pytest.raises(ValueError, match="duplicate test ID"):
        validate_suite("step", [function, function])


def test_repository_sources_cover_all_upstream_requirements() -> None:
    expected_counts = {
        "step": 20,
        "wait": 5,
        "child": 18,
        "callback": 19,
        "invoke": 16,
        "wait_for_condition": 13,
        "wait_for_callback": 15,
        "parallel": 22,
        "map": 20,
    }

    for suite, expected_count in expected_counts.items():
        functions = discover_suite(suite, conformance_dir=REPO_DIR / "conformance")
        assert sum(function.test_id is not None for function in functions) == (
            expected_count
        )


def test_build_template_adds_attempts_parameter_permissions_and_environment() -> None:
    function = ConformanceFunction(
        module="step.step_with_retry",
        logical_id="StepWithRetry",
        handler="step.step_with_retry.handler",
        description="Step with retry",
        test_id="1-11",
        uses_attempts_store=True,
    )

    template = build_template("step", [function], runtime="python3.14")

    assert template["Globals"]["Function"]["Runtime"] == "python3.14"
    role = template["Resources"]["DurableFunctionRole"]["Properties"]
    assert role["Policies"][0]["PolicyName"] == "ConformanceAttemptsParameterPolicy"
    properties = template["Resources"]["StepWithRetry"]["Properties"]
    assert properties["CodeUri"] == "../build/"
    assert properties["DurableConfig"] == DEFAULT_DURABLE_CONFIG
    assert properties["Environment"]["Variables"]["ATTEMPTS_PARAMETER_PREFIX"] == (
        "/durable-execution-conformance/attempts"
    )


def test_build_invoke_template_wires_targets() -> None:
    function = ConformanceFunction(
        module="invoke.invoke_with_tenant_id",
        logical_id="InvokeWithTenantId",
        handler="invoke.invoke_with_tenant_id.handler",
        description="Invoke with tenant ID",
        test_id="5-8",
        uses_attempts_store=False,
    )

    template = build_template("invoke", [function])

    resources = template["Resources"]
    assert resources["TargetEchoTenant"]["Properties"]["TenancyConfig"] == {
        "TenantIsolationMode": "PER_TENANT"
    }
    assert "DurableConfig" not in resources["TargetNonDurable"]["Properties"]
    variables = resources["InvokeWithTenantId"]["Properties"]["Environment"][
        "Variables"
    ]
    assert variables["TARGET_FUNCTION_NAME"] == {
        "Fn::Sub": "${TargetEchoTenant.Arn}:$LATEST"
    }


def test_generate_templates_writes_each_suite(tmp_path: Path) -> None:
    conformance_dir = REPO_DIR / "conformance"
    output_dir = tmp_path / "generated"

    generated = generate_templates(
        conformance_dir=conformance_dir,
        output_dir=output_dir,
        runtime="python3.14",
    )

    assert len(generated) == 9
    template = json.loads(
        (output_dir / "template_callback.json").read_text(encoding="utf-8")
    )
    assert template["Globals"]["Function"] == {
        "Runtime": "python3.14",
        "Timeout": 60,
        "MemorySize": 256,
    }
