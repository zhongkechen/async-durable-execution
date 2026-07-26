import json
from pathlib import Path
from unittest.mock import Mock

import pytest

from scripts import generate_sam_template as sam_module
from scripts.generate_sam_template import DEFAULT_DURABLE_CONFIG
from scripts.generate_sam_template import DEFAULT_LAMBDA_ENDPOINT
from scripts.generate_sam_template import DEFAULT_RUNTIME
from scripts.generate_sam_template import SDK_LAYER_LOGICAL_ID
from scripts.generate_sam_template import build_example_entry
from scripts.generate_sam_template import build_examples_catalog
from scripts.generate_sam_template import build_template
from scripts.generate_sam_template import find_handler_node
from scripts.generate_sam_template import first_line
from scripts.generate_sam_template import generate_sam_template
from scripts.generate_sam_template import get_description
from scripts.generate_sam_template import load_catalog
from scripts.generate_sam_template import main
from scripts.generate_sam_template import to_example_name
from scripts.generate_sam_template import validate_catalog_test_coverage


def write_module(path: Path, source: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(source, encoding="utf-8")


def test_first_line_returns_first_non_empty_docstring_line() -> None:
    assert first_line("\n\n  First line.  \nSecond line.") == "First line."
    assert first_line("\n  \n") == ""


def test_to_example_name_removes_repeated_prefix_words() -> None:
    assert to_example_name(Path("wait_for_callback/wait_for_callback_timeout.py")) == (
        "Wait For Callback Timeout"
    )


def test_find_handler_node_finds_sync_or_async_handler() -> None:
    sync_tree = sam_module.ast.parse("def handler(event, context):\n    return event\n")
    async_tree = sam_module.ast.parse(
        "async def handler(event, context):\n    return event\n"
    )
    missing_tree = sam_module.ast.parse("def helper():\n    return None\n")

    assert find_handler_node(sync_tree) is not None
    assert find_handler_node(async_tree) is not None
    assert find_handler_node(missing_tree) is None


def test_get_description_prefers_handler_docstring_then_module_docstring() -> None:
    handler_tree = sam_module.ast.parse(
        '"""Module docs."""\nasync def handler(event):\n    """Handler docs."""\n'
    )
    handler_node = find_handler_node(handler_tree)
    assert handler_node is not None

    assert (
        get_description(handler_tree, handler_node, Path("one.py")) == "Handler docs."
    )

    module_tree = sam_module.ast.parse(
        '"""Module docs."""\nasync def handler(event):\n    pass\n'
    )
    module_node = find_handler_node(module_tree)
    assert module_node is not None
    assert get_description(module_tree, module_node, Path("two.py")) == "Module docs."

    fallback_tree = sam_module.ast.parse("async def handler(event):\n    pass\n")
    fallback_node = find_handler_node(fallback_tree)
    assert fallback_node is not None
    assert get_description(fallback_tree, fallback_node, Path("plain_example.py")) == (
        "Example for Plain Example."
    )


def test_build_example_entry_reads_handler_metadata(tmp_path: Path) -> None:
    source_root = tmp_path / sam_module.PACKAGE_PREFIX
    module_path = source_root / "logger_example" / "logger_example.py"
    write_module(
        module_path,
        """
async def handler(event):
    \"\"\"Log durable execution details.\"\"\"
    return event
""",
    )

    entry = build_example_entry(module_path, source_root)

    assert entry is not None
    assert entry == {
        "name": "Logger Example",
        "description": "Log durable execution details.",
        "handler": ("examples.logger_example.logger_example.handler"),
        "integration": True,
        "durableConfig": DEFAULT_DURABLE_CONFIG,
        "path": ("./examples/logger_example/logger_example.py"),
        "loggingConfig": {"ApplicationLogLevel": "INFO", "LogFormat": "JSON"},
    }
    assert entry["durableConfig"] is not DEFAULT_DURABLE_CONFIG
    assert (
        entry["loggingConfig"]
        is not sam_module.SPECIAL_LOGGING_CONFIG["logger_example/logger_example.py"]
    )


def test_build_example_entry_returns_none_without_handler(tmp_path: Path) -> None:
    source_root = tmp_path / sam_module.PACKAGE_PREFIX
    module_path = source_root / "helper.py"
    write_module(module_path, "def helper():\n    return None\n")

    assert build_example_entry(module_path, source_root) is None


def test_build_examples_catalog_scans_package_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_dir = tmp_path / "repo"
    scripts_dir = repo_dir / "scripts"
    scripts_dir.mkdir(parents=True)
    source_root = repo_dir / sam_module.PACKAGE_PREFIX
    write_module(source_root / "__init__.py", "")
    write_module(source_root / "__about__.py", "__version__ = '1.0.0'\n")
    write_module(
        source_root / "step" / "step.py",
        '"""Step module docs."""\nasync def handler(event):\n    return event\n',
    )
    write_module(source_root / "no_handler.py", "VALUE = 1\n")
    monkeypatch.setattr(
        sam_module, "__file__", str(scripts_dir / "generate_sam_template.py")
    )

    catalog = build_examples_catalog()

    assert catalog["packageName"] == sam_module.PACKAGE_NAME
    assert catalog["examples"] == [
        {
            "name": "Step",
            "description": "Step module docs.",
            "handler": "examples.step.step.handler",
            "integration": True,
            "durableConfig": DEFAULT_DURABLE_CONFIG,
            "path": "./examples/step/step.py",
        }
    ]


def test_load_catalog_delegates_to_catalog_builder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog = {"packageName": "test", "examples": []}
    monkeypatch.setattr(
        sam_module, "build_examples_catalog", Mock(return_value=catalog)
    )

    assert load_catalog() is catalog


def test_build_template_adds_layer_role_and_functions() -> None:
    template = build_template(
        [
            {
                "handler": "examples.step.step.handler",
                "description": "Step example.",
                "durableConfig": {"ExecutionTimeout": 10},
            }
        ],
        runtime="python3.14",
    )

    assert template["Globals"]["Function"]["Runtime"] == "python3.14"
    assert (
        template["Parameters"]["LambdaEndpoint"]["Default"] == DEFAULT_LAMBDA_ENDPOINT
    )
    assert template["Resources"][SDK_LAYER_LOGICAL_ID]["Properties"][
        "CompatibleRuntimes"
    ] == ["python3.14"]
    function = template["Resources"]["AsyncDurableExecutionExamplesStepStep"][
        "Properties"
    ]
    assert function["Handler"] == "examples.step.step.handler"
    assert function["DurableConfig"] == {"ExecutionTimeout": 10}
    assert function["FunctionName"] == {"Fn::Sub": "${FunctionNamePrefix}StepStep"}


def test_validate_catalog_test_coverage_accepts_known_handlers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    scripts_dir = tmp_path / "repo" / "scripts"
    scripts_dir.mkdir(parents=True)
    monkeypatch.setattr(
        sam_module, "__file__", str(scripts_dir / "generate_sam_template.py")
    )
    monkeypatch.setattr(
        sam_module,
        "load_test_handlers",
        Mock(return_value={"examples.step.step.handler"}),
    )

    validate_catalog_test_coverage(
        {"examples": [{"handler": "examples.step.step.handler"}]}
    )


def test_validate_catalog_test_coverage_reports_missing_handlers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    scripts_dir = tmp_path / "repo" / "scripts"
    scripts_dir.mkdir(parents=True)
    monkeypatch.setattr(
        sam_module, "__file__", str(scripts_dir / "generate_sam_template.py")
    )
    monkeypatch.setattr(
        sam_module,
        "load_test_handlers",
        Mock(return_value={"examples.step.step.handler"}),
    )

    with pytest.raises(SystemExit, match="missing from the generated examples catalog"):
        validate_catalog_test_coverage({"examples": []})


def test_generate_sam_template_writes_template(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    catalog = {
        "examples": [
            {
                "handler": "examples.step.step.handler",
                "description": "Step example.",
                "durableConfig": {"ExecutionTimeout": 10},
            }
        ]
    }
    validate = Mock()
    monkeypatch.setattr(sam_module, "load_catalog", Mock(return_value=catalog))
    monkeypatch.setattr(sam_module, "validate_catalog_test_coverage", validate)

    output_path = generate_sam_template(
        output_path=tmp_path / "template.json",
        runtime="python3.14",
    )

    assert output_path == tmp_path / "template.json"
    template = json.loads(output_path.read_text(encoding="utf-8"))
    assert template["Globals"]["Function"]["Runtime"] == "python3.14"
    assert "AsyncDurableExecutionExamplesStepStep" in template["Resources"]
    validate.assert_called_once_with(catalog)


def test_generate_sam_template_selects_named_example(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    catalog = {
        "examples": [
            {
                "name": "Hello World",
                "handler": "examples.hello_world.handler",
                "description": "Hello World example.",
            },
            {
                "name": "Step",
                "handler": "examples.step.step.handler",
                "description": "Step example.",
            },
        ]
    }
    monkeypatch.setattr(sam_module, "load_catalog", Mock(return_value=catalog))
    monkeypatch.setattr(sam_module, "validate_catalog_test_coverage", Mock())

    output_path = generate_sam_template(
        output_path=tmp_path / "template.json",
        example_name="hello world",
    )

    template = json.loads(output_path.read_text(encoding="utf-8"))
    assert "AsyncDurableExecutionExamplesHelloWorld" in template["Resources"]
    assert "AsyncDurableExecutionExamplesStepStep" not in template["Resources"]


def test_generate_sam_template_rejects_unknown_example(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    catalog = {
        "examples": [
            {
                "name": "Hello World",
                "handler": "examples.hello_world.handler",
                "description": "Hello World example.",
            }
        ]
    }
    monkeypatch.setattr(sam_module, "load_catalog", Mock(return_value=catalog))
    monkeypatch.setattr(sam_module, "validate_catalog_test_coverage", Mock())

    with pytest.raises(ValueError, match="Unknown example 'Missing'"):
        generate_sam_template(
            output_path=tmp_path / "template.json",
            example_name="Missing",
        )


def test_main_generates_template_and_prints_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    output_path = tmp_path / "template.json"
    generate = Mock(return_value=output_path)
    monkeypatch.setattr(
        sam_module.sys,
        "argv",
        [
            "generate_sam_template.py",
            "--output",
            str(output_path),
            "--runtime",
            "python3.14",
            "--example-name",
            "Hello World",
        ],
    )
    monkeypatch.setattr(sam_module, "generate_sam_template", generate)

    assert main() == 0
    assert str(output_path) in capsys.readouterr().out
    generate.assert_called_once_with(
        output_path=output_path,
        runtime="python3.14",
        example_name="Hello World",
    )
