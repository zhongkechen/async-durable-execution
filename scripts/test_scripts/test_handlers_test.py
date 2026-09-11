from pathlib import Path

from scripts.test_handlers import load_test_handlers
from scripts.test_handlers import load_test_handlers_from_file


def test_every_example_handler_has_a_runner_test() -> None:
    from scripts.generate_sam_template import build_examples_catalog

    repo_root = Path(__file__).resolve().parents[2]
    examples = {example["handler"] for example in build_examples_catalog()["examples"]}
    tested = load_test_handlers(repo_root / "test_examples")

    assert examples <= tested, (
        f"Examples missing runner tests: {sorted(examples - tested)}"
    )


def test_load_test_handlers_from_runner_calls(tmp_path: Path) -> None:
    test_file = tmp_path / "test_example.py"
    test_file.write_text(
        """
from examples.step import step as step_example
from examples.wait import wait
from unrelated import other

async def test_example(durable_runner):
    async with durable_runner(handler=step_example.handler, input=None) as runner:
        await runner.run()
    async with durable_runner(handler=wait.handler) as runner:
        await runner.run()
    durable_runner(handler=other.handler)
    unrelated_factory(handler=step_example.handler)
""",
        encoding="utf-8",
    )

    assert load_test_handlers_from_file(test_file) == {
        "examples.step.step.handler",
        "examples.wait.wait.handler",
    }


def test_load_test_handlers_from_file_extracts_marker_handlers(tmp_path: Path) -> None:
    test_file = tmp_path / "test_example.py"
    test_file.write_text(
        """
import pytest
from examples.step import step as step_example
from examples.wait import wait
from unrelated import module

pytestmark = pytest.mark.durable_execution(handler=step_example.handler)
pytest.mark.durable_execution(timeout=1, handler=step_example.handler)
pytest.mark.durable_execution(handler=wait.handler)
pytest.mark.other(handler=module.handler)
pytest.mark.durable_execution(handler="not-an-attribute")
""",
        encoding="utf-8",
    )

    assert load_test_handlers_from_file(test_file) == {
        "examples.step.step.handler",
        "examples.wait.wait.handler",
    }


def test_load_test_handlers_scans_test_files_only(tmp_path: Path) -> None:
    package_dir = tmp_path / "nested"
    package_dir.mkdir()
    (package_dir / "test_one.py").write_text(
        """
import pytest
from examples.parallel import parallel

pytest.mark.durable_execution(handler=parallel.handler)
""",
        encoding="utf-8",
    )
    (package_dir / "helper.py").write_text(
        """
import pytest
from examples.step import step

pytest.mark.durable_execution(handler=step.handler)
""",
        encoding="utf-8",
    )

    assert load_test_handlers(tmp_path) == {"examples.parallel.parallel.handler"}
