from pathlib import Path

from scripts.test_handlers import load_test_handlers
from scripts.test_handlers import load_test_handlers_from_file


def test_load_test_handlers_from_file_extracts_marker_handlers(tmp_path: Path) -> None:
    test_file = tmp_path / "test_example.py"
    test_file.write_text(
        """
import pytest
from examples.primitive.step import step as step_example
from examples.primitive.wait import wait
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
        "examples.primitive.step.step.handler",
        "examples.primitive.wait.wait.handler",
    }


def test_load_test_handlers_scans_test_files_only(tmp_path: Path) -> None:
    package_dir = tmp_path / "nested"
    package_dir.mkdir()
    (package_dir / "test_one.py").write_text(
        """
import pytest
from examples.extension.parallel import parallel

pytest.mark.durable_execution(handler=parallel.handler)
""",
        encoding="utf-8",
    )
    (package_dir / "helper.py").write_text(
        """
import pytest
from examples.primitive.step import step

pytest.mark.durable_execution(handler=step.handler)
""",
        encoding="utf-8",
    )

    assert load_test_handlers(tmp_path) == {
        "examples.extension.parallel.parallel.handler"
    }
