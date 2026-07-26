from pathlib import Path
from unittest.mock import Mock

import pytest

from scripts import build_examples as build_examples_module


def test_build_examples_recreates_build_dir_and_copies_package(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_dir = tmp_path / "repo"
    scripts_dir = repo_dir / "scripts"
    examples_dir = repo_dir / "examples"
    build_dir = repo_dir / "build" / "lambda"
    scripts_dir.mkdir(parents=True)
    examples_dir.mkdir()
    (examples_dir / "__init__.py").write_text("", encoding="utf-8")
    pycache_dir = examples_dir / "__pycache__"
    pycache_dir.mkdir()
    (pycache_dir / "ignored.pyc").write_bytes(b"compiled")
    build_dir.mkdir(parents=True)
    (build_dir / "old.txt").write_text("old", encoding="utf-8")

    monkeypatch.setattr(
        build_examples_module, "__file__", str(scripts_dir / "build_examples.py")
    )

    build_examples_module.build_examples()

    assert build_dir.exists()
    assert not (build_dir / "old.txt").exists()
    assert (build_dir / "examples" / "__init__.py").is_file()
    assert not (build_dir / "examples" / "__pycache__").exists()


def test_main_returns_error_when_build_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        build_examples_module,
        "build_examples",
        Mock(side_effect=OSError("copy failed")),
    )

    assert build_examples_module.main() == 1


def test_main_returns_success(monkeypatch: pytest.MonkeyPatch) -> None:
    build = Mock()
    monkeypatch.setattr(build_examples_module, "build_examples", build)

    assert build_examples_module.main() == 0
    build.assert_called_once_with()
