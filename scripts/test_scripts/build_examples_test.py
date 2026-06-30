import subprocess
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

from scripts import build_examples as build_examples_module


def test_build_examples_recreates_build_dir_and_installs_package(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_dir = tmp_path / "repo"
    scripts_dir = repo_dir / "scripts"
    examples_dir = repo_dir / "async-durable-execution-examples"
    build_dir = examples_dir / "build"
    scripts_dir.mkdir(parents=True)
    build_dir.mkdir(parents=True)
    (build_dir / "old.txt").write_text("old", encoding="utf-8")

    run = Mock()
    monkeypatch.setattr(
        build_examples_module, "__file__", str(scripts_dir / "build_examples.py")
    )
    monkeypatch.setattr(build_examples_module.subprocess, "run", run)

    build_examples_module.build_examples()

    assert build_dir.exists()
    assert not (build_dir / "old.txt").exists()
    run.assert_called_once_with(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--upgrade",
            "--no-deps",
            "--target",
            str(build_dir),
            str(examples_dir),
        ],
        check=True,
    )


def test_main_returns_error_when_build_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        build_examples_module,
        "build_examples",
        Mock(side_effect=subprocess.CalledProcessError(1, ["pip"])),
    )

    assert build_examples_module.main() == 1


def test_main_returns_success(monkeypatch: pytest.MonkeyPatch) -> None:
    build = Mock()
    monkeypatch.setattr(build_examples_module, "build_examples", build)

    assert build_examples_module.main() == 0
    build.assert_called_once_with()
