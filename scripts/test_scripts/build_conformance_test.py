from pathlib import Path
from unittest.mock import Mock

import pytest

from scripts import build_conformance as build_module
from scripts.build_conformance import build_conformance_bundle


def test_build_conformance_bundle_installs_sdk_and_copies_suites(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_dir = tmp_path / "repo"
    suite_dir = repo_dir / "conformance" / "step"
    suite_dir.mkdir(parents=True)
    (suite_dir / "__init__.py").write_text("", encoding="utf-8")
    (suite_dir / "step_basic.py").write_text("VALUE = 1\n", encoding="utf-8")
    generated_dir = repo_dir / "conformance" / "generated"
    generated_dir.mkdir()
    (generated_dir / "template.json").write_text("{}", encoding="utf-8")
    run = Mock()
    monkeypatch.setattr(build_module.subprocess, "run", run)
    output_dir = repo_dir / "conformance" / "build"

    build_conformance_bundle(repo_dir=repo_dir, output_dir=output_dir)

    run.assert_called_once()
    command = run.call_args.args[0]
    assert str(repo_dir) in command
    assert "boto3>=1.42.90,<1.43.1" in command
    assert run.call_args.kwargs == {"check": True}
    assert (output_dir / "step" / "step_basic.py").read_text() == "VALUE = 1\n"
    assert not (output_dir / "generated").exists()
