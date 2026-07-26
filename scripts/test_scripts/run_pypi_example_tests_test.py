from pathlib import Path
from subprocess import CompletedProcess
from unittest.mock import Mock

import pytest

from scripts import run_pypi_example_tests as pypi_tests


def create_test_sources(repo_root: Path) -> None:
    for directory in pypi_tests.STAGED_DIRECTORIES:
        package_dir = repo_root / directory
        package_dir.mkdir(parents=True)
        (package_dir / "__init__.py").write_text("", encoding="utf-8")

    (repo_root / "examples" / "function_naming.py").write_text("", encoding="utf-8")


def test_stage_test_sources_copies_only_required_sources(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()
    create_test_sources(repo_root)
    pycache_dir = repo_root / "examples" / "__pycache__"
    pycache_dir.mkdir()
    (pycache_dir / "ignored.pyc").write_bytes(b"compiled")

    pypi_tests.stage_test_sources(repo_root, staging_dir)

    assert (staging_dir / "examples" / "__init__.py").is_file()
    assert (staging_dir / "examples" / "function_naming.py").is_file()
    assert (staging_dir / "test_examples" / "__init__.py").is_file()
    assert not (staging_dir / "scripts").exists()
    assert not (staging_dir / "examples" / "__pycache__").exists()
    assert not (staging_dir / "async_durable_execution").exists()


def test_isolated_environment_removes_pythonpath(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PYTHONPATH", "/repo")
    monkeypatch.setenv("PRESERVED", "value")

    environment = pypi_tests.isolated_environment()

    assert "PYTHONPATH" not in environment
    assert environment["PRESERVED"] == "value"


def test_find_sdk_source_returns_imported_package_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = Mock(
        return_value=CompletedProcess(
            args=[],
            returncode=0,
            stdout="/venv/site-packages/async_durable_execution/__init__.py\n",
            stderr="",
        )
    )
    monkeypatch.setattr(pypi_tests.subprocess, "run", run)

    source = pypi_tests.find_sdk_source(tmp_path, {"TEST": "1"})

    assert source == Path("/venv/site-packages/async_durable_execution/__init__.py")
    assert run.call_args.kwargs["cwd"] == tmp_path
    assert run.call_args.kwargs["env"] == {"TEST": "1"}


def test_find_sdk_source_reports_import_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        pypi_tests.subprocess,
        "run",
        Mock(
            return_value=CompletedProcess(
                args=[],
                returncode=1,
                stdout="",
                stderr="module not found",
            )
        ),
    )

    with pytest.raises(RuntimeError, match="module not found"):
        pypi_tests.find_sdk_source(tmp_path, {})


def test_find_sdk_source_requires_site_packages(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        pypi_tests.subprocess,
        "run",
        Mock(
            return_value=CompletedProcess(
                args=[],
                returncode=1,
                stdout="",
                stderr=(
                    "RuntimeError: SDK was not imported from site-packages: "
                    "/checkout/async_durable_execution/__init__.py"
                ),
            )
        ),
    )

    with pytest.raises(RuntimeError, match="not imported from site-packages"):
        pypi_tests.find_sdk_source(tmp_path, {})


def test_run_staged_tests_rejects_local_sdk(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_root = tmp_path / "repo"
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()
    create_test_sources(repo_root)
    local_sdk = repo_root / "async_durable_execution" / "__init__.py"
    monkeypatch.setattr(pypi_tests, "find_sdk_source", Mock(return_value=local_sdk))

    with pytest.raises(RuntimeError, match="imported the local SDK"):
        pypi_tests.run_staged_tests(repo_root, staging_dir, [])


def test_run_staged_tests_runs_pytest_from_staging(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_root = tmp_path / "repo"
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()
    create_test_sources(repo_root)
    monkeypatch.setattr(
        pypi_tests,
        "find_sdk_source",
        Mock(
            return_value=Path("/venv/site-packages/async_durable_execution/__init__.py")
        ),
    )
    run = Mock(return_value=CompletedProcess(args=[], returncode=0))
    monkeypatch.setattr(pypi_tests.subprocess, "run", run)

    result = pypi_tests.run_staged_tests(repo_root, staging_dir, ["-q"])

    assert result == 0
    command = run.call_args.args[0]
    assert command[:3] == [pypi_tests.sys.executable, "-m", "pytest"]
    assert command[-2:] == ["test_examples", "-q"]
    assert run.call_args.kwargs["cwd"] == staging_dir
    assert "PYTHONPATH" not in run.call_args.kwargs["env"]


def test_main_returns_error_when_staging_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        pypi_tests,
        "run_tests",
        Mock(side_effect=RuntimeError("local SDK imported")),
    )

    assert pypi_tests.main() == 1


def test_main_returns_test_exit_code(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pypi_tests, "run_tests", Mock(return_value=5))

    assert pypi_tests.main() == 5
