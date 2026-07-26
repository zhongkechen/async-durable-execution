from __future__ import annotations

import subprocess
import zipfile
from pathlib import Path
from typing import Sequence
from unittest.mock import Mock

import pytest

from scripts import build_layer as build_layer_module
from scripts.build_layer import LayerBuildResult
from scripts.build_layer import _load_version_from_path
from scripts.build_layer import _pip_install
from scripts.build_layer import build_layer
from scripts.build_layer import create_layer_archive
from scripts.build_layer import default_sdk_spec
from scripts.build_layer import main
from scripts.build_layer import parse_args


def test_default_sdk_spec_uses_shared_version() -> None:
    namespace: dict[str, str] = {}
    version_file = (
        Path(__file__).resolve().parents[2] / "async_durable_execution" / "__about__.py"
    )
    exec(version_file.read_text(encoding="utf-8"), namespace)

    assert default_sdk_spec() == f"async-durable-execution=={namespace['__version__']}"


def test_create_layer_archive_zips_python_directory(tmp_path: Path) -> None:
    staging_dir = tmp_path / "staging"
    package_dir = staging_dir / "python" / "async_durable_execution"
    package_dir.mkdir(parents=True)
    (package_dir / "__init__.py").write_text("value = 1\n")
    pycache_dir = package_dir / "__pycache__"
    pycache_dir.mkdir()
    (pycache_dir / "__init__.cpython-313.pyc").write_bytes(b"compiled")

    output_path = tmp_path / "layer.zip"

    file_count = create_layer_archive(staging_dir, output_path)

    assert file_count == 1
    with zipfile.ZipFile(output_path) as archive:
        assert archive.namelist() == ["python/async_durable_execution/__init__.py"]
        info = archive.getinfo("python/async_durable_execution/__init__.py")
        assert info.date_time == (1980, 1, 1, 0, 0, 0)


def test_create_layer_archive_replaces_existing_archive(tmp_path: Path) -> None:
    staging_dir = tmp_path / "staging"
    package_dir = staging_dir / "python" / "async_durable_execution"
    package_dir.mkdir(parents=True)
    (package_dir / "module.py").write_text("value = 1\n")
    (package_dir / "module.pyo").write_bytes(b"compiled")

    output_path = tmp_path / "layer.zip"
    output_path.write_text("old archive")

    file_count = create_layer_archive(staging_dir, output_path)

    assert file_count == 1
    with zipfile.ZipFile(output_path) as archive:
        assert archive.namelist() == ["python/async_durable_execution/module.py"]


def test_create_layer_archive_requires_python_directory(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="python"):
        create_layer_archive(tmp_path, tmp_path / "layer.zip")


def test_build_layer_installs_sdk_and_returns_archive_details(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[dict[str, object]] = []

    def fake_pip_install(
        *,
        python_executable: str,
        target_dir: Path,
        sdk_source: str,
        pip_args: Sequence[str],
    ) -> None:
        calls.append(
            {
                "python_executable": python_executable,
                "target_dir": target_dir,
                "sdk_source": sdk_source,
                "pip_args": pip_args,
            }
        )
        (target_dir / "async_durable_execution").mkdir()
        (target_dir / "async_durable_execution" / "__init__.py").write_text(
            "value = 1\n"
        )

    monkeypatch.setattr(build_layer_module, "_pip_install", fake_pip_install)

    output_path = tmp_path / "dist" / "layer.zip"
    result = build_layer(
        output_path=output_path,
        sdk_source=".",
        python_executable="python-test",
        pip_args=("--quiet",),
    )

    assert result == LayerBuildResult(
        output_path=output_path.resolve(),
        sdk_source=".",
        file_count=1,
        size_bytes=output_path.stat().st_size,
    )
    assert len(calls) == 1
    call = calls[0]
    assert call["python_executable"] == "python-test"
    assert isinstance(call["target_dir"], Path)
    assert call["target_dir"].name == "python"
    assert call["sdk_source"] == "."
    assert call["pip_args"] == ("--quiet",)


def test_pip_install_invokes_pip_with_extra_args(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = Mock()
    monkeypatch.setattr(build_layer_module.subprocess, "run", run)

    _pip_install(
        python_executable="python-test",
        target_dir=tmp_path / "python",
        sdk_source="async-durable-execution==1.2.3",
        pip_args=("--no-deps", "--quiet"),
    )

    run.assert_called_once_with(
        [
            "python-test",
            "-m",
            "pip",
            "install",
            "--upgrade",
            "--no-compile",
            "--target",
            str(tmp_path / "python"),
            "--no-deps",
            "--quiet",
            "async-durable-execution==1.2.3",
        ],
        check=True,
    )


def test_load_version_from_path_requires_loadable_spec(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    version_path = tmp_path / "__about__.py"
    version_path.write_text("__version__ = '1.2.3'\n")
    monkeypatch.setattr(
        build_layer_module.importlib.util,
        "spec_from_file_location",
        lambda *args: None,
    )

    with pytest.raises(RuntimeError, match="Unable to load version"):
        _load_version_from_path(version_path)


def test_parse_args_reads_layer_options(tmp_path: Path) -> None:
    args = parse_args(
        [
            "--output",
            str(tmp_path / "layer.zip"),
            "--sdk-source",
            "./sdk",
            "--python",
            "python-test",
            "--pip-arg=--no-deps",
            "--pip-arg=--quiet",
            "--verbose",
        ]
    )

    assert args.output == tmp_path / "layer.zip"
    assert args.sdk_source == "./sdk"
    assert args.python == "python-test"
    assert args.pip_arg == ["--no-deps", "--quiet"]
    assert args.verbose is True


def test_main_prints_build_summary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    result = LayerBuildResult(
        output_path=tmp_path / "layer.zip",
        sdk_source="./sdk",
        file_count=2,
        size_bytes=123,
    )
    build = Mock(return_value=result)
    monkeypatch.setattr(build_layer_module, "build_layer", build)

    exit_code = main(["--output", str(tmp_path / "layer.zip"), "--sdk-source", "./sdk"])

    assert exit_code == 0
    assert "Built Lambda layer" in capsys.readouterr().out
    build.assert_called_once()


def test_main_returns_error_for_pip_failure(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(
        build_layer_module,
        "build_layer",
        Mock(side_effect=subprocess.CalledProcessError(1, ["pip"])),
    )

    assert main([]) == 1
    assert "Failed to install layer dependencies" in capsys.readouterr().err


def test_main_returns_error_for_build_failure(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(
        build_layer_module,
        "build_layer",
        Mock(side_effect=ValueError("bad staging")),
    )

    assert main([]) == 1
    assert "Failed to build layer: bad staging" in capsys.readouterr().err
