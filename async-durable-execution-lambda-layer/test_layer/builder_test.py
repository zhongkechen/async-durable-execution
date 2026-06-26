from __future__ import annotations

import zipfile
from pathlib import Path

import pytest

from async_durable_execution_lambda_layer.builder import create_layer_archive
from async_durable_execution_lambda_layer.builder import default_sdk_spec


def test_default_sdk_spec_uses_shared_version() -> None:
    assert default_sdk_spec() == "async-durable-execution==2.0.0b1"


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


def test_create_layer_archive_requires_python_directory(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="python"):
        create_layer_archive(tmp_path, tmp_path / "layer.zip")
