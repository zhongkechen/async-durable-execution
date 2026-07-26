"""Tests for DurableExecutionsPythonLanguageSDK module."""

import importlib.metadata
import runpy
from pathlib import Path

import pytest


def test_version_is_accessible():
    """Test __version__ is accessible from package root."""
    import async_durable_execution  # noqa: PLC0415

    assert hasattr(async_durable_execution, "__version__")
    assert isinstance(async_durable_execution.__version__, str)
    assert len(async_durable_execution.__version__) > 0


def test_read_repo_version_finds_parent_version_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    from async_durable_execution import __about__  # noqa: PLC0415

    package_file = tmp_path / "pkg" / "async_durable_execution" / "__about__.py"
    package_file.parent.mkdir(parents=True)
    version_file = tmp_path / "pkg" / "VERSION.py"
    version_file.write_text('__version__ = "1.2.3"\n', encoding="utf-8")
    monkeypatch.setattr(__about__, "__file__", str(package_file))

    assert __about__._find_version_file() == version_file  # noqa: SLF001
    assert __about__._read_repo_version() == "1.2.3"  # noqa: SLF001


def test_read_repo_version_falls_back_when_version_file_is_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    from async_durable_execution import __about__  # noqa: PLC0415

    package_file = tmp_path / "pkg" / "async_durable_execution" / "__about__.py"
    package_file.parent.mkdir(parents=True)
    monkeypatch.setattr(__about__, "__file__", str(package_file))

    assert __about__._find_version_file() is None  # noqa: SLF001
    assert __about__._read_repo_version() == "0.0.0"  # noqa: SLF001


def test_version_uses_repo_fallback_when_distribution_metadata_is_missing(
    monkeypatch: pytest.MonkeyPatch,
):
    from async_durable_execution import __about__  # noqa: PLC0415

    def raise_package_not_found(_dist_name: str) -> str:
        raise importlib.metadata.PackageNotFoundError

    monkeypatch.setattr(importlib.metadata, "version", raise_package_not_found)

    module_globals = runpy.run_path(__about__.__file__)

    assert module_globals["__version__"]
