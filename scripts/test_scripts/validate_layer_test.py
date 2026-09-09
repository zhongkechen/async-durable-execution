"""Archive validation must inspect the installed artifact, not host dependencies."""

from __future__ import annotations

import json
from pathlib import Path
import zipfile

import pytest

from scripts.validate_layer import main, validate_layer


CLIENT = """
class AsyncClient:
    async def __aenter__(self):
        return self
    async def __aexit__(self, *args):
        pass
"""


def layer_archive(path: Path, *, versions=("4.14.2",), client=CLIENT) -> Path:
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(
            "python/async_durable_execution/__init__.py", "__version__ = 'test'\n"
        )
        archive.writestr("python/anyio/__init__.py", "")
        if client is not None:
            archive.writestr("python/httpx/__init__.py", client)
        for version in versions:
            archive.writestr(
                f"python/anyio-{version}.dist-info/METADATA",
                f"Metadata-Version: 2.1\nName: anyio\nVersion: {version}\n",
            )
    return path


def test_valid_layer_uses_its_own_packages(tmp_path: Path) -> None:
    layer = layer_archive(tmp_path / "layer.zip")
    result = json.loads(validate_layer(layer, expected_anyio="4.14.2"))
    assert result["sdk"] == "test"
    assert result["anyio"] == "4.14.2"


@pytest.mark.parametrize("versions", [("4.15.1",), (), ("4.14.2", "4.15.1")])
def test_incompatible_missing_or_duplicate_anyio_is_rejected(
    tmp_path: Path, versions
) -> None:
    layer = layer_archive(tmp_path / "layer.zip", versions=versions)
    with pytest.raises(ValueError, match="AnyIO"):
        validate_layer(layer, expected_anyio="4.14.2")


def test_host_httpx_cannot_conceal_a_missing_layer_dependency(tmp_path: Path) -> None:
    layer = layer_archive(tmp_path / "layer.zip", client=None)
    with pytest.raises(ValueError, match="No module named 'httpx'"):
        validate_layer(layer, expected_anyio="4.14.2")


def test_runtime_failure_blocks_validation_even_with_correct_version(
    tmp_path: Path,
) -> None:
    layer = layer_archive(
        tmp_path / "layer.zip",
        client=CLIENT.replace(
            "        pass", "        raise RuntimeError('broken backend')"
        ),
    )
    with pytest.raises(ValueError, match="broken backend"):
        validate_layer(layer, expected_anyio="4.14.2")


def test_cli_returns_failure_for_incompatible_archive(tmp_path: Path, capsys) -> None:
    layer = layer_archive(tmp_path / "layer.zip", versions=("4.15.1",))
    assert main([str(layer), "--expected-anyio", "4.14.2"]) == 1
    assert "found 4.15.1" in capsys.readouterr().err
