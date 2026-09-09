"""Validate dependencies and smoke-test the actual Lambda layer archive."""

from __future__ import annotations

import argparse
from email.parser import BytesParser
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from typing import Sequence
import zipfile


SMOKE_PROGRAM = """
import asyncio
import builtins
import importlib.metadata
import json
from pathlib import Path
import sys

layer = Path(sys.argv[1]).resolve()
sys.path.insert(0, str(layer))
import async_durable_execution
import httpx
import anyio

for module in (async_durable_execution, httpx, anyio):
    if not Path(module.__file__).resolve().is_relative_to(layer):
        raise RuntimeError(f"{module.__name__} was imported outside the layer")

# Bootstrap this interpreter's stdlib before emulating the Lambda 3.15 preview.
if sys.version_info[:2] == (3, 15) and hasattr(builtins, "sentinel"):
    del builtins.sentinel

async def smoke():
    async with httpx.AsyncClient():
        pass

asyncio.run(smoke())
print(json.dumps({"python": sys.version.split()[0],
                  "sdk": async_durable_execution.__version__,
                  "anyio": importlib.metadata.version("anyio")}))
"""


def validate_layer(layer: Path, *, expected_anyio: str) -> str:
    """Check packaged metadata, then import and close HTTPX using only this ZIP.

    The child interpreter disables site packages, so dependencies installed on the
    build runner cannot conceal missing or incompatible packages in the layer.
    """
    with zipfile.ZipFile(layer) as archive:
        metadata = [
            name
            for name in archive.namelist()
            if name.startswith("python/anyio-") and name.endswith(".dist-info/METADATA")
        ]
        if len(metadata) != 1:
            raise ValueError("Layer must contain exactly one AnyIO distribution")
        package = BytesParser().parsebytes(archive.read(metadata[0]))
        if (
            package.get("Name", "").lower() != "anyio"
            or package.get("Version") != expected_anyio
        ):
            raise ValueError(
                f"Layer must contain AnyIO {expected_anyio}; found {package.get('Version')}"
            )
        # Keep scratch data under TMPDIR when supplied, otherwise in the caller's cwd.
        with tempfile.TemporaryDirectory(
            prefix="validate-layer-", dir=os.environ.get("TMPDIR") or Path.cwd()
        ) as temp:
            root = Path(temp).resolve()
            for member in archive.infolist():
                if not (root / member.filename).resolve().is_relative_to(root):
                    raise ValueError(
                        "Layer contains a path outside the extraction directory"
                    )
            archive.extractall(root)
            result = subprocess.run(
                [sys.executable, "-I", "-S", "-c", SMOKE_PROGRAM, str(root / "python")],
                cwd=root,
                capture_output=True,
                text=True,
                timeout=60,
            )
            if result.returncode:
                raise ValueError(f"Layer runtime smoke test failed:\n{result.stderr}")
            return result.stdout.strip()


def main(argv: Sequence[str] | None = None) -> int:
    """Exit nonzero when metadata or isolated runtime validation fails."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("layer", type=Path)
    parser.add_argument("--expected-anyio", required=True)
    args = parser.parse_args(argv)
    try:
        print(validate_layer(args.layer, expected_anyio=args.expected_anyio))
    except (
        OSError,
        ValueError,
        zipfile.BadZipFile,
        subprocess.SubprocessError,
    ) as error:
        print(f"Layer validation failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
