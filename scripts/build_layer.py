"""Build a Lambda layer zip containing the async durable execution SDK."""

from __future__ import annotations

import argparse
import importlib.util
import logging
import subprocess
import sys
import tempfile
import zipfile
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version
from pathlib import Path
from typing import Sequence


logger = logging.getLogger(__name__)
DEFAULT_OUTPUT = Path("dist") / "async-durable-execution-layer.zip"
LAYER_ROOT = "python"


@dataclass(frozen=True)
class LayerBuildResult:
    """Details about a generated Lambda layer archive."""

    output_path: Path
    sdk_source: str
    file_count: int
    size_bytes: int


def default_sdk_spec() -> str:
    """Return the SDK package spec matching this layer package version."""

    version = _read_packaged_version()
    return f"async-durable-execution=={version}"


def build_layer(
    *,
    output_path: Path = DEFAULT_OUTPUT,
    sdk_source: str | None = None,
    python_executable: str = sys.executable,
    pip_args: Sequence[str] = (),
) -> LayerBuildResult:
    """Install the SDK into a Lambda layer layout and zip it.

    The generated archive has the standard Python layer layout:
    ``python/<installed packages>``.
    """

    resolved_source = sdk_source or default_sdk_spec()
    output_path = output_path.resolve()

    with tempfile.TemporaryDirectory(prefix="async-durable-layer-") as temp_dir:
        staging_dir = Path(temp_dir)
        python_dir = staging_dir / LAYER_ROOT
        python_dir.mkdir(parents=True)

        _pip_install(
            python_executable=python_executable,
            target_dir=python_dir,
            sdk_source=resolved_source,
            pip_args=pip_args,
        )

        file_count = create_layer_archive(staging_dir, output_path)

    return LayerBuildResult(
        output_path=output_path,
        sdk_source=resolved_source,
        file_count=file_count,
        size_bytes=output_path.stat().st_size,
    )


def create_layer_archive(staging_dir: Path, output_path: Path) -> int:
    """Create a deterministic zip archive from a staged Lambda layer directory."""

    python_dir = staging_dir / LAYER_ROOT
    if not python_dir.is_dir():
        msg = f"Layer staging directory must contain a {LAYER_ROOT!r} directory"
        raise ValueError(msg)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        output_path.unlink()

    file_count = 0
    with zipfile.ZipFile(output_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(staging_dir.rglob("*")):
            if not path.is_file():
                continue
            if _should_skip_archive_path(path):
                continue
            archive_path = path.relative_to(staging_dir).as_posix()
            info = zipfile.ZipInfo(archive_path)
            info.date_time = (1980, 1, 1, 0, 0, 0)
            info.external_attr = (0o644 & 0xFFFF) << 16
            with path.open("rb") as file:
                archive.writestr(info, file.read(), compress_type=zipfile.ZIP_DEFLATED)
            file_count += 1

    return file_count


def _pip_install(
    *,
    python_executable: str,
    target_dir: Path,
    sdk_source: str,
    pip_args: Sequence[str],
) -> None:
    command = [
        python_executable,
        "-m",
        "pip",
        "install",
        "--upgrade",
        "--no-compile",
        "--target",
        str(target_dir),
        *pip_args,
        sdk_source,
    ]
    logger.info("Installing %s into %s", sdk_source, target_dir)
    subprocess.run(command, check=True)


def _should_skip_archive_path(path: Path) -> bool:
    return "__pycache__" in path.parts or path.suffix in {".pyc", ".pyo"}


def _read_packaged_version() -> str:
    source_path = Path(__file__).resolve()
    for parent in source_path.parents:
        version_path = parent / "async_durable_execution" / "__about__.py"
        if version_path.exists():
            return _load_version_from_path(version_path)

    try:
        return version("async-durable-execution")
    except PackageNotFoundError as error:
        msg = "Unable to resolve async-durable-execution version"
        raise RuntimeError(msg) from error


def _load_version_from_path(path: Path) -> str:
    spec = importlib.util.spec_from_file_location("_async_durable_layer_version", path)
    if spec is None or spec.loader is None:
        msg = f"Unable to load version from {path}"
        raise RuntimeError(msg)

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.__version__


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build an AWS Lambda layer zip for async-durable-execution"
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Layer zip output path (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--sdk-source",
        default=None,
        help=(
            "Package spec or local path to install into the layer "
            f"(default: {default_sdk_spec()})"
        ),
    )
    parser.add_argument(
        "--python",
        default=sys.executable,
        help="Python executable used to run pip",
    )
    parser.add_argument(
        "--pip-arg",
        action="append",
        default=[],
        help="Additional argument to pass to pip install; repeat for multiple args",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(levelname)s: %(message)s",
    )

    try:
        result = build_layer(
            output_path=args.output,
            sdk_source=args.sdk_source,
            python_executable=args.python,
            pip_args=args.pip_arg,
        )
    except subprocess.CalledProcessError as error:
        print(f"Failed to install layer dependencies: {error}", file=sys.stderr)
        return 1
    except (OSError, RuntimeError, ValueError, zipfile.BadZipFile) as error:
        print(f"Failed to build layer: {error}", file=sys.stderr)
        return 1

    print(
        "Built Lambda layer "
        f"{result.output_path} with {result.file_count} files "
        f"({result.size_bytes} bytes) from {result.sdk_source}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
