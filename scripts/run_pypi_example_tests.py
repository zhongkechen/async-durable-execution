#!/usr/bin/env python3

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Sequence
from pathlib import Path


logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

STAGED_DIRECTORIES = ("examples", "test_examples")


def stage_test_sources(repo_root: Path, staging_dir: Path) -> None:
    """Copy example test sources without exposing the local SDK package."""
    ignore = shutil.ignore_patterns("__pycache__", "*.pyc")
    for directory in STAGED_DIRECTORIES:
        shutil.copytree(
            repo_root / directory,
            staging_dir / directory,
            ignore=ignore,
        )


def isolated_environment() -> dict[str, str]:
    """Return an environment that cannot inherit repository import paths."""
    environment = os.environ.copy()
    environment.pop("PYTHONPATH", None)
    return environment


def find_sdk_source(staging_dir: Path, environment: dict[str, str]) -> Path:
    """Resolve the SDK imported by the isolated interpreter."""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "from pathlib import Path\n"
                "import site\n"
                "import async_durable_execution\n"
                "sdk_source = Path(async_durable_execution.__file__).resolve()\n"
                "site_packages = [\n"
                "    Path(path).resolve() for path in site.getsitepackages()\n"
                "]\n"
                "if not any(\n"
                "    sdk_source.is_relative_to(path) for path in site_packages\n"
                "):\n"
                "    raise RuntimeError(\n"
                "        f'SDK was not imported from site-packages: {sdk_source}'\n"
                "    )\n"
                "print(sdk_source)"
            ),
        ],
        cwd=staging_dir,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        details = result.stderr.strip() or "unknown import error"
        msg = f"Unable to import the published SDK: {details}"
        raise RuntimeError(msg)
    return Path(result.stdout.strip()).resolve()


def run_staged_tests(
    repo_root: Path,
    staging_dir: Path,
    pytest_args: Sequence[str],
) -> int:
    """Run example tests from an isolated staging directory."""
    stage_test_sources(repo_root, staging_dir)
    environment = isolated_environment()
    sdk_source = find_sdk_source(staging_dir, environment)
    if sdk_source.is_relative_to(repo_root.resolve()):
        msg = f"PyPI test imported the local SDK from {sdk_source}"
        raise RuntimeError(msg)

    logger.info("Testing published SDK from %s", sdk_source)
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-v",
            "--strict-markers",
            "--import-mode=importlib",
            "-o",
            "asyncio_mode=auto",
            "test_examples",
            *pytest_args,
        ],
        cwd=staging_dir,
        env=environment,
        check=False,
    )
    return result.returncode


def run_tests(pytest_args: Sequence[str]) -> int:
    """Stage and run the PyPI example tests."""
    repo_root = Path(__file__).resolve().parent.parent
    with tempfile.TemporaryDirectory(
        prefix="async-durable-execution-pypi-"
    ) as temp_dir:
        return run_staged_tests(repo_root, Path(temp_dir), pytest_args)


def main() -> int:
    try:
        return run_tests(sys.argv[1:])
    except (OSError, RuntimeError):
        logger.exception("Failed to run examples against the published SDK")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
