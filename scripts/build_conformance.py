#!/usr/bin/env python3

import argparse
import logging
import shutil
import subprocess
import sys
from pathlib import Path


logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def build_conformance_bundle(
    *,
    repo_dir: Path,
    output_dir: Path,
) -> None:
    """Bundle the local SDK, runtime dependencies, and conformance handlers."""
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)

    logger.info("Installing SDK and runtime dependencies into %s", output_dir)
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--upgrade",
            "--no-compile",
            "--target",
            str(output_dir),
            str(repo_dir / "async-durable-execution"),
            "boto3>=1.42.90,<1.43.1",
        ],
        check=True,
    )

    conformance_dir = repo_dir / "conformance"
    for source_dir in sorted(conformance_dir.iterdir()):
        if not source_dir.is_dir() or source_dir.name in {"build", "generated"}:
            continue
        destination = output_dir / source_dir.name
        shutil.copytree(
            source_dir,
            destination,
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
        )

    logger.info("Conformance bundle completed successfully")


def main() -> int:
    repo_dir = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(
        description="Build the Python conformance Lambda bundle"
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=repo_dir / "conformance" / "build",
    )
    args = parser.parse_args()

    try:
        build_conformance_bundle(repo_dir=repo_dir, output_dir=args.output)
    except subprocess.CalledProcessError:
        logger.exception("Failed to build conformance bundle")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
