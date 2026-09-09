#!/usr/bin/env python3

import logging
import shutil
from pathlib import Path


logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def build_examples() -> None:
    """Build examples for SAM packaging.

    The SDK is deployed as a Lambda layer by the e2e workflow, so the function
    artifact only needs the example handlers.
    """
    repo_dir = Path(__file__).resolve().parent.parent
    examples_dir = repo_dir / "examples"
    build_dir = repo_dir / "build" / "lambda"

    if build_dir.exists():
        logger.info("Cleaning existing build directory")
        shutil.rmtree(build_dir)
    build_dir.mkdir(parents=True)

    logger.info("Copying example handlers into %s", build_dir)
    shutil.copytree(
        examples_dir,
        build_dir / "examples",
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )

    logger.info("Build completed successfully")


def main() -> int:
    try:
        build_examples()
    except OSError:
        logger.exception("Failed to build examples")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
