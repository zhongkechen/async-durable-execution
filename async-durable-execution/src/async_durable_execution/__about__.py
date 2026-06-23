# SPDX-FileCopyrightText: 2025-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path


_DIST_NAME = "async-durable-execution"


def _read_repo_version() -> str:
    version_file = _find_version_file()
    if version_file is None:
        return "0.0.0"

    namespace: dict[str, str] = {}
    exec(version_file.read_text(encoding="utf-8"), namespace)
    return namespace["__version__"]


def _find_version_file() -> Path | None:
    current = Path(__file__).resolve()
    for parent in current.parents:
        candidate = parent / "VERSION.py"
        if candidate.exists():
            return candidate
    return None


try:
    __version__ = version(_DIST_NAME)
except PackageNotFoundError:
    __version__ = _read_repo_version()
