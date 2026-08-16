#!/usr/bin/env python3

from __future__ import annotations

import re
import sys
from pathlib import Path


FENCE_START = re.compile(r"^ {0,3}(`{3,}|~{3,})(.*)$")
RESERVED_METADATA = re.compile(
    r"<!-- ai-pr-review:"
    r"(?:claude|codex|inline:(?:claude|codex):[0-9]+:[0-9]+:(?:primary|retry))"
    r" -->"
)


def contains_reserved_metadata(summary: str) -> bool:
    fence_character = ""
    fence_length = 0

    for line in summary.splitlines():
        if fence_character:
            closing_fence = re.fullmatch(
                rf" {{0,3}}{re.escape(fence_character)}{{{fence_length},}}[ \t]*",
                line,
            )
            if closing_fence:
                fence_character = ""
                fence_length = 0
            continue

        opening_fence = FENCE_START.match(line)
        if opening_fence:
            fence = opening_fence.group(1)
            info = opening_fence.group(2)
            if fence[0] != "`" or "`" not in info:
                fence_character = fence[0]
                fence_length = len(fence)
                continue

        if RESERVED_METADATA.fullmatch(line):
            return True

    return False


def main() -> int:
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <summary-file>", file=sys.stderr)
        return 2

    try:
        summary = Path(sys.argv[1]).read_text(encoding="utf-8")
    except OSError as error:
        print(f"unable to read AI review summary: {error}", file=sys.stderr)
        return 2

    return 1 if contains_reserved_metadata(summary) else 0


if __name__ == "__main__":
    raise SystemExit(main())
