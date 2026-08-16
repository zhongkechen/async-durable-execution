"""Unit tests for AI review metadata validation."""

from __future__ import annotations

import pytest

from scripts.validate_ai_review_summary import contains_reserved_metadata


@pytest.mark.parametrize(
    "marker",
    [
        "<!-- ai-pr-review:claude -->",
        "<!-- ai-pr-review:codex -->",
        "<!-- ai-pr-review:inline:claude:123:1:primary -->",
        "<!-- ai-pr-review:inline:codex:123:2:retry -->",
    ],
)
def test_detects_standalone_reserved_metadata(marker: str) -> None:
    assert contains_reserved_metadata(marker)


@pytest.mark.parametrize(
    "summary",
    [
        "The marker `<!-- ai-pr-review:claude -->` is quoted.",
        "```\n<!-- ai-pr-review:claude -->",
        "   ~~~markdown\n<!-- ai-pr-review:codex -->\n   ~~~",
        "````\n```\n<!-- ai-pr-review:claude -->\n````",
        "```\n<!-- ai-pr-review:claude -->\n```\n~~~\n<!-- ai-pr-review:codex -->\n~~~",
    ],
)
def test_ignores_reserved_metadata_inside_markdown_examples(summary: str) -> None:
    assert not contains_reserved_metadata(summary)


@pytest.mark.parametrize(
    "summary",
    [
        "    ```\n<!-- ai-pr-review:claude -->",
        "```invalid`info\n<!-- ai-pr-review:claude -->",
        "````\n<!-- ai-pr-review:claude -->\n```\n"
        "<!-- ai-pr-review:codex -->\n````\n"
        "<!-- ai-pr-review:claude -->",
    ],
)
def test_detects_metadata_outside_valid_fences(summary: str) -> None:
    assert contains_reserved_metadata(summary)
