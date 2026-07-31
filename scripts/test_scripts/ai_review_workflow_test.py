"""Tests for the AI review workflow's permission boundaries."""

from __future__ import annotations

import re
from pathlib import Path

import pytest


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_FILE = REPOSITORY_ROOT / ".github" / "workflows" / "ai-pr-review.yml"
JOB_HEADER = re.compile(r"^  ([a-z0-9_-]+):\n", re.MULTILINE)


def _jobs() -> dict[str, str]:
    workflow = WORKFLOW_FILE.read_text(encoding="utf-8").split("jobs:\n", 1)[1]
    matches = list(JOB_HEADER.finditer(workflow))
    return {
        match.group(1): workflow[
            match.start() : (
                matches[index + 1].start()
                if index + 1 < len(matches)
                else len(workflow)
            )
        ]
        for index, match in enumerate(matches)
    }


@pytest.mark.parametrize(
    ("generate_job", "post_job", "invocation_marker"),
    [
        (
            "claude-review",
            "post-claude-review",
            "anthropics/claude-code-action@",
        ),
        (
            "codex-review",
            "post-codex-review",
            "--model openai.gpt-",
        ),
    ],
)
def test_ai_review_generation_is_separate_from_posting(
    generate_job: str,
    post_job: str,
    invocation_marker: str,
) -> None:
    jobs = _jobs()
    generation = jobs[generate_job]
    posting = jobs[post_job]

    assert "pull-requests: read" in generation
    assert "pull-requests: write" not in generation
    assert "id-token: write" in generation
    assert "environment: ai-pr-review-runtime" in generation
    assert invocation_marker in generation
    assert "base64 -w 0" in generation
    assert "scripts/post_ai_review_summary.sh" not in generation

    assert f"needs: {generate_job}" in posting
    assert "pull-requests: write" in posting
    assert "id-token:" not in posting
    assert "environment: ai-pr-review-runtime" not in posting
    assert invocation_marker not in posting
    assert "base64 --decode" in posting
    assert "scripts/post_ai_review_summary.sh" in posting


def test_only_posting_jobs_can_write_pull_requests() -> None:
    jobs = _jobs()
    write_jobs = {
        job_id for job_id, job in jobs.items() if "pull-requests: write" in job
    }

    assert write_jobs == {"post-claude-review", "post-codex-review"}
