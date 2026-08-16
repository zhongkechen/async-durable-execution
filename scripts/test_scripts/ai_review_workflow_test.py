"""Tests for the AI review workflow behavior and permission boundaries."""

from __future__ import annotations

import re
from pathlib import Path

import pytest


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_FILE = REPOSITORY_ROOT / ".github" / "workflows" / "ai-pr-review.yml"
JOB_HEADER = re.compile(r"^  ([a-z0-9_-]+):\n", re.MULTILINE)


def _workflow() -> str:
    return WORKFLOW_FILE.read_text(encoding="utf-8")


def _jobs() -> dict[str, str]:
    workflow = _workflow().split("jobs:\n", 1)[1]
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
    posting_condition = (
        f"if: \"!cancelled() && needs.{generate_job}.result == 'success'\""
    )
    assert posting_condition in posting
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


def test_untrusted_reviews_require_environment_approval() -> None:
    jobs = _jobs()
    approval = jobs["approve_external"]
    approval_condition = """\
if: >-
      (
        github.event.pull_request.user.login == 'dependabot[bot]' ||
        github.event.pull_request.draft ||
        github.event.pull_request.head.repo.full_name != github.repository
      )
"""
    generation_condition = """\
if: >-
      !cancelled() &&
      (
        (
          github.event.pull_request.user.login != 'dependabot[bot]' &&
          !github.event.pull_request.draft &&
          github.event.pull_request.head.repo.full_name == github.repository
        ) ||
        needs.approve_external.result == 'success'
      )
"""

    assert approval_condition in approval
    assert "HEAD_SHA: ${{ github.event.pull_request.head.sha }}" in approval
    assert 'run: echo "Approved review of $HEAD_SHA"' in approval
    assert (
        "${{ github.event.pull_request.head.sha }}" not in approval.split("run:", 1)[1]
    )
    for job_id in ("claude-review", "codex-review"):
        assert generation_condition in jobs[job_id]


@pytest.mark.parametrize(
    "posting_job",
    ["post-claude-review", "post-codex-review"],
)
def test_posting_validates_base_and_head_revisions(posting_job: str) -> None:
    posting = _jobs()[posting_job]

    assert "EXPECTED_BASE_SHA: ${{ github.event.pull_request.base.sha }}" in posting
    assert "EXPECTED_HEAD_SHA: ${{ github.event.pull_request.head.sha }}" in posting
    assert '"$EXPECTED_BASE_SHA"' in posting
    assert '"$EXPECTED_HEAD_SHA"' in posting


def test_converting_to_draft_cancels_previous_review() -> None:
    workflow = _workflow()
    jobs = _jobs()

    assert (
        "types: [opened, synchronize, reopened, ready_for_review, "
        "converted_to_draft]" in workflow
    )
    for job_id in (
        "claude-review",
        "post-claude-review",
        "codex-review",
        "post-codex-review",
    ):
        assert "!cancelled()" in jobs[job_id]
        assert "always()" not in jobs[job_id]


def test_claude_review_uses_sonnet_5_for_both_attempts() -> None:
    claude_review = _jobs()["claude-review"]

    assert claude_review.count("--model us.anthropic.claude-sonnet-5") == 2
    assert "--model us.anthropic.claude-opus-" not in claude_review


def test_claude_review_relies_on_os_isolation_without_tool_limits() -> None:
    claude_review = _jobs()["claude-review"]

    assert claude_review.count("scripts/run_claude_isolated.sh") == 2
    assert "bash scripts/prepare_ai_review_user.sh claude-review" in claude_review
    assert "--allowedTools" not in claude_review
    assert "--disallowedTools" not in claude_review
