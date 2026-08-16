"""Tests for the AI review workflow behavior and permission boundaries."""

from __future__ import annotations

import re
from pathlib import Path

import pytest


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_FILE = REPOSITORY_ROOT / ".github" / "workflows" / "ai-pr-review.yml"
CLAUDE_WRAPPER_FILE = REPOSITORY_ROOT / "scripts" / "run_claude_isolated.sh"
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
    ("generate_job", "invocation_marker"),
    [
        (
            "claude-review",
            "anthropics/claude-code-action@",
        ),
        (
            "codex-review",
            "--model openai.gpt-",
        ),
    ],
)
def test_ai_review_generation_is_separate_from_posting(
    generate_job: str,
    invocation_marker: str,
) -> None:
    jobs = _jobs()
    generation = jobs[generate_job]
    posting = jobs["post-reviews"]

    assert "pull-requests: read" in generation
    assert "pull-requests: write" not in generation
    assert "id-token: write" in generation
    assert "environment: ai-pr-review-runtime" in generation
    assert invocation_marker in generation
    assert "base64 -w 0" in generation
    assert "scripts/post_ai_review_summary.sh" not in generation

    assert "needs: [claude-review, codex-review]" in posting
    assert f"needs.{generate_job}.result == 'success'" in posting
    assert f"needs.{generate_job}.outputs.summary_base64" in posting
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

    assert write_jobs == {"post-reviews"}


def test_shared_posting_runs_for_each_successful_generator() -> None:
    posting = _jobs()["post-reviews"]

    assert "needs: [claude-review, codex-review]" in posting
    assert (
        """\
if: >-
      !cancelled() &&
      (
        needs.claude-review.result == 'success' ||
        needs.codex-review.result == 'success'
      )
"""
        in posting
    )
    assert "CLAUDE_REVIEW_RESULT: ${{ needs.claude-review.result }}" in posting
    assert "CODEX_REVIEW_RESULT: ${{ needs.codex-review.result }}" in posting
    assert "posting_failed=false" in posting
    assert posting.count("if ! post_review \\") == 2


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


def test_shared_posting_validates_base_target_and_head_revision() -> None:
    posting = _jobs()["post-reviews"]

    assert "EXPECTED_BASE_REF: ${{ github.event.pull_request.base.ref }}" in posting
    assert "${{ github.event.pull_request.base.repo.full_name }}" in posting
    assert "EXPECTED_HEAD_SHA: ${{ github.event.pull_request.head.sha }}" in posting
    assert '"$EXPECTED_BASE_REPOSITORY"' in posting
    assert '"$EXPECTED_BASE_REF"' in posting
    assert '"$EXPECTED_HEAD_SHA"' in posting
    assert "post_review \\\n            claude" in posting
    assert "post_review \\\n            codex" in posting


def test_converting_to_draft_cancels_previous_review() -> None:
    workflow = _workflow()
    jobs = _jobs()

    assert (
        "types: [opened, synchronize, reopened, ready_for_review, "
        "converted_to_draft]" in workflow
    )
    for job_id in (
        "claude-review",
        "codex-review",
        "post-reviews",
    ):
        assert "!cancelled()" in jobs[job_id]
        assert "always()" not in jobs[job_id]


def test_claude_review_uses_single_sonnet_5_attempt() -> None:
    claude_review = _jobs()["claude-review"]

    assert claude_review.count("--model us.anthropic.claude-sonnet-5") == 1
    assert "--model us.anthropic.claude-opus-" not in claude_review
    assert "continue-on-error: true" not in claude_review
    assert "review-retry" not in claude_review


def test_claude_review_uses_hardened_os_isolation_without_tool_limits() -> None:
    claude_review = _jobs()["claude-review"]

    assert (
        "step-security/harden-runner@05e31511f85b41b11d1cf0ef85d0992719546e2c"
        in claude_review
    )
    assert "egress-policy: block" in claude_review
    assert "disable-telemetry: true" in claude_review
    endpoint_block = claude_review.split("allowed-endpoints: |", 1)[1].split("\n\n", 1)[
        0
    ]
    allowed_endpoints = {
        line.strip() for line in endpoint_block.splitlines() if line.strip()
    }
    assert allowed_endpoints == {
        "api.github.com:443",
        "azure.archive.ubuntu.com:80",
        "bedrock-runtime.us-east-1.amazonaws.com:443",
        "github.com:443",
        "objects.githubusercontent.com:443",
        "pipelines.actions.githubusercontent.com:443",
        "raw.githubusercontent.com:443",
        "registry.npmjs.org:443",
        "release-assets.githubusercontent.com:443",
        "sts.us-east-1.amazonaws.com:443",
        "token.actions.githubusercontent.com:443",
    }
    assert claude_review.count("scripts/run_claude_isolated.sh") == 1
    assert "bash scripts/prepare_ai_review_user.sh claude-review" in claude_review
    assert 'CLAUDE_CODE_SUBPROCESS_ENV_SCRUB: "1"' in claude_review
    assert claude_review.count("--bare") == 1
    assert claude_review.count("--permission-mode bypassPermissions") == 1
    assert "--allowedTools" not in claude_review
    assert "--disallowedTools" not in claude_review
    assert (
        "sudo apt-get install -y --no-install-recommends bubblewrap socat"
        in claude_review
    )
    assert "sudo sysctl -w kernel.unprivileged_userns_clone=1" in claude_review
    assert (
        "sudo sysctl -w kernel.apparmor_restrict_unprivileged_userns=0" in claude_review
    )

    claude_wrapper = CLAUDE_WRAPPER_FILE.read_text(encoding="utf-8")
    assert 'CLAUDE_CODE_SUBPROCESS_ENV_SCRUB:-}" != "1"' in claude_wrapper
    assert "command -v bwrap" in claude_wrapper
    assert "sudo -H -u claude-review -- env HOST_PID=" in claude_wrapper
    assert "--unshare-pid" in claude_wrapper
    assert 'test ! -e "/proc/${HOST_PID}"' in claude_wrapper
