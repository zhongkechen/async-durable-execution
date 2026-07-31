"""Tests for preparing the AI pull request review context."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PREPARE_CONTEXT_SCRIPT = REPOSITORY_ROOT / "scripts" / "prepare_ai_review_context.sh"

MOCK_GH = """\
#!/usr/bin/env python3

import json
import os
import sys
from pathlib import Path

args = sys.argv[1:]
with Path(os.environ["MOCK_GH_CALLS"]).open("a", encoding="utf-8") as calls:
    calls.write(json.dumps(args) + "\\n")

pull_path = f"repos/{os.environ['GITHUB_REPOSITORY']}/pulls/{os.environ['PR_NUMBER']}"
compare_path = (
    f"repos/{os.environ['GITHUB_REPOSITORY']}/compare/"
    f"{os.environ['EXPECTED_BASE_SHA']}...{os.environ['EXPECTED_HEAD_SHA']}"
    "?per_page=1&page=2"
)

if args[:2] == ["api", compare_path]:
    print(os.environ["MERGE_BASE_SHA"])
elif args[:2] != ["api", pull_path]:
    raise SystemExit(f"unexpected gh invocation: {args}")
elif "--jq" in args:
    print(os.environ["EXPECTED_HEAD_SHA"])
else:
    print(
        json.dumps(
            {
                "number": int(os.environ["PR_NUMBER"]),
                "title": "Test pull request",
                "body": "Test body",
                "html_url": "https://github.example/pull/42",
                "draft": False,
                "author_association": "OWNER",
                "additions": 1,
                "deletions": 0,
                "changed_files": 1,
                "user": {"login": "alice"},
                "base": {"ref": "main"},
                "head": {"ref": "feature"},
            }
        )
    )
"""


def _git(cwd: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        check=True,
        cwd=cwd,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def test_prepare_context_generates_complete_local_diff(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    _git(source, "init", "--initial-branch=main")
    _git(source, "config", "user.name", "Test User")
    _git(source, "config", "user.email", "test@example.com")

    (source / "common.txt").write_text("common\n", encoding="utf-8")
    _git(source, "add", "common.txt")
    _git(source, "commit", "-m", "Add common file")
    merge_base_sha = _git(source, "rev-parse", "HEAD")

    _git(source, "switch", "-c", "feature")
    (source / "feature.txt").write_text("feature\n", encoding="utf-8")
    _git(source, "add", "feature.txt")
    _git(source, "commit", "-m", "Add feature file")
    expected_head_sha = _git(source, "rev-parse", "HEAD")

    _git(source, "switch", "main")
    (source / "base-only.txt").write_text("base\n", encoding="utf-8")
    _git(source, "add", "base-only.txt")
    _git(source, "commit", "-m", "Advance base branch")
    expected_base_sha = _git(source, "rev-parse", "HEAD")

    origin = tmp_path / "origin.git"
    _git(tmp_path, "init", "--bare", str(origin))
    _git(source, "remote", "add", "origin", str(origin))
    _git(source, "push", "origin", "main")
    _git(
        source,
        "push",
        "origin",
        f"{expected_head_sha}:refs/pull/42/head",
    )

    workspace = tmp_path / "workspace"
    _git(
        tmp_path,
        "clone",
        "--depth=1",
        "--branch=main",
        f"file://{origin}",
        str(workspace),
    )

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    mock_gh = bin_dir / "gh"
    mock_gh.write_text(MOCK_GH, encoding="utf-8")
    mock_gh.chmod(0o755)
    gh_calls = tmp_path / "gh-calls.jsonl"

    environment = os.environ.copy()
    environment.update(
        {
            "PATH": f"{bin_dir}{os.pathsep}{environment['PATH']}",
            "GH_TOKEN": "test-token",
            "GITHUB_REPOSITORY": "example/repository",
            "GITHUB_WORKSPACE": str(workspace),
            "PR_NUMBER": "42",
            "EXPECTED_BASE_SHA": expected_base_sha,
            "EXPECTED_HEAD_SHA": expected_head_sha,
            "MERGE_BASE_SHA": merge_base_sha,
            "MOCK_GH_CALLS": str(gh_calls),
        }
    )

    result = subprocess.run(
        ["bash", PREPARE_CONTEXT_SCRIPT],
        check=False,
        cwd=workspace,
        env=environment,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    context_dir = workspace / ".ai-review-context"
    diff = (context_dir / "pr.diff").read_text(encoding="utf-8")
    assert "diff --git a/feature.txt b/feature.txt" in diff
    assert "base-only.txt" not in diff
    assert not (context_dir / "pr.raw.json").exists()

    metadata = json.loads((context_dir / "pr.json").read_text(encoding="utf-8"))
    assert metadata["base"]["sha"] == expected_base_sha
    assert metadata["head"]["sha"] == expected_head_sha

    calls = [
        json.loads(line) for line in gh_calls.read_text(encoding="utf-8").splitlines()
    ]
    assert len(calls) == 4
    compare_calls = [
        call for call in calls if any("compare" in argument for argument in call)
    ]
    assert len(compare_calls) == 1
    assert "?per_page=1&page=2" in compare_calls[0][1]
    assert "application/vnd.github.v3.diff" not in compare_calls[0]
