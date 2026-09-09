"""Check benchmark correctness boundaries without fetching SDK baselines."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
import subprocess
import sys

import pytest

from benchmarks import run
from benchmarks.async_workloads import workflow
from benchmarks.backend import AsyncBackend, SyncBackend
from benchmarks.metrics import has_failures, render_report, source_metrics
from benchmarks.worker import EXPECTED, measure_codec, measure_workflow


@pytest.mark.parametrize("case", EXPECTED)
def test_current_workflows_preserve_outputs_and_do_not_repeat_effects(
    case: str,
) -> None:
    row = measure_workflow(case, 0, workflow, AsyncBackend, samples=1, warmups=0)
    assert row["first_valid"] and row["replay_valid"]
    assert row["samples"][0]["first_output"] == EXPECTED[case]
    assert row["samples"][0]["replay_output"] == EXPECTED[case]
    assert row["samples"][0]["replay_effects"] == 0
    assert row["samples"][0]["replay_checkpoint_calls"] == 0


@pytest.mark.parametrize("backend", [SyncBackend, AsyncBackend])
def test_shared_service_rotates_tokens_and_returns_independent_histories(
    backend,
) -> None:
    api = backend()
    request = {
        "CheckpointToken": "token-0",
        "Updates": [
            {"Id": "step", "Type": "STEP", "Action": "START", "ParentId": "root"},
            {"Id": "step", "Type": "STEP", "Action": "SUCCEED", "Payload": "42"},
        ],
    }

    def checkpoint(payload: dict) -> dict:
        result = api.checkpoint_durable_execution(**payload)
        return asyncio.run(result) if isinstance(api, AsyncBackend) else result

    response = checkpoint(request)
    assert response["CheckpointToken"] == "token-1"
    records = response["NewExecutionState"]["Operations"]
    assert len(records) == 1
    assert records[0]["Status"] == "SUCCEEDED"
    event = api.event()
    assert isinstance(
        event["InitialExecutionState"]["Operations"][0]["StartTimestamp"], int
    )
    records[0]["StepDetails"]["Result"] = "changed"
    assert api.ops["step"]["StepDetails"]["Result"] == "42"
    with pytest.raises(ValueError, match="Stale checkpoint token"):
        checkpoint(request)
    assert api.calls == 1
    assert api.updates == 2


@pytest.mark.parametrize("fault", ["output", "effects", "checkpoints"])
def test_replay_validation_rejects_equal_timing_but_incorrect_execution(
    fault: str,
) -> None:
    def factory(case, api):
        calls = 0

        def handler(event, context):
            nonlocal calls
            calls += 1
            if calls == 1 or fault == "effects":
                for _ in range(10):
                    api.effect()
            if calls > 1 and fault == "checkpoints":
                api.calls += 1
            value = 0 if calls > 1 and fault == "output" else 45
            return {"Status": "SUCCEEDED", "Result": json.dumps(value)}

        return handler

    row = measure_workflow("sequential_10", 0, factory, SyncBackend, 1, 0)
    assert row["first_valid"]
    assert not row["replay_valid"]
    assert row["failures"]


def test_serializer_must_round_trip_before_its_timing_is_usable() -> None:
    row = measure_codec([1, 2], json.dumps, lambda data: [], 1, 0, 1)
    assert row["valid"] is False
    assert "encode_ms" not in row


def _packet(row: dict) -> dict:
    return {
        "created_at": "test",
        "python": "test",
        "configuration": {},
        "versions": {
            "current": {
                "commit": "a" * 40,
                "wheel_bytes": 1,
                "source": dict.fromkeys(
                    (
                        "modules",
                        "physical_lines",
                        "code_lines",
                        "classes",
                        "functions",
                        "python_bytes",
                    ),
                    1,
                ),
                "imports": {"ms": 1.0, "peak_rss_kib": None},
                "measurements": {
                    "version": "test",
                    "workflows": {"sequential_10": {"0": row}},
                    "serialization": {},
                },
            },
        },
    }


def _invalid_row() -> dict:
    return {
        "first_valid": True,
        "replay_valid": False,
        "first_ms": 2.0,
        "replay_ms": 987654.321,
        "checkpoint_calls": 1,
        "operation_updates": 2,
        "request_bytes": 300,
        "failures": ["replay repeated completed step effects"],
    }


def test_report_withholds_invalid_replay_timing_and_explains_failure() -> None:
    packet = _packet(_invalid_row())
    report = render_report(packet)
    assert has_failures(packet["versions"])
    assert "987654.321" not in report
    assert "INVALID" in report
    assert "replay repeated completed step effects" in report
    assert "a" * 40 in report


@pytest.mark.skipif(
    sys.version_info < (3, 11), reason="Comparison CLI requires Python 3.11+"
)
@pytest.mark.parametrize("allow_invalid", [False, True])
def test_invalid_exit_status_preserves_report_and_raw_results(
    tmp_path: Path, monkeypatch, allow_invalid: bool
) -> None:
    monkeypatch.setattr(
        run, "run_comparison", lambda args, work: _packet(_invalid_row())
    )
    output = tmp_path / "results"
    args = ["--output", str(output)] + (["--allow-invalid"] if allow_invalid else [])
    assert run.main(args) == (0 if allow_invalid else 1)
    assert "INVALID" in (output / "comparison.md").read_text()
    packet = json.loads((output / "metrics.json").read_text())
    assert (
        packet["versions"]["current"]["measurements"]["workflows"]["sequential_10"][
            "0"
        ]["replay_ms"]
        == 987654.321
    )
    assert not list(output.glob(".work-*"))


def test_existing_output_is_not_overwritten(tmp_path: Path) -> None:
    if sys.version_info < (3, 11):
        pytest.skip("Comparison CLI requires Python 3.11+")
    sentinel = tmp_path / "metrics.json"
    sentinel.write_text("keep")
    assert run.main(["--output", str(tmp_path)]) == 2
    assert sentinel.read_text() == "keep"


def test_source_excludes_docstrings_but_counts_string_assignments(
    tmp_path: Path,
) -> None:
    source = '"""Module documentation.\nMore documentation.\n"""\n# comment\nvalue = "retained code"\n"""Attribute documentation."""\ndef f():\n    """Function docs."""\n    return value\n'
    (tmp_path / "module.py").write_text(source)
    counts = source_metrics(tmp_path)
    assert counts["modules"] == 1
    assert counts["code_lines"] == 3
    assert counts["physical_lines"] == 9
    assert counts["functions"] == 1


def test_snapshot_uses_commit_without_changing_branch_or_dirty_files(
    tmp_path: Path,
) -> None:
    repository = tmp_path / "repo"
    repository.mkdir()
    subprocess.run(["git", "init", "--quiet", str(repository)], check=True)
    tracked = repository / "data.txt"
    tracked.write_text("committed")
    subprocess.run(["git", "add", "data.txt"], cwd=repository, check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=Benchmark test",
            "-c",
            "user.email=test@example.com",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "--quiet",
            "-m",
            "fixture",
        ],
        cwd=repository,
        check=True,
    )
    before = run.revision(repository, "HEAD")
    tracked.write_text("uncommitted")
    (repository / "untracked.txt").write_text("untracked")
    destination = tmp_path / "snapshot"
    assert run.snapshot(repository, "HEAD", destination) == before
    assert (destination / "data.txt").read_text() == "committed"
    assert not (destination / "untracked.txt").exists()
    assert run.revision(repository, "HEAD") == before
    assert tracked.read_text() == "uncommitted"


@pytest.mark.parametrize(
    "option", ["--samples", "--import-samples", "--iterations", "--timeout"]
)
def test_empty_sampling_is_rejected(option: str) -> None:
    with pytest.raises(SystemExit):
        run.parse_args(["--output", "unused", option, "0"])
