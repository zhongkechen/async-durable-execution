"""Build and compare pinned SDK revisions with one shared dependency environment."""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import statistics
import subprocess
import sys
import tarfile
import tempfile

from .metrics import has_failures, render_report, source_metrics
from .worker import EXPECTED, nonnegative_int, positive_int

HERE = Path(__file__).resolve().parent
REPOSITORY = HERE.parent
ASYNC_REPOSITORY = "https://github.com/zhongkechen/async-durable-execution.git"
OFFICIAL_REPOSITORY = "https://github.com/aws/aws-durable-execution-sdk-python.git"
V2_COMMIT = "86b680f5502e29254dfa8e89cad5e310a45f0d24"
OFFICIAL_COMMIT = "d61985ef697dd9144f986d837475efee91d26a3e"


def command(
    args: list[str], *, cwd: Path, env: dict[str, str], log: Path, timeout: int
) -> None:
    """Keep setup/worker output in an artifact and bound subprocess lifetime."""
    with log.open("a", encoding="utf-8") as output:
        output.write("\n" + repr(args) + "\n")
        output.flush()
        subprocess.run(
            args,
            cwd=cwd,
            env=env,
            stdout=output,
            stderr=subprocess.STDOUT,
            check=True,
            timeout=timeout,
        )


def revision(repository: Path, ref: str) -> str:
    """Resolve a ref to the exact commit that will be archived."""
    return subprocess.check_output(
        ["git", "rev-parse", "--verify", "--end-of-options", ref + "^{commit}"],
        cwd=repository,
        text=True,
    ).strip()


def snapshot(repository: Path, ref: str, destination: Path) -> str:
    """Export committed files without checking out or modifying the caller’s branch."""
    commit = revision(repository, ref)
    destination.mkdir()
    archive = destination.parent / (destination.name + ".tar")
    subprocess.run(
        ["git", "archive", "--format=tar", "--output", str(archive), commit],
        cwd=repository,
        check=True,
    )
    with tarfile.open(archive) as contents:
        contents.extractall(destination, filter="data")
    archive.unlink()
    return commit


def child_environment(work: Path) -> dict[str, str]:
    """Avoid ambient Python paths and disable EC2 credential discovery."""
    env = {
        key: value
        for key, value in os.environ.items()
        if key not in ("PYTHONPATH", "PYTHONHOME", "VIRTUAL_ENV")
        and not key.startswith("AWS_")
    }
    scratch = work / "tmp"
    scratch.mkdir(exist_ok=True)
    env.update(
        TMPDIR=str(scratch),
        PYTHONDONTWRITEBYTECODE="1",
        PYTHONNOUSERSITE="1",
        AWS_EC2_METADATA_DISABLED="true",
        PIP_DISABLE_PIP_VERSION_CHECK="1",
    )
    return env


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse source pins and measurement controls."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="new artifact directory (must not exist)",
    )
    parser.add_argument(
        "--variants",
        choices=("current", "v2", "official"),
        nargs="+",
        default=["current", "v2", "official"],
    )
    parser.add_argument(
        "--current-ref", default="HEAD", help="committed ref in this checkout"
    )
    parser.add_argument("--v2-ref", default=V2_COMMIT)
    parser.add_argument(
        "--official-ref",
        default=OFFICIAL_COMMIT,
        help="pin by default; use main to resolve the current official head",
    )
    parser.add_argument("--samples", type=positive_int, default=5)
    parser.add_argument("--warmups", type=nonnegative_int, default=1)
    parser.add_argument("--import-samples", type=positive_int, default=7)
    parser.add_argument("--iterations", type=positive_int, default=100)
    parser.add_argument(
        "--latencies-ms", type=nonnegative_int, nargs="+", default=[0, 20, 150]
    )
    parser.add_argument(
        "--cases", choices=tuple(EXPECTED), nargs="+", default=list(EXPECTED)
    )
    parser.add_argument(
        "--timeout",
        type=positive_int,
        default=300,
        help="timeout in seconds for each setup/worker subprocess",
    )
    parser.add_argument(
        "--quick",
        action="store_true",
        help="one sample, no warmups, zero latency; correctness smoke only",
    )
    parser.add_argument(
        "--allow-invalid",
        action="store_true",
        help="exit zero after reporting incorrect benchmark outcomes",
    )
    args = parser.parse_args(argv)
    args.variants = list(dict.fromkeys(args.variants))
    args.cases = list(dict.fromkeys(args.cases))
    args.latencies_ms = list(dict.fromkeys(args.latencies_ms))
    if args.quick:
        args.samples = args.import_samples = args.iterations = 1
        args.warmups = 0
        args.latencies_ms = [0]
    return args


def run_comparison(args: argparse.Namespace, work: Path) -> dict:
    """Prepare all SDKs before collecting sequential, unprofiled measurements."""
    env = child_environment(work)
    output = args.output
    log = output / "setup.log"
    interpreter = work / "environment"
    command(
        [sys.executable, "-m", "venv", str(interpreter)],
        cwd=work,
        env=env,
        log=log,
        timeout=args.timeout,
    )
    python = interpreter / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    command(
        [
            str(python),
            "-m",
            "pip",
            "--isolated",
            "install",
            "--no-compile",
            "--index-url",
            "https://pypi.org/simple",
            "-r",
            str(HERE / "requirements.txt"),
        ],
        cwd=work,
        env=env,
        log=log,
        timeout=args.timeout,
    )
    harness = work / "harness"
    shutil.copytree(
        HERE, harness / "benchmarks", ignore=shutil.ignore_patterns("__pycache__")
    )
    versions = {}
    environments = {}
    for name in args.variants:
        print(f"Preparing {name}…", flush=True)
        source = work / (name + "-source")
        ref = getattr(args, name + "_ref")
        if name == "current":
            commit = snapshot(REPOSITORY, ref, source)
        else:
            repository = work / (name + "-git")
            repository.mkdir()
            url = OFFICIAL_REPOSITORY if name == "official" else ASYNC_REPOSITORY
            for git_args in (
                ["init", "--quiet"],
                ["remote", "add", "origin", url],
                ["fetch", "--depth=1", "--", "origin", ref],
            ):
                command(
                    ["git", *git_args],
                    cwd=repository,
                    env=env,
                    log=log,
                    timeout=args.timeout,
                )
            commit = snapshot(repository, "FETCH_HEAD", source)
        project = (
            source / "packages/aws-durable-execution-sdk-python"
            if name == "official"
            else source
        )
        command(
            [sys.executable, "-m", "hatchling", "build", "-t", "wheel"],
            cwd=project,
            env=env,
            log=log,
            timeout=args.timeout,
        )
        (wheel,) = (project / "dist").glob("*.whl")
        target = work / (name + "-installed")
        command(
            [
                str(python),
                "-m",
                "pip",
                "--isolated",
                "install",
                "--no-deps",
                "--no-compile",
                "--target",
                str(target),
                str(wheel),
            ],
            cwd=work,
            env=env,
            log=log,
            timeout=args.timeout,
        )
        package = (
            "aws_durable_execution_sdk_python"
            if name == "official"
            else "async_durable_execution"
        )
        versions[name] = {
            "commit": commit,
            "requested_ref": ref,
            "wheel_bytes": wheel.stat().st_size,
            "wheel_sha256": hashlib.sha256(wheel.read_bytes()).hexdigest(),
            "source": source_metrics(target / package),
        }
        environments[name] = env | {
            "PYTHONPATH": os.pathsep.join((str(target), str(harness)))
        }
        command(
            [str(python), "-m", "pip", "--isolated", "check"],
            cwd=work,
            env=environments[name],
            log=log,
            timeout=args.timeout,
        )
    # Warm filesystem caches equally; never run competing timing processes.
    imports: dict[str, list[dict]] = {name: [] for name in versions}
    for repeat in range(args.warmups + args.import_samples):
        order = (
            args.variants[repeat % len(versions) :]
            + args.variants[: repeat % len(versions)]
        )
        for name in order:
            raw = subprocess.check_output(
                [str(python), "-m", "benchmarks.import_probe", name],
                cwd=work,
                env=environments[name],
                text=True,
                timeout=args.timeout,
            )
            if repeat >= args.warmups:
                imports[name].append(json.loads(raw))
    for name, version in versions.items():
        version["imports"] = {
            "samples": imports[name],
            "ms": statistics.median(row["ms"] for row in imports[name]),
            "peak_rss_kib": statistics.median(
                row["peak_rss_kib"] for row in imports[name]
            )
            if imports[name][0]["peak_rss_kib"] is not None
            else None,
        }
        print(f"Measuring {name} ({version['commit'][:12]})…", flush=True)
        result_file = output / (name + ".json")
        command(
            [
                str(python),
                "-m",
                "benchmarks.worker",
                "--sdk",
                "official" if name == "official" else "async",
                "--output",
                str(result_file),
                "--samples",
                str(args.samples),
                "--warmups",
                str(args.warmups),
                "--iterations",
                str(args.iterations),
                "--latencies-ms",
                *map(str, args.latencies_ms),
                "--cases",
                *args.cases,
            ],
            cwd=work,
            env=environments[name],
            log=output / (name + ".log"),
            timeout=args.timeout,
        )
        version["measurements"] = json.loads(result_file.read_text(encoding="utf-8"))
        expected = work / (name + "-installed")
        if (
            Path(version["measurements"]["installed_package_root"]).resolve()
            != expected.resolve()
        ):
            raise RuntimeError(
                f"{name} loaded an SDK outside its isolated target directory"
            )
    dependencies = [
        version["measurements"]["dependencies"] for version in versions.values()
    ]
    if any(value != dependencies[0] for value in dependencies):
        raise RuntimeError("SDK processes did not use identical dependency versions")
    return {
        "schema_version": 1,
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "python": sys.version,
        "configuration": vars(args) | {"output": str(output)},
        "harness_sha256": {
            path.name: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(HERE.iterdir())
            if path.suffix in (".py", ".txt")
        },
        "versions": versions,
    }


def main(argv: list[str] | None = None) -> int:
    """Publish artifacts before returning a failure for invalid measurements."""
    args = parse_args(argv)
    if sys.version_info < (3, 11):
        raise SystemExit(
            "The three-SDK comparison requires Python 3.11+; use the Hatch benchmarks environment."
        )
    args.output = args.output.resolve()
    try:
        args.output.mkdir(parents=True, exist_ok=False)
        # Transient clones/environments stay inside the chosen artifact directory.
        with tempfile.TemporaryDirectory(prefix=".work-", dir=args.output) as scratch:
            packet = run_comparison(args, Path(scratch))
        (args.output / "metrics.json").write_text(
            json.dumps(packet, indent=2) + "\n", encoding="utf-8"
        )
        (args.output / "comparison.md").write_text(
            render_report(packet), encoding="utf-8"
        )
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        print(
            f"Benchmark setup/execution failed: {error}. Logs: {args.output}",
            file=sys.stderr,
        )
        return 2
    invalid = has_failures(packet["versions"])
    print(f"Report: {args.output / 'comparison.md'}", flush=True)
    if invalid:
        print(
            "Correctness failures recorded; invalid timings are excluded from the report.",
            file=sys.stderr,
        )
    return 1 if invalid and not args.allow_invalid else 0


if __name__ == "__main__":
    raise SystemExit(main())
