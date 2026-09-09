"""Count package structure and render measured results without timing invalid work."""

from __future__ import annotations

import ast
import io
import tokenize
from pathlib import Path


LABELS = {
    "sequential_10": "10 sequential steps",
    "parallel_20": "20 parallel branches",
    "map_32_limit_8": "32 map items, concurrency 8",
    "dag_diamond_4": "4-node diamond DAG",
    "large_flat_4x80k": "4 flat branches × 80,000-byte results",
}


def source_metrics(package: Path) -> dict[str, int]:
    """Count physical code lines, excluding blanks, comments and docstring tokens."""
    result = dict.fromkeys(
        (
            "modules",
            "python_bytes",
            "physical_lines",
            "code_lines",
            "classes",
            "functions",
        ),
        0,
    )
    for path in sorted(package.rglob("*.py")):
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        nodes = list(ast.walk(tree))
        # Include attribute docstrings and other standalone documentation strings,
        # matching the original comparison. Assigned/returned strings remain code.
        docs = [
            ((node.lineno, node.col_offset), (node.end_lineno, node.end_col_offset))
            for node in nodes
            if isinstance(node, ast.Expr)
            and isinstance(node.value, ast.Constant)
            and isinstance(node.value.value, str)
        ]
        lines: set[int] = set()
        for token in tokenize.generate_tokens(io.StringIO(source).readline):
            if token.type in (
                tokenize.COMMENT,
                tokenize.NL,
                tokenize.NEWLINE,
                tokenize.INDENT,
                tokenize.DEDENT,
                tokenize.ENDMARKER,
                tokenize.ENCODING,
            ):
                continue
            if token.type == tokenize.STRING and any(
                start <= token.start and token.end <= end for start, end in docs
            ):
                continue
            lines.update(range(token.start[0], token.end[0] + 1))
        result["modules"] += 1
        result["python_bytes"] += len(source.encode())
        result["physical_lines"] += len(source.splitlines())
        result["code_lines"] += len(lines)
        result["classes"] += sum(isinstance(node, ast.ClassDef) for node in nodes)
        result["functions"] += sum(
            isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) for node in nodes
        )
    return result


def has_failures(versions: dict) -> bool:
    """Treat incorrect outputs, repeated effects and extra replay writes as failures."""
    for version in versions.values():
        for group in version["measurements"]["workflows"].values():
            if "unavailable" in group:
                continue
            if any(
                not row["first_valid"] or not row["replay_valid"]
                for row in group.values()
            ):
                return True
        if any(
            not row["valid"]
            for row in version["measurements"]["serialization"].values()
        ):
            return True
    return False


def render_report(packet: dict) -> str:
    """Render a comparison with revision pins, methodology and correctness outcomes."""
    versions = packet["versions"]
    names = list(versions)
    lines = ["Benchmark comparison", "", f"Measured at {packet['created_at']}.", ""]
    if has_failures(versions):
        lines += [
            "**Correctness failures detected. Invalid execution/replay timings are withheld below.**",
            "",
        ]

    def table(label: str, rows: list[list[str]]) -> None:
        lines.extend(
            [
                "| " + " | ".join([label, *names]) + " |",
                "| " + " | ".join(["---", *["---:"] * len(names)]) + " |",
            ]
        )
        lines.extend("| " + " | ".join(row) + " |" for row in rows)
        lines.append("")

    table(
        "Source",
        [
            ["Commit", *[f"`{versions[n]['commit']}`" for n in names]],
            [
                "Package version",
                *[versions[n]["measurements"]["version"] for n in names],
            ],
        ],
    )
    table(
        "Package footprint",
        [
            [label, *[f"{versions[n]['source'][key]:,}" for n in names]]
            for label, key in [
                ("Modules", "modules"),
                ("Physical Python lines", "physical_lines"),
                ("Code lines excluding comments/docstrings", "code_lines"),
                ("Classes", "classes"),
                ("Function definitions", "functions"),
                ("Python source bytes", "python_bytes"),
            ]
        ]
        + [
            [
                "Wheel bytes (dependencies excluded)",
                *[f"{versions[n]['wheel_bytes']:,}" for n in names],
            ]
        ],
    )
    table(
        "Fresh-process imports",
        [
            [
                "Import median, ms",
                *[f"{versions[n]['imports']['ms']:.3f}" for n in names],
            ],
            [
                "Peak process RSS, KiB",
                *[
                    str(versions[n]["imports"]["peak_rss_kib"])
                    if versions[n]["imports"]["peak_rss_kib"] is not None
                    else "N/A"
                    for n in names
                ],
            ],
        ],
    )
    for phase in ("first", "replay"):
        rows = []
        for case, label in LABELS.items():
            latencies = sorted(
                {
                    int(k)
                    for version in versions.values()
                    for k in version["measurements"]["workflows"].get(case, {})
                    if k != "unavailable"
                }
            )
            for latency in latencies:
                cells = [f"{label}; {latency} ms/checkpoint"]
                for version in versions.values():
                    row = (
                        version["measurements"]["workflows"]
                        .get(case, {})
                        .get(str(latency))
                    )
                    cells.append(
                        "N/A"
                        if row is None
                        else "INVALID"
                        if not row[phase + "_valid"]
                        else f"{row[phase + '_ms']:.3f}"
                    )
                rows.append(cells)
        table(f"{phase.capitalize()} invocation median, ms", rows)
    rows = []
    for case, label in LABELS.items():
        cells = [label]
        for version in versions.values():
            group = version["measurements"]["workflows"].get(case, {})
            row = next(
                (row for key, row in group.items() if key != "unavailable"), None
            )
            cells.append(
                "N/A"
                if row is None
                else "INVALID"
                if not row["first_valid"]
                else f"{row['checkpoint_calls']} / {row['operation_updates']} / {row['request_bytes']}"
            )
        rows.append(cells)
    table("Checkpoint requests / updates / request bytes", rows)
    table(
        "Serializer bytes; encode / decode median ms",
        [
            [
                name,
                *[
                    f"{r['encoded_bytes']:,}; {r['encode_ms']:.3f} / {r['decode_ms']:.3f}"
                    if r["valid"]
                    else "INVALID"
                    for v in versions.values()
                    for r in [v["measurements"]["serialization"][name]]
                ],
            ]
            for name in next(iter(versions.values()))["measurements"]["serialization"]
        ],
    )
    lines += [
        f"Python: `{packet['python']}`. Configuration: `{packet['configuration']}`.",
        "",
        "SDKs run sequentially with the same interpreter and pinned dependencies. Each first invocation has fresh history; "
        "replay uses that SDK’s completed history. Validation checks expected outputs, effect counts, zero repeated effects, "
        "and zero new replay checkpoint requests. Serializer values must round-trip unchanged.",
        "",
        "The shared fake service uses sync methods for AWS and async methods for the async SDKs. Async step effects yield once; "
        "sync effects return directly. Default checkpoint scheduling is preserved. No live AWS calls are made. "
        "The fixture does not enforce AWS quotas or payload limits, and does not measure retries, suspension, real I/O, or saturation. "
        "Checkpoint counts are not billing estimates. N/A denotes an unselected workload or no direct public API counterpart.",
        "",
        "Imports use fresh processes in rotated order after warmup, with warm filesystem caches and bytecode writes disabled. "
        "AWS imports its context/execution/config/serdes public modules; the async SDK exposes these features at package level. "
        "RSS includes the interpreter and dependencies; N/A means unsupported on this platform.",
        "",
        "Source and wheel sizes cover whole SDK packages with different feature scopes: async runner support is bundled, "
        "while AWS testing tools are separate. Source counts use AST/token analysis. Test coverage and documentation coverage "
        "are not benchmarked. See [metrics.json](metrics.json) for all samples, traces, dependencies and validity flags.",
        "",
    ]
    for name, version in versions.items():
        for case, group in version["measurements"]["workflows"].items():
            if "unavailable" in group:
                continue
            for latency, row in group.items():
                if not row["first_valid"] or not row["replay_valid"]:
                    lines.append(
                        f"- {name}, {case}, {latency} ms: {row.get('error') or '; '.join(row['failures'])}"
                    )
        for payload, row in version["measurements"]["serialization"].items():
            if not row["valid"]:
                lines.append(f"- {name}, serializer {payload}: {row['error']}")
    return "\n".join(lines) + "\n"
