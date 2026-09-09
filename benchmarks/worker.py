"""Measure one installed SDK in its own process using public APIs only."""

from __future__ import annotations

import argparse
import gc
import importlib.metadata
import json
import platform
import statistics
import sys
import time
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable

from .backend import CONTEXT, AsyncBackend, Backend, SyncBackend

EXPECTED = {
    "sequential_10": 45,
    "parallel_20": 190,
    "map_32_limit_8": list(range(32)),
    "dag_diamond_4": 5,
    "large_flat_4x80k": {"items": 4, "bytes": 320000},
}
EFFECTS = dict(zip(EXPECTED, (10, 20, 32, 4, 4)))
PAYLOADS = {
    "integers_1000": list(range(1000)),
    "orders_100": [
        {
            "id": i,
            "name": f"product-{i}",
            "price": Decimal("12.34"),
            "tags": ["sale", "stock"],
        }
        for i in range(100)
    ],
    "text_64k": "x" * 65536,
}


def positive_int(value: str) -> int:
    """Parse a sample count that can produce a median."""
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return number


def nonnegative_int(value: str) -> int:
    """Parse a warmup count or latency in milliseconds."""
    number = int(value)
    if number < 0:
        raise argparse.ArgumentTypeError("must be nonnegative")
    return number


def load_adapter(kind: str) -> tuple[Callable, Callable, Callable, type[Backend]]:
    """Select only the public API corresponding to the installed SDK."""
    if kind == "official":
        from aws_durable_execution_sdk_python.serdes import ExtendedTypeSerDes
        from .official_workloads import workflow

        codec = ExtendedTypeSerDes()
        return workflow, codec.serialize, codec.deserialize, SyncBackend
    from async_durable_execution import ExtendedTypeSerDes as AsyncSerDes
    from .async_workloads import workflow as async_workflow

    codec = AsyncSerDes()
    return async_workflow, codec.serialize_sync, codec.deserialize_sync, AsyncBackend


def decode_result(outcome: dict) -> Any:
    """Decode the public Lambda invocation result without accepting suspension."""
    if outcome.get("Status") != "SUCCEEDED":
        raise ValueError(f"handler returned {outcome.get('Status')!r}")
    return json.loads(outcome["Result"])


def measure_workflow(
    case: str,
    latency_ms: int,
    factory: Callable,
    backend: type[Backend],
    samples: int,
    warmups: int,
) -> dict:
    """Check fresh execution and completed replay, retaining all measured samples."""
    api = backend(latency_ms / 1000)
    handler = factory(case, api)
    measured = []
    first_valid = replay_valid = True
    failures: set[str] = set()
    for repeat in range(warmups + samples):
        api.reset()
        event = api.event()
        gc.collect()
        api.started = time.perf_counter()
        start = time.perf_counter()
        first = decode_result(handler(event, CONTEXT))
        first_ms = (time.perf_counter() - start) * 1000
        if first != EXPECTED[case] or api.effects != EFFECTS[case]:
            first_valid = False
            failures.add("first output or effect count differs from the workload")
        metrics = {
            "checkpoint_calls": api.calls,
            "operation_updates": api.updates,
            "request_bytes": api.request_bytes,
            "operation_records": len(api.ops) - 1,
            "effects": api.effects,
        }
        trace = api.trace[:]
        replay_event = api.event()
        previous_calls = api.calls
        api.effects = 0
        start = time.perf_counter()
        replay = decode_result(handler(replay_event, CONTEXT))
        replay_ms = (time.perf_counter() - start) * 1000
        if replay != EXPECTED[case]:
            replay_valid = False
            failures.add("replay output differs from the workload")
        if api.effects:
            replay_valid = False
            failures.add("replay repeated completed step effects")
        if api.calls != previous_calls:
            replay_valid = False
            failures.add("replay issued new checkpoint requests")
        if repeat >= warmups:
            measured.append(
                {
                    "first_ms": first_ms,
                    "replay_ms": replay_ms,
                    "first_output": first,
                    "replay_output": replay,
                    "replay_effects": api.effects,
                    "replay_checkpoint_calls": api.calls - previous_calls,
                    "checkpoint_trace": trace,
                    **metrics,
                }
            )
    return {
        "first_valid": first_valid,
        "replay_valid": replay_valid,
        "failures": sorted(failures),
        "samples": measured,
        **{
            key: statistics.median(row[key] for row in measured)
            for key in ("first_ms", "replay_ms", *metrics)
        },
    }


def measure_codec(
    value: Any,
    encode: Callable,
    decode: Callable,
    samples: int,
    warmups: int,
    iterations: int,
) -> dict:
    """Measure a checked serializer round trip in repeated blocks."""
    encoded = encode(value)
    if decode(encoded) != value:
        return {"valid": False, "error": "serializer round trip changed the value"}
    measured = []
    for repeat in range(warmups + samples):
        gc.collect()
        start = time.perf_counter()
        for _ in range(iterations):
            encode(value)
        encode_ms = (time.perf_counter() - start) * 1000 / iterations
        start = time.perf_counter()
        for _ in range(iterations):
            decode(encoded)
        decode_ms = (time.perf_counter() - start) * 1000 / iterations
        if repeat >= warmups:
            measured.append({"encode_ms": encode_ms, "decode_ms": decode_ms})
    return {
        "valid": True,
        "encoded_bytes": len(encoded.encode()),
        "samples": measured,
        **{
            key: statistics.median(row[key] for row in measured)
            for key in ("encode_ms", "decode_ms")
        },
    }


def main() -> None:
    """Write results even when a workflow fails its correctness checks."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sdk", choices=("async", "official"), required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--samples", type=positive_int, default=5)
    parser.add_argument("--warmups", type=nonnegative_int, default=1)
    parser.add_argument("--iterations", type=positive_int, default=100)
    parser.add_argument(
        "--latencies-ms", type=nonnegative_int, nargs="+", default=[0, 20, 150]
    )
    parser.add_argument(
        "--cases", choices=tuple(EXPECTED), nargs="+", default=list(EXPECTED)
    )
    args = parser.parse_args()
    factory, encode, decode, backend = load_adapter(args.sdk)
    package = (
        "aws-durable-execution-sdk-python"
        if args.sdk == "official"
        else "async-durable-execution"
    )
    distribution = importlib.metadata.distribution(package)
    result: dict[str, Any] = {
        "schema_version": 1,
        "package": package,
        "version": distribution.version,
        "python": sys.version,
        "platform": platform.platform(),
        "installed_package_root": str(distribution.locate_file("")),
        "dependencies": {
            d.metadata["Name"]: d.version
            for d in importlib.metadata.distributions()
            if d.metadata["Name"]
            not in ("aws-durable-execution-sdk-python", "async-durable-execution")
        },
        "configuration": vars(args) | {"output": str(args.output)},
        "workflows": {},
        "serialization": {},
    }
    for case in args.cases:
        if case == "dag_diamond_4" and args.sdk == "official":
            result["workflows"][case] = {
                "unavailable": "No direct public API counterpart"
            }
            continue
        result["workflows"][case] = {}
        for latency in args.latencies_ms if case == "sequential_10" else [0]:
            try:
                row = measure_workflow(
                    case, latency, factory, backend, args.samples, args.warmups
                )
            except Exception as error:
                row = {
                    "first_valid": False,
                    "replay_valid": False,
                    "error": f"{type(error).__name__}: {error}",
                }
            result["workflows"][case][str(latency)] = row
            print(
                f"{case} ({latency} ms): first={row['first_valid']} replay={row['replay_valid']}",
                flush=True,
            )
    for name, value in PAYLOADS.items():
        try:
            row = measure_codec(
                value, encode, decode, args.samples, args.warmups, args.iterations
            )
        except Exception as error:
            row = {"valid": False, "error": f"{type(error).__name__}: {error}"}
        result["serialization"][name] = row
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
