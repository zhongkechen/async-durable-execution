"""Measure fresh-process imports needed to define these workflow/codec tests."""

import json

import sys
import time


def peak_rss_kib():
    try:
        import resource
    except ImportError:
        return None
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return value / (1024 if sys.platform == "darwin" else 1)


start = time.perf_counter()
if sys.argv[1] == "official":
    from aws_durable_execution_sdk_python.context import DurableContext
    from aws_durable_execution_sdk_python.execution import durable_execution
    from aws_durable_execution_sdk_python.config import MapConfig, ParallelConfig
    from aws_durable_execution_sdk_python.serdes import ExtendedTypeSerDes
else:
    import async_durable_execution
elapsed = time.perf_counter() - start
print(json.dumps({"ms": elapsed * 1000, "peak_rss_kib": peak_rss_kib()}))
