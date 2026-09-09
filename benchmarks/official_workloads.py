"""Synchronous public-API counterparts of the existing async workloads."""

from aws_durable_execution_sdk_python.config import (
    CompletionConfig,
    MapConfig,
    NestingType,
    ParallelConfig,
)
from aws_durable_execution_sdk_python.execution import durable_execution


def workflow(case, api):
    def effect(value):
        api.effect()
        return value

    def branch(context, index):
        return context.step(lambda step_context: effect(index), name=f"step-{index}")

    @durable_execution(boto3_client=api)
    def handler(event, context):
        if case == "sequential_10":
            return sum(branch(context, i) for i in range(10))
        if case == "parallel_20":
            result = context.parallel(
                [lambda child, i=i: branch(child, i) for i in range(20)],
                name="parallel",
            )
            return sum(result.get_results())
        if case == "map_32_limit_8":
            return context.map(
                list(range(32)),
                lambda child, item, index, items: branch(child, item),
                name="map",
                config=MapConfig(
                    max_concurrency=8,
                    completion_config=CompletionConfig.all_successful(),
                ),
            ).get_results()
        if case == "large_flat_4x80k":

            def large(child):
                return child.step(lambda step_context: effect("x" * 80000), name="blob")

            result = context.parallel(
                [large] * 4,
                name="large",
                config=ParallelConfig(nesting_type=NestingType.FLAT),
            )
            return {
                "items": len(result.get_results()),
                "bytes": sum(map(len, result.get_results())),
            }
        raise ValueError(case)

    return handler
