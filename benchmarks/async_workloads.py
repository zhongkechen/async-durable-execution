"""The same async public workflows run against v2 and the current SDK."""

import asyncio
import async_durable_execution as sdk


def workflow(case, api):
    async def effect(value):
        api.effect()
        await asyncio.sleep(0)
        return value

    async def branch(index):
        return await sdk.step(lambda: effect(index), name=f"step-{index}")

    @sdk.durable_node
    async def source():
        return await sdk.step(lambda: effect(1), name="source-step")

    @sdk.durable_node
    async def middle(value, offset):
        return await sdk.step(lambda: effect(value + offset), name=f"middle-{offset}")

    @sdk.durable_node
    async def sink(a, b):
        return await sdk.step(lambda: effect(a + b), name="sink-step")

    @sdk.durable_dag
    def diamond():
        a = sdk.node(source(), name="source")
        b = sdk.node(middle(a.outcome, 1), name="left")
        c = sdk.node(middle(a.outcome, 2), name="right")
        d = sdk.node(sink(b.outcome, c.outcome), name="sink")
        return d.outcome

    def bind_branch(index):
        async def run_branch():
            return await branch(index)

        return run_branch

    @sdk.durable_execution(boto3_client=api)
    async def handler(event):
        if case == "sequential_10":
            values = []
            for i in range(10):
                values.append(await branch(i))
            return sum(values)
        if case == "parallel_20":
            result = await sdk.parallel(
                [bind_branch(i) for i in range(20)], name="parallel"
            )
            return sum(result.get_results())
        if case == "map_32_limit_8":
            return (
                await sdk.map(branch, range(32), max_concurrency=8, name="map")
            ).get_results()
        if case == "dag_diamond_4":
            return (await sdk.flow(diamond(), name="diamond")).output
        if case == "large_flat_4x80k":

            async def large():
                return await sdk.step(lambda: effect("x" * 80000), name="blob")

            result = await sdk.parallel(
                [large] * 4, name="large", nesting_type=sdk.NestingType.FLAT
            )
            return {
                "items": len(result.get_results()),
                "bytes": sum(map(len, result.get_results())),
            }
        raise ValueError(case)

    return handler
