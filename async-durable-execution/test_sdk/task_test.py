import asyncio

from async_durable_execution.task import create_eager_task


async def test_create_eager_task_fallback_starts_before_await(monkeypatch):
    monkeypatch.setattr(asyncio, "eager_task_factory", None, raising=False)
    started = []

    async def operation() -> str:
        started.append("operation")
        await asyncio.sleep(0)
        return "done"

    task = create_eager_task(operation)
    started.append("after-call")

    assert isinstance(task, asyncio.Task)
    assert started == ["operation", "after-call"]
    assert await task == "done"


async def test_create_eager_task_fallback_handles_first_suspension_future(monkeypatch):
    monkeypatch.setattr(asyncio, "eager_task_factory", None, raising=False)
    loop = asyncio.get_running_loop()
    future = loop.create_future()
    started = []

    async def operation() -> str:
        started.append("operation")
        result = await future
        started.append(result)
        return result

    task = create_eager_task(operation)
    started.append("after-call")

    assert isinstance(task, asyncio.Task)
    assert started == ["operation", "after-call"]

    future.set_result("done")
    assert await task == "done"
    assert started == ["operation", "after-call", "done"]
