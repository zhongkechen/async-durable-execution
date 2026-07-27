import asyncio

from async_durable_execution._core.task import create_eager_task


async def test_create_eager_task_fallback_uses_lazy_task(monkeypatch):
    monkeypatch.setattr(asyncio, "eager_task_factory", None, raising=False)
    started = []

    async def operation() -> str:
        started.append("operation")
        await asyncio.sleep(0)
        return "done"

    task = create_eager_task(operation)
    started.append("after-call")

    assert isinstance(task, asyncio.Task)
    assert started == ["after-call"]
    assert await task == "done"
    assert started == ["after-call", "operation"]


async def test_create_eager_task_uses_eager_task_factory_when_available(monkeypatch):
    started = []

    def task_factory(
        loop: asyncio.AbstractEventLoop,
        coro,
    ) -> asyncio.Task[str]:
        started.append("factory")
        return loop.create_task(coro)

    async def operation() -> str:
        return "done"

    monkeypatch.setattr(
        asyncio,
        "eager_task_factory",
        task_factory,
        raising=False,
    )

    task = create_eager_task(operation)

    assert isinstance(task, asyncio.Task)
    assert started == ["factory"]
    assert await task == "done"
