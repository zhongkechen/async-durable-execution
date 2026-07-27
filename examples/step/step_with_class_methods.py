from typing import Any

from async_durable_execution import (
    durable_callable,
    durable_execution,
    step,
)


class OrderCalculator:
    def __init__(self, tax_rate: float) -> None:
        self.tax_rate = tax_rate

    @durable_callable
    async def subtotal(self, prices: list[float]) -> float:
        return sum(prices)

    @durable_callable
    @classmethod
    async def discount(cls, subtotal: float) -> float:
        del cls
        return subtotal * 0.1

    @staticmethod
    @durable_callable
    async def total(subtotal: float, discount: float, tax_rate: float) -> float:
        return round((subtotal - discount) * (1 + tax_rate), 2)


@durable_execution
async def handler(event: dict[str, Any]) -> dict[str, float]:
    calculator = OrderCalculator(tax_rate=float(event.get("tax_rate", 0.08)))
    prices = [float(price) for price in event.get("prices", [25.0, 15.0, 10.0])]

    subtotal = await step(calculator.subtotal(prices), name="subtotal")
    discount = await step(OrderCalculator.discount(subtotal), name="discount")
    total = await step(
        OrderCalculator.total(subtotal, discount, calculator.tax_rate),
        name="total",
    )

    return {
        "subtotal": subtotal,
        "discount": discount,
        "total": total,
    }
