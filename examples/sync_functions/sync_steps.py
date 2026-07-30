"""Example using synchronous functions and methods as durable steps."""

from typing import Any

from async_durable_execution import durable_callable, durable_execution, step


@durable_callable
def load_unit_price(sku: str) -> float:
    """Represent a synchronous call to an existing pricing client."""
    prices = {"book": 12.5, "pen": 2.0}
    return prices.get(sku, 5.0)


class OrderCalculator:
    def __init__(self, tax_rate: float):
        self.tax_rate = tax_rate

    @durable_callable
    def calculate_total(self, unit_price: float, quantity: int) -> float:
        """Calculate an order total in a synchronous bound method."""
        subtotal = unit_price * quantity
        return round(subtotal * (1 + self.tax_rate), 2)


@durable_execution
async def handler(event: dict[str, Any]) -> dict[str, Any]:
    """Compose synchronous durable callables from an async handler."""
    sku = str(event.get("sku", "book"))
    quantity = int(event.get("quantity", 2))
    calculator = OrderCalculator(tax_rate=float(event.get("tax_rate", 0.08)))

    unit_price = await step(load_unit_price(sku), name="load-unit-price")
    total = await step(
        calculator.calculate_total(unit_price, quantity),
        name="calculate-total",
    )
    return {
        "sku": sku,
        "quantity": quantity,
        "unit_price": unit_price,
        "total": total,
    }
