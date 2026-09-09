"""User-facing call shapes, independent of private module layout or class bases."""

import dataclasses
import enum
import importlib
import inspect
import json
from pathlib import Path
import pytest

CONTRACT = json.loads(
    (Path(__file__).parent / "contracts/public-api.json").read_text()
)["modules"]["async_durable_execution"]


def normalized(value):
    if isinstance(value, enum.Enum):
        return {"enum": type(value).__name__, "value": value.value}
    if isinstance(value, (list, tuple)):
        return list(value)
    if value is getattr(dataclasses, "_HAS_DEFAULT_FACTORY"):
        return "<factory>"
    return value


def check_signature(value, expected):
    observed = list(inspect.signature(value).parameters.values())
    expected = expected["parameters"]
    assert [p.name for p in observed] == [p["name"] for p in expected]
    assert [p.kind.name for p in observed] == [p["kind"] for p in expected]
    for actual, wanted in zip(observed, expected):
        if "default" not in wanted:
            assert actual.default is inspect.Parameter.empty
        elif isinstance(wanted["default"], str) and wanted["default"].startswith(
            "<async_durable_execution."
        ):
            assert callable(actual.default), (
                "Default summary generator must remain callable"
            )
        else:
            assert normalized(actual.default) == wanted["default"]


@pytest.mark.parametrize("name", CONTRACT)
def test_public_contract(name):
    mod = importlib.import_module("async_durable_execution")
    spec = CONTRACT[name]
    value = getattr(mod, name)
    if hasattr(mod, "__all__"):
        assert name in mod.__all__
    if "members" in spec:
        assert {k: v.value for k, v in value.__members__.items()} == spec["members"]
        return
    if "signature" in spec:
        check_signature(value, spec["signature"])
    if "fields" in spec:
        assert [
            f.name for f in dataclasses.fields(value) if not f.name.startswith("_")
        ] == spec["fields"]
        assert value.__dataclass_params__.frozen == spec["frozen"]
    for method, sig in spec.get("methods", {}).items():
        check_signature(getattr(value, method), sig)
    for prop in spec.get("properties", []):
        assert isinstance(getattr(value, prop), property)
    if "async" in spec:
        assert inspect.iscoroutinefunction(value) == spec["async"]
