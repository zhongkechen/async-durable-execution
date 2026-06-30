from __future__ import annotations

import hashlib

import pytest

from scripts.function_naming import HASH_LENGTH
from scripts.function_naming import HANDLER_PACKAGE_PREFIX
from scripts.function_naming import to_function_name_suffix
from scripts.function_naming import to_logical_id


def test_to_logical_id_removes_handler_suffix_and_separators() -> None:
    assert to_logical_id("map.map-with_custom.handler") == "MapMapWithCustom"


def test_to_function_name_suffix_strips_example_package_prefix() -> None:
    assert (
        to_function_name_suffix(
            f"{HANDLER_PACKAGE_PREFIX}wait_for_callback.wait_for_callback.handler"
        )
        == "WaitForCallbackWaitForCallback"
    )


def test_to_function_name_suffix_truncates_with_stable_hash() -> None:
    handler_name = (
        f"{HANDLER_PACKAGE_PREFIX}"
        "this_is_a_very_long_module_name.with_a_very_long_handler_name.handler"
    )
    digest = hashlib.sha1(handler_name.encode("utf-8")).hexdigest()[:HASH_LENGTH]

    suffix = to_function_name_suffix(handler_name, max_length=24)

    assert suffix == f"ThisIsAVeryLong-{digest}"
    assert len(suffix) == 24


def test_to_function_name_suffix_rejects_too_small_max_length() -> None:
    with pytest.raises(ValueError, match="max_length must be at least 10"):
        to_function_name_suffix(
            "this_name_is_long_enough_to_require_truncation.handler",
            max_length=HASH_LENGTH + 1,
        )
