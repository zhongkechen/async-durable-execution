from __future__ import annotations

import hashlib

import pytest

from examples.function_naming import HASH_LENGTH
from examples.function_naming import HANDLER_PACKAGE_PREFIX
from examples.function_naming import LEGACY_HANDLER_PACKAGE_PREFIX
from examples.function_naming import to_function_name_suffix
from examples.function_naming import to_legacy_handler_name
from examples.function_naming import to_logical_id


def test_to_logical_id_removes_handler_suffix_and_separators() -> None:
    assert to_logical_id("map.map-with_custom.handler") == "MapMapWithCustom"


def test_to_function_name_suffix_strips_example_package_prefix() -> None:
    assert (
        to_function_name_suffix(
            f"{HANDLER_PACKAGE_PREFIX}"
            "extension.wait_for_callback.wait_for_callback.handler"
        )
        == "WaitForCallbackWaitForCallback"
    )


@pytest.mark.parametrize(
    ("handler_name", "expected"),
    [
        (
            "examples.core.hello_world.handler",
            "async_durable_execution_examples.hello_world.handler",
        ),
        (
            "examples.primitive.step.step.handler",
            "async_durable_execution_examples.step.step.handler",
        ),
        (
            "examples.primitive.child.run_in_child_context.handler",
            "async_durable_execution_examples.run_in_child_context."
            "run_in_child_context.handler",
        ),
        (
            "examples.primitive.child.block_example.handler",
            "async_durable_execution_examples.block_example.block_example.handler",
        ),
        (
            "examples.extension.recurse.recurse.handler",
            "async_durable_execution_examples.invoke.recurse.handler",
        ),
    ],
)
def test_to_legacy_handler_name_preserves_preorganization_path(
    handler_name: str, expected: str
) -> None:
    assert to_legacy_handler_name(handler_name) == expected


def test_to_function_name_suffix_truncates_with_stable_hash() -> None:
    handler_without_package = (
        "this_is_a_very_long_module_name.with_a_very_long_handler_name.handler"
    )
    handler_name = f"{HANDLER_PACKAGE_PREFIX}{handler_without_package}"
    stable_handler_name = f"{LEGACY_HANDLER_PACKAGE_PREFIX}{handler_without_package}"
    digest = hashlib.sha1(stable_handler_name.encode("utf-8")).hexdigest()[:HASH_LENGTH]

    suffix = to_function_name_suffix(handler_name, max_length=24)

    assert suffix == f"ThisIsAVeryLong-{digest}"
    assert len(suffix) == 24


@pytest.mark.parametrize(
    ("handler_name", "expected"),
    [
        (
            "examples.extension.wait_for_callback.wait_for_callback_multiple_invocations.handler",
            "WaitForCallbackWaitForCallbackMultipleI-dcfc7f44",
        ),
        (
            "examples.extension.wait_for_callback."
            "wait_for_callback_submitter_failure_catchable.handler",
            "WaitForCallbackWaitForCallbackSubmitter-5fe5aebf",
        ),
    ],
)
def test_to_function_name_suffix_preserves_deployed_names(
    handler_name: str, expected: str
) -> None:
    assert to_function_name_suffix(handler_name) == expected


def test_to_function_name_suffix_rejects_too_small_max_length() -> None:
    with pytest.raises(ValueError, match="max_length must be at least 10"):
        to_function_name_suffix(
            "this_name_is_long_enough_to_require_truncation.handler",
            max_length=HASH_LENGTH + 1,
        )
