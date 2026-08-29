"""Tests for structured SerDes previews."""

import pytest

from async_durable_execution import (
    FieldMatchMode,
    PreviewConfig,
    PreviewField,
    PreviewMode,
    build_preview,
)


def test_preview_include_exclude_mask_and_paths() -> None:
    value = {
        "id": "order-1",
        "email": "customer@example.com",
        "nested": {
            "email": "nested@example.com",
            "visible": True,
        },
        "items": [{"sku": "a"}, {"sku": "b"}],
    }
    preview = build_preview(
        value,
        PreviewConfig(
            mode=PreviewMode.INCLUDE_ALL,
            exclude=(PreviewField("items"),),
            mask=(PreviewField("email"),),
        ),
    )

    assert preview == {
        "id": "order-1",
        "email": "***",
        "nested": {
            "email": "***",
            "visible": True,
        },
    }

    exact = build_preview(
        value,
        PreviewConfig(
            mode=PreviewMode.EXCLUDE_ALL,
            include=(PreviewField("nested.visible", FieldMatchMode.PATH),),
        ),
    )
    assert exact == {"nested": {"visible": True}}


def test_preview_respects_utf8_byte_budget() -> None:
    preview = build_preview(
        {"first": "🚀", "second": "too large"},
        PreviewConfig(
            mode=PreviewMode.INCLUDE_ALL,
            max_preview_bytes=17,
        ),
    )

    assert preview == {"first": "🚀"}


def test_preview_returns_none_for_non_mapping_or_no_visible_fields() -> None:
    config = PreviewConfig(mode=PreviewMode.EXCLUDE_ALL)

    assert build_preview(["not", "an", "object"], config) is None
    assert build_preview({"hidden": True}, config) is None


def test_preview_validates_field_and_budget_configuration() -> None:
    with pytest.raises(ValueError, match="name"):
        PreviewField("")
    with pytest.raises(ValueError, match="positive"):
        PreviewConfig(
            mode=PreviewMode.INCLUDE_ALL,
            max_preview_bytes=0,
        )


def test_preview_exclusion_wins_and_arrays_merge_visible_fields() -> None:
    preview = build_preview(
        {
            "items": [
                {"id": "first", "secret": "one"},
                {"id": "second", "secret": "two"},
            ],
            "literal.dot": "ignored",
        },
        PreviewConfig(
            mode=PreviewMode.EXCLUDE_ALL,
            include=(PreviewField("id"),),
            exclude=(PreviewField("secret"),),
            mask=(PreviewField("secret"),),
        ),
    )

    assert preview == {"items": {"id": "second"}}
