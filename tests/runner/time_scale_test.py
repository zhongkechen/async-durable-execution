"""Tests for local runner time scaling helpers."""

import logging

from async_durable_execution._runner.local.time_scale import scale_delay
from async_durable_execution._runner.local.time_scale import get_time_scale


def test_get_time_scale_defaults_to_one(monkeypatch) -> None:
    monkeypatch.delenv("DURABLE_EXECUTION_TIME_SCALE", raising=False)

    assert get_time_scale() == 1.0


def test_get_time_scale_reads_valid_float(monkeypatch) -> None:
    monkeypatch.setenv("DURABLE_EXECUTION_TIME_SCALE", "0.25")

    assert get_time_scale() == 0.25


def test_get_time_scale_ignores_invalid_value(monkeypatch, caplog) -> None:
    monkeypatch.setenv("DURABLE_EXECUTION_TIME_SCALE", "nope")

    with caplog.at_level(logging.WARNING):
        assert get_time_scale() == 1.0

    assert "Ignoring invalid DURABLE_EXECUTION_TIME_SCALE value: nope" in caplog.text


def test_get_time_scale_ignores_negative_value(monkeypatch, caplog) -> None:
    monkeypatch.setenv("DURABLE_EXECUTION_TIME_SCALE", "-1")

    with caplog.at_level(logging.WARNING):
        assert get_time_scale() == 1.0

    assert "Ignoring negative DURABLE_EXECUTION_TIME_SCALE value: -1" in caplog.text


def test_scale_delay_without_minimum(monkeypatch) -> None:
    monkeypatch.setenv("DURABLE_EXECUTION_TIME_SCALE", "0.5")

    assert scale_delay(10, minimum=0) == 5.0


def test_scale_delay_respects_minimum_without_exceeding_original(monkeypatch) -> None:
    monkeypatch.setenv("DURABLE_EXECUTION_TIME_SCALE", "0.1")

    assert scale_delay(10, minimum=3) == 3.0
    assert scale_delay(2, minimum=3) == 2.0
