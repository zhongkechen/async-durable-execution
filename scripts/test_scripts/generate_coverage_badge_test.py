import json

import pytest

from scripts.generate_coverage_badge import badge_color
from scripts.generate_coverage_badge import format_coverage_percent
from scripts.generate_coverage_badge import generate_badge
from scripts.generate_coverage_badge import read_coverage_percent


def test_read_coverage_percent_reads_total_from_coverage_json(tmp_path):
    coverage_json = tmp_path / "coverage.json"
    coverage_json.write_text(
        json.dumps({"totals": {"percent_covered": 98.31}}),
        encoding="utf-8",
    )

    assert read_coverage_percent(coverage_json) == 98.31


def test_read_coverage_percent_requires_totals(tmp_path):
    coverage_json = tmp_path / "coverage.json"
    coverage_json.write_text(json.dumps({}), encoding="utf-8")

    with pytest.raises(ValueError, match="totals object"):
        read_coverage_percent(coverage_json)


def test_read_coverage_percent_requires_numeric_percent(tmp_path):
    coverage_json = tmp_path / "coverage.json"
    coverage_json.write_text(
        json.dumps({"totals": {"percent_covered": "98"}}),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="must be numeric"):
        read_coverage_percent(coverage_json)


@pytest.mark.parametrize(
    ("percent", "expected"),
    [
        (95, "#4c1"),
        (90, "#97ca00"),
        (80, "#a4a61d"),
        (70, "#dfb317"),
        (60, "#fe7d37"),
        (59.99, "#e05d44"),
    ],
)
def test_badge_color_thresholds(percent, expected):
    assert badge_color(percent) == expected


def test_format_coverage_percent_rounds_to_integer_percent():
    assert format_coverage_percent(98.31) == "98%"
    assert format_coverage_percent(98.5) == "98%"


def test_generate_badge_writes_svg_with_current_coverage(tmp_path):
    coverage_json = tmp_path / "coverage.json"
    output_path = tmp_path / "coverage" / "badge.svg"
    coverage_json.write_text(
        json.dumps({"totals": {"percent_covered": 92.4}}),
        encoding="utf-8",
    )

    generate_badge(coverage_json, output_path)

    svg = output_path.read_text(encoding="utf-8")
    assert svg.startswith("<svg")
    assert "coverage: 92%" in svg
    assert ">92%<" in svg
    assert "#97ca00" in svg
