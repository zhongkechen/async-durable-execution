"""Generate a small SVG badge from coverage.py JSON output."""

from __future__ import annotations

import argparse
import html
import json
from pathlib import Path


def read_coverage_percent(coverage_json_path: Path) -> float:
    coverage_data = json.loads(coverage_json_path.read_text(encoding="utf-8"))
    totals = coverage_data.get("totals")
    if not isinstance(totals, dict):
        raise ValueError("coverage JSON does not contain a totals object")

    percent_covered = totals.get("percent_covered")
    if not isinstance(percent_covered, int | float):
        raise ValueError("coverage JSON totals.percent_covered must be numeric")

    return float(percent_covered)


def format_coverage_percent(percent: float) -> str:
    return f"{percent:.0f}%"


def badge_color(percent: float) -> str:
    if percent >= 95:
        return "#4c1"
    if percent >= 90:
        return "#97ca00"
    if percent >= 80:
        return "#a4a61d"
    if percent >= 70:
        return "#dfb317"
    if percent >= 60:
        return "#fe7d37"
    return "#e05d44"


def estimate_text_width(text: str) -> int:
    return len(text) * 7 + 10


def build_badge_svg(label: str, message: str, color: str) -> str:
    label_width = estimate_text_width(label)
    message_width = estimate_text_width(message)
    total_width = label_width + message_width
    label_text_x = label_width // 2
    message_text_x = label_width + message_width // 2

    escaped_label = html.escape(label)
    escaped_message = html.escape(message)
    escaped_color = html.escape(color, quote=True)

    return f"""<svg xmlns="http://www.w3.org/2000/svg" width="{total_width}" height="20" role="img" aria-label="{escaped_label}: {escaped_message}">
  <linearGradient id="s" x2="0" y2="100%">
    <stop offset="0" stop-color="#bbb" stop-opacity=".1"/>
    <stop offset="1" stop-opacity=".1"/>
  </linearGradient>
  <clipPath id="r">
    <rect width="{total_width}" height="20" rx="3" fill="#fff"/>
  </clipPath>
  <g clip-path="url(#r)">
    <rect width="{label_width}" height="20" fill="#555"/>
    <rect x="{label_width}" width="{message_width}" height="20" fill="{escaped_color}"/>
    <rect width="{total_width}" height="20" fill="url(#s)"/>
  </g>
  <g fill="#fff" text-anchor="middle" font-family="Verdana,Geneva,DejaVu Sans,sans-serif" text-rendering="geometricPrecision" font-size="110">
    <text aria-hidden="true" x="{label_text_x * 10}" y="150" fill="#010101" fill-opacity=".3" transform="scale(.1)" textLength="{(label_width - 10) * 10}">{escaped_label}</text>
    <text x="{label_text_x * 10}" y="140" transform="scale(.1)" textLength="{(label_width - 10) * 10}">{escaped_label}</text>
    <text aria-hidden="true" x="{message_text_x * 10}" y="150" fill="#010101" fill-opacity=".3" transform="scale(.1)" textLength="{(message_width - 10) * 10}">{escaped_message}</text>
    <text x="{message_text_x * 10}" y="140" transform="scale(.1)" textLength="{(message_width - 10) * 10}">{escaped_message}</text>
  </g>
</svg>
"""


def generate_badge(coverage_json_path: Path, output_path: Path) -> None:
    percent = read_coverage_percent(coverage_json_path)
    svg = build_badge_svg(
        label="coverage",
        message=format_coverage_percent(percent),
        color=badge_color(percent),
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(svg, encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate an SVG coverage badge from coverage.py JSON output."
    )
    parser.add_argument("coverage_json_path", type=Path)
    parser.add_argument("output_path", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    generate_badge(args.coverage_json_path, args.output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
