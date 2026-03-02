#!/usr/bin/env python3
"""
apply_stw_column_map.py -- Normalize STW combined data using a dated column map.

The cleaned combined STW file stores the first three columns as:

    year, day_of_year, hhmm

Every column after that is a raw measurement position whose meaning can change
over time. ``STW programs/column_map.csv`` defines which metric lives in which
1-based measurement column at each effective datetime.

This script applies the latest mapping whose ``Datetime`` is less than or equal
to each row's timestamp and writes a stable, reproducible CSV with a single
``datetime`` column plus one column per metric (GHI, DNI, DHI, etc.). If a
metric is not mapped yet, or the input row does not have that many measurement
columns, the output value is ``NaN``.
"""

from __future__ import annotations

import argparse
import csv
from bisect import bisect_right
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT = BASE_DIR / "cleaned_output" / "combined" / "stw_combined_cleaned.csv"
DEFAULT_MAP = BASE_DIR.parent / "STW programs" / "column_map.csv"
DEFAULT_OUTPUT = BASE_DIR / "cleaned_output" / "combined" / "stw_combined_mapped.csv"

TIME_KEY_COLUMNS = 3
MAP_TIME_FORMAT = "%m/%d/%Y %H:%M"
OUTPUT_TIME_FORMAT = "%Y/%m/%d %H:%M"
MISSING_VALUE = "NaN"
EXCLUDED_METRICS = {"SZA", "AZM"}


@dataclass(frozen=True)
class MappingSnapshot:
    """A single effective-dated column layout."""

    effective_at: datetime
    metric_positions: dict[str, int | None]


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Apply STW's effective-dated column map to the combined cleaned data "
            "and write a stable datetime-plus-metrics CSV."
        )
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help=f"Path to the combined cleaned CSV (default: {DEFAULT_INPUT})",
    )
    parser.add_argument(
        "--column-map",
        type=Path,
        default=DEFAULT_MAP,
        help=f"Path to the dated column map CSV (default: {DEFAULT_MAP})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Path for the mapped output CSV (default: {DEFAULT_OUTPUT})",
    )
    return parser.parse_args()


def parse_required_int(value: str, field_name: str, context: str) -> int:
    """Parse an integer field with a useful error message."""
    text = value.strip()
    try:
        return int(text)
    except ValueError as exc:
        raise ValueError(f"{context}: invalid {field_name!r} value {value!r}") from exc


def parse_hhmm_to_offset(hhmm: int, context: str) -> timedelta:
    """Convert an HHMM integer into a timedelta since midnight."""
    if hhmm == 2400:
        return timedelta(days=1)
    if hhmm < 0:
        raise ValueError(f"{context}: HHMM cannot be negative ({hhmm})")
    hour, minute = divmod(hhmm, 100)
    if hour > 23 or minute > 59:
        raise ValueError(f"{context}: invalid HHMM value {hhmm}")
    return timedelta(hours=hour, minutes=minute)


def build_row_datetime(year_text: str, day_text: str, hhmm_text: str, context: str) -> datetime:
    """Build a real datetime from year/day-of-year/HHMM."""
    year = parse_required_int(year_text, "year", context)
    day_of_year = parse_required_int(day_text, "day_of_year", context)
    hhmm = parse_required_int(hhmm_text, "hhmm", context)

    start_of_year = datetime(year, 1, 1)
    return start_of_year + timedelta(days=day_of_year - 1) + parse_hhmm_to_offset(hhmm, context)


def load_column_map(column_map_path: Path) -> tuple[list[str], list[MappingSnapshot], list[datetime]]:
    """Read the dated column map."""
    with column_map_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError(f"{column_map_path} is empty.")
        if "Datetime" not in reader.fieldnames:
            raise ValueError(f"{column_map_path} must contain a 'Datetime' column.")

        metric_names = [
            name for name in reader.fieldnames if name != "Datetime" and name not in EXCLUDED_METRICS
        ]
        if not metric_names:
            raise ValueError(f"{column_map_path} does not define any metric columns.")

        snapshots: list[MappingSnapshot] = []
        for row_num, row in enumerate(reader, start=2):
            datetime_text = (row.get("Datetime", "") or "").strip()
            if not datetime_text:
                raise ValueError(f"{column_map_path}:{row_num} is missing a Datetime value.")
            try:
                effective_at = datetime.strptime(datetime_text, MAP_TIME_FORMAT)
            except ValueError as exc:
                raise ValueError(
                    f"{column_map_path}:{row_num} has invalid Datetime {datetime_text!r}; "
                    f"expected {MAP_TIME_FORMAT!r}."
                ) from exc

            metric_positions: dict[str, int | None] = {}
            for metric_name in metric_names:
                raw_value = (row.get(metric_name, "") or "").strip()
                if not raw_value:
                    metric_positions[metric_name] = None
                    continue
                position = parse_required_int(raw_value, metric_name, f"{column_map_path}:{row_num}")
                if position < 1:
                    raise ValueError(
                        f"{column_map_path}:{row_num} has invalid {metric_name!r} position {position}; "
                        "positions must be 1 or greater."
                    )
                metric_positions[metric_name] = position

            snapshots.append(MappingSnapshot(effective_at=effective_at, metric_positions=metric_positions))

    if not snapshots:
        raise ValueError(f"{column_map_path} does not contain any mapping rows.")

    snapshots.sort(key=lambda snapshot: snapshot.effective_at)
    effective_points = [snapshot.effective_at for snapshot in snapshots]
    return metric_names, snapshots, effective_points


def resolve_metric_value(measurements: list[str], position: int | None) -> str:
    """Return the mapped measurement value or NaN if it is unavailable."""
    if position is None or position > len(measurements):
        return MISSING_VALUE
    value = measurements[position - 1].strip()
    return value or MISSING_VALUE


def apply_column_map(input_path: Path, column_map_path: Path, output_path: Path) -> int:
    """Apply the dated column map to the combined cleaned data."""
    metric_names, snapshots, effective_points = load_column_map(column_map_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    rows_written = 0
    with input_path.open("r", encoding="utf-8", newline="") as src, output_path.open(
        "w", encoding="utf-8", newline=""
    ) as dst:
        reader = csv.reader(src)
        writer = csv.writer(dst)
        writer.writerow(["datetime", *metric_names])

        for row_num, row in enumerate(reader, start=1):
            if not row:
                continue
            if len(row) < TIME_KEY_COLUMNS:
                raise ValueError(
                    f"{input_path}:{row_num} has {len(row)} columns; expected at least {TIME_KEY_COLUMNS}."
                )

            context = f"{input_path}:{row_num}"
            row_dt = build_row_datetime(row[0], row[1], row[2], context)
            snapshot_idx = bisect_right(effective_points, row_dt) - 1
            if snapshot_idx < 0:
                raise ValueError(
                    f"{context} has datetime {row_dt.strftime(OUTPUT_TIME_FORMAT)!r}, "
                    "which is earlier than the first mapping entry."
                )

            measurements = row[TIME_KEY_COLUMNS:]
            snapshot = snapshots[snapshot_idx]
            metric_values = [
                resolve_metric_value(measurements, snapshot.metric_positions[metric_name])
                for metric_name in metric_names
            ]

            writer.writerow([row_dt.strftime(OUTPUT_TIME_FORMAT), *metric_values])
            rows_written += 1

    return rows_written


def main() -> None:
    """Run the column-map application."""
    args = parse_args()
    rows_written = apply_column_map(args.input, args.column_map, args.output)
    print(
        f"Wrote {rows_written} rows to {args.output} using mappings from {args.column_map}.",
        flush=True,
    )


if __name__ == "__main__":
    main()
