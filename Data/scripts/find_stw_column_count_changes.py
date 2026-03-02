#!/usr/bin/env python3
"""
find_stw_column_count_changes.py -- Report rows where STW column counts change.

This scans the combined cleaned STW CSV and records every non-empty row where
the number of comma-separated columns differs from the previous non-empty row.
It is intended to highlight structural shifts in the raw measurement layout.
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR.parent
DEFAULT_INPUT = DATA_DIR / "cleaned_output" / "combined" / "stw_combined_cleaned.csv"
DEFAULT_OUTPUT = DATA_DIR / "cleaned_output" / "combined" / "stw_combined_column_count_changes.csv"


@dataclass(frozen=True)
class ColumnCountChange:
    """A point where the column count differs from the previous non-empty row."""

    row_number: int
    previous_row_number: int
    previous_column_count: int
    current_column_count: int
    year: str
    day_of_year: str
    hhmm: str


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Find rows in the STW combined cleaned CSV where the number of "
            "columns changes from the previous non-empty row."
        )
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help=f"Path to the combined cleaned CSV (default: {DEFAULT_INPUT})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=(
            "Path to write the change report CSV "
            f"(default: {DEFAULT_OUTPUT})"
        ),
    )
    return parser.parse_args()


def find_column_count_changes(input_path: Path) -> list[ColumnCountChange]:
    """Return every row where the column count changes."""
    changes: list[ColumnCountChange] = []
    previous_count: int | None = None
    previous_row_number: int | None = None

    with input_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        for row_number, row in enumerate(reader, start=1):
            if not row:
                continue

            current_count = len(row)
            if previous_count is not None and current_count != previous_count:
                year = row[0] if len(row) > 0 else ""
                day_of_year = row[1] if len(row) > 1 else ""
                hhmm = row[2] if len(row) > 2 else ""
                changes.append(
                    ColumnCountChange(
                        row_number=row_number,
                        previous_row_number=previous_row_number or row_number - 1,
                        previous_column_count=previous_count,
                        current_column_count=current_count,
                        year=year,
                        day_of_year=day_of_year,
                        hhmm=hhmm,
                    )
                )

            previous_count = current_count
            previous_row_number = row_number

    return changes


def write_report(output_path: Path, changes: list[ColumnCountChange]) -> None:
    """Write the detected changes to a CSV report."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "row_number",
                "previous_row_number",
                "previous_column_count",
                "current_column_count",
                "year",
                "day_of_year",
                "hhmm",
            ]
        )
        for change in changes:
            writer.writerow(
                [
                    change.row_number,
                    change.previous_row_number,
                    change.previous_column_count,
                    change.current_column_count,
                    change.year,
                    change.day_of_year,
                    change.hhmm,
                ]
            )


def main() -> None:
    """Run the column-count change scan."""
    args = parse_args()
    changes = find_column_count_changes(args.input)
    write_report(args.output, changes)
    print(
        f"Found {len(changes)} column-count changes in {args.input} "
        f"and wrote {args.output}.",
        flush=True,
    )


if __name__ == "__main__":
    main()
