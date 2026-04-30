#!/usr/bin/env python3
"""
convert_stw_mapped_to_mv.py -- Convert mapped STW values to mV and Irr.

Reads the mapped combined STW CSV, loads effective-dated M_program and
M_should_be values from matching metric worksheets in
STW_sitefile_and_mapping.xlsx, and writes a new CSV where convertible metrics
produce paired *_mV and *_Irr columns. It also writes a calibration-change log,
an outlier report, a Parquet copy of the final dataset, and a Jupyter notebook
for Datashader-based full-history exploration.
"""

from __future__ import annotations

import argparse
import csv
import json
import xml.etree.ElementTree as ET
from bisect import bisect_right
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parent
DEFAULT_INPUT = ROOT_DIR / "reports" / "stw_combined_mapped.csv"
DEFAULT_WORKBOOK = ROOT_DIR / "STW programs" / "STW_sitefile_and_mapping.xlsx"
DEFAULT_OUTPUT = ROOT_DIR / "final output" / "stw_mV_Irr.csv"
DEFAULT_PARQUET_OUTPUT = ROOT_DIR / "final output" / "stw_mV_Irr.parquet"
DEFAULT_NOTEBOOK_OUTPUT = ROOT_DIR / "plots" / "stw_mV_Irr_explorer.ipynb"
INPUT_TIME_FORMAT = "%Y/%m/%d %H:%M"
PASS_THROUGH_METRICS = {"TEMP", "SZA", "AZM"}
EXCLUDED_METRICS = {"PIR"}
OUTLIER_WINDOW = 241
OUTLIER_Z_THRESHOLD = 8.0
IRR_VALID_MIN = -100.0
IRR_VALID_MAX = 10000.0
PASS_THROUGH_VALUE_BOUNDS: dict[str, tuple[float, float]] = {
    "TEMP": (-40.0, 60.0),
}
PARQUET_CHUNK_SIZE = 200_000
OUTLIER_REPORT_COLUMNS = [
    "datetime",
    "column",
    "metric",
    "value_type",
    "value",
    "local_median",
    "local_mad",
    "robust_z",
    "rule",
]
XLSX_NS = {
    "main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "pkgrel": "http://schemas.openxmlformats.org/package/2006/relationships",
}
DOCREL_ID_ATTR = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"


@dataclass(frozen=True)
class MetricConversionTimeline:
    """Effective-dated conversion values for one metric."""

    metric_name: str
    effective_points: list[datetime]
    m_program_values: list[float]
    m_should_be_values: list[float]


@dataclass(frozen=True)
class ConversionArtifacts:
    """Paths and metadata produced by one conversion run."""

    rows_written: int
    log_output: Path
    output_path: Path
    converted_metrics: list[str]


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Convert mapped STW metric values to mV and Irr using effective-dated "
            "workbook calibration values, then write outlier, Parquet, and notebook artifacts."
        )
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help=f"Path to the mapped combined CSV (default: {DEFAULT_INPUT})",
    )
    parser.add_argument(
        "--workbook",
        type=Path,
        default=DEFAULT_WORKBOOK,
        help=f"Path to the workbook containing metric tabs (default: {DEFAULT_WORKBOOK})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Path to the converted mV/Irr CSV (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--log-output",
        type=Path,
        default=None,
        help="Optional path for the calibration-change log text file (default: reports/<output_stem>_calibration_changes.txt)",
    )
    parser.add_argument(
        "--outliers-output",
        type=Path,
        default=None,
        help="Optional path for the outliers CSV (default: reports/<output_stem>_outliers.csv)",
    )
    parser.add_argument(
        "--parquet-output",
        type=Path,
        default=DEFAULT_PARQUET_OUTPUT,
        help=f"Path to the Parquet copy of the converted output (default: {DEFAULT_PARQUET_OUTPUT})",
    )
    parser.add_argument(
        "--notebook-output",
        type=Path,
        default=DEFAULT_NOTEBOOK_OUTPUT,
        help=f"Path to the generated Jupyter notebook explorer (default: {DEFAULT_NOTEBOOK_OUTPUT})",
    )
    parser.add_argument(
        "--outlier-window",
        type=int,
        default=OUTLIER_WINDOW,
        help=f"Centered rolling window size for outlier detection (default: {OUTLIER_WINDOW})",
    )
    parser.add_argument(
        "--outlier-z-threshold",
        type=float,
        default=OUTLIER_Z_THRESHOLD,
        help=f"Robust z-score threshold for outlier detection (default: {OUTLIER_Z_THRESHOLD})",
    )
    return parser.parse_args()


def default_log_output_path(output_path: Path) -> Path:
    """Return the default log path for a given output CSV."""
    return ROOT_DIR / "reports" / f"{output_path.stem}_calibration_changes.txt"


def default_outliers_output_path(output_path: Path) -> Path:
    """Return the default outliers CSV path for a given output CSV."""
    return ROOT_DIR / "reports" / f"{output_path.stem}_outliers.csv"


def excel_column_name_to_index(column_name: str) -> int:
    """Convert an Excel column label like A or AA to a zero-based index."""
    value = 0
    for char in column_name:
        value = value * 26 + (ord(char.upper()) - ord("A") + 1)
    return value - 1


def split_excel_cell_reference(cell_ref: str) -> tuple[int, int]:
    """Split an Excel cell reference into zero-based row and column indexes."""
    letters: list[str] = []
    digits: list[str] = []
    for char in cell_ref:
        if char.isalpha():
            letters.append(char)
        elif char.isdigit():
            digits.append(char)
    if not letters or not digits:
        raise ValueError(f"Invalid Excel cell reference {cell_ref!r}.")
    return int("".join(digits)) - 1, excel_column_name_to_index("".join(letters))


def load_shared_strings(workbook_zip: ZipFile) -> list[str]:
    """Load the workbook shared string table, if present."""
    if "xl/sharedStrings.xml" not in workbook_zip.namelist():
        return []

    shared_strings_xml = ET.fromstring(workbook_zip.read("xl/sharedStrings.xml"))
    return [
        "".join(text_node.text or "" for text_node in item.iterfind(".//main:t", XLSX_NS))
        for item in shared_strings_xml.findall("main:si", XLSX_NS)
    ]


def extract_excel_cell_value(cell: ET.Element, shared_strings: list[str]) -> str:
    """Extract a string value from an XLSX cell element."""
    cell_type = cell.attrib.get("t")
    if cell_type == "inlineStr":
        return "".join(text_node.text or "" for text_node in cell.iterfind(".//main:t", XLSX_NS))

    value_node = cell.find("main:v", XLSX_NS)
    if value_node is None or value_node.text is None:
        return ""

    value = value_node.text
    if cell_type == "s":
        return shared_strings[int(value)]
    return value


def load_worksheet_grid(worksheet_xml: ET.Element, shared_strings: list[str]) -> list[list[str]]:
    """Expand worksheet cells into a row/column grid of strings."""
    grid: list[list[str]] = []
    for row in worksheet_xml.findall(".//main:sheetData/main:row", XLSX_NS):
        row_values: list[str] = []
        for cell in row.findall("main:c", XLSX_NS):
            cell_ref = cell.attrib.get("r")
            if not cell_ref:
                continue
            _, col_idx = split_excel_cell_reference(cell_ref)
            while len(row_values) <= col_idx:
                row_values.append("")
            row_values[col_idx] = extract_excel_cell_value(cell, shared_strings)
        grid.append(row_values)
    return grid


def load_workbook_rows(workbook_path: Path, sheet_name: str) -> list[dict[str, str]]:
    """Load one worksheet into a list of row dictionaries."""
    with ZipFile(workbook_path) as workbook_zip:
        workbook_xml = ET.fromstring(workbook_zip.read("xl/workbook.xml"))
        workbook_rels = ET.fromstring(workbook_zip.read("xl/_rels/workbook.xml.rels"))
        rel_map = {
            rel.attrib["Id"]: rel.attrib["Target"]
            for rel in workbook_rels.findall("pkgrel:Relationship", XLSX_NS)
        }

        target = None
        for sheet in workbook_xml.find("main:sheets", XLSX_NS):
            if sheet.attrib.get("name") == sheet_name:
                target = rel_map.get(sheet.attrib[DOCREL_ID_ATTR])
                break
        if target is None:
            raise ValueError(f"{workbook_path} does not contain a worksheet named {sheet_name!r}.")

        shared_strings = load_shared_strings(workbook_zip)
        worksheet_xml = ET.fromstring(workbook_zip.read(f"xl/{target}"))
        grid = load_worksheet_grid(worksheet_xml, shared_strings)

    if not grid:
        raise ValueError(f"{workbook_path}:{sheet_name} is empty.")

    header = [value.strip() for value in grid[0]]
    rows: list[dict[str, str]] = []
    for values in grid[1:]:
        if not any(cell.strip() for cell in values):
            continue
        rows.append({header[idx]: values[idx].strip() if idx < len(values) else "" for idx in range(len(header))})
    return rows


def parse_workbook_row_datetime(row: dict[str, str], context: str) -> datetime:
    """Parse a workbook row's Year/Month/Day/Hour/Minute into a datetime."""
    try:
        return datetime(
            int(row["Year"]),
            int(row["Month"]),
            int(row["Day"]),
            int(row["Hour"]),
            int(row["Minute"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"{context}: invalid Year/Month/Day/Hour/Minute fields.") from exc


def parse_required_float(value: str, field_name: str, context: str) -> float:
    """Parse a required numeric field with a clear error."""
    text = value.strip()
    if not text:
        raise ValueError(f"{context}: blank {field_name} value.")
    try:
        return float(text)
    except ValueError as exc:
        raise ValueError(f"{context}: non-numeric {field_name} value {value!r}.") from exc


def load_metric_timeline(workbook_path: Path, metric_name: str) -> MetricConversionTimeline:
    """Load and sort M_program and M_should_be values for one metric worksheet."""
    rows = load_workbook_rows(workbook_path, metric_name)
    timeline_rows: list[tuple[datetime, float, float]] = []

    for row_num, row in enumerate(rows, start=2):
        context = f"{workbook_path}:{metric_name}:{row_num}"
        effective_at = parse_workbook_row_datetime(row, context)
        m_program = parse_required_float((row.get("M_program", "") or ""), "M_program", context)
        m_should_be = parse_required_float((row.get("M_should_be", "") or ""), "M_should_be", context)
        timeline_rows.append((effective_at, m_program, m_should_be))

    if not timeline_rows:
        raise ValueError(f"{workbook_path}:{metric_name} does not contain any data rows.")

    timeline_rows.sort(key=lambda item: item[0])
    return MetricConversionTimeline(
        metric_name=metric_name,
        effective_points=[item[0] for item in timeline_rows],
        m_program_values=[item[1] for item in timeline_rows],
        m_should_be_values=[item[2] for item in timeline_rows],
    )


def resolve_timeline_values(
    timeline: MetricConversionTimeline,
    row_dt: datetime,
    context: str,
) -> tuple[float, float]:
    """Resolve the active conversion values for a mapped CSV row timestamp."""
    idx = bisect_right(timeline.effective_points, row_dt) - 1
    if idx < 0:
        first_point = timeline.effective_points[0].strftime(INPUT_TIME_FORMAT)
        raise ValueError(
            f"{context}: datetime {row_dt.strftime(INPUT_TIME_FORMAT)!r} is earlier than the first "
            f"{timeline.metric_name} calibration timestamp {first_point!r}."
        )
    return timeline.m_program_values[idx], timeline.m_should_be_values[idx]


def is_nan_like(value: str) -> bool:
    """Return True if the cell should be treated as missing."""
    return value.strip() == "" or value.strip().lower() == "nan"


def build_output_columns(
    input_columns: list[str],
    workbook_path: Path,
) -> tuple[list[str], dict[str, MetricConversionTimeline], list[str]]:
    """Build output headers and conversion timelines for convertible columns."""
    output_columns = ["datetime"]
    timelines: dict[str, MetricConversionTimeline] = {}
    converted_metrics: list[str] = []

    for column_name in input_columns:
        if column_name == "datetime" or column_name in EXCLUDED_METRICS:
            continue
        if column_name in PASS_THROUGH_METRICS:
            output_columns.append(column_name)
            continue

        timelines[column_name] = load_metric_timeline(workbook_path, column_name)
        converted_metrics.append(column_name)
        output_columns.extend([f"{column_name}_mV", f"{column_name}_Irr"])

    return output_columns, timelines, converted_metrics


def write_calibration_change_log(log_path: Path, timelines: dict[str, MetricConversionTimeline]) -> None:
    """Write a text log of M_program and M_should_be changes for converted metrics."""
    changes: list[tuple[datetime, str]] = []

    for metric_name, timeline in timelines.items():
        prev_program: float | None = None
        prev_should_be: float | None = None
        for effective_at, current_program, current_should_be in zip(
            timeline.effective_points,
            timeline.m_program_values,
            timeline.m_should_be_values,
        ):
            if prev_program is None:
                prev_program = current_program
                prev_should_be = current_should_be
                continue

            if current_program != prev_program:
                changes.append(
                    (
                        effective_at,
                        f"{metric_name} column M_program changed from {prev_program} to {current_program}, at {effective_at.strftime(INPUT_TIME_FORMAT)}",
                    )
                )
            if current_should_be != prev_should_be:
                changes.append(
                    (
                        effective_at,
                        f"{metric_name} column M_should_be changed from {prev_should_be} to {current_should_be}, at {effective_at.strftime(INPUT_TIME_FORMAT)}",
                    )
                )

            prev_program = current_program
            prev_should_be = current_should_be

    changes.sort(key=lambda item: (item[0], item[1]))
    lines = [item[1] for item in changes]
    log_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def convert_stw_mapped_to_mv_irr(
    input_path: Path,
    workbook_path: Path,
    output_path: Path,
    log_output: Path,
) -> ConversionArtifacts:
    """Convert mapped STW values to paired mV/Irr columns and write the output CSV."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with input_path.open("r", encoding="utf-8", newline="") as src:
        reader = csv.DictReader(src)
        if not reader.fieldnames:
            raise ValueError(f"{input_path} is empty.")
        if "datetime" not in reader.fieldnames:
            raise ValueError(f"{input_path} must contain a 'datetime' column.")

        output_columns, timelines, converted_metrics = build_output_columns(reader.fieldnames, workbook_path)

        rows_written = 0
        with output_path.open("w", encoding="utf-8", newline="") as dst:
            writer = csv.DictWriter(dst, fieldnames=output_columns)
            writer.writeheader()

            for row_num, row in enumerate(reader, start=2):
                context = f"{input_path}:{row_num}"
                datetime_text = (row.get("datetime", "") or "").strip()
                if not datetime_text:
                    raise ValueError(f"{context}: missing datetime value.")
                try:
                    row_dt = datetime.strptime(datetime_text, INPUT_TIME_FORMAT)
                except ValueError as exc:
                    raise ValueError(
                        f"{context}: invalid datetime {datetime_text!r}; expected {INPUT_TIME_FORMAT!r}."
                    ) from exc

                output_row = {"datetime": datetime_text}
                for column_name in reader.fieldnames:
                    if column_name == "datetime" or column_name in EXCLUDED_METRICS:
                        continue

                    cell_value = (row.get(column_name, "") or "").strip()
                    if column_name in PASS_THROUGH_METRICS:
                        output_row[column_name] = "NaN" if is_nan_like(cell_value) else cell_value
                        continue

                    mv_column = f"{column_name}_mV"
                    irr_column = f"{column_name}_Irr"
                    if is_nan_like(cell_value):
                        output_row[mv_column] = "NaN"
                        output_row[irr_column] = "NaN"
                        continue

                    try:
                        numeric_value = float(cell_value)
                    except ValueError as exc:
                        raise ValueError(
                            f"{context}: non-numeric value {cell_value!r} in column {column_name!r}."
                        ) from exc

                    m_program, m_should_be = resolve_timeline_values(timelines[column_name], row_dt, context)
                    mv_value = numeric_value / m_program
                    irr_value = mv_value * m_should_be
                    output_row[mv_column] = str(mv_value)
                    output_row[irr_column] = str(irr_value)

                writer.writerow(output_row)
                rows_written += 1

    write_calibration_change_log(log_output, timelines)
    return ConversionArtifacts(
        rows_written=rows_written,
        log_output=log_output,
        output_path=output_path,
        converted_metrics=converted_metrics,
    )


def empty_outlier_frame() -> pd.DataFrame:
    """Return an empty outlier frame with the expected schema."""
    return pd.DataFrame(columns=OUTLIER_REPORT_COLUMNS)


def metric_name_for_column(column_name: str) -> str:
    """Return the logical metric name represented by one output column."""
    if column_name.endswith(("_mV", "_Irr")):
        return column_name.rsplit("_", 1)[0]
    return column_name


def value_type_for_column(column_name: str) -> str:
    """Return the value-type label used in the outlier report."""
    if column_name.endswith("_mV"):
        return "mV"
    if column_name.endswith("_Irr"):
        return "Irr"
    return "raw"


def detect_outliers_frame(output_csv: Path, columns: list[str], window: int, threshold: float) -> pd.DataFrame:
    """Return an outlier report using rolling median and MAD per selected series."""
    if window < 5:
        raise ValueError("outlier_window must be at least 5.")
    if threshold <= 0:
        raise ValueError("outlier_z_threshold must be positive.")

    outlier_frames: list[pd.DataFrame] = []
    min_periods = max(11, window // 5)

    for column_name in columns:
        frame = pd.read_csv(
            output_csv,
            usecols=["datetime", column_name],
            parse_dates=["datetime"],
            na_values=["NaN"],
            keep_default_na=True,
        )
        values = pd.to_numeric(frame[column_name], errors="coerce")
        rolling_median = values.rolling(window=window, center=True, min_periods=min_periods).median()
        abs_dev = (values - rolling_median).abs()
        rolling_mad = abs_dev.rolling(window=window, center=True, min_periods=min_periods).median()
        denominator = rolling_mad * 1.4826
        robust_z = (values - rolling_median).abs() / denominator
        mask = denominator.notna() & (denominator > 0) & robust_z.ge(threshold)
        if not mask.any():
            continue

        kind = value_type_for_column(column_name)
        metric = metric_name_for_column(column_name)
        outlier_frame = pd.DataFrame(
            {
                "datetime": frame.loc[mask, "datetime"].dt.strftime(INPUT_TIME_FORMAT),
                "column": column_name,
                "metric": metric,
                "value_type": kind,
                "value": values.loc[mask],
                "local_median": rolling_median.loc[mask],
                "local_mad": rolling_mad.loc[mask],
                "robust_z": robust_z.loc[mask],
                "rule": "rolling_mad",
            }
        )
        outlier_frames.append(outlier_frame)

    if outlier_frames:
        return pd.concat(outlier_frames, ignore_index=True).sort_values(["datetime", "column"])

    return empty_outlier_frame()


def detect_bound_outliers_frame(
    output_csv: Path,
    bounds_by_column: dict[str, tuple[float, float]],
) -> pd.DataFrame:
    """Return rows whose values fall outside a configured absolute range."""
    outlier_frames: list[pd.DataFrame] = []

    for column_name, (min_value, max_value) in bounds_by_column.items():
        frame = pd.read_csv(
            output_csv,
            usecols=["datetime", column_name],
            parse_dates=["datetime"],
            na_values=["NaN"],
            keep_default_na=True,
        )
        values = pd.to_numeric(frame[column_name], errors="coerce")
        mask = values.notna() & (values.lt(min_value) | values.gt(max_value))
        if not mask.any():
            continue

        outlier_frames.append(
            pd.DataFrame(
                {
                    "datetime": frame.loc[mask, "datetime"].dt.strftime(INPUT_TIME_FORMAT),
                    "column": column_name,
                    "metric": metric_name_for_column(column_name),
                    "value_type": value_type_for_column(column_name),
                    "value": values.loc[mask],
                    "local_median": float("nan"),
                    "local_mad": float("nan"),
                    "robust_z": float("nan"),
                    "rule": f"{column_name}_bounds[{min_value:g},{max_value:g}]",
                }
            )
        )

    if outlier_frames:
        return pd.concat(outlier_frames, ignore_index=True).sort_values(["datetime", "column"])

    return empty_outlier_frame()


def detect_irr_bound_outliers_frame(
    output_csv: Path,
    metrics: list[str],
    min_irr: float = IRR_VALID_MIN,
    max_irr: float = IRR_VALID_MAX,
) -> pd.DataFrame:
    """Return irradiance rows that fall outside the allowed absolute range."""
    return detect_bound_outliers_frame(
        output_csv,
        {f"{metric_name}_Irr": (min_irr, max_irr) for metric_name in metrics},
    )


def detect_pass_through_bound_outliers_frame(output_csv: Path) -> pd.DataFrame:
    """Return pass-through metric rows that fall outside their allowed ranges."""
    return detect_bound_outliers_frame(output_csv, PASS_THROUGH_VALUE_BOUNDS)


def combine_outlier_frames(*frames: pd.DataFrame) -> pd.DataFrame:
    """Combine multiple outlier frames into one deduplicated report."""
    non_empty = [frame for frame in frames if not frame.empty]
    if not non_empty:
        return empty_outlier_frame()

    combined = pd.concat(non_empty, ignore_index=True, sort=False)

    def first_non_null(series: pd.Series):
        non_null = series.dropna()
        if non_null.empty:
            return pd.NA
        return non_null.iloc[0]

    result = (
        combined.groupby(
            ["datetime", "column", "metric", "value_type", "value"],
            as_index=False,
            dropna=False,
        )
        .agg(
            {
                "local_median": first_non_null,
                "local_mad": first_non_null,
                "robust_z": first_non_null,
                "rule": lambda values: ",".join(dict.fromkeys(str(value) for value in values if pd.notna(value))),
            }
        )
        .sort_values(["datetime", "column"])
    )
    return result[OUTLIER_REPORT_COLUMNS]


def write_outlier_report(outliers_output: Path, outliers_frame: pd.DataFrame) -> None:
    """Write the outlier report CSV."""
    outliers_frame.to_csv(outliers_output, index=False)


def build_outlier_lookup(outliers_frame: pd.DataFrame) -> dict[str, set[str]]:
    """Build a per-column lookup of datetimes that should be nulled out."""
    if outliers_frame.empty:
        return {}

    lookup: dict[str, set[str]] = {}
    for column_name, group in outliers_frame.groupby("column")["datetime"]:
        lookup[column_name] = set(group.astype(str))

    for metric_name, group in outliers_frame.groupby("metric")["datetime"]:
        metric_datetimes = set(group.astype(str))
        for suffix in ("_mV", "_Irr"):
            lookup.setdefault(f"{metric_name}{suffix}", set()).update(metric_datetimes)

    return lookup


def apply_outlier_nan_mask(
    output_csv: Path,
    outlier_lookup: dict[str, set[str]],
    chunk_size: int = PARQUET_CHUNK_SIZE,
) -> int:
    """Rewrite the exported CSV with flagged values replaced by NaN."""
    if not outlier_lookup:
        return 0

    temp_output = output_csv.with_name(f"{output_csv.stem}.tmp{output_csv.suffix}")
    cleaned_values = 0
    wrote_any = False

    try:
        for chunk in pd.read_csv(
            output_csv,
            chunksize=chunk_size,
            na_values=["NaN"],
            keep_default_na=True,
            dtype={"datetime": str},
        ):
            for column_name, flagged_datetimes in outlier_lookup.items():
                if column_name not in chunk.columns:
                    continue
                mask = chunk["datetime"].isin(flagged_datetimes) & chunk[column_name].notna()
                if not mask.any():
                    continue
                chunk.loc[mask, column_name] = pd.NA
                cleaned_values += int(mask.sum())

            chunk.to_csv(
                temp_output,
                mode="a" if wrote_any else "w",
                index=False,
                header=not wrote_any,
                na_rep="NaN",
            )
            wrote_any = True

        temp_output.replace(output_csv)
    finally:
        if temp_output.exists():
            temp_output.unlink()

    return cleaned_values


def detect_outliers(output_csv: Path, outliers_output: Path, columns: list[str], window: int, threshold: float) -> int:
    """Write a combined outlier report for rolling-MAD and absolute-bound filters."""
    metrics = sorted({column_name.rsplit("_", 1)[0] for column_name in columns if column_name.endswith("_Irr")})
    result = combine_outlier_frames(
        detect_outliers_frame(output_csv, columns, window, threshold),
        detect_irr_bound_outliers_frame(output_csv, metrics),
        detect_pass_through_bound_outliers_frame(output_csv),
    )
    write_outlier_report(outliers_output, result)
    return len(result)


def write_parquet_copy(csv_path: Path, parquet_path: Path, chunk_size: int = PARQUET_CHUNK_SIZE) -> int:
    """Write a Parquet copy of the converted CSV in chunks."""
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    if parquet_path.exists():
        parquet_path.unlink()

    row_count = 0
    writer: pq.ParquetWriter | None = None
    try:
        for chunk in pd.read_csv(
            csv_path,
            chunksize=chunk_size,
            na_values=["NaN"],
            keep_default_na=True,
            parse_dates=["datetime"],
        ):
            for column_name in chunk.columns:
                if column_name == "datetime":
                    continue
                chunk[column_name] = pd.to_numeric(chunk[column_name], errors="coerce")

            table = pa.Table.from_pandas(chunk, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(parquet_path, table.schema, compression="snappy")
            writer.write_table(table)
            row_count += len(chunk)
    finally:
        if writer is not None:
            writer.close()

    return row_count


def build_notebook_cells(parquet_rel: str, outliers_rel: str) -> list[dict[str, object]]:
    """Return notebook cells for the Datashader-based STW explorer."""
    notebook_markdown = """# STW Full-History Explorer

This notebook is the interactive viewer for the full STW mV/Irr dataset.

- Each metric is shown as a simple full-history line plot.
- Missing values and cleaned outliers remain `NaN`, which creates visible gaps without dropping timestamps.
- The notebook renders full-history views for `GHI`, `DHI`, and `DNI` across the complete dataset time range.
- Use `plot_metric_window(...)` only when you need exact values in a smaller time window.
"""
    notebook_setup = f"""from pathlib import Path

import holoviews as hv
import hvplot.pandas
import pandas as pd

hv.extension("bokeh")

ROOT_DIR = Path.cwd().resolve().parent if Path.cwd().name == "plots" else Path.cwd().resolve()
PARQUET_PATH = ROOT_DIR / {parquet_rel!r}

df = pd.read_parquet(PARQUET_PATH)
df["datetime"] = pd.to_datetime(df["datetime"])
metrics = sorted(column[:-3] for column in df.columns if column.endswith("_mV"))
"""
    notebook_functions = """def plot_metric(metric: str):
    mv_column = f"{metric}_mV"
    irr_column = f"{metric}_Irr"
    metric_df = df[["datetime", mv_column, irr_column]].copy()

    return metric_df.hvplot.line(
        x="datetime",
        y=[mv_column, irr_column],
        responsive=True,
        min_height=520,
        xlabel="Time",
        ylabel="Value",
        title=f"{metric} full history",
        legend="top",
    )


def plot_metric_window(metric: str, start: str, end: str):
    mv_column = f"{metric}_mV"
    irr_column = f"{metric}_Irr"
    window = df.loc[
        (df["datetime"] >= pd.Timestamp(start)) & (df["datetime"] <= pd.Timestamp(end)),
        ["datetime", mv_column, irr_column],
    ].copy()
    if window.empty:
        raise ValueError(f"No rows found for {metric} between {start} and {end}.")

    return window.hvplot.line(
        x="datetime",
        y=[mv_column, irr_column],
        responsive=True,
        min_height=520,
        xlabel="Time",
        ylabel="Value",
        title=f"{metric} detailed window: {start} to {end}",
        legend="top",
    )
"""
    notebook_ghi = """plot_metric("GHI")"""
    notebook_dhi = """plot_metric("DHI")"""
    notebook_dni = """plot_metric("DNI")"""
    notebook_drilldown = """# Optional exact-window helper example:
# plot_metric_window("GHI", "2024-02-01 00:00", "2024-02-03 00:00")"""

    def code_cell(source: str) -> dict[str, object]:
        return {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": source.splitlines(keepends=True),
        }

    return [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": notebook_markdown.splitlines(keepends=True),
        },
        code_cell(notebook_setup),
        code_cell(notebook_functions),
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## GHI Full History\n"],
        },
        code_cell(notebook_ghi),
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## DHI Full History\n"],
        },
        code_cell(notebook_dhi),
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## DNI Full History\n"],
        },
        code_cell(notebook_dni),
        code_cell(notebook_drilldown),
    ]


def write_explorer_notebook(notebook_path: Path, parquet_path: Path, outliers_output: Path) -> None:
    """Write the Jupyter notebook used for full-history interactive exploration."""
    notebook_path.parent.mkdir(parents=True, exist_ok=True)
    for stale in notebook_path.parent.glob("*.html"):
        stale.unlink()
    for stale in notebook_path.parent.glob("*.png"):
        stale.unlink()

    notebook = {
        "cells": build_notebook_cells(
            parquet_path.relative_to(ROOT_DIR).as_posix(),
            outliers_output.relative_to(ROOT_DIR).as_posix(),
        ),
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {
                "name": "python",
                "version": "3",
            },
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    notebook_path.write_text(json.dumps(notebook, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    """Run the conversion, outlier detection, Parquet export, and notebook generation."""
    args = parse_args()
    log_output = args.log_output or default_log_output_path(args.output)
    outliers_output = args.outliers_output or default_outliers_output_path(args.output)

    artifacts = convert_stw_mapped_to_mv_irr(
        args.input,
        args.workbook,
        args.output,
        log_output,
    )
    converted_columns = [
        column_name
        for metric_name in artifacts.converted_metrics
        for column_name in (f"{metric_name}_mV", f"{metric_name}_Irr")
    ]
    outliers_frame = combine_outlier_frames(
        detect_outliers_frame(
            args.output,
            converted_columns,
            args.outlier_window,
            args.outlier_z_threshold,
        ),
        detect_irr_bound_outliers_frame(args.output, artifacts.converted_metrics),
        detect_pass_through_bound_outliers_frame(args.output),
    )
    write_outlier_report(outliers_output, outliers_frame)
    cleaned_values = apply_outlier_nan_mask(args.output, build_outlier_lookup(outliers_frame))
    parquet_rows_written = write_parquet_copy(args.output, args.parquet_output)
    write_explorer_notebook(args.notebook_output, args.parquet_output, outliers_output)

    print(
        f"Wrote {artifacts.rows_written} rows to {args.output} using calibration values from {args.workbook}.",
        flush=True,
    )
    print(f"Wrote calibration-change log to {log_output}.", flush=True)
    print(f"Wrote {len(outliers_frame)} outlier rows to {outliers_output}.", flush=True)
    print(f"Replaced {cleaned_values} flagged values with NaN in {args.output}.", flush=True)
    print(f"Wrote {parquet_rows_written} rows to Parquet at {args.parquet_output}.", flush=True)
    print(f"Wrote Jupyter explorer notebook to {args.notebook_output}.", flush=True)


if __name__ == "__main__":
    main()
