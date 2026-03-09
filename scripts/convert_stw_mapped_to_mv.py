#!/usr/bin/env python3
"""
convert_stw_mapped_to_mv.py -- Convert mapped STW values to mV and Irr.

Reads the mapped combined STW CSV, loads effective-dated M_program and
M_should_be values from matching metric worksheets in
STW_sitefile_and_mapping.xlsx, and writes a new CSV where convertible metrics
produce paired *_mV and *_Irr columns. It also writes a calibration-change log,
an outlier report, and interactive HTML overview plots for each converted
metric.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import xml.etree.ElementTree as ET
from bisect import bisect_right
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zipfile import ZipFile

import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parent
DEFAULT_INPUT = ROOT_DIR / "reports" / "stw_combined_mapped.csv"
DEFAULT_WORKBOOK = ROOT_DIR / "STW programs" / "STW_sitefile_and_mapping.xlsx"
DEFAULT_OUTPUT = ROOT_DIR / "final output" / "stw_mV_Irr.csv"
INPUT_TIME_FORMAT = "%Y/%m/%d %H:%M"
PASS_THROUGH_METRICS = {"TEMP", "SZA", "AZM"}
EXCLUDED_METRICS = {"PIR"}
MAX_PLOT_POINTS = 10000
OUTLIER_WINDOW = 241
OUTLIER_Z_THRESHOLD = 8.0
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
            "workbook calibration values, then write outlier and HTML plot artifacts."
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
        "--plots-dir",
        type=Path,
        default=None,
        help="Optional directory for generated HTML plots (default: repo-root plots/ directory)",
    )
    parser.add_argument(
        "--max-plot-points",
        type=int,
        default=MAX_PLOT_POINTS,
        help=f"Maximum sampled points per overview plot (default: {MAX_PLOT_POINTS})",
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


def default_plots_dir(output_path: Path) -> Path:
    """Return the default plots directory for a given output CSV."""
    return ROOT_DIR / "plots"


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


def detect_outliers(output_csv: Path, outliers_output: Path, columns: list[str], window: int, threshold: float) -> int:
    """Write an outlier report using rolling median and MAD per converted series."""
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

        kind = "mV" if column_name.endswith("_mV") else "Irr"
        metric = column_name.rsplit("_", 1)[0]
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
            }
        )
        outlier_frames.append(outlier_frame)

    if outlier_frames:
        result = pd.concat(outlier_frames, ignore_index=True).sort_values(["datetime", "column"])
    else:
        result = pd.DataFrame(
            columns=["datetime", "column", "metric", "value_type", "value", "local_median", "local_mad", "robust_z"]
        )

    result.to_csv(outliers_output, index=False)
    return len(result)


def make_json_ready(values: list[float]) -> list[float | None]:
    """Convert NaN-containing float lists into JSON-safe values."""
    ready: list[float | None] = []
    for value in values:
        if math.isnan(value):
            ready.append(None)
        else:
            ready.append(value)
    return ready


def build_metric_overview_data(csv_path: Path, metric_name: str, max_plot_points: int) -> dict[str, list[float | None] | list[str]]:
    """Downsample one metric's mV and Irr series for HTML overview plots."""
    mv_column = f"{metric_name}_mV"
    irr_column = f"{metric_name}_Irr"
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        total_rows = sum(1 for _ in reader)

    sample_step = max(1, math.ceil(total_rows / max_plot_points))
    times: list[str] = []
    mv_values: list[float] = []
    irr_values: list[float] = []
    last_row: dict[str, str] | None = None
    last_row_index = 0
    sampled_last_index = 0

    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row_index, row in enumerate(reader, start=1):
            last_row = row
            last_row_index = row_index
            if (row_index - 1) % sample_step != 0:
                continue
            sampled_last_index = row_index
            times.append((row.get("datetime", "") or "").strip())
            mv_text = (row.get(mv_column, "") or "").strip()
            irr_text = (row.get(irr_column, "") or "").strip()
            mv_values.append(float("nan") if is_nan_like(mv_text) else float(mv_text))
            irr_values.append(float("nan") if is_nan_like(irr_text) else float(irr_text))

    if last_row is not None and last_row_index != sampled_last_index:
        times.append((last_row.get("datetime", "") or "").strip())
        mv_text = (last_row.get(mv_column, "") or "").strip()
        irr_text = (last_row.get(irr_column, "") or "").strip()
        mv_values.append(float("nan") if is_nan_like(mv_text) else float(mv_text))
        irr_values.append(float("nan") if is_nan_like(irr_text) else float(irr_text))

    return {
        "datetime": times,
        mv_column: make_json_ready(mv_values),
        irr_column: make_json_ready(irr_values),
    }


def build_outlier_lookup(outliers_output: Path) -> dict[tuple[str, str], list[dict[str, float | str]]]:
    """Load outliers.csv into a lookup keyed by (metric, value_type)."""
    lookup: dict[tuple[str, str], list[dict[str, float | str]]] = {}
    if not outliers_output.exists():
        return lookup

    with outliers_output.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            key = ((row.get("metric", "") or "").strip(), (row.get("value_type", "") or "").strip())
            lookup.setdefault(key, []).append(
                {
                    "datetime": (row.get("datetime", "") or "").strip(),
                    "value": (row.get("value", "") or "").strip(),
                    "robust_z": (row.get("robust_z", "") or "").strip(),
                }
            )
    return lookup


def sanitize_plot_filename(metric_name: str) -> str:
    """Return a safe HTML filename for a metric."""
    return f"{metric_name}.html"


def write_metric_html_plot(
    plots_dir: Path,
    metric_name: str,
    overview_data: dict[str, list[float | None] | list[str]],
    outlier_lookup: dict[tuple[str, str], list[dict[str, float | str]]],
) -> None:
    """Write one interactive HTML overview per converted metric."""
    mv_column = f"{metric_name}_mV"
    irr_column = f"{metric_name}_Irr"
    mv_outliers = outlier_lookup.get((metric_name, "mV"), [])
    irr_outliers = outlier_lookup.get((metric_name, "Irr"), [])

    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{metric_name} Overview</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; background: #f6f7f8; color: #111; }}
    h1 {{ margin: 0 0 8px; font-size: 24px; }}
    p {{ margin: 0 0 16px; color: #444; }}
    #plot {{ width: 100%; height: 78vh; background: white; border: 1px solid #ddd; }}
  </style>
</head>
<body>
  <h1>{metric_name} Overview</h1>
  <p>Zoom and pan to inspect candidate outlier periods. Lines are downsampled for responsiveness; outlier markers use exact timestamps.</p>
  <div id="plot"></div>
  <script>
    const overview = {json.dumps(overview_data)};
    const mvOutliers = {json.dumps(mv_outliers)};
    const irrOutliers = {json.dumps(irr_outliers)};
    const traces = [
      {{
        x: overview.datetime,
        y: overview["{mv_column}"],
        type: "scattergl",
        mode: "lines",
        name: "{mv_column}",
        line: {{ color: "#1f77b4", width: 1 }}
      }},
      {{
        x: overview.datetime,
        y: overview["{irr_column}"],
        type: "scattergl",
        mode: "lines",
        name: "{irr_column}",
        line: {{ color: "#d62728", width: 1 }}
      }},
      {{
        x: mvOutliers.map(item => item.datetime),
        y: mvOutliers.map(item => Number(item.value)),
        type: "scattergl",
        mode: "markers",
        name: "{mv_column} outliers",
        marker: {{ color: "#1f77b4", size: 6, symbol: "circle-open" }},
        hovertemplate: "datetime=%{{x}}<br>value=%{{y}}<br>robust_z=%{{text}}<extra></extra>",
        text: mvOutliers.map(item => item.robust_z)
      }},
      {{
        x: irrOutliers.map(item => item.datetime),
        y: irrOutliers.map(item => Number(item.value)),
        type: "scattergl",
        mode: "markers",
        name: "{irr_column} outliers",
        marker: {{ color: "#d62728", size: 6, symbol: "diamond-open" }},
        hovertemplate: "datetime=%{{x}}<br>value=%{{y}}<br>robust_z=%{{text}}<extra></extra>",
        text: irrOutliers.map(item => item.robust_z)
      }}
    ];
    const layout = {{
      hovermode: "x unified",
      dragmode: "zoom",
      showlegend: true,
      xaxis: {{ title: "Time", rangeslider: {{ visible: true }} }},
      yaxis: {{ title: "Value" }},
      margin: {{ l: 70, r: 20, t: 20, b: 60 }}
    }};
    Plotly.newPlot("plot", traces, layout, {{responsive: true, displaylogo: false}});
  </script>
</body>
</html>
'''
    (plots_dir / sanitize_plot_filename(metric_name)).write_text(html, encoding="utf-8")


def create_html_plots(
    csv_path: Path,
    plots_dir: Path,
    metrics: list[str],
    outliers_output: Path,
    max_plot_points: int,
) -> int:
    """Generate one interactive HTML overview per converted metric."""
    if max_plot_points < 1:
        raise ValueError("max_plot_points must be at least 1.")

    plots_dir.mkdir(parents=True, exist_ok=True)
    for stale in plots_dir.glob("*.png"):
        stale.unlink()
    for stale in plots_dir.glob("*.html"):
        stale.unlink()

    outlier_lookup = build_outlier_lookup(outliers_output)
    written = 0
    for metric_name in metrics:
        overview_data = build_metric_overview_data(csv_path, metric_name, max_plot_points)
        write_metric_html_plot(plots_dir, metric_name, overview_data, outlier_lookup)
        written += 1
    return written


def main() -> None:
    """Run the conversion, outlier detection, and HTML plot generation."""
    args = parse_args()
    log_output = args.log_output or default_log_output_path(args.output)
    outliers_output = args.outliers_output or default_outliers_output_path(args.output)
    plots_dir = args.plots_dir or default_plots_dir(args.output)

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
    outliers_written = detect_outliers(
        args.output,
        outliers_output,
        converted_columns,
        args.outlier_window,
        args.outlier_z_threshold,
    )
    html_plots_written = create_html_plots(
        args.output,
        plots_dir,
        artifacts.converted_metrics,
        outliers_output,
        args.max_plot_points,
    )

    print(
        f"Wrote {artifacts.rows_written} rows to {args.output} using calibration values from {args.workbook}.",
        flush=True,
    )
    print(f"Wrote calibration-change log to {log_output}.", flush=True)
    print(f"Wrote {outliers_written} outlier rows to {outliers_output}.", flush=True)
    print(f"Wrote {html_plots_written} interactive HTML plots to {plots_dir}.", flush=True)


if __name__ == "__main__":
    main()
