#!/usr/bin/env python3
"""
stw_pipeline_common.py -- Shared helpers for the split STW cleaning pipeline.

This script processes raw minute-resolution data files from the UO Solar
Radiation Monitoring Lab's STW station. Raw data arrives as monthly text files
inside year-named folders (e.g. ``STW_2020_Process0/``). Each row looks like:

    station_id, year, day_of_year, hhmm, measurement_1, measurement_2, ...

The cleaning pipeline (run via ``main()``) performs nine steps:

  1. Discover all source files under ``Raw Data/STW_*/``.
  2. Load rows into a SQLite database, dropping the station-ID column and
     keeping only the first row seen for each (year, day, hhmm) timestamp.
  3. Write one sorted CSV per year to ``yearly cleaned/``.
  4. For each year, compare observed timestamps against the full 1,440
     valid minute-of-day HHMM values and merge placeholder rows for gaps.
  5. Write a combined CSV covering all years. Merge placeholder rows
     (``NaN`` measurements) for every missing timestamp so the file has no
     gaps.
  6. Write a human-readable summary report.
  7. Re-ingest the 10-year file and verify zero duplicates and zero missing
     timestamps remain.
  8. Write a mapped combined CSV using the ``Column Mapping`` worksheet
     from ``STW programs/STW_sitefile_and_mapping.xlsx``.
  9. Delete temporary SQLite databases.

Key conventions:
  - **HHMM format**: An integer where HH = hours (00-23) and MM = minutes
    (00-59). Valid values run from 0001 to 2359, plus the special value 2400
    (end-of-day midnight). This yields 1,440 timestamps per day.
  - **Boundary grace**: On the very first day of a scope, timestamps earlier
    than the first observed reading are *not* counted as missing. On the very
    last day of a scope, timestamps later than the last observed reading are
    also *not* counted as missing.
  - **Deduplication key**: ``(year, day, hhmm)`` only. If multiple rows share
    the same timestamp, the first one seen is kept and later rows are ignored.
"""

from __future__ import annotations

import csv
import json
import sqlite3
import xml.etree.ElementTree as ET
from bisect import bisect_right
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable
from zipfile import ZipFile


# ---------------------------------------------------------------------------
# Directory layout -- all paths are relative to the repo root.
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent        # .../scripts/
ROOT_DIR = SCRIPT_DIR.parent                      # repo root
RAW_DATA_DIR = ROOT_DIR / "Raw Data"
YEARLY_CLEANED_DIR = ROOT_DIR / "yearly cleaned"
REPORTS_DIR = ROOT_DIR / "reports"
FINAL_OUTPUT_DIR = ROOT_DIR / "final output"
PLOTS_DIR = ROOT_DIR / "plots"
WORK_DB = REPORTS_DIR / "_stw_work.sqlite3"
RECHECK_DB = REPORTS_DIR / "_stw_recheck.sqlite3"
COLUMN_MAP_WORKBOOK = ROOT_DIR / "STW programs" / "STW_sitefile_and_mapping.xlsx"
COLUMN_MAP_SHEET_NAME = "Column Mapping"
COMBINED_CLEANED_FILE = REPORTS_DIR / "stw_combined_cleaned.csv"
MAPPED_COMBINED_FILE = REPORTS_DIR / "stw_combined_mapped.csv"
PIPELINE_STATE_FILE = REPORTS_DIR / "pipeline_state.json"
MAPPED_EXCLUDED_METRICS = {"PIR"}
XLSX_NS = {
    "main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "pkgrel": "http://schemas.openxmlformats.org/package/2006/relationships",
}
DOCREL_ID_ATTR = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"


# ---------------------------------------------------------------------------
# Dataclasses for tracking statistics across the pipeline.
# ---------------------------------------------------------------------------

@dataclass
class IngestStats:
    """Counters accumulated while reading raw source files."""
    files_seen: int = 0              # number of input files opened
    lines_seen: int = 0              # total lines read (including blanks)
    valid_rows: int = 0              # rows that parsed successfully
    invalid_short: int = 0           # rows rejected for too few columns
    invalid_key: int = 0             # rows where year/day/time wasn't an int


@dataclass
class MissingSummary:
    """Counters accumulated while scanning for missing timestamps."""
    days_total: int = 0              # total (year, day) groups examined
    days_with_missing: int = 0       # groups that had at least one gap
    missing_timestamps_total: int = 0  # sum of individual missing minutes
    out_of_range_timestamps: int = 0   # timestamps outside valid HHMM range


@dataclass(frozen=True)
class MappingSnapshot:
    """A single effective-dated measurement-column layout."""
    effective_at: datetime
    metric_positions: dict[str, int | None]


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def parse_int(value: str) -> int | None:
    """Try to convert a string to an integer; return None instead of raising."""
    try:
        return int(value.strip())
    except ValueError:
        return None


def discover_input_files(base_dir: Path) -> list[Path]:
    """Find all raw data files inside ``STW_*`` folders under *base_dir*.

    Only regular files whose path contains a directory component starting
    with ``STW_`` are included.  Results are sorted by path for determinism.
    """
    files: list[Path] = []
    for f in sorted(base_dir.rglob("*")):
        if not f.is_file():
            continue
        if any(part.startswith("STW_") for part in f.parts):
            files.append(f)
    return files


# ---------------------------------------------------------------------------
# SQLite helpers -- used as a fast deduplication engine
# ---------------------------------------------------------------------------

def init_db(db_path: Path) -> sqlite3.Connection:
    """Create (or recreate) a temporary SQLite database for deduplication.

    The ``dedup_rows`` table uses ``(year, day, time)`` as its primary key so
    that ``INSERT OR IGNORE`` keeps only the first row seen for each timestamp.
    """
    if db_path.exists():
        db_path.unlink()
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA temp_store = MEMORY;")
    conn.execute(
        """
        CREATE TABLE dedup_rows (
            year INTEGER NOT NULL,
            day INTEGER NOT NULL,
            time INTEGER NOT NULL,
            row_tail TEXT NOT NULL,
            PRIMARY KEY (year, day, time)
        );
        """
    )
    return conn


def ingest_files(conn: sqlite3.Connection, files: Iterable[Path], drop_first_col: bool) -> IngestStats:
    """Parse CSV files and insert unique rows into the database.

    Args:
        conn: Open SQLite connection with the ``dedup_rows`` table.
        files: Paths to the CSV-like text files to read.
        drop_first_col: If True the first comma-separated field (station ID)
            is stripped before storing the row.  Set to True for raw input
            files and False when re-ingesting already-cleaned files.

    Returns:
        An ``IngestStats`` recording how many rows were seen, kept, or
        rejected.

    Rows are batched in groups of 10,000 for efficient bulk inserts.
    Later rows with the same ``(year, day, hhmm)`` timestamp are silently
    skipped by the ``INSERT OR IGNORE`` strategy.
    """
    stats = IngestStats()
    batch: list[tuple[str, int, int, int]] = []

    def flush_batch() -> None:
        if not batch:
            return
        conn.executemany(
            "INSERT OR IGNORE INTO dedup_rows (row_tail, year, day, time) VALUES (?, ?, ?, ?);",
            batch,
        )
        batch.clear()

    for f in files:
        stats.files_seen += 1
        with f.open("r", encoding="utf-8", errors="replace", newline="") as handle:
            for raw_line in handle:
                stats.lines_seen += 1
                line = raw_line.strip()
                if not line:
                    continue
                parts = [p.strip() for p in line.split(",")]

                # When reading raw files the layout is:
                #   station_id, year, day, hhmm, meas1, meas2, ...
                # When re-reading cleaned files the station_id is already gone:
                #   year, day, hhmm, meas1, meas2, ...
                if drop_first_col:
                    if len(parts) < 4:
                        stats.invalid_short += 1
                        continue
                    year = parse_int(parts[1])
                    day = parse_int(parts[2])
                    time = parse_int(parts[3])
                    row_tail = ",".join(parts[1:])  # everything after station_id
                else:
                    if len(parts) < 3:
                        stats.invalid_short += 1
                        continue
                    year = parse_int(parts[0])
                    day = parse_int(parts[1])
                    time = parse_int(parts[2])
                    row_tail = ",".join(parts)

                if year is None or day is None or time is None:
                    stats.invalid_key += 1
                    continue

                batch.append((row_tail, year, day, time))
                stats.valid_rows += 1
                if len(batch) >= 10000:
                    flush_batch()
        flush_batch()

    conn.commit()
    return stats


# ---------------------------------------------------------------------------
# HHMM time-format helpers
#
# The station records time as an integer where the hundreds represent the
# hour and the units represent the minute.  For example:
#   0001 = 00:01,  0930 = 09:30,  2359 = 23:59,  2400 = end-of-day midnight.
#
# There are exactly 1,440 valid values per day (one per minute).  The helpers
# below convert between this HHMM encoding and a plain 0-based minute index
# (0 = the first minute of the day, 1439 = the last).
# ---------------------------------------------------------------------------

def is_valid_hhmm(value: int) -> bool:
    """Return True if *value* is a legal HHMM timestamp (0001--2359 or 2400)."""
    if value == 2400:
        return True
    if value < 1 or value > 2359:
        return False
    hh = value // 100
    mm = value % 100
    return 0 <= hh <= 23 and 0 <= mm <= 59


def hhmm_to_minute_index(value: int) -> int:
    """Convert an HHMM value to a 0-based minute index (0--1439).

    ``0001 -> 0``, ``0002 -> 1``, ... ``2400 -> 1439``.
    """
    if value == 2400:
        return 1439
    hh = value // 100
    mm = value % 100
    return hh * 60 + mm - 1


def minute_index_to_hhmm(idx: int) -> int:
    """Inverse of ``hhmm_to_minute_index``: 0-based index back to HHMM.

    ``0 -> 0001``, ``1 -> 0002``, ... ``1439 -> 2400``.
    """
    total = idx + 1
    hh = total // 60
    mm = total % 60
    return hh * 100 + mm


# Pre-built list of all 1,440 valid HHMM values in chronological order.
VALID_HHMM_TIMES = [minute_index_to_hhmm(i) for i in range(1440)]


def compress_hhmm_ranges(values: list[int]) -> str:
    """Compress a sorted list of HHMM timestamps into a compact range string.

    Consecutive minutes are collapsed into ``start-end`` ranges separated by
    semicolons.  For example, ``[1, 2, 3, 100, 200, 201]`` becomes
    ``"1-3;100;200-201"``.  Returns ``"NONE"`` for an empty list.
    """
    if not values:
        return "NONE"

    ranges: list[str] = []
    start = prev = values[0]
    for v in values[1:]:
        if hhmm_to_minute_index(v) == hhmm_to_minute_index(prev) + 1:
            prev = v
            continue
        if start == prev:
            ranges.append(str(start))
        else:
            ranges.append(f"{start}-{prev}")
        start = prev = v
    if start == prev:
        ranges.append(str(start))
    else:
        ranges.append(f"{start}-{prev}")
    return ";".join(ranges)


def expand_hhmm_ranges(ranges_text: str) -> list[int]:
    """Inverse of ``compress_hhmm_ranges``: expand a range string back to
    individual HHMM integers.

    ``"1-3;100"`` -> ``[1, 2, 3, 100]``.
    """
    expanded: list[int] = []
    if not ranges_text or ranges_text == "NONE":
        return expanded

    for token in ranges_text.split(";"):
        item = token.strip()
        if not item:
            continue
        if "-" in item:
            start_text, end_text = item.split("-", 1)
            start = parse_int(start_text)
            end = parse_int(end_text)
            if start is None or end is None:
                continue
            if not (is_valid_hhmm(start) and is_valid_hhmm(end)):
                continue
            start_idx = hhmm_to_minute_index(start)
            end_idx = hhmm_to_minute_index(end)
            if end_idx < start_idx:
                start_idx, end_idx = end_idx, start_idx
            for idx in range(start_idx, end_idx + 1):
                expanded.append(minute_index_to_hhmm(idx))
        else:
            value = parse_int(item)
            if value is not None and is_valid_hhmm(value):
                expanded.append(value)
    return expanded


# ---------------------------------------------------------------------------
# Merging missing-timestamp placeholders into cleaned CSV files
# ---------------------------------------------------------------------------

def max_columns_in_csv(csv_path: Path) -> int:
    """Scan *csv_path* and return the widest row's column count (min 3).

    This is needed so that placeholder rows can be padded with the right
    number of ``NaN`` fields to match the existing measurement columns.
    """
    max_cols = 3
    if not csv_path.exists():
        return max_cols
    with csv_path.open("r", encoding="utf-8", errors="replace", newline="") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            cols = len(text.split(","))
            if cols > max_cols:
                max_cols = cols
    return max_cols


def parse_key_from_line(line: str) -> tuple[int, int, int] | None:
    """Extract the (year, day, hhmm) sort key from a cleaned CSV row."""
    parts = line.split(",", 3)
    if len(parts) < 3:
        return None
    year = parse_int(parts[0])
    day = parse_int(parts[1])
    hhmm = parse_int(parts[2])
    if year is None or day is None or hhmm is None:
        return None
    return (year, day, hhmm)


def merge_missing_rows_into_csv(
    data_csv_path: Path,
    missing_rows: Iterable[tuple[int, int, str]],
) -> int:
    """Insert placeholder rows for every missing timestamp into a cleaned CSV.

    Reads compressed missing ranges from *missing_rows*, builds ``NaN``-padded
    rows for each gap, and merge-sorts them into the existing data file so that
    the output remains in (year, day, hhmm) order.

    The merge is done via a temporary file that atomically replaces the
    original.

    Returns:
        The number of placeholder rows added.
    """
    if not data_csv_path.exists():
        return 0

    # Figure out how many measurement columns to pad with NaN.
    max_cols = max_columns_in_csv(data_csv_path)
    placeholder_count = max(0, max_cols - 3)  # 3 key cols: year, day, hhmm

    # Build the list of synthetic rows from the compressed missing ranges.
    synthetic_rows: list[tuple[int, int, int, str]] = []
    for year, day, ranges_text in missing_rows:
        if not ranges_text:
            continue
        for hhmm in expand_hhmm_ranges(ranges_text):
            values = [str(year), str(day), str(hhmm)]
            if placeholder_count:
                values.extend(["NaN"] * placeholder_count)
            synthetic_rows.append((year, day, hhmm, ",".join(values)))

    if not synthetic_rows:
        return 0

    synthetic_rows.sort(key=lambda t: (t[0], t[1], t[2]))

    # Merge-sort: walk through the existing file and interleave synthetic rows.
    tmp_path = data_csv_path.with_suffix(data_csv_path.suffix + ".tmp")
    rows_added = 0
    idx = 0  # pointer into synthetic_rows

    with data_csv_path.open("r", encoding="utf-8", errors="replace", newline="") as src, tmp_path.open(
        "w", encoding="utf-8", newline=""
    ) as out:
        for raw in src:
            line = raw.rstrip("\r\n")
            if not line:
                continue
            key = parse_key_from_line(line)
            if key is None:
                out.write(line + "\n")
                continue

            # Flush any synthetic rows that sort before the current real row.
            while idx < len(synthetic_rows) and synthetic_rows[idx][:3] < key:
                out.write(synthetic_rows[idx][3] + "\n")
                rows_added += 1
                idx += 1

            # Skip synthetic rows whose key already exists in the real data.
            while idx < len(synthetic_rows) and synthetic_rows[idx][:3] == key:
                idx += 1

            out.write(line + "\n")

        # Append any remaining synthetic rows after the last real row.
        while idx < len(synthetic_rows):
            out.write(synthetic_rows[idx][3] + "\n")
            rows_added += 1
            idx += 1

    tmp_path.replace(data_csv_path)
    return rows_added


# ---------------------------------------------------------------------------
# Writing cleaned output files
# ---------------------------------------------------------------------------

def get_years(conn: sqlite3.Connection) -> list[int]:
    """Return a sorted list of all distinct years in the database."""
    rows = conn.execute("SELECT DISTINCT year FROM dedup_rows ORDER BY year;").fetchall()
    return [r[0] for r in rows]


def write_scope_file(conn: sqlite3.Connection, years: list[int], out_path: Path) -> int:
    """Write all rows for the given *years* to *out_path*, sorted by
    (year, day, time).  Returns the number of rows written."""
    placeholders = ",".join("?" for _ in years)
    query = (
        "SELECT row_tail FROM dedup_rows "
        f"WHERE year IN ({placeholders}) "
        "ORDER BY year, day, time, row_tail;"
    )
    rows_written = 0
    with out_path.open("w", encoding="utf-8", newline="") as handle:
        for (row_tail,) in conn.execute(query, years):
            handle.write(row_tail + "\n")
            rows_written += 1
    return rows_written


def write_year_files(conn: sqlite3.Connection, years: list[int], out_dir: Path) -> dict[int, int]:
    """Write one ``stw_<year>_cleaned.csv`` file per year.

    Returns a dict mapping year -> row count.
    """
    written: dict[int, int] = {}
    for year in years:
        out_path = out_dir / f"stw_{year}_cleaned.csv"
        count = write_scope_file(conn, [year], out_path)
        written[year] = count
    return written


def select_combined_years(years: list[int], year_counts: dict[int, int]) -> list[int]:
    """Pick which years go into the combined file.

    Current strategy: include all available years.
    """
    del year_counts  # unused; kept in signature for future flexibility
    return sorted(years)


# ---------------------------------------------------------------------------
# Missing-timestamp detection
# ---------------------------------------------------------------------------

def write_missing_timestamps_log(log_path: Path, missing_rows: Iterable[tuple[int, int, str]]) -> None:
    """Persist compressed missing ranges to a CSV log."""
    with log_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["year", "day_of_year", "missing_hhmm_ranges"])
        for year, day, ranges_text in missing_rows:
            writer.writerow([year, day, ranges_text])


def format_hhmm_token(hhmm: int) -> str:
    """Render an HHMM integer as ``HH:MM`` (with 2400 shown as 24:00)."""
    if hhmm == 2400:
        return "24:00"
    text = str(hhmm).zfill(4)
    return f"{text[:2]}:{text[2:]}"


def write_formatted_missing_timestamps(log_path: Path, missing_rows: Iterable[tuple[int, int, str]]) -> None:
    """Write a human-readable missing-timestamp report.

    Each compressed HHMM range is expanded into one line in the form:

        YYYY/MM/DD HH:MM - HH:MM
    """
    lines: list[str] = []
    for year, day_of_year, ranges_text in missing_rows:
        day_date = date(year, 1, 1) + timedelta(days=day_of_year - 1)
        date_text = day_date.strftime("%Y/%m/%d")
        for token in ranges_text.split(";"):
            item = token.strip()
            if not item:
                continue
            if "-" in item:
                start_text, end_text = item.split("-", 1)
            else:
                start_text = item
                end_text = item
            start = parse_int(start_text)
            end = parse_int(end_text)
            if start is None or end is None:
                continue
            if not (is_valid_hhmm(start) and is_valid_hhmm(end)):
                continue
            lines.append(f"{date_text} {format_hhmm_token(start)} - {format_hhmm_token(end)}")

    log_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def log_missing_timestamps(
    conn: sqlite3.Connection,
    years: list[int],
    log_path: Path | None = None,
) -> tuple[MissingSummary, list[tuple[int, int, str]]]:
    """Scan the database for gaps in minute-resolution coverage.

    For every (year, day) group the function builds the set of observed HHMM
    timestamps and compares it against the full 1,440-value reference list.
    Any values that are absent are captured as compressed HHMM ranges (see
    ``compress_hhmm_ranges``). If *log_path* is provided, those ranges are also
    written to disk as a CSV log.

    Special cases:
      - **First-day grace**: timestamps before the first observed reading on
        the very first day are not counted as missing, since the station may
        not have been active yet.
      - **Last-day grace**: timestamps after the last observed reading on the
        very last day are not counted as missing, since the dataset may end
        before the day is complete.
      - **Entirely missing days**: if two consecutive observed days within the
        same year have a gap (e.g. day 5 then day 8), the intervening days
        (6 and 7) are logged as fully missing (all 1,440 timestamps).

    Returns:
        A ``(summary, missing_rows)`` tuple where ``missing_rows`` contains
        ``(year, day_of_year, compressed_ranges)`` entries.
    """
    placeholders = ",".join("?" for _ in years)
    query = (
        "SELECT year, day, time FROM dedup_rows "
        f"WHERE year IN ({placeholders}) "
        "ORDER BY year, day, time;"
    )

    summary = MissingSummary()
    missing_rows: list[tuple[int, int, str]] = []
    current_key: tuple[int, int] | None = None  # (year, day) being accumulated
    times_seen: set[int] = set()                 # valid HHMM values seen so far
    out_of_range = 0                             # invalid HHMM count in group
    first_valid_key: tuple[int, int] | None = None   # used for first-day grace
    first_valid_time: int | None = None
    last_valid_key: tuple[int, int] | None = None    # used for last-day grace
    last_valid_time: int | None = None

    def find_boundary_valid_time(descending: bool) -> tuple[tuple[int, int] | None, int | None]:
        """Find the first or last valid timestamp in the requested direction."""
        order = "DESC" if descending else "ASC"
        boundary_query = (
            "SELECT year, day, time FROM dedup_rows "
            f"WHERE year IN ({placeholders}) "
            f"ORDER BY year {order}, day {order}, time {order};"
        )
        for year, day, time in conn.execute(boundary_query, years):
            if is_valid_hhmm(time):
                return (year, day), time
        return None, None

    first_valid_key, first_valid_time = find_boundary_valid_time(descending=False)
    last_valid_key, last_valid_time = find_boundary_valid_time(descending=True)

    def flush_group() -> None:
        """Finish processing the current (year, day) group."""
        nonlocal current_key, times_seen, out_of_range
        if current_key is None:
            return
        year, day = current_key
        missing = [t for t in VALID_HHMM_TIMES if t not in times_seen]
        # Boundary grace: don't flag timestamps outside the observed window.
        if current_key == first_valid_key and first_valid_time is not None:
            missing = [t for t in missing if t >= first_valid_time]
        if current_key == last_valid_key and last_valid_time is not None:
            missing = [t for t in missing if t <= last_valid_time]
        summary.days_total += 1
        summary.missing_timestamps_total += len(missing)
        summary.out_of_range_timestamps += out_of_range
        if missing:
            summary.days_with_missing += 1
            missing_rows.append((year, day, compress_hhmm_ranges(missing)))
        times_seen = set()
        out_of_range = 0

    def log_fully_missing_day(year: int, day: int) -> None:
        """Record a day with zero observations (all 1,440 timestamps missing)."""
        summary.days_total += 1
        summary.days_with_missing += 1
        summary.missing_timestamps_total += len(VALID_HHMM_TIMES)
        missing_rows.append((year, day, "1-2400"))

    for year, day, time in conn.execute(query, years):
        key = (year, day)
        if current_key != key:
            previous_key = current_key
            flush_group()
            # Fill in entirely missing days between consecutive observed days.
            if previous_key is not None:
                prev_year, prev_day = previous_key
                if year == prev_year and day > prev_day + 1:
                    for missing_day in range(prev_day + 1, day):
                        log_fully_missing_day(year, missing_day)
            current_key = key
        if is_valid_hhmm(time):
            times_seen.add(time)
        else:
            out_of_range += 1
    flush_group()

    if log_path is not None:
        write_missing_timestamps_log(log_path, missing_rows)

    return summary, missing_rows


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def write_summary_report(
    out_path: Path,
    ingest: IngestStats,
    unique_rows: int,
    years: list[int],
    year_counts: dict[int, int],
    combined_years: list[int],
    combined_rows: int,
    combined_missing: MissingSummary,
    yearly_rows_added: int,
    combined_rows_added: int,
) -> None:
    """Write a human-readable summary of the entire pipeline run."""
    duplicates_removed = ingest.valid_rows - unique_rows
    lines = [
        f"Source files scanned: {ingest.files_seen}",
        f"Total lines seen: {ingest.lines_seen}",
        f"Valid rows parsed: {ingest.valid_rows}",
        f"Invalid rows (too few columns): {ingest.invalid_short}",
        f"Invalid rows (non-integer year/day/time): {ingest.invalid_key}",
        f"Unique cleaned rows: {unique_rows}",
        f"Duplicate timestamp rows removed (same year/day/time): {duplicates_removed}",
        f"Years found: {', '.join(map(str, years))}",
        "Yearly row counts:",
    ]
    for year in years:
        lines.append(f"  {year}: {year_counts[year]}")
    lines.extend(
        [
            f"Combined years: {', '.join(map(str, combined_years))}",
            f"Combined rows written: {combined_rows}",
            f"Rows merged into yearly files from detected gaps: {yearly_rows_added}",
            f"Rows merged from missing logs (combined file): {combined_rows_added}",
            f"Combined day groups checked: {combined_missing.days_total}",
            f"Combined days with missing timestamps: {combined_missing.days_with_missing}",
            f"Combined missing timestamp total: {combined_missing.missing_timestamps_total}",
            f"Combined out-of-range timestamps: {combined_missing.out_of_range_timestamps}",
        ]
    )
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def recheck_combined_file(combined_file: Path, missing_combined_dir: Path) -> tuple[int, int, MissingSummary]:
    """Re-ingest the combined file and verify it has no duplicates or gaps.

    This is a sanity check: after merging placeholder rows into the combined
    file, we re-read it through the same dedup + missing-timestamp logic to
    confirm the output is complete and consistent.

    Returns:
        ``(total_rows, unique_rows, missing_summary)``
    """
    conn = init_db(RECHECK_DB)
    stats = ingest_files(conn, [combined_file], drop_first_col=False)
    unique_rows = conn.execute("SELECT COUNT(*) FROM dedup_rows;").fetchone()[0]
    years = get_years(conn)
    missing, _ = log_missing_timestamps(
        conn,
        years,
        missing_combined_dir / "missing_timestamps_combined_recheck.csv",
    )
    conn.close()
    if RECHECK_DB.exists():
        RECHECK_DB.unlink()
    return stats.valid_rows, unique_rows, missing




# ---------------------------------------------------------------------------
# Column-map application for the combined cleaned file
# ---------------------------------------------------------------------------

def excel_column_name_to_index(column_name: str) -> int:
    """Convert an Excel column label like ``A`` or ``AA`` to a zero-based index."""
    value = 0
    for char in column_name:
        value = value * 26 + (ord(char.upper()) - ord("A") + 1)
    return value - 1


def split_excel_cell_reference(cell_ref: str) -> tuple[int, int]:
    """Split an Excel cell reference into zero-based (row, column) indexes."""
    letters: list[str] = []
    digits: list[str] = []
    for char in cell_ref:
        if char.isalpha():
            letters.append(char)
        elif char.isdigit():
            digits.append(char)
    if not letters or not digits:
        raise ValueError(f"Invalid Excel cell reference {cell_ref!r}.")
    row_idx = int("".join(digits)) - 1
    col_idx = excel_column_name_to_index("".join(letters))
    return row_idx, col_idx


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
    year = parse_int(year_text)
    day_of_year = parse_int(day_text)
    hhmm = parse_int(hhmm_text)
    if year is None or day_of_year is None or hhmm is None:
        raise ValueError(f"{context}: invalid year/day_of_year/hhmm values.")
    start_of_year = datetime(year, 1, 1)
    return start_of_year + timedelta(days=day_of_year - 1) + parse_hhmm_to_offset(hhmm, context)


def load_shared_strings(workbook_zip: ZipFile) -> list[str]:
    """Load the workbook's shared string table."""
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


def load_column_map_workbook(workbook_path: Path) -> tuple[list[str], list[MappingSnapshot], list[datetime]]:
    """Read mapping snapshots from the workbook's ``Column Mapping`` sheet."""
    with ZipFile(workbook_path) as workbook_zip:
        workbook_xml = ET.fromstring(workbook_zip.read("xl/workbook.xml"))
        workbook_rels = ET.fromstring(workbook_zip.read("xl/_rels/workbook.xml.rels"))
        rel_map = {
            rel.attrib["Id"]: rel.attrib["Target"]
            for rel in workbook_rels.findall("pkgrel:Relationship", XLSX_NS)
        }

        target = None
        for sheet in workbook_xml.find("main:sheets", XLSX_NS):
            if sheet.attrib.get("name") == COLUMN_MAP_SHEET_NAME:
                target = rel_map.get(sheet.attrib[DOCREL_ID_ATTR])
                break
        if target is None:
            raise ValueError(f"{workbook_path} does not contain a worksheet named {COLUMN_MAP_SHEET_NAME!r}.")

        shared_strings = load_shared_strings(workbook_zip)
        worksheet_xml = ET.fromstring(workbook_zip.read(f"xl/{target}"))
        grid = load_worksheet_grid(worksheet_xml, shared_strings)
        if not grid:
            raise ValueError(f"{workbook_path}:{COLUMN_MAP_SHEET_NAME} is empty.")

        header = [value.strip() for value in grid[0]]
        if "Datetime" not in header:
            raise ValueError(f"{workbook_path}:{COLUMN_MAP_SHEET_NAME} must contain a 'Datetime' column.")

        rows: list[dict[str, str]] = []
        for values in grid[1:]:
            if not any(cell.strip() for cell in values):
                continue
            row = {header[idx]: values[idx].strip() if idx < len(values) else "" for idx in range(len(header))}
            rows.append(row)

    if not rows:
        raise ValueError(f"{workbook_path}:{COLUMN_MAP_SHEET_NAME} does not contain any mapping rows.")

    metric_names = [
        name for name in rows[0].keys() if name != "Datetime" and name not in MAPPED_EXCLUDED_METRICS
    ]
    if not metric_names:
        raise ValueError(f"{workbook_path}:{COLUMN_MAP_SHEET_NAME} does not define any metric columns.")

    snapshots: list[MappingSnapshot] = []
    for row_num, row in enumerate(rows, start=2):
        datetime_text = (row.get("Datetime", "") or "").strip()
        if not datetime_text:
            raise ValueError(f"{workbook_path}:{row_num} is missing a Datetime value.")
        try:
            effective_at = datetime.strptime(datetime_text, "%m/%d/%Y %H:%M")
        except ValueError as exc:
            raise ValueError(
                f"{workbook_path}:{row_num} has invalid Datetime {datetime_text!r}; expected '%m/%d/%Y %H:%M'."
            ) from exc

        metric_positions: dict[str, int | None] = {}
        for metric_name in metric_names:
            raw_value = (row.get(metric_name, "") or "").strip()
            if not raw_value:
                metric_positions[metric_name] = None
                continue
            position = parse_int(raw_value)
            if position is None:
                raise ValueError(f"{workbook_path}:{row_num} has invalid {metric_name!r} value {raw_value!r}.")
            if position < 1:
                raise ValueError(
                    f"{workbook_path}:{row_num} has invalid {metric_name!r} position {position}; positions must be 1 or greater."
                )
            metric_positions[metric_name] = position

        snapshots.append(MappingSnapshot(effective_at=effective_at, metric_positions=metric_positions))

    snapshots.sort(key=lambda snapshot: snapshot.effective_at)
    effective_points = [snapshot.effective_at for snapshot in snapshots]
    return metric_names, snapshots, effective_points


def resolve_metric_value(measurements: list[str], position: int | None) -> str:
    """Return the mapped measurement value or NaN if it is unavailable."""
    if position is None or position > len(measurements):
        return "NaN"
    value = measurements[position - 1].strip()
    return value or "NaN"


def write_mapped_combined_file(input_path: Path, workbook_path: Path, output_path: Path) -> int:
    """Write the metric-mapped combined CSV using the workbook column map."""
    metric_names, snapshots, effective_points = load_column_map_workbook(workbook_path)
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
            if len(row) < 3:
                raise ValueError(f"{input_path}:{row_num} has {len(row)} columns; expected at least 3.")

            context = f"{input_path}:{row_num}"
            row_dt = build_row_datetime(row[0], row[1], row[2], context)
            snapshot_idx = bisect_right(effective_points, row_dt) - 1
            if snapshot_idx < 0:
                raise ValueError(
                    f"{context} has datetime {row_dt.strftime('%Y/%m/%d %H:%M')!r}, which is earlier than the first mapping entry."
                )

            measurements = row[3:]
            snapshot = snapshots[snapshot_idx]
            metric_values = [
                resolve_metric_value(measurements, snapshot.metric_positions[metric_name])
                for metric_name in metric_names
            ]
            writer.writerow([row_dt.strftime("%Y/%m/%d %H:%M"), *metric_values])
            rows_written += 1

    return rows_written

# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Split-pipeline state helpers and step runners
# ---------------------------------------------------------------------------

def ensure_pipeline_dirs() -> None:
    """Create the directories used by the split pipeline."""
    YEARLY_CLEANED_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    FINAL_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)


def read_pipeline_state(state_path: Path = PIPELINE_STATE_FILE) -> dict:
    """Load the JSON pipeline state written between step scripts."""
    if not state_path.exists():
        return {}
    with state_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_pipeline_state(state: dict, state_path: Path = PIPELINE_STATE_FILE) -> None:
    """Persist the JSON pipeline state for later pipeline steps."""
    state_path.parent.mkdir(parents=True, exist_ok=True)
    with state_path.open("w", encoding="utf-8") as handle:
        json.dump(state, handle, indent=2, sort_keys=True)
        handle.write("\n")


def _normalize_year_counts(raw_counts: dict) -> dict[int, int]:
    """Convert JSON-loaded year-count keys back to integers."""
    return {int(year): int(count) for year, count in raw_counts.items()}


def run_step_01_ingest_raw_to_yearly_cleaned() -> None:
    """Ingest raw files, deduplicate by timestamp, and write yearly cleaned CSVs."""
    ensure_pipeline_dirs()
    print("[Step 1/6] Ingesting raw data and writing yearly cleaned files...", flush=True)
    files = discover_input_files(RAW_DATA_DIR)
    conn = init_db(WORK_DB)
    try:
        ingest_stats = ingest_files(conn, files, drop_first_col=True)
        unique_rows = conn.execute("SELECT COUNT(*) FROM dedup_rows;").fetchone()[0]
        years = get_years(conn)
        year_counts = write_year_files(conn, years, YEARLY_CLEANED_DIR)
    finally:
        conn.close()
        if WORK_DB.exists():
            WORK_DB.unlink()

    state = {
        "ingest": asdict(ingest_stats),
        "unique_rows": unique_rows,
        "years": years,
        "year_counts": {str(year): count for year, count in year_counts.items()},
        "yearly_rows_added": 0,
        "combined_years": [],
        "combined_rows": 0,
        "combined_rows_added": 0,
        "combined_missing": asdict(MissingSummary()),
    }
    write_pipeline_state(state)
    print(
        f"Step 1 complete: {ingest_stats.valid_rows} valid rows ingested across {len(years)} years.",
        flush=True,
    )


def run_step_02_fill_yearly_gaps() -> None:
    """Fill missing timestamps inside each yearly cleaned file."""
    ensure_pipeline_dirs()
    state = read_pipeline_state()
    years = [int(year) for year in state.get("years", [])]
    if not years:
        raise ValueError("pipeline_state.json does not contain years. Run step 01 first.")

    conn = init_db(WORK_DB)
    try:
        yearly_files = [YEARLY_CLEANED_DIR / f"stw_{year}_cleaned.csv" for year in years]
        ingest_files(conn, yearly_files, drop_first_col=False)
        yearly_rows_added = 0
        print("[Step 2/6] Filling yearly timestamp gaps...", flush=True)
        for year in years:
            yearly_missing, yearly_missing_rows = log_missing_timestamps(conn, [year])
            yearly_file_path = YEARLY_CLEANED_DIR / f"stw_{year}_cleaned.csv"
            added = merge_missing_rows_into_csv(yearly_file_path, yearly_missing_rows)
            yearly_rows_added += added
            print(
                f"Year {year}: {yearly_missing.missing_timestamps_total} timestamps missing; {added} rows merged.",
                flush=True,
            )
    finally:
        conn.close()
        if WORK_DB.exists():
            WORK_DB.unlink()

    state["yearly_rows_added"] = yearly_rows_added
    write_pipeline_state(state)
    print(f"Step 2 complete: {yearly_rows_added} yearly gap rows merged.", flush=True)


def run_step_03_build_combined_cleaned() -> None:
    """Build the combined cleaned CSV and merge combined missing timestamps."""
    ensure_pipeline_dirs()
    state = read_pipeline_state()
    years = [int(year) for year in state.get("years", [])]
    year_counts = _normalize_year_counts(state.get("year_counts", {}))
    if not years or not year_counts:
        raise ValueError("pipeline_state.json is missing years/year_counts. Run step 01 first.")

    conn = init_db(WORK_DB)
    try:
        yearly_files = [YEARLY_CLEANED_DIR / f"stw_{year}_cleaned.csv" for year in years]
        ingest_files(conn, yearly_files, drop_first_col=False)
        combined_years = select_combined_years(years, year_counts)
        combined_rows = write_scope_file(conn, combined_years, COMBINED_CLEANED_FILE)
        combined_missing_log_path = REPORTS_DIR / "missing_timestamps_combined.csv"
        combined_missing_formatted_path = REPORTS_DIR / "missing_timestamps_combined_formatted.txt"
        combined_missing, combined_missing_rows = log_missing_timestamps(
            conn,
            combined_years,
            combined_missing_log_path,
        )
        write_formatted_missing_timestamps(combined_missing_formatted_path, combined_missing_rows)
        combined_rows_added = merge_missing_rows_into_csv(COMBINED_CLEANED_FILE, combined_missing_rows)
        combined_rows += combined_rows_added
    finally:
        conn.close()
        if WORK_DB.exists():
            WORK_DB.unlink()

    state["combined_years"] = combined_years
    state["combined_rows"] = combined_rows
    state["combined_rows_added"] = combined_rows_added
    state["combined_missing"] = asdict(combined_missing)
    write_pipeline_state(state)
    print(
        f"Step 3 complete: combined file built for {combined_years[0]}-{combined_years[-1]} with {combined_rows_added} merged gap rows.",
        flush=True,
    )


def run_step_04_write_reports_and_recheck() -> None:
    """Write summary artifacts and recheck the combined cleaned file."""
    ensure_pipeline_dirs()
    state = read_pipeline_state()
    if not state:
        raise ValueError("pipeline_state.json is missing. Run earlier steps first.")

    ingest = IngestStats(**state["ingest"])
    year_counts = _normalize_year_counts(state["year_counts"])
    combined_missing = MissingSummary(**state["combined_missing"])
    combined_years = [int(year) for year in state["combined_years"]]

    print("[Step 4/6] Writing reports and rechecking combined output...", flush=True)
    write_summary_report(
        REPORTS_DIR / "processing_summary.txt",
        ingest=ingest,
        unique_rows=int(state["unique_rows"]),
        years=[int(year) for year in state["years"]],
        year_counts=year_counts,
        combined_years=combined_years,
        combined_rows=int(state["combined_rows"]),
        combined_missing=combined_missing,
        yearly_rows_added=int(state.get("yearly_rows_added", 0)),
        combined_rows_added=int(state.get("combined_rows_added", 0)),
    )

    checked_rows, unique_checked_rows, checked_missing = recheck_combined_file(
        COMBINED_CLEANED_FILE,
        REPORTS_DIR,
    )
    duplicates_after_recheck = checked_rows - unique_checked_rows
    recheck_report = [
        f"Combined file: {COMBINED_CLEANED_FILE.name}",
        f"Rows read: {checked_rows}",
        f"Unique rows: {unique_checked_rows}",
        f"Duplicate timestamps found on recheck: {duplicates_after_recheck}",
        f"Day groups checked: {checked_missing.days_total}",
        f"Days with missing timestamps: {checked_missing.days_with_missing}",
        f"Missing timestamps total: {checked_missing.missing_timestamps_total}",
        f"Out-of-range timestamps: {checked_missing.out_of_range_timestamps}",
    ]
    (REPORTS_DIR / "combined_recheck_report.txt").write_text("\n".join(recheck_report) + "\n", encoding="utf-8")

    state["recheck"] = {
        "rows_read": checked_rows,
        "unique_rows": unique_checked_rows,
        "duplicates_after_recheck": duplicates_after_recheck,
        "missing": asdict(checked_missing),
    }
    write_pipeline_state(state)
    print(
        f"Step 4 complete: recheck found {checked_missing.missing_timestamps_total} missing timestamps after fill.",
        flush=True,
    )


def run_step_05_map_combined_columns() -> None:
    """Apply the workbook column mapping to the combined cleaned CSV."""
    ensure_pipeline_dirs()
    print("[Step 5/6] Writing mapped combined file...", flush=True)
    mapped_rows = write_mapped_combined_file(
        COMBINED_CLEANED_FILE,
        COLUMN_MAP_WORKBOOK,
        MAPPED_COMBINED_FILE,
    )
    state = read_pipeline_state()
    state["mapped_rows"] = mapped_rows
    write_pipeline_state(state)
    print(f"Step 5 complete: wrote {mapped_rows} mapped rows.", flush=True)
