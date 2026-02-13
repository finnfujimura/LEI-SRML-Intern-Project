#!/usr/bin/env python3
"""
clean_stw_data.py -- Clean and consolidate STW solar-radiation data.

This script processes raw minute-resolution data files from the UO Solar
Radiation Monitoring Lab's STW station. Raw data arrives as monthly text files
inside year-named folders (e.g. ``STW_2020_Process0/``). Each row looks like:

    station_id, year, day_of_year, hhmm, measurement_1, measurement_2, ...

The cleaning pipeline (run via ``main()``) performs eight steps:

  1. Discover all source files under ``Raw Data/STW_*/``.
  2. Load rows into a SQLite database, dropping the station-ID column and
     removing exact duplicates (rows identical from column 2 onward).
  3. Write one sorted CSV per year to ``cleaned_output/yearly_cleaned/``.
  4. For each year, compare observed timestamps against the full 1,440
     valid minute-of-day HHMM values and log gaps to ``missing_logs/yearly/``.
  5. Write a combined CSV covering all years. Merge placeholder rows
     (``NaN`` measurements) for every missing timestamp so the file has no
     gaps.
  6. Write a human-readable summary report.
  7. Re-ingest the 10-year file and verify zero duplicates and zero missing
     timestamps remain.
  8. Delete temporary SQLite databases.

Key conventions:
  - **HHMM format**: An integer where HH = hours (00-23) and MM = minutes
    (00-59). Valid values run from 0001 to 2359, plus the special value 2400
    (end-of-day midnight). This yields 1,440 timestamps per day.
  - **First-day grace**: On the very first day of a scope, timestamps earlier
    than the first observed reading are *not* counted as missing (the station
    may not have been running yet).
  - **Deduplication key**: Everything after the station-ID column. Two rows
    with the same year/day/time *and* identical measurements are duplicates.
"""

from __future__ import annotations

import csv
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


# ---------------------------------------------------------------------------
# Directory layout -- all paths are relative to this script's parent folder.
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent          # .../Data/
OUT_DIR = BASE_DIR / "cleaned_output"               # root of all generated output
YEARLY_CLEANED_DIR = OUT_DIR / "yearly_cleaned"     # one CSV per year
COMBINED_DIR = OUT_DIR / "combined"                 # all-years merged CSV
MISSING_DIR = OUT_DIR / "missing_logs"
MISSING_YEARLY_DIR = MISSING_DIR / "yearly"         # gap logs per year
MISSING_COMBINED_DIR = MISSING_DIR / "combined"     # gap log for combined file
REPORTS_DIR = OUT_DIR / "reports"                    # summary & recheck reports
WORK_DB = OUT_DIR / "_stw_work.sqlite3"             # temp DB (deleted at end)
RECHECK_DB = OUT_DIR / "_stw_recheck.sqlite3"       # temp DB (deleted at end)


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

    The ``dedup_rows`` table uses the full row text (minus the station-ID
    column) as a primary key so that ``INSERT OR IGNORE`` silently skips
    duplicates.  An index on (year, day, time) supports the ordered queries
    used later when writing output files.
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
            row_tail TEXT PRIMARY KEY,
            year INTEGER NOT NULL,
            day INTEGER NOT NULL,
            time INTEGER NOT NULL
        );
        """
    )
    conn.execute("CREATE INDEX idx_year_day_time ON dedup_rows (year, day, time);")
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
    Duplicates are silently skipped by the ``INSERT OR IGNORE`` strategy.
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


def merge_missing_rows_into_csv(data_csv_path: Path, missing_log_path: Path) -> int:
    """Insert placeholder rows for every missing timestamp into a cleaned CSV.

    Reads the missing-timestamp log (produced by ``log_missing_timestamps``),
    builds ``NaN``-padded rows for each gap, and merge-sorts them into the
    existing data file so that the output remains in (year, day, hhmm) order.

    The merge is done via a temporary file that atomically replaces the
    original.

    Returns:
        The number of placeholder rows added.
    """
    if not data_csv_path.exists() or not missing_log_path.exists():
        return 0

    # Figure out how many measurement columns to pad with NaN.
    max_cols = max_columns_in_csv(data_csv_path)
    placeholder_count = max(0, max_cols - 3)  # 3 key cols: year, day, hhmm

    # Build the list of synthetic rows from the missing-timestamp log.
    synthetic_rows: list[tuple[int, int, int, str]] = []
    with missing_log_path.open("r", encoding="utf-8", errors="replace", newline="") as missing_file:
        reader = csv.DictReader(missing_file)
        for row in reader:
            year = parse_int(row.get("year", ""))
            day = parse_int(row.get("day_of_year", ""))
            ranges_text = (row.get("missing_hhmm_ranges", "") or "").strip()
            if year is None or day is None or not ranges_text:
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

def log_missing_timestamps(
    conn: sqlite3.Connection,
    years: list[int],
    log_path: Path,
) -> MissingSummary:
    """Scan the database for gaps in minute-resolution coverage and write a log.

    For every (year, day) group the function builds the set of observed HHMM
    timestamps and compares it against the full 1,440-value reference list.
    Any values that are absent are written to *log_path* as compressed HHMM
    ranges (see ``compress_hhmm_ranges``).

    Special cases:
      - **First-day grace**: timestamps before the first observed reading on
        the very first day are not counted as missing, since the station may
        not have been active yet.
      - **Entirely missing days**: if two consecutive observed days within the
        same year have a gap (e.g. day 5 then day 8), the intervening days
        (6 and 7) are logged as fully missing (all 1,440 timestamps).

    Returns:
        A ``MissingSummary`` with aggregate gap statistics.
    """
    placeholders = ",".join("?" for _ in years)
    query = (
        "SELECT year, day, time FROM dedup_rows "
        f"WHERE year IN ({placeholders}) "
        "ORDER BY year, day, time;"
    )

    summary = MissingSummary()
    current_key: tuple[int, int] | None = None  # (year, day) being accumulated
    times_seen: set[int] = set()                 # valid HHMM values seen so far
    out_of_range = 0                             # invalid HHMM count in group
    first_valid_key: tuple[int, int] | None = None   # used for first-day grace
    first_valid_time: int | None = None

    with log_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["year", "day_of_year", "missing_hhmm_ranges"])

        def flush_group() -> None:
            """Finish processing the current (year, day) group."""
            nonlocal current_key, times_seen, out_of_range
            if current_key is None:
                return
            year, day = current_key
            missing = [t for t in VALID_HHMM_TIMES if t not in times_seen]
            # First-day grace: don't flag timestamps before the station started.
            if current_key == first_valid_key and first_valid_time is not None:
                missing = [t for t in missing if t >= first_valid_time]
            summary.days_total += 1
            summary.missing_timestamps_total += len(missing)
            summary.out_of_range_timestamps += out_of_range
            if missing:
                summary.days_with_missing += 1
                writer.writerow([year, day, compress_hhmm_ranges(missing)])
            times_seen = set()
            out_of_range = 0

        def log_fully_missing_day(year: int, day: int) -> None:
            """Record a day with zero observations (all 1,440 timestamps missing)."""
            summary.days_total += 1
            summary.days_with_missing += 1
            summary.missing_timestamps_total += len(VALID_HHMM_TIMES)
            writer.writerow([year, day, "1-2400"])

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
                if first_valid_key is None:
                    first_valid_key = key
                    first_valid_time = time
                times_seen.add(time)
            else:
                out_of_range += 1
        flush_group()

    return summary


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
        f"Duplicate rows removed (based on columns 2+): {duplicates_removed}",
        f"Years found: {', '.join(map(str, years))}",
        "Yearly row counts:",
    ]
    for year in years:
        lines.append(f"  {year}: {year_counts[year]}")
    lines.extend(
        [
            f"Combined years: {', '.join(map(str, combined_years))}",
            f"Combined rows written: {combined_rows}",
            f"Rows merged from missing logs (all yearly files): {yearly_rows_added}",
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
    missing = log_missing_timestamps(
        conn,
        years,
        missing_combined_dir / "missing_timestamps_combined_recheck.csv",
    )
    conn.close()
    if RECHECK_DB.exists():
        RECHECK_DB.unlink()
    return stats.valid_rows, unique_rows, missing


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def main() -> None:
    """Run the full 8-step cleaning pipeline.  See module docstring for details."""
    # Ensure all output directories exist.
    OUT_DIR.mkdir(exist_ok=True)
    YEARLY_CLEANED_DIR.mkdir(parents=True, exist_ok=True)
    COMBINED_DIR.mkdir(parents=True, exist_ok=True)
    MISSING_YEARLY_DIR.mkdir(parents=True, exist_ok=True)
    MISSING_COMBINED_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    print("[1/8] Discovering source files...", flush=True)
    input_files = discover_input_files(BASE_DIR)
    print(f"Found {len(input_files)} source files.", flush=True)

    print("[2/8] Loading and deduplicating rows...", flush=True)
    conn = init_db(WORK_DB)
    ingest_stats = ingest_files(conn, input_files, drop_first_col=True)

    unique_rows = conn.execute("SELECT COUNT(*) FROM dedup_rows;").fetchone()[0]
    years = get_years(conn)
    if not years:
        raise RuntimeError("No valid data rows found.")
    print(
        f"Loaded {ingest_stats.valid_rows} valid rows; {unique_rows} unique rows across {len(years)} years.",
        flush=True,
    )

    print("[3/8] Writing yearly cleaned CSV files...", flush=True)
    year_counts = write_year_files(conn, years, YEARLY_CLEANED_DIR)
    print(f"Wrote {len(year_counts)} yearly cleaned files.", flush=True)

    print("[4/8] Looking for missing timestamps in yearly files...", flush=True)
    yearly_rows_added = 0
    for year in years:
        missing_log_path = MISSING_YEARLY_DIR / f"missing_timestamps_{year}.csv"
        yearly_missing = log_missing_timestamps(
            conn,
            [year],
            missing_log_path,
        )
        yearly_file_path = YEARLY_CLEANED_DIR / f"stw_{year}_cleaned.csv"
        added = merge_missing_rows_into_csv(yearly_file_path, missing_log_path)
        yearly_rows_added += added
        print(
            f"Year {year}: {yearly_missing.missing_timestamps_total} timestamps missing; {added} rows merged.",
            flush=True,
        )

    print("[5/8] Building and filling combined file...", flush=True)
    combined_years = select_combined_years(years, year_counts)
    combined_file = COMBINED_DIR / "stw_combined_cleaned.csv"
    combined_rows = write_scope_file(conn, combined_years, combined_file)
    combined_missing_log_path = MISSING_COMBINED_DIR / "missing_timestamps_combined.csv"
    combined_missing = log_missing_timestamps(
        conn,
        combined_years,
        combined_missing_log_path,
    )
    combined_rows_added = merge_missing_rows_into_csv(combined_file, combined_missing_log_path)
    combined_rows += combined_rows_added
    print(
        f"Combined {combined_years[0]}-{combined_years[-1]}: {combined_missing.missing_timestamps_total} timestamps missing; "
        f"{combined_rows_added} rows merged.",
        flush=True,
    )

    print("[6/8] Writing summary report...", flush=True)
    write_summary_report(
        REPORTS_DIR / "processing_summary.txt",
        ingest=ingest_stats,
        unique_rows=unique_rows,
        years=years,
        year_counts=year_counts,
        combined_years=combined_years,
        combined_rows=combined_rows,
        combined_missing=combined_missing,
        yearly_rows_added=yearly_rows_added,
        combined_rows_added=combined_rows_added,
    )

    print("[7/8] Rechecking combined file...", flush=True)
    checked_rows, unique_checked_rows, checked_missing = recheck_combined_file(
        combined_file,
        MISSING_COMBINED_DIR,
    )
    duplicates_after_recheck = checked_rows - unique_checked_rows
    recheck_report = [
        f"Combined file: {combined_file.name}",
        f"Rows read: {checked_rows}",
        f"Unique rows: {unique_checked_rows}",
        f"Duplicates found on recheck: {duplicates_after_recheck}",
        f"Day groups checked: {checked_missing.days_total}",
        f"Days with missing timestamps: {checked_missing.days_with_missing}",
        f"Missing timestamps total: {checked_missing.missing_timestamps_total}",
        f"Out-of-range timestamps: {checked_missing.out_of_range_timestamps}",
    ]
    (REPORTS_DIR / "combined_recheck_report.txt").write_text("\n".join(recheck_report) + "\n", encoding="utf-8")
    print(
        f"Recheck complete: {checked_missing.missing_timestamps_total} timestamps missing "
        f"across {checked_missing.days_with_missing} days.",
        flush=True,
    )

    conn.close()
    if WORK_DB.exists():
        WORK_DB.unlink()

    print("[8/8] Pipeline complete.", flush=True)


if __name__ == "__main__":
    main()
