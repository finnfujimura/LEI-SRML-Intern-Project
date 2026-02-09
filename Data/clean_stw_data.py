#!/usr/bin/env python3
"""
Clean and consolidate monthly files.

Rules:
- The first column is ignored for de-duplication.
- Year/day/time are read from columns 2/3/4.
- Duplicate rows are removed.
- Missing timestamps are logged for each (year, day) using valid HHMM minutes:
  0001..2359 (where MM is 00-59), plus 2400.
- For the first observed day in a scope, timestamps earlier than the first
  observed HHMM are not treated as missing.
- One cleaned file is produced per year.
- One cleaned 10-year file is produced starting from the first year in the
  dataset.
- The 10-year file is rechecked for duplicates and missing timestamps.
"""

from __future__ import annotations

import csv
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


BASE_DIR = Path(__file__).resolve().parent
OUT_DIR = BASE_DIR / "cleaned_output"
YEARLY_CLEANED_DIR = OUT_DIR / "yearly_cleaned"
COMBINED_DIR = OUT_DIR / "combined"
MISSING_DIR = OUT_DIR / "missing_logs"
MISSING_YEARLY_DIR = MISSING_DIR / "yearly"
MISSING_COMBINED_DIR = MISSING_DIR / "combined"
REPORTS_DIR = OUT_DIR / "reports"
WORK_DB = OUT_DIR / "_stw_work.sqlite3"
RECHECK_DB = OUT_DIR / "_stw_recheck.sqlite3"


@dataclass
class IngestStats:
    files_seen: int = 0
    lines_seen: int = 0
    valid_rows: int = 0
    invalid_short: int = 0
    invalid_key: int = 0


@dataclass
class MissingSummary:
    days_total: int = 0
    days_with_missing: int = 0
    missing_timestamps_total: int = 0
    out_of_range_timestamps: int = 0


def parse_int(value: str) -> int | None:
    "Safely parses a string into an integer, returning None on failure."
    try:
        return int(value.strip())
    except ValueError:
        return None


def discover_input_files(base_dir: Path) -> list[Path]:
    "Recursively finds 'STW_' input files in the given directory."
    files: list[Path] = []
    for f in sorted(base_dir.rglob("*")):
        if not f.is_file():
            continue
        if any(part.startswith("STW_") for part in f.parts):
            files.append(f)
    return files


def init_db(db_path: Path) -> sqlite3.Connection:
    "Initializes a SQLite database with deduplication tables and indices."
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
    "Reads input files and inserts unique rows into the database."
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

                if drop_first_col:
                    if len(parts) < 4:
                        stats.invalid_short += 1
                        continue
                    year = parse_int(parts[1])
                    day = parse_int(parts[2])
                    time = parse_int(parts[3])
                    row_tail = ",".join(parts[1:])
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


def is_valid_hhmm(value: int) -> bool:
    "Validates if an integer represents a valid HHMM time or 2400."
    if value == 2400:
        return True
    if value < 1 or value > 2359:
        return False
    hh = value // 100
    mm = value % 100
    return 0 <= hh <= 23 and 0 <= mm <= 59


def hhmm_to_minute_index(value: int) -> int:
    "Converts HHMM time format to a 0-indexed minute of the day."
    if value == 2400:
        return 1439
    hh = value // 100
    mm = value % 100
    return hh * 60 + mm - 1


def minute_index_to_hhmm(idx: int) -> int:
    "Converts a 0-indexed minute of the day back to HHMM format."
    total = idx + 1
    hh = total // 60
    mm = total % 60
    return hh * 100 + mm


VALID_HHMM_TIMES = [minute_index_to_hhmm(i) for i in range(1440)]


def compress_hhmm_ranges(values: list[int]) -> str:
    "Compresses a list of HHMM times into a concise range string."
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


def get_years(conn: sqlite3.Connection) -> list[int]:
    "Retrieves a sorted list of unique years present in the database."
    rows = conn.execute("SELECT DISTINCT year FROM dedup_rows ORDER BY year;").fetchall()
    return [r[0] for r in rows]


def write_scope_file(conn: sqlite3.Connection, years: list[int], out_path: Path) -> int:
    "Writes sorted data rows for specified years to an output file."
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
    "Generates individual cleaned CSV files for each year."
    written: dict[int, int] = {}
    for year in years:
        out_path = out_dir / f"stw_{year}_cleaned.csv"
        count = write_scope_file(conn, [year], out_path)
        written[year] = count
    return written


def select_ten_year_window(years: list[int], year_counts: dict[int, int]) -> list[int]:
    "Selects the first 10 years of available data for processing."
    del year_counts
    if len(years) <= 10:
        return years

    sorted_years = sorted(years)
    return sorted_years[:10]


def log_missing_timestamps(
    conn: sqlite3.Connection,
    years: list[int],
    log_path: Path,
) -> MissingSummary:
    "Identifies and logs missing timestamps for specified years."
    placeholders = ",".join("?" for _ in years)
    query = (
        "SELECT year, day, time FROM dedup_rows "
        f"WHERE year IN ({placeholders}) "
        "ORDER BY year, day, time;"
    )

    summary = MissingSummary()
    current_key: tuple[int, int] | None = None
    times_seen: set[int] = set()
    out_of_range = 0
    first_valid_key: tuple[int, int] | None = None
    first_valid_time: int | None = None

    with log_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "year",
                "day_of_year",
                "missing_hhmm_ranges",
            ]
        )

        def flush_group() -> None:
            nonlocal current_key, times_seen, out_of_range
            if current_key is None:
                return
            year, day = current_key
            missing = [t for t in VALID_HHMM_TIMES if t not in times_seen]
            if current_key == first_valid_key and first_valid_time is not None:
                missing = [t for t in missing if t >= first_valid_time]
            summary.days_total += 1
            summary.missing_timestamps_total += len(missing)
            summary.out_of_range_timestamps += out_of_range
            if missing:
                summary.days_with_missing += 1
                writer.writerow(
                    [
                        year,
                        day,
                        compress_hhmm_ranges(missing),
                    ]
                )
            times_seen = set()
            out_of_range = 0

        for year, day, time in conn.execute(query, years):
            key = (year, day)
            if current_key != key:
                flush_group()
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


def write_summary_report(
    out_path: Path,
    ingest: IngestStats,
    unique_rows: int,
    years: list[int],
    year_counts: dict[int, int],
    ten_years: list[int],
    ten_year_rows: int,
    ten_year_missing: MissingSummary,
) -> None:
    "Generates a comprehensive summary report of the data processing."
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
            f"10-year selection (starting at first year): {', '.join(map(str, ten_years))}",
            f"10-year rows written: {ten_year_rows}",
            f"10-year day groups checked: {ten_year_missing.days_total}",
            f"10-year days with missing timestamps: {ten_year_missing.days_with_missing}",
            f"10-year missing timestamp total: {ten_year_missing.missing_timestamps_total}",
            f"10-year out-of-range timestamps: {ten_year_missing.out_of_range_timestamps}",
        ]
    )
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def recheck_ten_year_file(ten_year_file: Path, missing_combined_dir: Path) -> tuple[int, int, MissingSummary]:
    "Verifies the integrity and completeness of the 10-year combined file."
    conn = init_db(RECHECK_DB)
    stats = ingest_files(conn, [ten_year_file], drop_first_col=False)
    unique_rows = conn.execute("SELECT COUNT(*) FROM dedup_rows;").fetchone()[0]
    years = get_years(conn)
    missing = log_missing_timestamps(
        conn,
        years,
        missing_combined_dir / "missing_timestamps_10year_recheck.csv",
    )
    conn.close()
    if RECHECK_DB.exists():
        RECHECK_DB.unlink()
    return stats.valid_rows, unique_rows, missing


def main() -> None:
    "Orchestrates the entire data cleaning and consolidation workflow."
    OUT_DIR.mkdir(exist_ok=True)
    YEARLY_CLEANED_DIR.mkdir(parents=True, exist_ok=True)
    COMBINED_DIR.mkdir(parents=True, exist_ok=True)
    MISSING_YEARLY_DIR.mkdir(parents=True, exist_ok=True)
    MISSING_COMBINED_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    input_files = discover_input_files(BASE_DIR)

    conn = init_db(WORK_DB)
    ingest_stats = ingest_files(conn, input_files, drop_first_col=True)

    unique_rows = conn.execute("SELECT COUNT(*) FROM dedup_rows;").fetchone()[0]
    years = get_years(conn)
    if not years:
        raise RuntimeError("No valid data rows found.")

    year_counts = write_year_files(conn, years, YEARLY_CLEANED_DIR)
    for year in years:
        log_missing_timestamps(
            conn,
            [year],
            MISSING_YEARLY_DIR / f"missing_timestamps_{year}.csv",
        )

    ten_years = select_ten_year_window(years, year_counts)
    ten_year_file = COMBINED_DIR / "stw_10_years_cleaned.csv"
    ten_year_rows = write_scope_file(conn, ten_years, ten_year_file)
    ten_year_missing = log_missing_timestamps(
        conn,
        ten_years,
        MISSING_COMBINED_DIR / "missing_timestamps_10year.csv",
    )

    write_summary_report(
        REPORTS_DIR / "processing_summary.txt",
        ingest=ingest_stats,
        unique_rows=unique_rows,
        years=years,
        year_counts=year_counts,
        ten_years=ten_years,
        ten_year_rows=ten_year_rows,
        ten_year_missing=ten_year_missing,
    )

    checked_rows, unique_checked_rows, checked_missing = recheck_ten_year_file(
        ten_year_file,
        MISSING_COMBINED_DIR,
    )
    duplicates_after_recheck = checked_rows - unique_checked_rows
    recheck_report = [
        f"10-year file: {ten_year_file.name}",
        f"Rows read: {checked_rows}",
        f"Unique rows: {unique_checked_rows}",
        f"Duplicates found on recheck: {duplicates_after_recheck}",
        f"Day groups checked: {checked_missing.days_total}",
        f"Days with missing timestamps: {checked_missing.days_with_missing}",
        f"Missing timestamps total: {checked_missing.missing_timestamps_total}",
        f"Out-of-range timestamps: {checked_missing.out_of_range_timestamps}",
    ]
    (REPORTS_DIR / "ten_year_recheck_report.txt").write_text("\n".join(recheck_report) + "\n", encoding="utf-8")

    conn.close()
    if WORK_DB.exists():
        WORK_DB.unlink()


if __name__ == "__main__":
    main()
