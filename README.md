# LEI SRML Intern Project

**Data Cleaning Pipeline for the Solar Radiation Monitoring Lab (SRML)**
*Part of the Leadership Enrichment Internship (LEI)*

## Overview

The UO Solar Radiation Monitoring Lab (SRML) collects minute-resolution solar
radiation measurements from station STW. Raw data arrives as monthly text files
with one row per minute of observation. This project takes those raw files,
removes duplicate readings, identifies gaps where data is missing, and produces
clean yearly and multi-year CSV files ready for analysis.

## What the Pipeline Does

Running `clean_stw_data.py` performs an 8-step process:

1. **Discover source files** -- Recursively scans `Data/Raw Data/` for all
   files inside `STW_*` folders.
2. **Load and deduplicate** -- Reads every row into a SQLite database. The first
   column (station ID) is stripped; rows that are identical from column 2 onward
   are kept only once.
3. **Write yearly cleaned files** -- Exports one sorted CSV per year to
   `cleaned_output/yearly_cleaned/`.
4. **Log missing timestamps** -- For each year, compares observed timestamps
   against the full set of 1,440 valid minute-of-day values and writes a log of
   any gaps.
5. **Build combined file** -- Merges all years into a single CSV.
   Missing-timestamp placeholder rows (filled with `NaN`) are inserted so
   the file has a row for every expected minute.
6. **Write summary report** -- Saves counts of files, rows, duplicates, and
   missing data to `reports/processing_summary.txt`.
7. **Recheck the combined file** -- Re-ingests the combined file to verify that
   no duplicates or missing timestamps remain.
8. **Clean up** -- Removes the temporary SQLite databases.

## Data Format

### Raw input (one row per minute)

```
station_id, year, day_of_year, hhmm, measurement_1, measurement_2, ...
506,        2020, 1,           1,    -6.043,         0,             11.62, 13.32
```

| Column | Meaning |
|--------|---------|
| 1 | Station ID (e.g. `506`) -- dropped during cleaning |
| 2 | Four-digit year |
| 3 | Day of year (1--366) |
| 4 | Time in HHMM format (see below) |
| 5+ | Sensor measurements (vary by year/station) |

### HHMM time format

Times are encoded as integers where the hundreds digit is the hour and the
tens/units digit is the minute: `0001` is 00:01 AM, `1230` is 12:30 PM, `2359`
is 11:59 PM, and `2400` represents midnight at the end of the day. This gives
1,440 valid timestamps per day (one per minute).

### Cleaned output

Same format as raw input but without the station ID column, sorted by
year/day/time, with duplicates removed. Missing-timestamp rows contain the
year, day, and time followed by `NaN` for every measurement column.

## Project Structure

```
LEI/
├── README.md
├── .gitignore
├── Data/
│   ├── clean_stw_data.py          # Main cleaning script (run this)
│   ├── Raw Data/                   # Source files (one folder per year)
│   │   ├── STW_2015_Process0/
│   │   ├── STW_2016_Process0/
│   │   ├── ...
│   │   └── STW_2025/
│   └── cleaned_output/             # All generated output
│       ├── yearly_cleaned/         # One CSV per year (e.g. stw_2020_cleaned.csv)
│       ├── combined/               # All-years merged CSV
│       ├── missing_logs/
│       │   ├── yearly/             # Missing-timestamp log per year
│       │   └── combined/           # Missing-timestamp log for the 10-year file
│       └── reports/                # processing_summary.txt, recheck report
└── STW programs/                   # Related STW programs (not part of pipeline)
```

## Usage

```bash
python3 Data/clean_stw_data.py
```

The script prints progress to the terminal as it runs. All output is written
under `Data/cleaned_output/`. Running it again will overwrite previous output.

## Requirements

- Python 3.10+ (uses `X | Y` union type syntax)
- No external dependencies -- only standard library modules (`csv`, `sqlite3`,
  `pathlib`, `dataclasses`).
