# LEI SRML Intern Project

Data cleaning pipeline for the Solar Radiation Monitoring Lab (SRML).

## Overview

This repo processes minute-resolution STW solar radiation data. The pipeline:

1. Scans `Data/Raw Data/` for STW source files.
2. Removes duplicate timestamps, keeping the first row seen for each `year/day_of_year/hhmm`.
3. Writes cleaned yearly CSVs.
4. Detects missing timestamps within the observed data window.
5. Builds a combined CSV and inserts `NaN` placeholder rows only for interior gaps.
6. Writes summary and recheck reports.

## Repo Layout

```text
LEI-SRML-Intern-Project/
|-- README.md
|-- .gitignore
|-- Data/
|   |-- clean_stw_data.py
|   |-- scripts/
|   |   |-- apply_stw_column_map.py
|   |   `-- find_stw_column_count_changes.py
|   |-- Raw Data/
|   `-- cleaned_output/
|       |-- yearly_cleaned/
|       |-- combined/
|       |-- missing_logs/
|       |   `-- combined/
|       `-- reports/
`-- STW programs/
    |-- column_map.csv
    `-- Other/
```

## Key Files

- `Data/clean_stw_data.py`: main cleaning pipeline.
- `Data/scripts/apply_stw_column_map.py`: applies `STW programs/column_map.csv` to the combined cleaned file and writes a stable metric-per-column CSV.
- `Data/scripts/find_stw_column_count_changes.py`: reports rows where the combined cleaned file changes column count.
- `STW programs/column_map.csv`: effective-dated mapping of raw measurement positions to metric names.

## Usage

```bash
python3 Data/clean_stw_data.py
python3 Data/scripts/apply_stw_column_map.py
python3 Data/scripts/find_stw_column_count_changes.py
```

All generated outputs are written under `Data/cleaned_output/`.

## Requirements

- Python 3.10+
- Standard library only
