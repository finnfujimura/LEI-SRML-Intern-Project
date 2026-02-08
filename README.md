# LEI SRML Intern Project

**Data Cleaning Pipeline for the Solar Radiation Monitoring Lab (SRML)**
*Part of the Leadership Enrichment Internship (LEI)*

## Overview
This project processes, cleans, and consolidates STW data files. The pipeline handles missing timestamps, removes duplicates, and generates yearly and multi-year datasets for analysis.

## Project Structure

### `Data/`
The main data processing directory.

- **`clean_stw_data.py`**: The core Python script that:
  - Reads raw CSV files from `Raw Data/`.
  - Cleans data (removes duplicates, validates timestamps).
  - Identifies missing time intervals.
  - Outputs cleaned files to `cleaned_output/`.

- **`Raw Data/`**: Contains the source CSV files organized by year (e.g., `STW_2015_Process0`, `STW_2025`).

- **`cleaned_output/`**: Generated output directory containing:
  - **`combined/`**: Consolidated 10-year dataset (e.g., `stw_10_years_cleaned.csv`).
  - **`yearly_cleaned/`**: Individual cleaned CSV files for each year.
  - **`missing_logs/`**: detailed logs of missing timestamps found during processing.
  - **`reports/`**: Summary reports (`processing_summary.txt`) and recheck logs.

## Usage
To run the cleaning pipeline:

```bash
python3 Data/clean_stw_data.py
```

## Requirements
- Python 3.x
- No external dependencies (uses standard library `csv`, `sqlite3`, `pathlib`).
