# LEI SRML Intern Project

Data cleaning and calibration pipeline for SRML Data.

## Usage

Run the full pipeline:

```bash
python run_stw_pipeline.py
```

Run individual steps for debugging:

```bash
python scripts/01_ingest_raw_to_yearly_cleaned.py
python scripts/02_fill_yearly_gaps.py
python scripts/03_build_combined_cleaned.py
python scripts/04_write_reports_and_recheck.py
python scripts/05_map_combined_columns.py
python scripts/06_convert_mapped_to_mv_irr.py
```

## Outputs

- `final output/stw_mV_Irr.csv`: final calibrated output.
- `yearly cleaned/`: yearly cleaned CSVs.
- `reports/`: missing-timestamp logs, processing summary, recheck report, mapped intermediate CSVs, outlier report, and pipeline state.
- `plots/`: interactive metric plots.
