# LEI SRML Intern Project

Data cleaning and calibration pipeline for SRML Data.

## Dependencies

Base pipeline:

```bash
python -m pip install pandas pyarrow
```

Notebook plotting stack:

```bash
python -m pip install holoviews hvplot notebook bokeh
```

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

Launch the full-history notebook explorer:

```bash
python -m notebook plots/stw_mV_Irr_explorer.ipynb
```

## Outputs

- `final output/stw_mV_Irr.csv`: final calibrated output.
- `final output/stw_mV_Irr.parquet`: Parquet copy for interactive notebook exploration.
- `yearly cleaned/`: yearly cleaned CSVs.
- `reports/`: missing-timestamp logs, processing summary, recheck report, mapped intermediate CSVs, outlier report, and pipeline state.
- `plots/stw_mV_Irr_explorer.ipynb`: Datashader notebook for full-history interactive plots.
