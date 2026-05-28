# LEI SRML Intern Project — STW Data Pipeline

Data cleaning and calibration pipeline for the University of Oregon Solar
Radiation Monitoring Lab (SRML) **STW** station.

It ingests raw minute-resolution logger files spanning many years, removes
duplicates and fills timestamp gaps, applies the lab's column mapping and
effective-dated calibration constants, and produces a single calibrated
dataset (`*_mV` and `*_Irr` columns) plus reports, outlier logs, and plots.

---

## 1. Quick start

```bash
# Install everything (core pipeline + plotting/notebooks)
python -m pip install -r requirements.txt

# Run the whole thing
python run_stw_pipeline.py
```

The core pipeline (steps 01–07) only needs `pandas`, `pyarrow`, and `numpy`;
the rest of `requirements.txt` is for the plotting tools and notebooks.

That's it for the core pipeline. Everything reads inputs from and writes
outputs to the directories named in `pipeline_config.json` (see
[Configuration](#4-configuration)).

> The Excel workbook is parsed directly from its XML (via `zipfile` +
> `xml.etree`), so **`openpyxl` is not required**.

---

## 2. Repository layout

```
LEI-SRML-Intern-Project/
├── run_stw_pipeline.py          # Orchestrator: runs steps 01–07 in order
├── pipeline_config.json         # Deployment config (paths, filenames) — see §4
│
├── scripts/
│   ├── 01_..._07_*.py           # Thin step wrappers (3 lines each)
│   ├── pipeline_config.py       # Config loader (env var -> json -> defaults)
│   ├── stw_pipeline_common.py   # Heavy lifting for steps 01–05
│   ├── convert_stw_mapped_to_mv.py        # Step 06: calibration -> mV/Irr
│   ├── stw_irr_irregularity_events.py     # Step 07: irregularity detection
│   ├── export_yearly_metric_plots.py      # Static PNG exporter
│   └── stw_metric_plot_notebook_common.py # Shared plotting helpers
│
├── Raw Data/                    # INPUT: raw logger files in STW_<year>* folders
├── STW programs/
│   ├── STW_sitefile_and_mapping.xlsx      # Column map + calibration constants
│   └── STW/, Other/             # Reference: Campbell logger programs (not used by code)
│
├── yearly cleaned/              # OUTPUT (gitignored): one cleaned CSV per year
├── reports/                     # OUTPUT (gitignored): logs, summaries, intermediates
├── final output/                # OUTPUT (gitignored): final calibrated CSV + Parquet
└── plots/                       # Notebooks + generated PNGs (PNG dirs gitignored)
```

The numbered `scripts/0X_*.py` files are deliberately tiny — each just imports
and calls one function from the heavy modules. Edit logic in the heavy modules
(`stw_pipeline_common.py`, `convert_stw_mapped_to_mv.py`,
`stw_irr_irregularity_events.py`), not in the wrappers.

### A note on repo size

`Raw Data/` (~520 MB of `.dat` logger files) is **committed to git** so the
pipeline can be run end-to-end on a fresh clone. The generated directories
(`yearly cleaned/`, `reports/`, `final output/`, the `plots/*_plots/` PNG
folders) are gitignored and recreated on every run.

---

## 3. How the pipeline works

The orchestrator runs seven steps in order. Each step reads the previous
step's output, so run them in sequence (or just use `run_stw_pipeline.py`).

| Step | Script | Reads | Writes |
|------|--------|-------|--------|
| 01 | `01_ingest_raw_to_yearly_cleaned.py` | `Raw Data/STW_*/` | `yearly cleaned/stw_<year>_cleaned.csv`, `reports/pipeline_state.json` |
| 02 | `02_fill_yearly_gaps.py` | yearly CSVs | yearly CSVs (gaps filled with NaN rows) |
| 03 | `03_build_combined_cleaned.py` | yearly CSVs | `reports/stw_combined_cleaned.csv` |
| 04 | `04_write_reports_and_recheck.py` | combined CSV | `reports/processing_summary.txt`, `reports/combined_recheck_report.txt`, missing-timestamp logs |
| 05 | `05_map_combined_columns.py` | combined CSV + workbook | `reports/stw_combined_mapped.csv` |
| 06 | `06_convert_mapped_to_mv_irr.py` | mapped CSV + workbook | `final output/stw_mV_Irr.{csv,parquet}`, calibration + outlier logs, explorer notebook |
| 07 | `07_detect_irr_irregularity_events.py` | final Parquet | `reports/stw_irr_irregularity_events.csv` |

Run an individual step for debugging:

```bash
python scripts/03_build_combined_cleaned.py
```

### What each step does

1. **Ingest** — Discovers every file inside folders matching the configured
   prefix (`STW_` by default) under `Raw Data/`. Parses rows of the form
   `station_id, year, day_of_year, hhmm, m1, m2, …`, drops the station-ID
   column, keeps the **first** row seen per `(year, day, hhmm)` timestamp, and
   writes one sorted CSV per year. Rows after the cutoff date are skipped
   (see [Key concepts](#5-key-concepts--conventions)).
2. **Fill yearly gaps** — For each year, compares observed timestamps against
   the full 1,440 valid minute-of-day values per day and inserts placeholder
   (NaN) rows for any missing timestamps.
3. **Build combined** — Concatenates the yearly files into one gap-free
   `stw_combined_cleaned.csv` covering all years.
4. **Reports & recheck** — Writes a human-readable processing summary, then
   re-ingests the combined file to verify zero duplicates and zero missing
   timestamps remain.
5. **Map columns** — Renames/selects columns using the `Column Mapping`
   worksheet in `STW_sitefile_and_mapping.xlsx`, producing
   `stw_combined_mapped.csv`. The `PIR` metric is excluded.
6. **Convert to mV / Irr** — The calibration step (see below). Reads
   effective-dated `M_program` / `M_should_be` constants from per-metric
   worksheets and produces paired `<METRIC>_mV` and `<METRIC>_Irr` columns,
   cleans outliers, and writes the final CSV + Parquet.
7. **Detect irregularities** — Scans the final irradiance series for large,
   sustained windows that suggest a bad column map or structural break.

### The calibration / mapping workbook

`STW programs/STW_sitefile_and_mapping.xlsx` drives steps 05 and 06:

- The **`Column Mapping`** sheet maps raw column positions to metric names.
- Each **per-metric sheet** holds effective-dated calibration constants
  (`M_program`, the value the logger used; `M_should_be`, the corrected value).
  When these change over time, the converter applies the right constant for
  each timestamp and logs every change to
  `reports/stw_mV_Irr_calibration_changes.txt`.
- `TEMP`, `SZA`, `AZM` are **pass-through** metrics (copied, not converted).
  `PIR` is excluded entirely.

---

## 4. Configuration

All deployment-specific paths and filenames flow through
`scripts/pipeline_config.py`, so the pipeline can be relocated or re-pointed
**without editing code**. Resolution order:

1. `STW_CONFIG` environment variable — path to a JSON config file. Errors if
   the path doesn't exist.
2. `pipeline_config.json` — searched upward from `scripts/`. **This is the
   default** and what ships in the repo root.
3. In-code defaults — used if no config file is found (reproduces the original
   hard-coded behavior).

Semantics:

- Missing keys fall back to defaults, so an empty `{}` behaves like no config.
- **Unknown keys are rejected** to catch typos early.
- Relative paths resolve against the directory holding the JSON file (the repo
  root for the shipped config).

To run against a different data location, copy `pipeline_config.json`, edit the
paths, and point at it:

```bash
STW_CONFIG=/path/to/my_config.json python run_stw_pipeline.py
```

### Cutoff date

`pipeline_cutoff_date` (default `"2024-08-31"`, format `YYYY-MM-DD`) sets the
last observation date the pipeline keeps; rows observed after it are skipped at
ingest. Edit it in `pipeline_config.json` to extend or shrink the range — an
invalid date errors loudly at startup.

### `force_nan` — manually blanking bad windows

`pipeline_config.json` accepts an optional `force_nan` list. Each rule nulls
specific columns over an inclusive datetime window during step 06 — useful for
known-bad periods that the automatic outlier cleaning doesn't catch:

```json
"force_nan": [
  {
    "columns": ["GHI", "DNI"],
    "start": "2020-08-01",
    "end": "2020-08-11",
    "reason": "known mapping error"
  }
]
```

A date-only `end` covers the full day. Referenced columns must exist in the
mapped CSV and not be excluded, or the run errors loudly. The default config
ships with an empty list.

---

## 5. Key concepts & conventions

- **HHMM format** — Integer where HH = hours (00–23), MM = minutes (00–59).
  Valid values run 0001–2359 plus the special `2400` (end-of-day midnight),
  giving 1,440 timestamps per day.
- **Cutoff date** — The pipeline scopes the dataset to **2024-08-31 and
  earlier**; later rows are skipped at ingest. This is configurable via
  `pipeline_cutoff_date` in `pipeline_config.json` (see
  [Configuration](#4-configuration)) — change it to extend or shrink the range.
- **Deduplication** — Key is `(year, day, hhmm)`. First row seen wins.
- **Boundary grace** — On the first/last day of a scope, timestamps before the
  first / after the last observed reading are not counted as missing.
- **Outlier cleaning (step 06)** — Converted `*_mV` / `*_Irr` outliers are
  rewritten to `NaN` using a rolling robust-z test (window 241, z-threshold 8).
  `TEMP` is also nulled outside −40 to 60 °C, and irradiance outside −100 to
  10000 is dropped. Plots keep the timestamps but show gaps instead of spikes.
  Every change is recorded in `reports/stw_mV_Irr_outliers.csv`.

---

## 6. Outputs reference

**`final output/`**
- `stw_mV_Irr.csv` — final calibrated dataset.
- `stw_mV_Irr.parquet` — Parquet copy used by the plotting tools.

**`reports/`**
- `processing_summary.txt` — counts of files/rows/duplicates/gaps per run.
- `combined_recheck_report.txt` — verification that the combined file is clean.
- `stw_combined_cleaned.csv` — gap-free combined data (step 03).
- `stw_combined_mapped.csv` — column-mapped intermediate (step 05).
- `stw_mV_Irr_calibration_changes.txt` — log of every calibration-constant change.
- `stw_mV_Irr_outliers.csv` — every cell nulled by outlier cleaning.
- `stw_irr_irregularity_events.csv` — detected irregularity windows (step 07).
- `pipeline_state.json` — discovered years, passed between steps.
- `missing_timestamps_combined*.csv` — gap logs (empty when the data is clean).

**`yearly cleaned/`** — `stw_<year>_cleaned.csv`, one per year.

---

## 7. Plotting

The plotting tools all read `final output/stw_mV_Irr.parquet`, so run the
pipeline first.

**Static yearly PNGs** (no notebook needed):

```bash
python scripts/export_yearly_metric_plots.py
# Options:
python scripts/export_yearly_metric_plots.py --metrics GHI TEMP --years 2020 2021
```

Writes PNGs into `plots/ghi_plots/`, `plots/dni_plots/`, `plots/dhi_plots/`,
and `plots/temp_plots/`. Metrics: `GHI DNI DHI TEMP`.

**Interactive notebooks** (`plots/`):

```bash
python -m notebook plots/combined_metric_plots.ipynb   # pick metrics/years, one combined plot
python -m notebook plots/stw_mV_Irr_explorer.ipynb     # Datashader full-history explorer
python -m notebook plots/irr_irregularity_events.ipynb # contextual plots for detected events
```

**Irregularity detector modes** — Step 07 defaults to the `mapping_window`
detector (large, sustained windows that flag bad-mapping periods / structural
breaks). The older minute-spike detector is still available:

```bash
python scripts/07_detect_irr_irregularity_events.py --mode first_difference
```

---

## 8. Notes for the next maintainer

- **Edit logic in the heavy modules**, not the numbered wrappers in `scripts/`.
- The cutoff date is configurable (`pipeline_cutoff_date` in
  `pipeline_config.json`), but the **outlier thresholds are still constants in
  code** — search for `OUTLIER_WINDOW`, `OUTLIER_Z_THRESHOLD`,
  `IRR_VALID_MIN/MAX`, and `PASS_THROUGH_VALUE_BOUNDS` if you need to change them.
- `STW programs/STW/` and `STW programs/Other/` are **reference copies of the
  Campbell datalogger programs** (`.CR6`, `.CSI`, `.tdf`, …). No pipeline code
  reads them; they're kept for context on how the raw data was produced.
- The full run is heavy (millions of rows; the final CSV is ~500 MB). Expect it
  to take a few minutes and a few GB of disk for the generated outputs.
