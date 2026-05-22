#!/usr/bin/env python3
"""Export one static yearly PNG per metric per available year."""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
from pathlib import Path

CACHE_DIR = Path(tempfile.gettempdir()) / "stw_plot_cache"
os.environ.setdefault("MPLCONFIGDIR", str(CACHE_DIR / "matplotlib"))
os.environ.setdefault("XDG_CACHE_HOME", str(CACHE_DIR / "xdg"))

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT_DIR / "scripts"))

from stw_metric_plot_notebook_common import (  # noqa: E402
    METRIC_CONFIGS,
    build_start_years,
    export_static_windows,
    load_metric,
    normalize_metrics,
)


OUTPUT_DIRS = {
    "GHI": ROOT_DIR / "plots" / "ghi_plots",
    "DNI": ROOT_DIR / "plots" / "dni_plots",
    "DHI": ROOT_DIR / "plots" / "dhi_plots",
    "TEMP": ROOT_DIR / "plots" / "temp_plots",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export static yearly PNG plots for selected STW metrics.",
    )
    parser.add_argument(
        "--metrics",
        nargs="+",
        default=list(METRIC_CONFIGS),
        help="Metrics to export. Choose from GHI DNI DHI TEMP. Defaults to all four.",
    )
    parser.add_argument(
        "--years",
        nargs="+",
        type=int,
        default=None,
        help="Calendar years to export. Defaults to every available year for each metric.",
    )
    parser.add_argument(
        "--parquet-path",
        type=Path,
        default=ROOT_DIR / "final output" / "stw_mV_Irr.parquet",
        help="Source parquet file. Defaults to final output/stw_mV_Irr.parquet.",
    )
    parser.add_argument(
        "--dpi",
        type=int,
        default=150,
        help="PNG export DPI. Defaults to 150.",
    )
    return parser.parse_args()


def export_metric(metric: str, years: list[int] | None, parquet_path: Path, dpi: int) -> list[dict[str, object]]:
    config = METRIC_CONFIGS[metric]
    metric_df = load_metric(
        ROOT_DIR,
        metric,
        parquet_path=parquet_path,
        value_column=config["column"],
    )
    available_start_years = build_start_years(metric_df, years=1)
    selected_years = available_start_years if years is None else [year for year in years if year in available_start_years]
    missing_years = [] if years is None else sorted(set(years) - set(selected_years))
    if missing_years:
        print(f"{metric}: skipping unavailable years {missing_years}")

    return export_static_windows(
        metric_df,
        metric,
        config["color"],
        OUTPUT_DIRS[metric],
        selected_years,
        years=1,
        dpi=dpi,
        value_column=config["column"],
        plot_title=config["label"],
    )


def main() -> None:
    args = parse_args()
    metrics = normalize_metrics(args.metrics)
    parquet_path = args.parquet_path.expanduser().resolve()

    all_results: list[dict[str, object]] = []
    for metric in metrics:
        results = export_metric(metric, args.years, parquet_path, args.dpi)
        all_results.extend(results)
        print(f"{metric}: exported {len(results)} plots to {OUTPUT_DIRS[metric]}")

    print(f"Done: exported {len(all_results)} static plots.")


if __name__ == "__main__":
    main()
