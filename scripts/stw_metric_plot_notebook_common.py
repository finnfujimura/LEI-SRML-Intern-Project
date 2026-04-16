#!/usr/bin/env python3
"""Shared helpers for metric plot notebooks."""

from __future__ import annotations

from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
from bokeh.models import ColumnDataSource
from bokeh.plotting import figure, save
from bokeh.resources import CDN


def load_metric(root_dir: Path, metric: str, parquet_path: Path | None = None) -> pd.DataFrame:
    """Load one irradiance metric from the parquet export."""
    irr_column = f"{metric}_Irr"
    source_path = parquet_path or root_dir / "final output" / "stw_mV_Irr.parquet"
    metric_df = pd.read_parquet(source_path, columns=["datetime", irr_column]).copy()
    metric_df["datetime"] = pd.to_datetime(metric_df["datetime"], errors="coerce")
    metric_df[irr_column] = pd.to_numeric(metric_df[irr_column], errors="coerce")
    metric_df = metric_df.dropna(subset=["datetime"]).sort_values("datetime")
    if metric_df.empty:
        raise ValueError(f"No rows found for {metric} in {source_path}.")
    return metric_df


def build_start_years(metric_df: pd.DataFrame, years: int) -> list[int]:
    """Return valid start years for the selected window size."""
    available_years = sorted(metric_df["datetime"].dt.year.dropna().astype(int).unique())
    available_year_set = set(available_years)
    if years == 1:
        return available_years
    return [year for year in available_years if year + years - 1 in available_year_set]


def slice_year_window(metric_df: pd.DataFrame, metric: str, start_year: int, years: int) -> tuple[pd.DataFrame, str]:
    """Slice one calendar-year window and return it with a filename suffix."""
    irr_column = f"{metric}_Irr"
    start = pd.Timestamp(year=start_year, month=1, day=1)
    end = pd.Timestamp(year=start_year + years, month=1, day=1)
    window = metric_df.loc[
        (metric_df["datetime"] >= start) & (metric_df["datetime"] < end),
        ["datetime", irr_column],
    ].copy()
    if window.empty:
        raise ValueError(f"No rows found for {metric} between {start.date()} and {end.date()}.")

    end_year = start_year if years == 1 else start_year + years - 1
    suffix = f"{start_year}" if years == 1 else f"{start_year}_{end_year}"
    return window, suffix


def export_static_windows(
    metric_df: pd.DataFrame,
    metric: str,
    color: str,
    output_dir: Path,
    start_years: list[int],
    years: int = 1,
    dpi: int = 150,
) -> list[dict[str, object]]:
    """Export static PNG plots for a list of year windows."""
    irr_column = f"{metric}_Irr"
    output_dir.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, object]] = []

    for start_year in start_years:
        window, suffix = slice_year_window(metric_df, metric, start_year, years)
        output_path = output_dir / f"{metric.lower()}_irr_{suffix}.png"

        fig, ax = plt.subplots(figsize=(14, 6))
        ax.plot(window["datetime"], window[irr_column], color=color, linewidth=0.6)
        ax.set_title(f"{metric} IRR ({suffix.replace('_', '-')})")
        ax.set_xlabel("Datetime")
        ax.set_ylabel("IRR")
        ax.grid(alpha=0.25, linewidth=0.6)
        ax.margins(x=0)
        locator = mdates.AutoDateLocator()
        ax.xaxis.set_major_locator(locator)
        ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(locator))
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        fig.tight_layout()
        fig.savefig(output_path, dpi=dpi, bbox_inches="tight")
        plt.close(fig)

        results.append(
            {
                "start_year": start_year,
                "rows": len(window),
                "output_path": output_path,
                "kind": "png",
            }
        )

    return results


def maybe_resample_window(window: pd.DataFrame, metric: str, downsample_rule: str | None) -> pd.DataFrame:
    """Optionally resample the interactive plot to make HTML lighter."""
    if not downsample_rule:
        return window

    irr_column = f"{metric}_Irr"
    return (
        window.set_index("datetime")[[irr_column]]
        .resample(downsample_rule)
        .mean()
        .reset_index()
    )


def export_interactive_windows(
    metric_df: pd.DataFrame,
    metric: str,
    color: str,
    output_dir: Path,
    selected_years: list[int],
    years: int = 1,
    downsample_rule: str | None = None,
) -> list[dict[str, object]]:
    """Export standalone zoomable HTML plots for selected year windows."""
    irr_column = f"{metric}_Irr"
    html_dir = output_dir / "interactive"
    html_dir.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, object]] = []

    for start_year in selected_years:
        window, suffix = slice_year_window(metric_df, metric, start_year, years)
        plot_df = maybe_resample_window(window, metric, downsample_rule)
        output_path = html_dir / f"{metric.lower()}_irr_{suffix}.html"
        title = f"{metric} IRR ({suffix.replace('_', '-')})"

        source = ColumnDataSource(plot_df)
        plot = figure(
            title=title,
            x_axis_type="datetime",
            width=1400,
            height=500,
            tools="pan,wheel_zoom,box_zoom,reset,save",
            active_scroll="wheel_zoom",
            output_backend="webgl",
        )
        plot.line("datetime", irr_column, source=source, line_width=1, line_color=color)
        plot.xaxis.axis_label = "Datetime"
        plot.yaxis.axis_label = "IRR"
        plot.grid.grid_line_alpha = 0.25
        plot.toolbar.logo = None

        save(plot, filename=str(output_path), title=title, resources=CDN)
        results.append(
            {
                "start_year": start_year,
                "rows": len(plot_df),
                "source_rows": len(window),
                "output_path": output_path,
                "kind": "html",
                "downsample_rule": downsample_rule,
            }
        )

    return results
