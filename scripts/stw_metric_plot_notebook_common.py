#!/usr/bin/env python3
"""Shared helpers for yearly metric-plot notebooks."""

from __future__ import annotations

from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
from bokeh.io import output_notebook, show
from bokeh.models import ColumnDataSource, HoverTool
from bokeh.plotting import figure, save
from bokeh.resources import CDN


def resolve_value_column(metric: str, value_column: str | None = None) -> str:
    """Resolve the parquet column name used for plotting."""
    return value_column or f"{metric}_Irr"


def default_plot_title(metric: str, value_column: str | None = None) -> str:
    """Return the default plot title prefix for the selected series."""
    column_name = resolve_value_column(metric, value_column)
    if column_name == f"{metric}_Irr":
        return f"{metric} IRR"
    return column_name


def default_y_axis_label(metric: str, value_column: str | None = None) -> str:
    """Return the default y-axis label for the selected series."""
    column_name = resolve_value_column(metric, value_column)
    if column_name == f"{metric}_Irr":
        return "IRR"
    return column_name


def default_output_stem(metric: str, value_column: str | None = None) -> str:
    """Return the default filename stem for exported plots."""
    column_name = resolve_value_column(metric, value_column)
    if column_name == f"{metric}_Irr":
        return f"{metric.lower()}_irr"
    return column_name.lower()


def load_metric(
    root_dir: Path,
    metric: str,
    parquet_path: Path | None = None,
    value_column: str | None = None,
) -> pd.DataFrame:
    """Load one metric column from the parquet export."""
    column_name = resolve_value_column(metric, value_column)
    source_path = parquet_path or root_dir / "final output" / "stw_mV_Irr.parquet"
    metric_df = pd.read_parquet(source_path, columns=["datetime", column_name]).copy()
    metric_df["datetime"] = pd.to_datetime(metric_df["datetime"], errors="coerce")
    metric_df[column_name] = pd.to_numeric(metric_df[column_name], errors="coerce")
    metric_df = metric_df.dropna(subset=["datetime"]).sort_values("datetime")
    if metric_df.empty:
        raise ValueError(f"No rows found for {column_name} in {source_path}.")
    return metric_df


def build_start_years(metric_df: pd.DataFrame, years: int) -> list[int]:
    """Return valid start years for the selected window size."""
    available_years = sorted(metric_df["datetime"].dt.year.dropna().astype(int).unique())
    available_year_set = set(available_years)
    if years == 1:
        return available_years
    return [year for year in available_years if year + years - 1 in available_year_set]


def slice_year_window(
    metric_df: pd.DataFrame,
    metric: str,
    start_year: int,
    years: int,
    value_column: str | None = None,
) -> tuple[pd.DataFrame, str]:
    """Slice one calendar-year window and return it with a filename suffix."""
    column_name = resolve_value_column(metric, value_column)
    start = pd.Timestamp(year=start_year, month=1, day=1)
    end = pd.Timestamp(year=start_year + years, month=1, day=1)
    window = metric_df.loc[
        (metric_df["datetime"] >= start) & (metric_df["datetime"] < end),
        ["datetime", column_name],
    ].copy()
    if window.empty:
        raise ValueError(f"No rows found for {column_name} between {start.date()} and {end.date()}.")

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
    value_column: str | None = None,
    plot_title: str | None = None,
    y_axis_label: str | None = None,
    output_stem: str | None = None,
) -> list[dict[str, object]]:
    """Export static PNG plots for a list of year windows."""
    column_name = resolve_value_column(metric, value_column)
    title_prefix = plot_title or default_plot_title(metric, value_column)
    y_label = y_axis_label or default_y_axis_label(metric, value_column)
    file_stem = output_stem or default_output_stem(metric, value_column)
    output_dir.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, object]] = []

    for start_year in start_years:
        window, suffix = slice_year_window(metric_df, metric, start_year, years, value_column=value_column)
        output_path = output_dir / f"{file_stem}_{suffix}.png"

        fig, ax = plt.subplots(figsize=(14, 6))
        ax.plot(window["datetime"], window[column_name], color=color, linewidth=0.6)
        ax.set_title(f"{title_prefix} ({suffix.replace('_', '-')})")
        ax.set_xlabel("Datetime")
        ax.set_ylabel(y_label)
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


def maybe_resample_window(
    window: pd.DataFrame,
    metric: str,
    downsample_rule: str | None,
    value_column: str | None = None,
) -> pd.DataFrame:
    """Optionally resample the interactive plot to make HTML lighter."""
    if not downsample_rule:
        return window

    column_name = resolve_value_column(metric, value_column)
    return (
        window.set_index("datetime")[[column_name]]
        .resample(downsample_rule)
        .mean()
        .reset_index()
    )


def build_interactive_window_plot(
    metric_df: pd.DataFrame,
    metric: str,
    color: str,
    start_year: int,
    years: int = 1,
    downsample_rule: str | None = None,
    value_column: str | None = None,
    plot_title: str | None = None,
    y_axis_label: str | None = None,
) -> tuple[figure, pd.DataFrame, pd.DataFrame, str, str]:
    """Build one interactive Bokeh plot plus its source-window metadata."""
    column_name = resolve_value_column(metric, value_column)
    title_prefix = plot_title or default_plot_title(metric, value_column)
    y_label = y_axis_label or default_y_axis_label(metric, value_column)

    window, suffix = slice_year_window(metric_df, metric, start_year, years, value_column=value_column)
    plot_df = maybe_resample_window(window, metric, downsample_rule, value_column=value_column)
    title = f"{title_prefix} ({suffix.replace('_', '-')})"

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
    line_renderer = plot.line("datetime", column_name, source=source, line_width=1, line_color=color)
    plot.add_tools(
        HoverTool(
            renderers=[line_renderer],
            tooltips=[
                ("Datetime", "@datetime{%Y-%m-%d %H:%M}"),
                (y_label, f"@{{{column_name}}}"),
            ],
            formatters={"@datetime": "datetime"},
            mode="vline",
        )
    )
    plot.xaxis.axis_label = "Datetime"
    plot.yaxis.axis_label = y_label
    plot.grid.grid_line_alpha = 0.25
    plot.toolbar.logo = None
    return plot, plot_df, window, suffix, title


def show_interactive_windows(
    metric_df: pd.DataFrame,
    metric: str,
    color: str,
    selected_years: list[int],
    years: int = 1,
    downsample_rule: str | None = None,
    value_column: str | None = None,
    plot_title: str | None = None,
    y_axis_label: str | None = None,
) -> list[dict[str, object]]:
    """Render zoomable interactive plots inline inside a Jupyter notebook."""
    results: list[dict[str, object]] = []
    if selected_years:
        output_notebook(hide_banner=True)

    for start_year in selected_years:
        plot, plot_df, window, _, title = build_interactive_window_plot(
            metric_df,
            metric,
            color,
            start_year,
            years=years,
            downsample_rule=downsample_rule,
            value_column=value_column,
            plot_title=plot_title,
            y_axis_label=y_axis_label,
        )
        show(plot)
        results.append(
            {
                "start_year": start_year,
                "rows": len(plot_df),
                "source_rows": len(window),
                "title": title,
                "kind": "inline",
                "downsample_rule": downsample_rule,
            }
        )

    return results


def export_interactive_windows(
    metric_df: pd.DataFrame,
    metric: str,
    color: str,
    output_dir: Path,
    selected_years: list[int],
    years: int = 1,
    downsample_rule: str | None = None,
    value_column: str | None = None,
    plot_title: str | None = None,
    y_axis_label: str | None = None,
    output_stem: str | None = None,
) -> list[dict[str, object]]:
    """Export standalone zoomable HTML plots for selected year windows."""
    file_stem = output_stem or default_output_stem(metric, value_column)
    html_dir = output_dir / "interactive"
    html_dir.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, object]] = []

    for start_year in selected_years:
        plot, plot_df, window, suffix, title = build_interactive_window_plot(
            metric_df,
            metric,
            color,
            start_year,
            years=years,
            downsample_rule=downsample_rule,
            value_column=value_column,
            plot_title=plot_title,
            y_axis_label=y_axis_label,
        )
        output_path = html_dir / f"{file_stem}_{suffix}.html"

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
