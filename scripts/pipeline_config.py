#!/usr/bin/env python3
"""
pipeline_config.py -- Centralized configuration for the STW pipeline.

All deployment-specific paths and filenames flow through this module so that
the pipeline can be relocated, renamed, or re-pointed without editing code.

Discovery order:
  1. ``STW_CONFIG`` environment variable (absolute or relative path to a JSON
     file). Errors loudly if the path does not exist.
  2. ``pipeline_config.json`` searched upward from this file's directory.
  3. If nothing is found, the in-code defaults are used and the implicit base
     directory is the repository root.

Merge semantics:
  - Missing keys fall back to the in-code defaults, so an empty ``{}`` JSON
    behaves exactly like having no config file.
  - Unknown keys are rejected to catch typos early.
  - String values that name a directory or workbook are resolved to absolute
    paths.  Relative paths resolve against the directory holding the JSON
    file (or the repo root when no config file is present).
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path


CONFIG_FILE_NAME = "pipeline_config.json"
CONFIG_ENV_VAR = "STW_CONFIG"

_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent

# In-code defaults -- these match the historical hard-coded constants so that
# omitting pipeline_config.json reproduces the original behavior exactly.
_DEFAULTS: dict[str, str] = {
    # Top-level directories (resolved as paths)
    "raw_data_dir": "Raw Data",
    "yearly_cleaned_dir": "yearly cleaned",
    "reports_dir": "reports",
    "final_output_dir": "final output",
    "plots_dir": "plots",
    "column_map_workbook": "STW programs/STW_sitefile_and_mapping.xlsx",

    # Names of the worksheet inside the workbook
    "column_map_sheet_name": "Column Mapping",

    # File names that live inside ``reports_dir``
    "combined_cleaned_file_name": "stw_combined_cleaned.csv",
    "mapped_combined_file_name": "stw_combined_mapped.csv",
    "pipeline_state_file_name": "pipeline_state.json",
    "irr_irregularity_events_name": "stw_irr_irregularity_events.csv",
    "irr_irregularity_plot_index_name": "stw_irr_irregularity_plot_index.csv",

    # File names that live inside ``final_output_dir``
    "mv_irr_csv_name": "stw_mV_Irr.csv",
    "mv_irr_parquet_name": "stw_mV_Irr.parquet",

    # Outputs that live inside ``plots_dir``
    "mv_irr_explorer_notebook_name": "stw_mV_Irr_explorer.ipynb",
    "irr_irregularity_plots_subdir": "irr_irregularity_event_plots",

    # Input layout patterns
    "input_file_folder_prefix": "STW_",
    "yearly_file_name_template": "stw_{year}_cleaned.csv",
}

# Keys whose values should be resolved as filesystem paths.  Everything else
# is treated as a plain string.
_PATH_KEYS: frozenset[str] = frozenset({
    "raw_data_dir",
    "yearly_cleaned_dir",
    "reports_dir",
    "final_output_dir",
    "plots_dir",
    "column_map_workbook",
})

# Keys handled by dedicated parsers rather than the generic string/path path.
_SPECIAL_KEYS: frozenset[str] = frozenset({"force_nan", "pipeline_cutoff_date"})

# Default pipeline cutoff date: rows observed after this date are skipped at
# ingest.  Adjustable via ``pipeline_cutoff_date`` in pipeline_config.json.
_DEFAULT_CUTOFF_DATE = "2024-08-31"

# Allowed top-level keys = string defaults + specially-handled keys.
_ALLOWED_KEYS: frozenset[str] = frozenset(_DEFAULTS) | _SPECIAL_KEYS

# Date/datetime formats accepted in force_nan rules, tried in order.
_DATETIME_FORMATS: tuple[str, ...] = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M",
)
_DATE_ONLY_FORMAT = "%Y-%m-%d"


@dataclass(frozen=True)
class ForceNanRule:
    """A request to force the listed columns to NaN over an inclusive window.

    A date-only ``end`` is interpreted as end-of-day (23:59:59.999999) so that
    a rule like ``{"start": "2020-03-15", "end": "2020-03-15"}`` covers the
    full day, not just one minute past midnight.
    """
    columns: frozenset[str]
    start: datetime
    end: datetime
    reason: str | None = None


@dataclass(frozen=True)
class PipelineConfig:
    """Immutable snapshot of the resolved pipeline configuration."""

    config_path: Path | None
    base_dir: Path

    raw_data_dir: Path
    yearly_cleaned_dir: Path
    reports_dir: Path
    final_output_dir: Path
    plots_dir: Path
    column_map_workbook: Path

    column_map_sheet_name: str

    combined_cleaned_file_name: str
    mapped_combined_file_name: str
    pipeline_state_file_name: str
    irr_irregularity_events_name: str
    irr_irregularity_plot_index_name: str

    mv_irr_csv_name: str
    mv_irr_parquet_name: str

    mv_irr_explorer_notebook_name: str
    irr_irregularity_plots_subdir: str

    input_file_folder_prefix: str
    yearly_file_name_template: str

    pipeline_cutoff_date: date = date(2024, 8, 31)
    force_nan_rules: tuple[ForceNanRule, ...] = field(default_factory=tuple)

    # -- Convenience composed paths ---------------------------------------

    @property
    def combined_cleaned_file(self) -> Path:
        return self.reports_dir / self.combined_cleaned_file_name

    @property
    def mapped_combined_file(self) -> Path:
        return self.reports_dir / self.mapped_combined_file_name

    @property
    def pipeline_state_file(self) -> Path:
        return self.reports_dir / self.pipeline_state_file_name

    @property
    def irr_irregularity_events_file(self) -> Path:
        return self.reports_dir / self.irr_irregularity_events_name

    @property
    def irr_irregularity_plot_index_file(self) -> Path:
        return self.reports_dir / self.irr_irregularity_plot_index_name

    @property
    def mv_irr_csv(self) -> Path:
        return self.final_output_dir / self.mv_irr_csv_name

    @property
    def mv_irr_parquet(self) -> Path:
        return self.final_output_dir / self.mv_irr_parquet_name

    @property
    def mv_irr_explorer_notebook(self) -> Path:
        return self.plots_dir / self.mv_irr_explorer_notebook_name

    @property
    def irr_irregularity_plots_dir(self) -> Path:
        return self.plots_dir / self.irr_irregularity_plots_subdir

    def yearly_cleaned_file(self, year: int) -> Path:
        return self.yearly_cleaned_dir / self.yearly_file_name_template.format(year=year)


def _discover_config_path() -> Path | None:
    """Find the active config file or return None when no override is set."""
    override = os.environ.get(CONFIG_ENV_VAR)
    if override:
        candidate = Path(override).expanduser()
        if not candidate.is_absolute():
            candidate = (Path.cwd() / candidate).resolve()
        if not candidate.is_file():
            raise FileNotFoundError(
                f"{CONFIG_ENV_VAR}={override!r} does not point at an existing file."
            )
        return candidate

    for directory in (_SCRIPT_DIR, *_SCRIPT_DIR.parents):
        candidate = directory / CONFIG_FILE_NAME
        if candidate.is_file():
            return candidate
    return None


def _resolve_path(base_dir: Path, value: str | os.PathLike[str]) -> Path:
    """Resolve *value* into an absolute path, relative paths against *base_dir*."""
    p = Path(value).expanduser()
    return p if p.is_absolute() else (base_dir / p).resolve()


def _parse_rule_datetime(value: object, where: str, *, is_end: bool) -> datetime:
    """Parse a JSON value into a datetime, accepting date or datetime strings.

    When *is_end* is True and the input is a date-only string, the result is
    padded out to end-of-day so date-only windows cover the full final day.
    """
    if not isinstance(value, str) or not value.strip():
        raise ValueError(
            f"{where}: expected a non-empty date/datetime string, got {value!r}"
        )
    text = value.strip()
    for fmt in _DATETIME_FORMATS:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    try:
        parsed = datetime.strptime(text, _DATE_ONLY_FORMAT)
    except ValueError as exc:
        raise ValueError(
            f"{where}: invalid date/time {text!r}. Accepted formats: "
            f"'YYYY-MM-DD', 'YYYY-MM-DD HH:MM', 'YYYY-MM-DD HH:MM:SS'."
        ) from exc
    if is_end:
        return parsed.replace(hour=23, minute=59, second=59, microsecond=999999)
    return parsed


def _parse_cutoff_date(raw: object, source: object) -> date:
    """Parse the ``pipeline_cutoff_date`` config value into a date."""
    if raw is None:
        raw = _DEFAULT_CUTOFF_DATE
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError(
            f"{source}: 'pipeline_cutoff_date' must be a 'YYYY-MM-DD' string, got {raw!r}."
        )
    try:
        return datetime.strptime(raw.strip(), _DATE_ONLY_FORMAT).date()
    except ValueError as exc:
        raise ValueError(
            f"{source}: invalid 'pipeline_cutoff_date' {raw!r}. Expected 'YYYY-MM-DD'."
        ) from exc


def _parse_force_nan_rules(raw: object, source: object) -> tuple[ForceNanRule, ...]:
    """Validate and parse the ``force_nan`` entry of the config."""
    if raw is None:
        return ()
    if not isinstance(raw, list):
        raise ValueError(
            f"{source}: 'force_nan' must be a list of rule objects, got "
            f"{type(raw).__name__}."
        )

    allowed_rule_keys = {"columns", "start", "end", "reason"}
    required_rule_keys = {"columns", "start", "end"}
    rules: list[ForceNanRule] = []
    for idx, entry in enumerate(raw):
        where = f"{source}: force_nan[{idx}]"
        if not isinstance(entry, dict):
            raise ValueError(f"{where}: each rule must be a JSON object.")

        unknown = sorted(set(entry) - allowed_rule_keys)
        if unknown:
            raise ValueError(
                f"{where}: unknown keys {unknown}. Allowed: {sorted(allowed_rule_keys)}."
            )
        missing = sorted(required_rule_keys - set(entry))
        if missing:
            raise ValueError(f"{where}: missing required keys {missing}.")

        raw_columns = entry["columns"]
        if not isinstance(raw_columns, list) or not raw_columns:
            raise ValueError(
                f"{where}.columns must be a non-empty list of strings."
            )
        cleaned_columns: list[str] = []
        for col_idx, col in enumerate(raw_columns):
            if not isinstance(col, str) or not col.strip():
                raise ValueError(
                    f"{where}.columns[{col_idx}] must be a non-empty string, got {col!r}."
                )
            cleaned_columns.append(col.strip())

        start = _parse_rule_datetime(entry["start"], f"{where}.start", is_end=False)
        end = _parse_rule_datetime(entry["end"], f"{where}.end", is_end=True)
        if start > end:
            raise ValueError(
                f"{where}: 'start' ({start.isoformat()}) is after 'end' ({end.isoformat()})."
            )

        reason = entry.get("reason")
        if reason is not None and not isinstance(reason, str):
            raise ValueError(f"{where}.reason must be a string when provided.")

        rules.append(
            ForceNanRule(
                columns=frozenset(cleaned_columns),
                start=start,
                end=end,
                reason=reason,
            )
        )
    return tuple(rules)


@lru_cache(maxsize=1)
def load_config() -> PipelineConfig:
    """Load and cache the active configuration."""
    config_path = _discover_config_path()
    if config_path is not None:
        with config_path.open("r", encoding="utf-8") as handle:
            overrides = json.load(handle)
        if not isinstance(overrides, dict):
            raise ValueError(
                f"{config_path} must contain a JSON object at the top level."
            )
        base_dir = config_path.parent
    else:
        overrides = {}
        base_dir = _REPO_ROOT

    unknown = sorted(set(overrides) - _ALLOWED_KEYS)
    if unknown:
        location = config_path if config_path is not None else "<defaults>"
        raise ValueError(
            f"Unknown config key(s) in {location}: {unknown}. Allowed keys: "
            f"{sorted(_ALLOWED_KEYS)}."
        )

    source_label = config_path if config_path is not None else "<defaults>"
    merged: dict[str, object] = {"config_path": config_path, "base_dir": base_dir}
    for key, default_value in _DEFAULTS.items():
        raw_value = overrides.get(key, default_value)
        if key in _PATH_KEYS:
            if not isinstance(raw_value, (str, os.PathLike)):
                raise ValueError(
                    f"{key!r} in {source_label} must be a string path, "
                    f"got {type(raw_value).__name__}."
                )
            merged[key] = _resolve_path(base_dir, raw_value)
        else:
            if not isinstance(raw_value, str):
                raise ValueError(
                    f"{key!r} in {source_label} must be a string, "
                    f"got {type(raw_value).__name__}."
                )
            merged[key] = raw_value

    merged["pipeline_cutoff_date"] = _parse_cutoff_date(
        overrides.get("pipeline_cutoff_date"), source_label
    )
    merged["force_nan_rules"] = _parse_force_nan_rules(
        overrides.get("force_nan"), source_label
    )

    return PipelineConfig(**merged)  # type: ignore[arg-type]


def reload_config() -> PipelineConfig:
    """Clear the cache and reload the config -- useful for tests and notebooks."""
    load_config.cache_clear()
    return load_config()
