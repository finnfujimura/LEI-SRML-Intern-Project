#!/usr/bin/env python3
"""Run the STW pipeline step scripts in order."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = ROOT_DIR / "scripts"
STEP_SCRIPTS = [
    SCRIPTS_DIR / "01_ingest_raw_to_yearly_cleaned.py",
    SCRIPTS_DIR / "02_fill_yearly_gaps.py",
    SCRIPTS_DIR / "03_build_combined_cleaned.py",
    SCRIPTS_DIR / "04_write_reports_and_recheck.py",
    SCRIPTS_DIR / "05_map_combined_columns.py",
    SCRIPTS_DIR / "06_convert_mapped_to_mv_irr.py",
]


def main() -> None:
    for index, script_path in enumerate(STEP_SCRIPTS, start=1):
        print(f"[Pipeline {index}/{len(STEP_SCRIPTS)}] Running {script_path.name}...", flush=True)
        subprocess.run([sys.executable, str(script_path)], cwd=ROOT_DIR, check=True)
    print("Pipeline complete.", flush=True)


if __name__ == "__main__":
    main()
