"""
Safe project entry point for the VWS CP-SAT roster pipeline.

Terminal output  ->  status, solve time, and output file path only.
Everything else  ->  output/solver_log.txt  (via logger.py)

Supports BOTH input styles:
1. Pre-built JSON:     python Main.py ../../data/vws_data.json
2. Cleaned Excel:      python Main.py ../../data/workbook_clean.xlsx

Command examples:
    python Main.py
    python Main.py ../../data/vws_data.json
    python Main.py ../../data/workbook_clean.xlsx
    python Main.py ../../data/vws_data.json ../../output/workbook_with_roster.xlsx
    python Main.py ../../data/vws_data.json ../../output/roster.xlsx ../../data/workbook_clean.xlsx
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict

from ortools.sat.python import cp_model

import logger

from Scheduler import build_and_solve
from OutputFormatter import write_roster

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "data_clean"))
from data_cleaning import load_and_validate  # noqa: E402


VALID_SOLUTION_STATUSES = {cp_model.OPTIMAL, cp_model.FEASIBLE}
EXCEL_SUFFIXES = {".xlsx", ".xlsm", ".xls"}
JSON_SUFFIXES = {".json"}


def _status_name(status: int) -> str:
    names = {
        cp_model.OPTIMAL: "OPTIMAL",
        cp_model.FEASIBLE: "FEASIBLE",
        cp_model.INFEASIBLE: "INFEASIBLE",
        cp_model.MODEL_INVALID: "MODEL_INVALID",
        cp_model.UNKNOWN: "UNKNOWN",
    }
    return names.get(status, str(status))


def _default_project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _load_data(data_input_path: Path) -> Dict[str, Any]:
    suffix = data_input_path.suffix.lower()

    if suffix in JSON_SUFFIXES:
        logger.log_print(f"Loading solver JSON: {data_input_path}")
        with data_input_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            raise ValueError("JSON input must contain a dictionary/object at the top level.")
        return data

    if suffix in EXCEL_SUFFIXES:
        logger.log_print(f"Loading cleaned Excel workbook: {data_input_path}")
        loaded = load_and_validate(data_input_path)
        return loaded

    raise ValueError(
        f"Unsupported data input file type: {data_input_path.suffix}\n"
        "Use either .json for solver-ready data or .xlsx/.xlsm/.xls for the cleaned workbook."
    )


def _resolve_paths():
    project_root = _default_project_root()

    default_json_path     = project_root / "data"   / "vws_data.json"
    default_workbook_path = project_root / "data"   / "workbook_clean.xlsx"
    default_output_path   = project_root / "output" / "workbook_with_roster.xlsx"
    default_log_path      = project_root / "output" / "solver_log.txt"

    default_data_input = default_json_path if default_json_path.exists() else default_workbook_path

    data_input_path = Path(sys.argv[1]).resolve() if len(sys.argv) >= 2 else default_data_input
    output_path     = Path(sys.argv[2]).resolve() if len(sys.argv) >= 3 else default_output_path

    if len(sys.argv) >= 4:
        template_workbook_path = Path(sys.argv[3]).resolve()
    elif data_input_path.suffix.lower() in EXCEL_SUFFIXES:
        template_workbook_path = data_input_path
    else:
        template_workbook_path = default_workbook_path

    return data_input_path, output_path, template_workbook_path, default_log_path


def main() -> int:
    data_input_path, output_path, template_workbook_path, log_path = _resolve_paths()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    logger.setup_logger(log_path)

    total_start = time.time()

    try:
        if not data_input_path.exists():
            print(f"ERROR: Data input file not found: {data_input_path}", file=sys.stderr)
            return 1

        if not template_workbook_path.exists():
            print(f"ERROR: Excel template workbook not found: {template_workbook_path}", file=sys.stderr)
            return 1

        logger.log_print(f"Data input:  {data_input_path}")
        logger.log_print(f"Template:    {template_workbook_path}")
        logger.log_print(f"Output:      {output_path}")
        logger.log_print(f"Log:         {log_path}")
        logger.log_print("")

        data = _load_data(data_input_path)
        logger.log_print(
            f"Data loaded: {len(data.get('volunteer', []))} volunteers, "
            f"{len(data.get('date', []))} dates, "
            f"{len(data.get('base', []))} bases"
        )
        logger.log_print("")

        logger.log_print("Running CP-SAT solver...")
        solution = build_and_solve(data)

        if not isinstance(solution, tuple) or len(solution) < 2:
            print("ERROR: Scheduler did not return the expected solution tuple.", file=sys.stderr)
            return 1

        status      = solution[1]
        status_name = _status_name(status)
        elapsed     = time.time() - total_start

        if status not in VALID_SOLUTION_STATUSES:
            print(f"\nNo feasible solution found.")
            print(f"CP-SAT status : {status_name}")
            print(f"Time elapsed  : {elapsed:.1f}s")
            print(f"See log for details: {log_path}")
            logger.log_print(f"\nNo feasible/optimal solution — CP-SAT returned {status_name}.")
            return 1

        logger.log_print(f"\nWriting roster to workbook: {output_path}")
        write_roster(solution, str(template_workbook_path), str(output_path))

        total_elapsed = time.time() - total_start
        logger.log_print(f"\nPipeline complete. Total wall time: {total_elapsed:.1f}s")

        # ── Only these 4 lines go to the terminal ──────────────────────────
        print(f"\nSolution status : {status_name}")
        print(f"Time elapsed    : {total_elapsed:.1f}s")
        print(f"Roster written  : {output_path}")
        print(f"Diagnostic log  : {log_path}")

        return 0

    except Exception as exc:
        import traceback
        tb = traceback.format_exc()
        logger.log_print(f"\nFATAL ERROR: {exc}\n{tb}")
        print(f"\nFATAL ERROR: {exc}", file=sys.stderr)
        print(f"See log for full traceback: {log_path}", file=sys.stderr)
        return 1

    finally:
        logger.close_logger()


if __name__ == "__main__":
    raise SystemExit(main())