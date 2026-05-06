'''
Take the final CP-SAT solution and populate the existing SU/VWS workbook.

This version writes into the actual roster template/grid instead of replacing
it with a flat table. It also avoids appending fallback rows below the Control
and Dispatch templates.

It populates:
    - VWS Roster: by matching Date + Base row, then filling the correct role slot
    - Control Roster: by matching Week/Date row, then filling control role slots
    - Dispatch Roster: by matching Week/Date row, then filling dispatch role slots

It preserves the workbook layout as far as possible and only clears/writes the
assignment cells, not the date/base/heading structure.
'''

from __future__ import annotations

from datetime import date as DateType, datetime
from pathlib import Path
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

from openpyxl import load_workbook
from openpyxl.cell.cell import MergedCell
from openpyxl.utils import get_column_letter
from ortools.sat.python import cp_model


FEASIBLE_STATUSES = {cp_model.OPTIMAL, cp_model.FEASIBLE}

VWS_SHEET = "VWS Roster"
CONTROL_SHEET = "Control Roster"
DISPATCH_SHEET = "Dispatch Roster"
HELITACK_SHEET = "Helitack"

# Final output should only contain the roster result tabs.
# Helitack assignments are written into the VWS Roster as Helitack STB rows,
# so the standalone Helitack/source/cleaning sheets are removed from the saved copy.
OUTPUT_ROSTER_SHEETS = [VWS_SHEET, CONTROL_SHEET, DISPATCH_SHEET]

def _keep_only_roster_sheets(wb) -> None:
    """Remove all non-roster sheets from the output workbook.

    This is done after the roster tabs are populated, so the input template can
    still contain all source/cleaning sheets while the saved output is a clean
    deliverable containing only VWS Roster, Control Roster, and Dispatch Roster.
    """
    keep = set(OUTPUT_ROSTER_SHEETS)
    missing = [name for name in OUTPUT_ROSTER_SHEETS if name not in wb.sheetnames]
    if missing:
        raise ValueError(f"Cannot make roster-only workbook; missing required roster sheets: {missing}")

    for sheet_name in list(wb.sheetnames):
        if sheet_name not in keep:
            del wb[sheet_name]

    # Put the sheets in a predictable order.
    wb._sheets.sort(key=lambda ws: OUTPUT_ROSTER_SHEETS.index(ws.title))



# ---------------------------------------------------------------------------
# Solution extraction
# ---------------------------------------------------------------------------

def _sort_key(row: Dict[str, Any]) -> Tuple[str, ...]:
    return tuple(str(row.get(k, "")) for k in ["date", "week", "base", "role", "volunteer_id"])


def _selected_rows_from_solution(solution) -> Dict[str, List[Dict[str, Any]]]:
    '''
    Convert Scheduler.build_and_solve(...) output into plain row dictionaries.

    Scheduler returns:
        solver, status, x, x_heli, x_ctrl, x_disp, model

    where:
        x[v, d, b, r]       = VWS standby assignment
        x_heli[v, d, r]     = helitack assignment
        x_ctrl[v, w]        = control assignment
        x_disp[v, w, r]     = dispatch assignment
    '''
    solver, status, x, x_heli, x_ctrl, x_disp, model = solution

    if status not in FEASIBLE_STATUSES:
        raise ValueError(
            "No feasible roster can be written because CP-SAT did not return "
            f"OPTIMAL or FEASIBLE. Status was: {solver.StatusName(status)}"
        )

    rows = {
        "vws": [],
        "helitack": [],
        "control": [],
        "dispatch": [],
    }

    for (v, d, b, r), var in x.items():
        if solver.Value(var) == 1:
            rows["vws"].append({
                "date": d,
                "base": b,
                "role": r,
                "volunteer_id": v,
            })

    for (v, d, r), var in x_heli.items():
        if solver.Value(var) == 1:
            rows["helitack"].append({
                "date": d,
                "role": r,
                "volunteer_id": v,
            })

    for (v, w), var in x_ctrl.items():
        if solver.Value(var) == 1:
            rows["control"].append({
                "week": w,
                "role": "Control",
                "volunteer_id": v,
            })

    for (v, w, r), var in x_disp.items():
        if solver.Value(var) == 1:
            rows["dispatch"].append({
                "week": w,
                "role": r,
                "volunteer_id": v,
            })

    for key in rows:
        rows[key] = sorted(rows[key], key=_sort_key)

    return rows


# ---------------------------------------------------------------------------
# Normalisation helpers
# ---------------------------------------------------------------------------

def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _norm_text(value: Any) -> str:
    return re.sub(r"\s+", " ", _clean_text(value).lower())


def _norm_key(value: Any) -> str:
    '''Uppercase alphanumeric key. Good for matching base/header variants.'''
    return re.sub(r"[^A-Z0-9]", "", _clean_text(value).upper())


def _norm_base(value: Any) -> str:
    '''Normalise base names from JSON and workbook cells to the same key.'''
    key = _norm_key(value)

    aliases = {
        "HDBHCSU": "HDBHCSU",
        "HDBHC": "HDBHCSU",
        "HDBSU": "HDBHCSU",
        "STBHC": "STBHC",
        "NWLHC1": "NWLHC1",
        "NWLHC2": "NWLHC2",
        "NWLSU": "NWLSU",
        "SPSHC": "SPSHC",
        "SPSBP": "SPSBP",
        "HELITACKSTB": "HELITACKSTB",
    }

    return aliases.get(key, key)


def _parse_date(value: Any) -> Optional[str]:
    '''Return YYYY-MM-DD where possible.'''
    if value is None or value == "":
        return None

    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, DateType):
        return value.strftime("%Y-%m-%d")

    text = _clean_text(value)
    if not text:
        return None

    # Remove repeated spaces and full stops sometimes used in base/date cells.
    # Some workbook cells display dates as "Wednesday,10 December 2025"
    # with no space after the comma. Python's strptime expects the space,
    # so normalise this before trying the known formats.
    text = re.sub(r"\s+", " ", text).strip().rstrip(".")
    text = re.sub(r",\s*", ", ", text)

    formats = [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%A, %d %B %Y",   # Saturday, 15 November 2025
        "%A %d %B %Y",    # Saturday 15 November 2025
        "%d %B %Y",       # 15 November 2025
        "%d %b %Y",       # 15 Nov 2025
    ]
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass

    # Last defensive attempt for strings that contain a date inside extra text.
    m = re.search(r"(\d{4}-\d{2}-\d{2})", text)
    if m:
        return m.group(1)

    return None



def _date_obj(value: Any) -> Optional[DateType]:
    """Return a Python date object when a value can be parsed as a date."""
    parsed = _parse_date(value)
    if not parsed:
        return None
    try:
        return datetime.strptime(parsed, "%Y-%m-%d").date()
    except ValueError:
        return None


def _days_between(a: Any, b: Any) -> Optional[int]:
    """Absolute number of days between two parseable date values."""
    da = _date_obj(a)
    db = _date_obj(b)
    if da is None or db is None:
        return None
    return abs((da - db).days)

def _role_category(role: Any) -> Optional[str]:
    '''Map solver role names to the type of column that should be filled.'''
    r = _norm_key(role)

    if r in {"CL", "CREWLEADER"}:
        return "CL"
    if r in {"ACL", "ASSISTANTCL", "ASSISTANTCREWLEADER", "TRAINEEACL"}:
        return "ACL"
    if r in {"FF", "FIREFIGHTER", "RECRUITFF", "FFNR", "FF2YR", "NR"}:
        return "FF"
    if r in {"CREWDRIVER", "TRUCKDRIVER", "DRIVER"}:
        return "CREW_DRIVER"
    if r == "SKIDDRIVER":
        return "SKID_DRIVER"
    if r in {"LOGISTICS", "LOGISTICSSUPPORT", "LOGISTICSANDSUPPORT"}:
        return "LOGISTICS"
    if r == "PLANNING":
        return "PLANNING"
    if r in {"CONTROL", "CTRL"}:
        return "CONTROL"
    if r in {"MGR", "DISPATCHMANAGER", "MANAGER"}:
        return "DISPATCH_MANAGER"
    if r in {"NORM", "DISPATCHER", "NORMALDISPATCHER"}:
        return "DISPATCHER"
    if r in {"TRAINEE", "TRAINEEDISPATCHER"}:
        return "TRAINEE_DISPATCHER"

    return None


def _header_category(header: Any) -> Optional[str]:
    '''Map workbook header cells to role categories.'''
    h = _norm_text(header)
    hk = _norm_key(header)

    if not h:
        return None

    # Important: assistant crew leader must be checked before crew leader,
    # because the header may contain both words.
    if "assistant" in h or hk in {"ACL", "ASSISTANTCL", "ASSISTANTCREWLEADER"}:
        return "ACL"
    if "crew leader" in h or hk in {"CL", "CREWLEADER"}:
        return "CL"
    if "firefighter" in h or hk in {"FF", "FIREFIGHTER"}:
        return "FF"
    if "crew driver" in h or hk == "CREWDRIVER":
        return "CREW_DRIVER"
    if "skid driver" in h or hk == "SKIDDRIVER":
        return "SKID_DRIVER"
    if "truck driver" in h or hk == "TRUCKDRIVER":
        # Some workbooks have a separate Truck Driver column. If not, truck
        # driver assignments will fall back to Crew Driver later.
        return "TRUCK_DRIVER"
    if "logistics" in h:
        return "LOGISTICS"
    if "planning" in h:
        return "PLANNING"
    if "control" in h:
        return "CONTROL"
    if "dispatch manager" in h or "manager" in h or hk in {"MGR", "DISPATCHMANAGER"}:
        return "DISPATCH_MANAGER"
    if "trainee" in h and "dispatch" in h:
        return "TRAINEE_DISPATCHER"
    if "dispatcher" in h or hk in {"NORM", "DISPATCHER"}:
        return "DISPATCHER"

    return None


# ---------------------------------------------------------------------------
# Workbook scanning helpers
# ---------------------------------------------------------------------------

def _require_sheet(wb, sheet_name: str):
    if sheet_name not in wb.sheetnames:
        raise ValueError(
            f"The workbook does not contain a sheet called '{sheet_name}'. "
            f"Available sheets are: {wb.sheetnames}"
        )
    return wb[sheet_name]


def _find_table_header_row(ws, required_labels: Iterable[str], max_scan_rows: int = 40) -> int:
    '''Find the row containing labels like Date/Base/Time/etc.'''
    required = {_norm_key(x) for x in required_labels}
    best_row = 1
    best_score = -1

    for r in range(1, min(ws.max_row, max_scan_rows) + 1):
        row_keys = {_norm_key(ws.cell(row=r, column=c).value) for c in range(1, ws.max_column + 1)}
        score = sum(1 for label in required if label in row_keys)
        if score > best_score:
            best_score = score
            best_row = r

    return best_row


def _find_column_by_header(ws, header_row: int, possible_labels: Iterable[str]) -> Optional[int]:
    possible = {_norm_key(x) for x in possible_labels}
    for c in range(1, ws.max_column + 1):
        if _norm_key(ws.cell(row=header_row, column=c).value) in possible:
            return c
    return None




def _detect_date_column_by_values(ws, max_scan_rows: int = 120) -> Tuple[Optional[int], Optional[int]]:
    """Return (date_column, first_date_row) by scanning actual date values.

    This is useful for Control/Dispatch sheets whose headings may not literally
    say Date or Week, but which still contain week/date values in the body.
    """
    best_col = None
    best_count = 0
    best_first_row = None

    for c in range(1, ws.max_column + 1):
        count = 0
        first_row = None
        for r in range(1, min(ws.max_row, max_scan_rows) + 1):
            if _parse_date(ws.cell(row=r, column=c).value):
                count += 1
                if first_row is None:
                    first_row = r
        if count > best_count:
            best_count = count
            best_col = c
            best_first_row = first_row

    if best_count == 0:
        return None, None
    return best_col, best_first_row


def _find_role_header_row(ws, max_scan_rows: int = 40) -> Optional[int]:
    """Find the row that has the most role-like headers."""
    best_row = None
    best_count = 0
    for r in range(1, min(ws.max_row, max_scan_rows) + 1):
        count = 0
        for c in range(1, ws.max_column + 1):
            if _header_category(ws.cell(row=r, column=c).value):
                count += 1
        if count > best_count:
            best_count = count
            best_row = r
    return best_row if best_count > 0 else None


def _role_columns_from_header(ws, header_row: int) -> Dict[str, List[int]]:
    '''Return category -> list of columns using role-like headers.'''
    columns: Dict[str, List[int]] = {}
    for c in range(1, ws.max_column + 1):
        category = _header_category(ws.cell(row=header_row, column=c).value)
        if category:
            columns.setdefault(category, []).append(c)
    return columns


def _is_writeable_cell(cell) -> bool:
    return not isinstance(cell, MergedCell)


def _range_intersects_area(cell_range, min_row: int, max_row: int, min_col: int, max_col: int) -> bool:
    return not (
        cell_range.max_row < min_row
        or cell_range.min_row > max_row
        or cell_range.max_col < min_col
        or cell_range.min_col > max_col
    )


def _unmerge_ranges_touching_columns(ws, start_row: int, columns: Iterable[int]) -> None:
    """Unmerge merged cells that block assignment cells in chosen columns.

    The VWS template has merged cells in parts of the roster. openpyxl treats
    the non-top-left cells of a merged range as read-only MergedCell objects.
    We only unmerge ranges that touch the output columns below the header, so
    title/header formatting is left alone as far as possible.
    """
    cols = sorted({int(c) for c in columns if c})
    if not cols:
        return

    for merged_range in list(ws.merged_cells.ranges):
        for col_idx in cols:
            if _range_intersects_area(
                merged_range,
                min_row=start_row,
                max_row=ws.max_row,
                min_col=col_idx,
                max_col=col_idx,
            ):
                ws.unmerge_cells(str(merged_range))
                break


def _clear_role_cells(ws, header_row: int, role_columns: Dict[str, List[int]]) -> None:
    '''Clear only assignment cells under role columns. Do not touch Date/Base.'''
    all_cols = sorted({c for cols in role_columns.values() for c in cols})
    if not all_cols:
        return

    # Make cells below the header writeable before clearing/writing.
    _unmerge_ranges_touching_columns(ws, header_row + 1, all_cols)

    for row_idx in range(header_row + 1, ws.max_row + 1):
        for col_idx in all_cols:
            cell = ws.cell(row=row_idx, column=col_idx)
            if _is_writeable_cell(cell):
                cell.value = None


def _get_available_columns(role_columns: Dict[str, List[int]], category: Optional[str]) -> List[int]:
    '''Return candidate columns for a role category, with sensible fallbacks.'''
    if not category:
        return []

    cols = list(role_columns.get(category, []))

    # Fallbacks based on how the VWS template may label columns.
    if not cols and category == "TRUCK_DRIVER":
        cols = list(role_columns.get("CREW_DRIVER", []))
    if not cols and category == "CREW_DRIVER":
        cols = list(role_columns.get("TRUCK_DRIVER", []))
    if not cols and category == "LOGISTICS":
        # Some templates use one shared logistics/planning/support slot.
        cols = list(role_columns.get("PLANNING", []))

    if not cols and category == "PLANNING":
        # In the SU/VWS roster, Planning may not always have its own visible
        # column. Keep the correct column first if it exists, then fall back to
        # Logistics, and only then to the driver/support slot. This prevents the
        # assignment from being lost while still keeping the best match first.
        cols = (
            list(role_columns.get("PLANNING", []))
            + list(role_columns.get("LOGISTICS", []))
            + list(role_columns.get("CREW_DRIVER", []))
        )

    return cols


def _place_value_in_first_empty(ws, row_idx: int, columns: List[int], value: Any) -> bool:
    '''Put value in the first empty cell from candidate columns.

    This helper is kept for Control/Dispatch, where there is normally only one
    template row per week. For VWS rows with possible continuation rows, use
    _place_value_across_candidate_rows(...) below so the formatter tries the
    second/continuation row before appending two IDs into one cell.
    '''
    for col_idx in columns:
        cell = ws.cell(row=row_idx, column=col_idx)
        if not _is_writeable_cell(cell):
            continue
        if cell.value is None or str(cell.value).strip() == "":
            cell.value = value
            return True

    # If all candidate cells were already full, append to the last writeable
    # cell so the assignment is not silently lost.
    for col_idx in reversed(columns):
        cell = ws.cell(row=row_idx, column=col_idx)
        if not _is_writeable_cell(cell):
            continue
        existing = "" if cell.value is None else str(cell.value)
        cell.value = f"{existing}; {value}" if existing else value
        return True

    return False


def _write_to_empty_cell_only(ws, row_idx: int, columns: List[int], value: Any) -> bool:
    '''Write value only if one of the candidate cells is empty.

    Unlike _place_value_in_first_empty, this function does not append into a
    full cell. This matters for the VWS roster because some base/date blocks
    have continuation rows. We want to use the continuation row before placing
    multiple IDs into the same visible cell.
    '''
    for col_idx in columns:
        cell = ws.cell(row=row_idx, column=col_idx)
        if not _is_writeable_cell(cell):
            continue
        if cell.value is None or str(cell.value).strip() == "":
            cell.value = value
            return True
    return False


def _append_value_to_last_writeable(ws, row_idx: int, columns: List[int], value: Any) -> bool:
    '''Append value to the last writeable candidate cell as a final fallback.'''
    for col_idx in reversed(columns):
        cell = ws.cell(row=row_idx, column=col_idx)
        if not _is_writeable_cell(cell):
            continue
        existing = "" if cell.value is None else str(cell.value)
        cell.value = f"{existing}; {value}" if existing else value
        return True
    return False


def _place_value_across_candidate_rows(ws, row_indices: List[int], columns: List[int], value: Any) -> bool:
    '''Write a VWS assignment across all matching date+base rows.

    The SU VWS template sometimes gives a base/date block more than one row,
    especially where the demand for a base is larger than the visible role
    slots in a single row. Earlier versions wrote everything into the first
    matching row and appended overflow into a single cell, leaving the second
    row blank. This function first tries every matching row for an empty slot.
    Only if all rows are full does it append to the last cell of the last row.
    '''
    for row_idx in row_indices:
        if _write_to_empty_cell_only(ws, row_idx, columns, value):
            return True

    for row_idx in reversed(row_indices):
        if _append_value_to_last_writeable(ws, row_idx, columns, value):
            return True

    return False


# ---------------------------------------------------------------------------
# VWS Roster grid writer
# ---------------------------------------------------------------------------

def _build_vws_row_lookup(ws, header_row: int, date_col: int, base_col: int) -> Dict[Tuple[str, str], List[int]]:
    '''Build (YYYY-MM-DD, base_key) -> list of row indices.

    The VWS template uses merged cells and continuation rows. A date may appear
    only once for a block of base rows, and a base may sometimes span/continue
    across more than one row. Therefore we carry forward both the current date
    and current base until a new date/base is encountered. This prevents rows
    like the second SPS BP row from being ignored.
    '''
    lookup: Dict[Tuple[str, str], List[int]] = {}
    current_date: Optional[str] = None
    current_base: Optional[str] = None

    # Stop once we are clearly past the roster body. This avoids accidentally
    # carrying the final base/date through unrelated notes or blank areas.
    blank_run = 0

    for r in range(header_row + 1, ws.max_row + 1):
        parsed = _parse_date(ws.cell(row=r, column=date_col).value)
        if parsed:
            current_date = parsed
            current_base = None
            blank_run = 0

        base_value = ws.cell(row=r, column=base_col).value
        base_key = _norm_base(base_value) if _clean_text(base_value) else None
        if base_key:
            current_base = base_key
            blank_run = 0

        # A row is considered part of the roster body if it has a date, a base,
        # or we are inside an existing date/base block and the row has anything
        # nearby. This lets us include continuation rows whose Date/Base cells
        # are visually merged/blank in Excel.
        row_has_anything_nearby = False
        for c in range(1, min(ws.max_column, base_col + 4) + 1):
            if not _is_blank(ws.cell(row=r, column=c).value):
                row_has_anything_nearby = True
                break

        if current_date and current_base and (base_key or parsed or row_has_anything_nearby):
            lookup.setdefault((current_date, current_base), []).append(r)
            blank_run = 0
        else:
            blank_run += 1
            if blank_run >= 5 and current_date is not None:
                # likely below the roster table
                current_base = None

    # Deduplicate while preserving order, just in case a merged/continued row
    # was encountered more than once.
    for key, row_list in list(lookup.items()):
        seen = set()
        lookup[key] = [rr for rr in row_list if not (rr in seen or seen.add(rr))]

    return lookup


def _candidate_vws_rows(
    row_lookup: Dict[Tuple[str, str], List[int]],
    assignment_date: str,
    assignment_base: str,
) -> List[int]:
    '''Find matching VWS template rows for a CP-SAT assignment.'''
    date_key = _parse_date(assignment_date) or str(assignment_date)
    base_key = _norm_base(assignment_base)

    exact = row_lookup.get((date_key, base_key), [])
    if exact:
        return exact

    # Defensive fallback: if the solver uses a broad base like SPS but the
    # template has SPS HC / SPS BP, allow prefix matching. Exact remains first.
    matches = []
    for (d, b), rows in row_lookup.items():
        if d != date_key:
            continue
        if b.startswith(base_key) or base_key.startswith(b):
            matches.extend(rows)
    return matches


def _write_vws_roster_grid(ws, rows: List[Dict[str, Any]]) -> List[str]:
    '''Populate the existing VWS Roster grid.'''
    warnings: List[str] = []

    header_row = _find_table_header_row(ws, ["Date", "Base"])
    date_col = _find_column_by_header(ws, header_row, ["Date"])
    base_col = _find_column_by_header(ws, header_row, ["Base"])

    if date_col is None or base_col is None:
        raise ValueError(
            f"Could not find Date/Base columns in '{ws.title}'. "
            "The formatter needs those columns to match CP-SAT assignments to template rows."
        )

    role_columns = _role_columns_from_header(ws, header_row)
    if not role_columns:
        raise ValueError(
            f"Could not detect role columns in '{ws.title}'. "
            "Expected headers such as Crew Leader, Assistant CL, Firefighter, Crew Driver, etc."
        )

    _clear_role_cells(ws, header_row, role_columns)
    row_lookup = _build_vws_row_lookup(ws, header_row, date_col, base_col)

    for assignment in rows:
        a_date = str(assignment.get("date", ""))
        a_base = str(assignment.get("base", ""))
        a_role = assignment.get("role", "")
        volunteer_id = assignment.get("volunteer_id", "")

        candidate_rows = _candidate_vws_rows(row_lookup, a_date, a_base)
        if not candidate_rows:
            warnings.append(
                f"VWS assignment not written: no matching row for date={a_date}, base={a_base}, "
                f"role={a_role}, volunteer={volunteer_id}"
            )
            continue

        category = _role_category(a_role)
        cols = _get_available_columns(role_columns, category)
        if not cols:
            warnings.append(
                f"VWS assignment not written: no matching role column for role={a_role}, "
                f"date={a_date}, base={a_base}, volunteer={volunteer_id}"
            )
            continue

        # Try every matching date+base row before appending overflow into a
        # full cell. This is important for bases such as SPS BP where the
        # template may have a continuation row for the same base/date.
        written = _place_value_across_candidate_rows(ws, candidate_rows, cols, volunteer_id)

        if not written:
            warnings.append(
                f"VWS assignment not written: could not write to any candidate cell for "
                f"date={a_date}, base={a_base}, role={a_role}, volunteer={volunteer_id}"
            )

    return warnings


# ---------------------------------------------------------------------------
# Control/Dispatch generic grid writer
# ---------------------------------------------------------------------------

def _build_week_row_lookup(ws, header_row: int, date_or_week_col: int) -> Dict[str, List[int]]:
    lookup: Dict[str, List[int]] = {}
    current_week: Optional[str] = None

    for r in range(header_row + 1, ws.max_row + 1):
        parsed = _parse_date(ws.cell(row=r, column=date_or_week_col).value)
        if parsed:
            current_week = parsed
        if current_week:
            lookup.setdefault(current_week, []).append(r)

    return lookup


def _parsed_date_rows(ws, header_row: int, date_or_week_col: int) -> List[Tuple[str, int]]:
    """Return all rows that contain an actual date in the detected date/week column."""
    out: List[Tuple[str, int]] = []
    for r in range(header_row + 1, ws.max_row + 1):
        parsed = _parse_date(ws.cell(row=r, column=date_or_week_col).value)
        if parsed:
            out.append((parsed, r))
    return out


def _last_used_row_in_columns(ws, columns: Iterable[int]) -> int:
    """Find the last row that has content in the given columns."""
    cols = sorted({int(c) for c in columns if c})
    if not cols:
        return ws.max_row

    last = 1
    for r in range(1, ws.max_row + 1):
        for c in cols:
            value = ws.cell(row=r, column=c).value
            if value is not None and str(value).strip() != "":
                last = max(last, r)
                break
    return last


def _get_or_create_week_row(
    ws,
    week_key: str,
    date_or_week_col: int,
    role_columns: Dict[str, List[int]],
    used_created_rows: Dict[str, int],
) -> int:
    """Create/use a row in the roster sheet when the template has no matching date row.

    This keeps the solution inside the correct Control/Dispatch/Helitack sheet rather
    than silently losing assignments or writing only a far-right fallback table.
    """
    if week_key in used_created_rows:
        return used_created_rows[week_key]

    all_role_cols = sorted({c for cols in role_columns.values() for c in cols})
    relevant_cols = [date_or_week_col] + all_role_cols
    new_row = _last_used_row_in_columns(ws, relevant_cols) + 1

    ws.cell(row=new_row, column=date_or_week_col).value = week_key
    used_created_rows[week_key] = new_row
    return new_row


def _candidate_week_rows(
    ws,
    week_key: str,
    row_lookup: Dict[str, List[int]],
    parsed_rows: List[Tuple[str, int]],
    date_or_week_col: int,
    role_columns: Dict[str, List[int]],
    used_created_rows: Dict[str, int],
) -> List[int]:
    """Find a row for a control/dispatch/helitack assignment.

    First tries exact matching. If the workbook uses a related date in the same
    week, for example a Friday/Saturday while the JSON has a Wednesday window
    start, it uses the closest row within 6 days. If the template does not have
    any matching row, it creates a new row in the same roster sheet.
    """
    exact = row_lookup.get(week_key, [])
    if exact:
        return exact

    best: List[Tuple[int, int]] = []
    for parsed, row_idx in parsed_rows:
        gap = _days_between(parsed, week_key)
        if gap is not None and gap <= 6:
            best.append((gap, row_idx))

    if best:
        best.sort(key=lambda x: (x[0], x[1]))
        min_gap = best[0][0]
        return [row_idx for gap, row_idx in best if gap == min_gap]

    return [_get_or_create_week_row(ws, week_key, date_or_week_col, role_columns, used_created_rows)]


def _write_week_role_grid(ws, rows: List[Dict[str, Any]], sheet_label: str) -> List[str]:
    """Populate Control/Dispatch/Helitack sheets if they are formatted as grids."""
    warnings: List[str] = []

    header_row = _find_table_header_row(ws, ["Date", "Week", "Window Start", "Window_Start"])
    date_or_week_col = _find_column_by_header(
        ws,
        header_row,
        [
            "Date", "Week", "Window Start", "Window_Start", "Week Starting",
            "Start", "Start Date", "From", "Period", "Duty Week",
        ],
    )

    if date_or_week_col is None:
        date_or_week_col, _ = _detect_date_column_by_values(ws)

    if date_or_week_col is None:
        date_or_week_col = ws.max_column + 2
        ws.cell(row=1, column=date_or_week_col).value = "week"
        header_row = 1
        warnings.append(
            f"{sheet_label}: could not detect a Week/Date column, so a simple week column "
            "was created on the right of the sheet."
        )

    role_header_row = _find_role_header_row(ws) or header_row
    role_columns = _role_columns_from_header(ws, role_header_row)

    if not role_columns:
        base_col = max(ws.max_column + 1, date_or_week_col + 1)
        if sheet_label.lower().startswith("control"):
            created = {"CONTROL": [base_col]}
            ws.cell(row=1, column=base_col).value = "Control"
        elif sheet_label.lower().startswith("dispatch"):
            created = {
                "DISPATCH_MANAGER": [base_col],
                "DISPATCHER": [base_col + 1],
                "TRAINEE_DISPATCHER": [base_col + 2],
            }
            ws.cell(row=1, column=base_col).value = "Dispatch Manager"
            ws.cell(row=1, column=base_col + 1).value = "Dispatcher"
            ws.cell(row=1, column=base_col + 2).value = "Trainee Dispatcher"
        else:
            created = {
                "CL": [base_col],
                "ACL": [base_col + 1],
                "FF": [base_col + 2],
            }
            ws.cell(row=1, column=base_col).value = "CL"
            ws.cell(row=1, column=base_col + 1).value = "ACL"
            ws.cell(row=1, column=base_col + 2).value = "FF"
        role_columns = created
        role_header_row = 1
        warnings.append(
            f"{sheet_label}: could not detect role columns, so simple role columns were "
            "created on the right of the sheet."
        )

    clear_start_header = max(header_row, role_header_row)
    _clear_role_cells(ws, clear_start_header, role_columns)
    row_lookup = _build_week_row_lookup(ws, clear_start_header, date_or_week_col)
    parsed_rows = _parsed_date_rows(ws, clear_start_header, date_or_week_col)
    created_rows: Dict[str, int] = {}

    for assignment in rows:
        week = str(assignment.get("week") or assignment.get("date") or "")
        week_key = _parse_date(week) or week
        role = assignment.get("role", "")
        volunteer_id = assignment.get("volunteer_id", "")
        category = _role_category(role)
        cols = _get_available_columns(role_columns, category)

        if not cols:
            warnings.append(
                f"{sheet_label} assignment not written: no matching role column for role={role}, "
                f"week/date={week}, volunteer={volunteer_id}"
            )
            continue

        candidate_rows = _candidate_week_rows(
            ws,
            week_key,
            row_lookup,
            parsed_rows,
            date_or_week_col,
            role_columns,
            created_rows,
        )

        written = False
        for row_idx in candidate_rows:
            if _place_value_in_first_empty(ws, row_idx, cols, volunteer_id):
                written = True
                break
        if not written:
            warnings.append(
                f"{sheet_label} assignment not written: could not write to any candidate cell for "
                f"week/date={week}, role={role}, volunteer={volunteer_id}"
            )

    return warnings



# ---------------------------------------------------------------------------
# Dedicated Control and Dispatch template writers
# ---------------------------------------------------------------------------

def _first_data_row_from_date_col(ws, header_row: int, date_col: int) -> int:
    """Return the first row below header_row that contains a parseable date."""
    for r in range(header_row + 1, ws.max_row + 1):
        if _parse_date(ws.cell(row=r, column=date_col).value):
            return r
    return header_row + 1


def _date_rows_from_column(ws, first_data_row: int, date_col: int) -> List[Tuple[str, int]]:
    """Return (YYYY-MM-DD, row) for all rows with dates in date_col."""
    out: List[Tuple[str, int]] = []
    for r in range(first_data_row, ws.max_row + 1):
        parsed = _parse_date(ws.cell(row=r, column=date_col).value)
        if parsed:
            out.append((parsed, r))
    return out


def _find_best_date_col_before_or_near_role(ws, role_header_row: int, role_cols: Iterable[int]) -> Optional[int]:
    """Find the likely week/date column for Control or Dispatch sheets.

    These sheets often have dates in the columns immediately before the role
    columns. We intentionally avoid using the role columns themselves, because
    writing a volunteer ID into a date column breaks formulas and creates #VALUE!.
    """
    role_cols = sorted({int(c) for c in role_cols if c})
    left_limit = min(role_cols) if role_cols else ws.max_column + 1

    # First try obvious header names on the same role-header row and nearby rows.
    labels = [
        "Week Starting", "Week Start", "Start", "Start Date", "Date", "Week",
        "Window Start", "Window_Start", "From", "Duty Week",
    ]
    for r in range(max(1, role_header_row - 3), min(ws.max_row, role_header_row + 3) + 1):
        for c in range(1, left_limit):
            if _norm_key(ws.cell(row=r, column=c).value) in {_norm_key(x) for x in labels}:
                return c

    # Then score all columns to the left of the role area by number of real date values.
    best_col = None
    best_count = 0
    for c in range(1, left_limit):
        count = 0
        for r in range(1, ws.max_row + 1):
            if _parse_date(ws.cell(row=r, column=c).value):
                count += 1
        if count > best_count:
            best_count = count
            best_col = c

    if best_col is not None and best_count > 0:
        return best_col

    # Final fallback: any date column in the sheet.
    detected, _ = _detect_date_column_by_values(ws)
    return detected


def _row_for_assignment_date(
    ws,
    target_date: str,
    date_rows: List[Tuple[str, int]],
    date_col: int,
    role_columns: Dict[str, List[int]],
    created_rows: Dict[str, int],
) -> Optional[int]:
    """Find the existing template row for target_date.

    Earlier versions appended rows below the Control/Dispatch templates when a
    date string could not be parsed. That is why duplicate/fallback rows appeared
    underneath the roster. This version only writes into the existing roster
    rows. If no row is found, it returns None and reports a warning.
    """
    # Exact match first.
    for parsed, row_idx in date_rows:
        if parsed == target_date:
            return row_idx

    # Some sheets use a related date in the same duty window. Use the closest
    # existing row within 6 days, but never create/append new rows.
    close: List[Tuple[int, int]] = []
    for parsed, row_idx in date_rows:
        gap = _days_between(parsed, target_date)
        if gap is not None and gap <= 6:
            close.append((gap, row_idx))
    if close:
        close.sort(key=lambda x: (x[0], x[1]))
        return close[0][1]

    return None


def _clear_specific_columns(ws, first_data_row: int, columns: Iterable[int]) -> None:
    """Clear only the intended assignment columns from first_data_row down."""
    cols = sorted({int(c) for c in columns if c})
    if not cols:
        return
    _unmerge_ranges_touching_columns(ws, first_data_row, cols)
    for r in range(first_data_row, ws.max_row + 1):
        for c in cols:
            cell = ws.cell(row=r, column=c)
            if _is_writeable_cell(cell):
                cell.value = None


def _clear_old_fallback_rows_below_template(ws, date_col: int, role_cols: Iterable[int], first_data_row: int) -> None:
    """Remove values from fallback rows created by older formatter versions.

    Those rows normally appear below the formatted table with ISO dates in the
    date column and volunteer IDs in the role columns. We clear only rows below
    the last formatted date row; this keeps the actual roster template intact.
    """
    cols = sorted({int(c) for c in role_cols if c})
    if not cols:
        return

    # Last row in the visible/formatted roster table: the last parseable date
    # before any plain ISO fallback block.
    date_rows = []
    for r in range(first_data_row, ws.max_row + 1):
        raw = ws.cell(row=r, column=date_col).value
        parsed = _parse_date(raw)
        if parsed:
            date_rows.append((r, str(raw)))

    if not date_rows:
        return

    # If a row below the table has a plain YYYY-MM-DD value, older versions
    # likely appended it. Clear those values and the assignment cells next to it.
    for r, raw_text in date_rows:
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw_text.strip()):
            ws.cell(row=r, column=date_col).value = None
            for c in cols:
                if _is_writeable_cell(ws.cell(row=r, column=c)):
                    ws.cell(row=r, column=c).value = None


def _write_control_roster_template(ws, rows: List[Dict[str, Any]]) -> List[str]:
    """Populate the existing Control Roster template safely.

    Expected layout in the SU workbook is roughly:
        # | Week Starting | Ending | Control
    The only cell that should receive the volunteer ID is the Control column.
    """
    warnings: List[str] = []
    if not rows:
        return warnings

    # Do not use the broad role-header detector first here: the title row often
    # contains words like "ControlDuty" and can be mistaken for the actual
    # Control assignment column. Prefer an exact cell that says "Control".
    role_header_row = 1
    role_columns: Dict[str, List[int]] = {}
    control_cols: List[int] = []
    for r in range(1, min(ws.max_row, 15) + 1):
        for c in range(1, ws.max_column + 1):
            if _norm_key(ws.cell(row=r, column=c).value) == "CONTROL":
                role_header_row = r
                control_cols = [c]
                role_columns = {"CONTROL": control_cols}
                break
        if control_cols:
            break

    if not control_cols:
        role_header_row = _find_role_header_row(ws) or _find_table_header_row(ws, ["Control"])
        role_columns = _role_columns_from_header(ws, role_header_row)
        control_cols = role_columns.get("CONTROL", [])

    if not control_cols:
        # Last resort: create a Control column to the right, but do not overwrite existing dates.
        c = ws.max_column + 1
        ws.cell(row=1, column=c).value = "Control"
        role_header_row = 1
        control_cols = [c]
        role_columns = {"CONTROL": control_cols}
        warnings.append("Control Roster: no Control column was detected, so one was created on the right.")

    date_col = _find_best_date_col_before_or_near_role(ws, role_header_row, control_cols)
    if date_col is None:
        # Create a week column just before/near the output area.
        date_col = max(1, min(control_cols) - 1)
        ws.cell(row=role_header_row, column=date_col).value = "Week Starting"
        warnings.append("Control Roster: no Week Starting column was detected, so one was created.")

    first_data_row = _first_data_row_from_date_col(ws, role_header_row, date_col)
    _clear_specific_columns(ws, first_data_row, control_cols)
    _clear_old_fallback_rows_below_template(ws, date_col, control_cols, first_data_row)

    date_rows = _date_rows_from_column(ws, first_data_row, date_col)
    created_rows: Dict[str, int] = {}

    for assignment in rows:
        week_raw = assignment.get("week") or assignment.get("date") or ""
        week_key = _parse_date(week_raw) or str(week_raw)
        volunteer_id = assignment.get("volunteer_id", "")
        row_idx = _row_for_assignment_date(ws, week_key, date_rows, date_col, role_columns, created_rows)
        if row_idx is None:
            warnings.append(
                f"Control Roster assignment not written: no existing template row for week/date={week_raw}, "
                f"volunteer={volunteer_id}"
            )
            continue
        if not _place_value_in_first_empty(ws, row_idx, control_cols, volunteer_id):
            warnings.append(
                f"Control Roster assignment not written: could not write week/date={week_raw}, "
                f"volunteer={volunteer_id}"
            )

    return warnings


def _write_dispatch_roster_template(ws, rows: List[Dict[str, Any]]) -> List[str]:
    """Populate the existing Dispatch Roster template safely.

    Expected layout in the SU workbook is roughly:
        Week Starting | Ending | Dispatch Manager | Dispatcher | Trainee Dispatcher
    """
    warnings: List[str] = []
    if not rows:
        return warnings

    role_header_row = _find_role_header_row(ws) or _find_table_header_row(ws, ["Dispatch Manager", "Dispatcher"])
    role_columns = _role_columns_from_header(ws, role_header_row)

    needed = ["DISPATCH_MANAGER", "DISPATCHER", "TRAINEE_DISPATCHER"]
    if not any(k in role_columns for k in needed):
        # Manual scan of top rows. This handles merged/title rows above the actual headers.
        best_row = role_header_row
        best_cols: Dict[str, List[int]] = {}
        best_count = 0
        for r in range(1, min(ws.max_row, 10) + 1):
            cols = _role_columns_from_header(ws, r)
            count = sum(len(cols.get(k, [])) for k in needed)
            if count > best_count:
                best_row = r
                best_cols = cols
                best_count = count
        if best_count:
            role_header_row = best_row
            role_columns = best_cols

    if not any(k in role_columns for k in needed):
        # Last resort: create the three dispatch role columns on the right.
        start = ws.max_column + 1
        role_header_row = 1
        role_columns = {
            "DISPATCH_MANAGER": [start],
            "DISPATCHER": [start + 1],
            "TRAINEE_DISPATCHER": [start + 2],
        }
        ws.cell(row=role_header_row, column=start).value = "Dispatch Manager"
        ws.cell(row=role_header_row, column=start + 1).value = "Dispatcher"
        ws.cell(row=role_header_row, column=start + 2).value = "Trainee Dispatcher"
        warnings.append("Dispatch Roster: dispatch role columns were not detected, so they were created on the right.")

    all_role_cols = sorted({c for k in needed for c in role_columns.get(k, [])})
    date_col = _find_best_date_col_before_or_near_role(ws, role_header_row, all_role_cols)
    if date_col is None:
        date_col = 1
        ws.cell(row=role_header_row, column=date_col).value = "Week Starting"
        warnings.append("Dispatch Roster: no Week Starting column was detected, so one was created.")

    first_data_row = _first_data_row_from_date_col(ws, role_header_row, date_col)
    _clear_specific_columns(ws, first_data_row, all_role_cols)
    _clear_old_fallback_rows_below_template(ws, date_col, all_role_cols, first_data_row)

    date_rows = _date_rows_from_column(ws, first_data_row, date_col)
    created_rows: Dict[str, int] = {}

    for assignment in rows:
        week_raw = assignment.get("week") or assignment.get("date") or ""
        week_key = _parse_date(week_raw) or str(week_raw)
        role = assignment.get("role", "")
        volunteer_id = assignment.get("volunteer_id", "")
        category = _role_category(role)
        cols = _get_available_columns(role_columns, category)

        if not cols:
            warnings.append(
                f"Dispatch Roster assignment not written: no matching role column for role={role}, "
                f"week/date={week_raw}, volunteer={volunteer_id}"
            )
            continue

        row_idx = _row_for_assignment_date(ws, week_key, date_rows, date_col, role_columns, created_rows)
        if row_idx is None:
            warnings.append(
                f"Dispatch Roster assignment not written: no existing template row for week/date={week_raw}, "
                f"role={role}, volunteer={volunteer_id}"
            )
            continue
        if not _place_value_in_first_empty(ws, row_idx, cols, volunteer_id):
            warnings.append(
                f"Dispatch Roster assignment not written: could not write week/date={week_raw}, "
                f"role={role}, volunteer={volunteer_id}"
            )

    return warnings

# ---------------------------------------------------------------------------
# Optional helitack writer
# ---------------------------------------------------------------------------

def _find_output_marker(ws, marker: str) -> Optional[int]:
    """Return the column where a previously-created output marker exists."""
    target = _norm_key(marker)
    for r in range(1, min(ws.max_row, 20) + 1):
        for c in range(1, ws.max_column + 1):
            if _norm_key(ws.cell(row=r, column=c).value) == target:
                return c
    return None


def _helitack_role_category(role: Any) -> Optional[str]:
    """Helitack uses its own small set of roles, including FF2YR as separate."""
    r = _norm_key(role)
    if r in {"CL", "CREWLEADER"}:
        return "CL"
    if r in {"ACL", "ASSISTANTCL", "ASSISTANTCREWLEADER"}:
        return "ACL"
    if r in {"FF2YR", "NR"}:
        return "FF2YR"
    if r in {"FF", "FIREFIGHTER", "RECRUITFF"}:
        return "FF"
    return None


def _write_helitack_fallback_table(ws, rows: List[Dict[str, Any]]) -> List[str]:
    """Write helitack assignments into a safe table on the Helitack sheet.

    The SU workbook's Helitack sheet is not laid out like the VWS/Control/
    Dispatch roster grids, so the generic writer may not find a Week/Date
    column or CL/ACL/FF columns. This function creates/reuses a clear CP-SAT
    output block on the right of the Helitack sheet and writes the selected
    assignments there without disturbing the existing template.
    """
    warnings: List[str] = []
    if not rows:
        return warnings

    marker = "CP-SAT Helitack Roster"
    start_col = _find_output_marker(ws, marker)
    if start_col is None:
        start_col = ws.max_column + 2

    title_row = 1
    header_row = 2
    first_data_row = 3

    headers = ["Date", "CL", "ACL", "FF 1", "FF 2", "FF 3", "FF2YR"]
    ws.cell(row=title_row, column=start_col).value = marker
    for offset, header in enumerate(headers):
        ws.cell(row=header_row, column=start_col + offset).value = header

    # Unmerge anything that blocks this output block, then clear previous values.
    output_cols = list(range(start_col, start_col + len(headers)))
    _unmerge_ranges_touching_columns(ws, first_data_row, output_cols)
    for r in range(first_data_row, ws.max_row + 1):
        for c in output_cols:
            cell = ws.cell(row=r, column=c)
            if _is_writeable_cell(cell):
                cell.value = None

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        d = _parse_date(row.get("date") or row.get("week")) or str(row.get("date") or row.get("week") or "")
        grouped.setdefault(d, []).append(row)

    role_cols = {
        "CL": [start_col + 1],
        "ACL": [start_col + 2],
        "FF": [start_col + 3, start_col + 4, start_col + 5],
        "FF2YR": [start_col + 6],
    }

    for row_idx, d in enumerate(sorted(grouped), start=first_data_row):
        ws.cell(row=row_idx, column=start_col).value = d
        for assignment in grouped[d]:
            role = assignment.get("role", "")
            volunteer_id = assignment.get("volunteer_id", "")
            category = _helitack_role_category(role)
            cols = role_cols.get(category or "", [])
            if not cols:
                warnings.append(
                    f"Helitack assignment not written: unknown helitack role={role}, "
                    f"week/date={d}, volunteer={volunteer_id}"
                )
                continue
            if not _place_value_in_first_empty(ws, row_idx, cols, volunteer_id):
                warnings.append(
                    f"Helitack assignment not written: could not write role={role}, "
                    f"week/date={d}, volunteer={volunteer_id}"
                )

    return warnings


def _write_helitack_if_present(wb, rows: List[Dict[str, Any]]) -> List[str]:
    warnings: List[str] = []
    if not rows:
        return warnings
    if HELITACK_SHEET not in wb.sheetnames:
        warnings.append(
            f"Helitack assignments exist ({len(rows)}), but no '{HELITACK_SHEET}' sheet was found."
        )
        return warnings

    ws = wb[HELITACK_SHEET]
    warnings.extend(_write_helitack_fallback_table(ws, rows))
    return warnings


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def _write_rows_into_template(
    rows: Dict[str, List[Dict[str, Any]]],
    template_excel_file: str,
    output_excel_file: Optional[str] = None,
) -> None:
    template_path = Path(template_excel_file)
    output_path = Path(output_excel_file) if output_excel_file else template_path

    if not template_path.exists():
        raise FileNotFoundError(f"Template workbook not found: {template_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    wb = load_workbook(template_path)

    vws_ws = _require_sheet(wb, VWS_SHEET)
    ctrl_ws = _require_sheet(wb, CONTROL_SHEET)
    disp_ws = _require_sheet(wb, DISPATCH_SHEET)

    warnings: List[str] = []

    # The SU workbook shows Helitack as "Helitack STB" rows inside the VWS
    # Roster grid. Therefore helitack CP-SAT assignments should be written into
    # the VWS Roster sheet, not dumped into a separate fallback table.
    vws_rows = list(rows["vws"])
    for h in rows.get("helitack", []):
        vws_rows.append({
            "date": h.get("date") or h.get("week"),
            "base": "Helitack STB",
            "role": h.get("role"),
            "volunteer_id": h.get("volunteer_id"),
        })

    warnings.extend(_write_vws_roster_grid(vws_ws, vws_rows))
    warnings.extend(_write_control_roster_template(ctrl_ws, rows["control"]))
    warnings.extend(_write_dispatch_roster_template(disp_ws, rows["dispatch"]))

    # The final deliverable should contain only the roster result sheets.
    _keep_only_roster_sheets(wb)

    wb.save(output_path)

    print(f"Populated workbook saved to: {output_path}")
    print(f"VWS assignments written into '{VWS_SHEET}': {len(rows['vws'])}")
    if rows.get("helitack"):
        print(f"Helitack assignments written into '{VWS_SHEET}' Helitack STB rows: {len(rows['helitack'])}")
    print(f"Control assignments processed for '{CONTROL_SHEET}': {len(rows['control'])}")
    print(f"Dispatch assignments processed for '{DISPATCH_SHEET}': {len(rows['dispatch'])}")
    print(f"Output workbook contains only: {', '.join(OUTPUT_ROSTER_SHEETS)}")

    if warnings:
        print("\nOutput warnings:")
        for warning in warnings:
            print(f"- {warning}")


def write_roster(solution, template_excel_file: str, output_excel_file: Optional[str] = None) -> None:
    '''Populate the existing SU/VWS workbook with the CP-SAT solution.'''
    rows = _selected_rows_from_solution(solution)
    _write_rows_into_template(rows, template_excel_file, output_excel_file)


def write_dummy_roster(template_excel_file: str, output_excel_file: Optional[str] = None) -> None:
    '''
    Testing helper: writes a dummy grid-style solution into the actual workbook.

    Use this to check that the formatter is populating the correct existing
    cells before relying on the real CP-SAT solution.
    '''
    rows = {
        "vws": [
            {"date": "2025-11-15", "base": "NWL HC1", "role": "CL", "volunteer_id": "dummy_CL_001"},
            {"date": "2025-11-15", "base": "NWL HC1", "role": "ACL", "volunteer_id": "dummy_ACL_001"},
            {"date": "2025-11-15", "base": "NWL HC1", "role": "FF", "volunteer_id": "dummy_FF_001"},
            {"date": "2025-11-15", "base": "NWL HC1", "role": "FF", "volunteer_id": "dummy_FF_002"},
            {"date": "2025-11-15", "base": "NWL HC1", "role": "Crew Driver", "volunteer_id": "dummy_DRIVER_001"},
            {"date": "2025-11-15", "base": "NWL HC1", "role": "Skid Driver", "volunteer_id": "dummy_SKID_001"},
        ],
        "helitack": [],
        "control": [
            {"week": "2025-11-28", "role": "Control", "volunteer_id": "dummy_CTRL_001"},
        ],
        "dispatch": [
            {"week": "2025-11-28", "role": "mgr", "volunteer_id": "dummy_DISP_MGR_001"},
            {"week": "2025-11-28", "role": "norm", "volunteer_id": "dummy_DISP_001"},
        ],
    }
    _write_rows_into_template(rows, template_excel_file, output_excel_file)


# ===========================================================================
# v7 OVERRIDES: robust Control + Dispatch table population
# ---------------------------------------------------------------------------
# These override the earlier Control/Dispatch writers above. They do NOT touch
# Scheduler.py or the VWS writer. The fix is specifically for templates where
# the displayed dates after the first row are Excel formulas. openpyxl reads
# those formula cells as strings like '=B3+7', so simple date parsing fails.
# These functions infer the weekly date sequence from the first real date row
# and then write assignments into the existing table rows only.
# ===========================================================================

from datetime import timedelta as _timedelta


def _cell_has_formula(value: Any) -> bool:
    return isinstance(value, str) and value.strip().startswith("=")


def _is_blank(value: Any) -> bool:
    return value is None or str(value).strip() == ""


def _find_exact_header_cell(ws, labels: Iterable[str], max_rows: int = 20) -> Optional[Tuple[int, int]]:
    """Find a cell whose normalised value exactly matches one of labels."""
    targets = {_norm_key(x) for x in labels}
    for r in range(1, min(ws.max_row, max_rows) + 1):
        for c in range(1, ws.max_column + 1):
            if _norm_key(ws.cell(row=r, column=c).value) in targets:
                return r, c
    return None


def _find_header_col_near_row(ws, header_row: int, labels: Iterable[str], max_col: Optional[int] = None) -> Optional[int]:
    """Find a column by exact header label on/near a known header row."""
    targets = {_norm_key(x) for x in labels}
    max_col = max_col or ws.max_column
    for r in range(max(1, header_row - 2), min(ws.max_row, header_row + 2) + 1):
        for c in range(1, max_col + 1):
            if _norm_key(ws.cell(row=r, column=c).value) in targets:
                return c
    return None


def _looks_like_repeated_header_row(ws, row_idx: int, role_cols: Iterable[int], date_col: Optional[int] = None, ending_col: Optional[int] = None) -> bool:
    """Return True for rows that repeat headers like Week Starting / Ending / Dispatcher."""
    keys = []
    if date_col:
        keys.append(_norm_key(ws.cell(row=row_idx, column=date_col).value))
    if ending_col:
        keys.append(_norm_key(ws.cell(row=row_idx, column=ending_col).value))
    for c in role_cols:
        keys.append(_norm_key(ws.cell(row=row_idx, column=c).value))

    header_words = {
        "WEEKSTARTING", "WEEKSTART", "DATE", "ENDING", "END", "CONTROL",
        "DISPATCHMANAGER", "DISPATCHER", "TRAINEEDISPATCHER",
    }
    hits = sum(1 for k in keys if k in header_words)
    return hits >= 2


def _weekly_date_row_map(ws, data_rows: List[int], date_col: int) -> Dict[str, int]:
    """Map YYYY-MM-DD -> row using actual dates, and infer formula rows weekly.

    Many SU roster templates have only the first date as an actual Excel date,
    while following rows are formulas such as '=B3+7'. openpyxl does not
    calculate formulas. This function uses the first parseable date as an
    anchor and maps the remaining data rows by adding 7 days per row.
    """
    mapping: Dict[str, int] = {}
    anchors: List[Tuple[int, DateType]] = []

    for idx, row_idx in enumerate(data_rows):
        parsed = _parse_date(ws.cell(row=row_idx, column=date_col).value)
        if parsed:
            try:
                anchors.append((idx, datetime.strptime(parsed, "%Y-%m-%d").date()))
            except ValueError:
                pass

    if anchors:
        anchor_idx, anchor_date = anchors[0]
        for idx, row_idx in enumerate(data_rows):
            inferred = anchor_date + _timedelta(days=7 * (idx - anchor_idx))
            mapping[inferred.strftime("%Y-%m-%d")] = row_idx

        # Exact parsed dates override inferred dates where Excel provided them.
        for idx, exact_date in anchors:
            mapping[exact_date.strftime("%Y-%m-%d")] = data_rows[idx]

    return mapping


def _clear_values_in_rows(ws, rows_to_clear: Iterable[int], columns: Iterable[int]) -> None:
    cols = sorted({int(c) for c in columns if c})
    if not cols:
        return
    if rows_to_clear:
        _unmerge_ranges_touching_columns(ws, min(rows_to_clear), cols)
    for r in rows_to_clear:
        for c in cols:
            cell = ws.cell(row=r, column=c)
            if _is_writeable_cell(cell):
                cell.value = None


def _clear_fallback_block_below(ws, last_template_row: int, date_cols: Iterable[int], role_cols: Iterable[int]) -> None:
    """Clear old fallback blocks that previous formatters may have appended.

    This does not delete rows. It only clears obvious generated values below
    the actual template table so the output workbook looks clean.
    """
    cols = sorted({int(c) for c in list(date_cols) + list(role_cols) if c})
    if not cols:
        return

    for r in range(last_template_row + 1, ws.max_row + 1):
        # Only clear rows that look like generated output: ISO date in a date
        # column or values in role columns while the row is outside the table.
        looks_generated = False
        for c in date_cols:
            v = ws.cell(row=r, column=c).value
            if isinstance(v, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", v.strip()):
                looks_generated = True
        for c in role_cols:
            v = ws.cell(row=r, column=c).value
            if v is not None and str(v).strip() != "":
                looks_generated = True
        if looks_generated:
            for c in cols:
                cell = ws.cell(row=r, column=c)
                if _is_writeable_cell(cell):
                    cell.value = None


def _control_template_rows(ws, header_row: int, control_col: int, week_col: int) -> List[int]:
    """Detect the real C1..C21 Control roster rows."""
    rows: List[int] = []

    # Best case: the first column has C1, C2, ..., C21.
    for r in range(header_row + 1, ws.max_row + 1):
        label = _clean_text(ws.cell(row=r, column=1).value)
        if re.fullmatch(r"C\d+", label, flags=re.IGNORECASE):
            rows.append(r)

    if rows:
        return rows

    # Fallback: consecutive rows below the header that have week/date content
    # or formulas in the week column. Stop after the first blank block.
    started = False
    blank_run = 0
    for r in range(header_row + 1, ws.max_row + 1):
        v = ws.cell(row=r, column=week_col).value
        if _parse_date(v) or _cell_has_formula(v) or not _is_blank(v):
            rows.append(r)
            started = True
            blank_run = 0
        elif started:
            blank_run += 1
            if blank_run >= 2:
                break
    return rows


def _write_control_roster_template(ws, rows: List[Dict[str, Any]]) -> List[str]:
    """Populate the existing Control Roster table only.

    Expected SU layout:
        A: # / C1..C21
        B: Week Starting
        C: Ending
        D: Control

    The formatter writes volunteer IDs into the Control column only. It does
    not append rows below the table.
    """
    warnings: List[str] = []
    if not rows:
        return warnings

    found = _find_exact_header_cell(ws, ["Control"], max_rows=20)
    if not found:
        warnings.append("Control Roster: could not find the existing Control column.")
        return warnings

    header_row, control_col = found
    week_col = _find_header_col_near_row(ws, header_row, ["Week Starting", "Week Start", "Start", "Date", "Week"], max_col=control_col - 1)
    if week_col is None:
        # In the provided SU template, the week starting column is immediately
        # before Ending, and two columns before Control.
        week_col = max(1, control_col - 2)
        warnings.append("Control Roster: Week Starting column was inferred from the template layout.")

    data_rows = _control_template_rows(ws, header_row, control_col, week_col)
    if not data_rows:
        warnings.append("Control Roster: no existing C1/C2/... template rows were found.")
        return warnings

    _clear_values_in_rows(ws, data_rows, [control_col])
    _clear_fallback_block_below(ws, max(data_rows), [week_col], [control_col])

    date_to_row = _weekly_date_row_map(ws, data_rows, week_col)

    # If the workbook has formulas and no parseable anchor, fall back to pure
    # ordering of the assignments against the template rows.
    unique_weeks = sorted({(_parse_date(a.get("week") or a.get("date")) or str(a.get("week") or a.get("date") or "")) for a in rows})
    order_to_row = {week: data_rows[i] for i, week in enumerate(unique_weeks) if i < len(data_rows)}

    for assignment in sorted(rows, key=lambda r: str(r.get("week") or r.get("date") or "")):
        week_raw = assignment.get("week") or assignment.get("date") or ""
        week_key = _parse_date(week_raw) or str(week_raw)
        volunteer_id = assignment.get("volunteer_id", "")

        row_idx = date_to_row.get(week_key) or order_to_row.get(week_key)
        if row_idx is None:
            warnings.append(
                f"Control Roster assignment not written: no existing template row for week/date={week_raw}, "
                f"volunteer={volunteer_id}"
            )
            continue

        if not _place_value_in_first_empty(ws, row_idx, [control_col], volunteer_id):
            warnings.append(
                f"Control Roster assignment not written: could not write week/date={week_raw}, "
                f"volunteer={volunteer_id}"
            )

    return warnings


def _dispatch_role_columns_exact(ws, max_rows: int = 10) -> Tuple[Optional[int], Dict[str, List[int]]]:
    """Find the real Dispatch Manager / Dispatcher / Trainee Dispatcher columns."""
    best_row: Optional[int] = None
    best_cols: Dict[str, List[int]] = {}
    best_score = 0

    for r in range(1, min(ws.max_row, max_rows) + 1):
        cols: Dict[str, List[int]] = {}
        for c in range(1, ws.max_column + 1):
            key = _norm_key(ws.cell(row=r, column=c).value)
            if key == "DISPATCHMANAGER":
                cols.setdefault("DISPATCH_MANAGER", []).append(c)
            elif key == "DISPATCHER":
                cols.setdefault("DISPATCHER", []).append(c)
            elif key == "TRAINEEDISPATCHER":
                cols.setdefault("TRAINEE_DISPATCHER", []).append(c)
        score = sum(len(v) for v in cols.values())
        if score > best_score:
            best_score = score
            best_row = r
            best_cols = cols

    return best_row, best_cols


def _dispatch_template_rows(ws, header_row: int, date_col: int, ending_col: Optional[int], role_cols: Iterable[int]) -> List[int]:
    """Detect the real Dispatch roster rows and exclude appended fallback rows."""
    role_cols = sorted({int(c) for c in role_cols if c})
    rows: List[int] = []
    started = False
    blank_run = 0

    # The actual template rows have a Week Starting value/formula and usually an
    # Ending value/formula. Old fallback rows had ISO dates but no Ending value.
    for r in range(header_row + 1, ws.max_row + 1):
        if _looks_like_repeated_header_row(ws, r, role_cols, date_col, ending_col):
            continue

        start_v = ws.cell(row=r, column=date_col).value
        end_v = ws.cell(row=r, column=ending_col).value if ending_col else None

        start_like = _parse_date(start_v) or _cell_has_formula(start_v) or not _is_blank(start_v)
        end_like = True if ending_col is None else (_parse_date(end_v) or _cell_has_formula(end_v) or not _is_blank(end_v))

        if start_like and end_like:
            rows.append(r)
            started = True
            blank_run = 0
        elif started:
            blank_run += 1
            if blank_run >= 2:
                break

    return rows


def _write_dispatch_roster_template(ws, rows: List[Dict[str, Any]]) -> List[str]:
    """Populate the existing Dispatch Roster table only.

    Expected SU layout:
        A: Week Starting
        B: Ending
        C: Dispatch Manager
        D: Dispatcher
        E: Trainee Dispatcher

    The formatter writes only to C:E and never appends fallback rows.
    """
    warnings: List[str] = []
    if not rows:
        return warnings

    role_header_row, role_columns = _dispatch_role_columns_exact(ws, max_rows=12)
    if role_header_row is None or not role_columns:
        warnings.append("Dispatch Roster: could not find the existing Dispatch Manager / Dispatcher columns.")
        return warnings

    all_role_cols = sorted({c for cols in role_columns.values() for c in cols})
    date_col = _find_header_col_near_row(
        ws,
        role_header_row,
        ["Week Starting", "Week Start", "Start", "Start Date", "Date", "Week"],
        max_col=min(all_role_cols) - 1,
    )
    ending_col = _find_header_col_near_row(
        ws,
        role_header_row,
        ["Ending", "End", "End Date", "To"],
        max_col=min(all_role_cols) - 1,
    )

    if date_col is None:
        date_col = 1
        warnings.append("Dispatch Roster: Week Starting column was inferred as column A.")
    if ending_col is None and min(all_role_cols) > 2:
        ending_col = min(all_role_cols) - 1

    data_rows = _dispatch_template_rows(ws, role_header_row, date_col, ending_col, all_role_cols)
    if not data_rows:
        warnings.append("Dispatch Roster: no existing template date rows were found.")
        return warnings

    _clear_values_in_rows(ws, data_rows, all_role_cols)
    _clear_fallback_block_below(ws, max(data_rows), [date_col] + ([ending_col] if ending_col else []), all_role_cols)

    date_to_row = _weekly_date_row_map(ws, data_rows, date_col)

    # Ordering fallback is deliberately secondary. The inferred weekly map is
    # preferred because Dispatch may have a first template week with no solver
    # assignment, and pure ordering would shift every assignment up by one row.
    unique_weeks = sorted({(_parse_date(a.get("week") or a.get("date")) or str(a.get("week") or a.get("date") or "")) for a in rows})
    order_to_row = {week: data_rows[i] for i, week in enumerate(unique_weeks) if i < len(data_rows)}

    for assignment in sorted(rows, key=lambda r: (str(r.get("week") or r.get("date") or ""), str(r.get("role", "")))):
        week_raw = assignment.get("week") or assignment.get("date") or ""
        week_key = _parse_date(week_raw) or str(week_raw)
        role = assignment.get("role", "")
        volunteer_id = assignment.get("volunteer_id", "")
        category = _role_category(role)
        cols = _get_available_columns(role_columns, category)

        if not cols:
            warnings.append(
                f"Dispatch Roster assignment not written: no matching role column for role={role}, "
                f"week/date={week_raw}, volunteer={volunteer_id}"
            )
            continue

        row_idx = date_to_row.get(week_key) or order_to_row.get(week_key)
        if row_idx is None:
            warnings.append(
                f"Dispatch Roster assignment not written: no existing template row for week/date={week_raw}, "
                f"role={role}, volunteer={volunteer_id}"
            )
            continue

        if not _place_value_in_first_empty(ws, row_idx, cols, volunteer_id):
            warnings.append(
                f"Dispatch Roster assignment not written: could not write week/date={week_raw}, "
                f"role={role}, volunteer={volunteer_id}"
            )

    return warnings
