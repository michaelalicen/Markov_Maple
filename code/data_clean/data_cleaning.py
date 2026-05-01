"""
VWS data loader + validator

This script is for the cleaned-workbook stage of the VWS rostering project.

It does three jobs:
1. load the cleaned sheets from the workbook,
2. validate the cleaned data,
3. convert the cleaned sheets into solver-ready Python structures.

It is intentionally based on the cleaned workbook, not the raw workbook.
"""

from __future__ import annotations

'''
    How the data should look:
    data = {
    --- general ---
    "volunteer": ["v001", "v002", ...],  # list of volunteer IDs
    
    "date": ["2025-11-15", "2025-11-16", ...],  # all standby dates in season
    
    "base": ["HDB", "STB", "SPS", "NWL"],
    
    "role": ["FF", "recruit_FF", "ACL", "trainee_ACL", "CL", 
              "crew_driver", "skid_driver", "logistics", "helitack",
              "control", "dispatch"],
    
    "availability": {
        "v001": {
            "2025-11-15": True,
            "2025-11-16": False,
            ...
        },
        ...
    },
    
    "qual": {
        "v001": {
            "role": ["CL", "ACL", "FF"],        # fully qualified roles
            "trainee_roles": [],                   # roles still in training
            "home_base": "HDB",
            "secondary_base": "STB",
            "tertiary_base": "None",
            "extra_shifts": 2,                     # how many extras willing to do
            "extra_shift_role": "CL",              # preferred role for extras
        },
        ...
    },
    
    "demand": {
        "HDB": {
            "2025-11-15": {
                "FF": 4,
                "CL": 1,
                "ACL": 1,
                "skid_driver": 1,
                "crew_driver": 1,
                "logistics": 1,
                ...
            },
            ...
        },
        ...
    },
    
    "pairing_requests": [
        ("v001", "v002"),  # these two have requested to be paired
        ...
    ],
    
    "weekend_map": {
        "2025-11-15": "weekend_01",  # map individual dates to their weekend
        "2025-11-16": "weekend_01",
        "2025-11-22": "weekend_02",
        ...
    },

    "weeks_to_weekends": {
        # maps the ctrl/disp weeks to the weekends so that there is no overlap
        "2025-11-26": ["2025-11-29", "2025-11-30"], 
        "2025-12-03": ["2025-12-06", "2025-12-07"],
    },

    "base_schedule": {
        "weekend_01": ["HDB", "SPS"],  # which bases are active this weekend
        "weekend_02": ["STB", "SPS"],  # HDB/STB alternate
        ...
    }

    #--- heli-tack ---
    "heli_volunteer": [<volunteer_id_heli>],
    "heli_week": [...],
    "heli_demand": {
        "2025-11-15": {
            "FF": 2,
            "CL": 1,
            "ACL": 1
        },
        ...
    },
    "heli_qual": {
        "v001": {
            "role": "CL",        # one of: "CL", "ACL", "FF", "FF2YR"
            "station": "NWL"     # one of: "NWL", "SPS", "STB", "HDB"
        },

    },

    #--- control ---
    "ctrl_volunteer": [<volunteer_id_ctrl>],
    "ctrl_week": [...],
    "ctrl_availability": {...},
    "ctrl_qual": {
        "v001": {
            "role": "Ctrl",
            "disp_control": True # whether this volunteer can be dispatched to control and dispatch simultaneously
        },
        ...
    },

    # --- dispatch ---
    "disp_volunteer": [<volunteer_id_disp>],
    "disp_week": [...],
    "disp_availability": {...},
    "disp_role": ["mgr", "norm", "trainee"],
    "disp_qual": {
        "v001": {
            "role": "Disp",
            "seniority": "mgr",

        },
        ...
    },

}

'''

import json
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import pandas as pd


EXPECTED_SHEETS = {
    "members": "Members Clean Draft",
    "quals": "Qualifications_Clean",
    "availability": "Availability_Clean",
    "demand": "Demand_Clean",
    "helitack": "Helitack_Clean",
    "ctrl_disp_availability": "ControlDispatch_Avail_Clean",
    "ctrl_disp_demand": "ControlDispatch_Demand_Clean",
    "preferences": "Preference_Flags_Clean",
}

REQUIRED_COLUMNS = {
    "members": ["Member_ID", "Home_Base"],
    "quals": ["Member_ID", "Role", "Qualified", "Trainee", "Senior", "Status"],
    "availability": [
        "Member_ID", "Date", "Day_Type", "Available", "Sat_Available",
        "Sun_Available", "Preferred_Base_1", "Preferred_Base_2",
        "Wants_Extra", "Notes", "Missing_Response",
    ],
    "demand": ["Date", "Base", "Role", "Required_Count", "Roster_Type"],
    "helitack": ["Member_ID", "Helitack_Role", "Station", "Qualified"],
    "ctrl_disp_availability": [
        "Member_ID", "Intended_Role", "Window_Start", "Window_End",
        "Window_Type", "Available", "Dual_Role_OK",
    ],
    "ctrl_disp_demand": [
        "Window_Start", "Window_End", "Role", "Required_Count", "Roster_Type",
    ],
}

ROLE_ALIASES = {
    "FF NR": "NR",
    "FF2YR": "NR",
}

CONTROL_ROLE_KEYWORDS = ("control",)
DISPATCH_ROLE_KEYWORDS = ("dispatch",)

DISPATCH_ROLE_MAP = {
    "dispatch manager": "mgr",
    "snr dispatch role": "mgr",
    "dispatcher": "norm",
    "ops dispatch role": "norm",
    "jr dispatch role": "trainee",
    "trainee dispatcher": "trainee",
}


# ---------------------------------------------------------------------------
# Output-format helpers
# ---------------------------------------------------------------------------
# The solver skeleton in DataCleaning(1).py expects a flatter JSON structure
# than the first version of this loader produced.  The helpers below keep the
# validation/reading logic intact, but convert the final output into that
# expected solver-ready shape.

CANONICAL_BASE_ORDER = [
    "HDB", "HDB_HC",
    "STB", "STB_HC",
    "SPS", "SPS_HC", "SPS_BP",
    "NWL", "NWL_HC1", "NWL_HC2", "NWL_SU",
]

CANONICAL_ROLE_ORDER = [
    "FF",
    "recruit_FF",
    "ACL",
    "trainee_ACL",
    "CL",
    "crew_driver",
    "skid_driver",
    "planning",
    "logistics",
    "helitack",
    "control",
    "dispatch",
]


def _normalise_base(value: Any) -> str | None:
    """Return the crew-level base code expected by the solver schema.

    Sub-crews are preserved so the solver can distinguish NWL_HC1 from
    NWL_HC2 from NWL_SU, SPS_BP from SPS_HC, etc.  The four short codes
    (HDB, STB, SPS, NWL) are only returned when no sub-crew suffix is
    present (e.g. from preference/qual fields that store just the base).
    Helitack STB is excluded here because it lives in heli_demand, not
    the main demand dict.
    """
    text = _safe_str(value)
    if not text:
        return None

    upper = text.upper().strip()

    # Explicit sub-crew mappings (Demand_Clean names → solver keys)
    _SUBCREW_MAP = {
        "NWL HC1":      "NWL_HC1",
        "NWL HC2":      "NWL_HC2",
        "NWL SU":       "NWL_SU",
        "SPS BP":       "SPS_BP",
        "SPS HC":       "SPS_HC",
        "HDB HC/SU":    "HDB_HC",
        "STB HC":       "STB_HC",
        "HELITACK STB": None,   # excluded — lives in heli_demand
    }
    if upper in _SUBCREW_MAP:
        return _SUBCREW_MAP[upper]

    # Plain 4-letter codes (used in qual / preference fields)
    if upper in {"HDB", "STB", "SPS", "NWL"}:
        return upper
    if "HELDERBERG" in upper:
        return "HDB"
    if "STELLENBOSCH" in upper:
        return "STB"
    if "SOUTH PENINSULA" in upper:
        return "SPS"
    if "NEWLANDS" in upper:
        return "NWL"

    return text


def _normalise_role(value: Any) -> str | None:
    """Return the compact role name expected by the solver schema."""
    text = _safe_str(value)
    if not text:
        return None

    lookup = {
        "FF": "FF",
        "FF NR": "recruit_FF",
        "FF2YR": "recruit_FF",
        "NR": "recruit_FF",
        "ACL": "ACL",
        "TRAINEE ACL": "trainee_ACL",
        "ACL (TRAINEE)": "trainee_ACL",
        "CL": "CL",
        "CREW DRIVER": "crew_driver",
        "TRUCK DRIVER": "crew_driver",
        "SKID DRIVER": "skid_driver",
        "PLAN": "planning",
        "PLANNING": "planning",
        "PLAN (TRAINEE)": "planning",
        "PLANNING (TRAINEE)": "planning",
        "PLANNING TRAINEE": "planning",
        "LOGISTICS & SUPPORT": "logistics",
        "LOGISTICS": "logistics",
        "HELITACK": "helitack",
        "CONTROL": "control",
        "CTRL": "control",
        "DISPATCH": "dispatch",
        "DISPATCHER": "dispatch",
    }
    upper = text.upper()
    if upper in lookup:
        return lookup[upper]

    # Keep uncommon roles, but make them solver/JSON friendly.
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


def _normalise_heli_role(value: Any) -> str | None:
    """Helitack roles are kept in the style shown in DataCleaning(1).py."""
    text = _safe_str(value)
    if not text:
        return None
    upper = text.upper()
    if upper in {"CL", "ACL", "FF", "FF2YR"}:
        return upper
    if upper in {"FF NR", "NR", "RECRUIT_FF"}:
        return "FF2YR"
    return text


def _ordered_unique(values: Iterable[Any], preferred_order: Iterable[str] | None = None) -> List[str]:
    cleaned = [_safe_str(v) for v in values if _safe_str(v)]
    seen = set(cleaned)
    ordered: List[str] = []

    if preferred_order is not None:
        for value in preferred_order:
            if value in seen:
                ordered.append(value)
                seen.remove(value)

    ordered.extend(sorted(seen))
    return ordered


def _simplify_availability(
    detailed_availability: Dict[str, Dict[str, Any]],
    volunteers: List[str],
    dates: List[str],
) -> Dict[str, Dict[str, bool]]:
    """
    Convert availability from:
        member -> date -> {available, sat, sun, notes, ...}
    to the DataCleaning(1).py shape:
        member -> date -> True/False
    """
    simple: Dict[str, Dict[str, bool]] = {}
    for member_id in volunteers:
        member_dates: Dict[str, bool] = {}
        raw_member = detailed_availability.get(member_id, {})
        for date_key in dates:
            value = raw_member.get(date_key, False)
            if isinstance(value, dict):
                member_dates[date_key] = bool(value.get("available", False))
            else:
                member_dates[date_key] = bool(value)
        simple[member_id] = member_dates
    return simple


def _select_extra_shift_role(record: Dict[str, Any], roles: List[str]) -> str:
    """Return the extra-shift role label expected by the group.

    Interpretation agreed for the JSON output:
    - extra_shifts = 2 means the person can do both FF and driver/planning.
    - extra_shifts = 1 with no FF flag means driver/planning, not None.
    """
    wants_extra = bool(record.get("wants_extra"))
    extra_as_ff = bool(record.get("extra_as_ff"))
    extra_secondary = bool(record.get("extra_secondary"))

    if not wants_extra and not extra_as_ff and not extra_secondary:
        return None

    if (wants_extra or extra_as_ff) and extra_secondary:
        return "FF, driver or planning"

    if extra_as_ff:
        return "FF"

    return "driver or planning"


def _simplify_quals(
    detailed_quals: Dict[str, Dict[str, Any]],
    volunteers: List[str],
) -> Dict[str, Dict[str, Any]]:
    """
    Convert qualification records from the richer validation format into the
    exact key names expected by the solver skeleton.
    """
    result: Dict[str, Dict[str, Any]] = {}

    for member_id in volunteers:
        record = detailed_quals.get(member_id, {})
        raw_roles = record.get("roles", record.get("role", []))
        if isinstance(raw_roles, str):
            raw_roles = [raw_roles]
        roles = _ordered_unique(
            [_normalise_role(role) for role in raw_roles],
            preferred_order=CANONICAL_ROLE_ORDER,
        )

        raw_trainee_roles = record.get("trainee_roles", [])
        if isinstance(raw_trainee_roles, str):
            raw_trainee_roles = [raw_trainee_roles]
        trainee_roles = _ordered_unique(
            [_normalise_role(role) for role in raw_trainee_roles],
            preferred_order=CANONICAL_ROLE_ORDER,
        )

        raw_preferred_bases = record.get("preferred_bases", [])
        if isinstance(raw_preferred_bases, str):
            raw_preferred_bases = [raw_preferred_bases]

        # Keep the preference sheet order exactly as entered.  Do not sort,
        # deduplicate, or remove the home base.  The only time secondary_base
        # or tertiary_base becomes "None" is when that specific preference cell
        # was empty.
        preferred_bases_in_order = list(raw_preferred_bases)[:3]
        while len(preferred_bases_in_order) < 3:
            preferred_bases_in_order.append(None)

        home_base = (
            _normalise_base(record.get("home_base"))
            or _normalise_base(preferred_bases_in_order[0])
            or None
        )
        secondary_base = _normalise_base(preferred_bases_in_order[1]) or None
        tertiary_base = _normalise_base(preferred_bases_in_order[2]) or None

        extra_shifts = 0
        if record.get("wants_extra") or record.get("extra_as_ff"):
            extra_shifts += 1
        if record.get("extra_secondary"):
            extra_shifts += 1

        result[member_id] = {
            "role": roles,
            "trainee_roles": trainee_roles,
            "home_base": home_base,
            "secondary_base": secondary_base,
            "tertiary_base": tertiary_base,
            "extra_shifts": extra_shifts,
            "extra_shift_role": _select_extra_shift_role(record, roles),
        }

    return result


def _convert_demand_to_base_first(
    date_first_demand: Dict[str, Dict[str, Dict[str, int]]]
) -> Dict[str, Dict[str, Dict[str, int]]]:
    """
    Convert demand from:
        date -> base -> role -> count
    to the DataCleaning(1).py shape:
        base -> date -> role -> count

    Helitack demand is excluded here because it is already written separately
    under heli_demand.
    """
    base_first: Dict[str, Dict[str, Dict[str, int]]] = defaultdict(lambda: defaultdict(dict))

    for date_key, base_map in date_first_demand.items():
        for raw_base, role_map in base_map.items():
            if "helitack" in _safe_str(raw_base).lower():
                continue
            base = _normalise_base(raw_base)
            if not base:
                continue
            for raw_role, raw_count in role_map.items():
                role = _normalise_role(raw_role)
                if not role:
                    continue
                count = int(raw_count)
                base_first[base][date_key][role] = base_first[base][date_key].get(role, 0) + count

    return {
        base: {date_key: dict(role_counts) for date_key, role_counts in sorted(date_map.items())}
        for base, date_map in sorted(base_first.items())
    }


def _normalise_base_schedule(base_schedule: Dict[str, List[str]]) -> Dict[str, List[str]]:
    result: Dict[str, List[str]] = {}
    for weekend_id, bases in base_schedule.items():
        cleaned = [
            base for base in (_normalise_base(base) for base in bases)
            if base and _safe_str(base).lower() != "helitack"
        ]
        result[weekend_id] = _ordered_unique(cleaned, preferred_order=CANONICAL_BASE_ORDER)
    return dict(sorted(result.items()))


def _normalise_heli_data(raw: Dict[str, Any]) -> Dict[str, Any]:
    heli_qual: Dict[str, Dict[str, Any]] = {}
    for member_id, record in raw.get("heli_qual", {}).items():
        # Keep only usable helitack records in the solver output.  If the
        # detailed file does not have a qualified flag, keep the record.
        if record.get("qualified", True) is False:
            continue
        heli_qual[member_id] = {
            "role": _normalise_heli_role(record.get("role")) or "FF",
            "station": _normalise_base(record.get("station")) or "None",
        }

    heli_demand: Dict[str, Dict[str, int]] = {}
    for date_key, role_map in raw.get("heli_demand", {}).items():
        cleaned_roles: Dict[str, int] = {}
        for raw_role, raw_count in role_map.items():
            role = _normalise_heli_role(raw_role) or _normalise_role(raw_role)
            if role:
                cleaned_roles[role] = cleaned_roles.get(role, 0) + int(raw_count)
        heli_demand[date_key] = cleaned_roles

    return {
        "heli_volunteer": sorted(heli_qual.keys()),
        "heli_week": sorted(heli_demand.keys()),
        "heli_demand": dict(sorted(heli_demand.items())),
        "heli_qual": heli_qual,
    }


def _simplify_window_availability(
    detailed_availability: Dict[str, Dict[str, Any]],
    volunteers: List[str],
    weeks: List[str],
) -> Dict[str, Dict[str, bool]]:
    simple: Dict[str, Dict[str, bool]] = {}
    for member_id in volunteers:
        raw_member = detailed_availability.get(member_id, {})
        simple[member_id] = {}
        for week_key in weeks:
            value = raw_member.get(week_key, False)
            if isinstance(value, dict):
                simple[member_id][week_key] = bool(value.get("available", False))
            else:
                simple[member_id][week_key] = bool(value)
    return simple


def convert_to_datacleaning_schema(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Convert the richer internal output into the DataCleaning(1).py schema."""
    volunteers = _ordered_unique(raw.get("volunteer", []))
    dates = sorted(raw.get("date", []))

    demand = _convert_demand_to_base_first(raw.get("demand", {}))
    qual = _simplify_quals(raw.get("quals", raw.get("qual", {})), volunteers)
    availability = _simplify_availability(raw.get("availability", {}), volunteers, dates)
    heli_data = _normalise_heli_data(raw)

    discovered_bases = []
    discovered_bases.extend(demand.keys())
    discovered_bases.extend(q.get("home_base") for q in qual.values())
    discovered_bases.extend(q.get("secondary_base") for q in qual.values())
    discovered_bases.extend(q.get("tertiary_base") for q in qual.values())
    bases = _ordered_unique(
        [base for base in discovered_bases if base and base != "None"],
        preferred_order=CANONICAL_BASE_ORDER,
    )

    discovered_roles: List[str] = []
    for record in qual.values():
        discovered_roles.extend(record.get("role", []))
        discovered_roles.extend(record.get("trainee_roles", []))
    for base_map in demand.values():
        for role_counts in base_map.values():
            discovered_roles.extend(role_counts.keys())
    discovered_roles.extend(["helitack", "control", "dispatch"])
    roles = _ordered_unique([*CANONICAL_ROLE_ORDER, *discovered_roles], preferred_order=CANONICAL_ROLE_ORDER)

    ctrl_volunteers = sorted(raw.get("ctrl_volunteer", []))
    ctrl_weeks = sorted(raw.get("ctrl_week", []))
    disp_volunteers = sorted(raw.get("disp_volunteer", []))
    disp_weeks = sorted(raw.get("disp_week", []))

    ctrl_qual = {
        member_id: {
            "role": "Ctrl",
            "disp_control": bool(record.get("disp_control", False)),
        }
        for member_id, record in raw.get("ctrl_qual", {}).items()
    }
    disp_qual = {
        member_id: {
            "role": "Disp",
            "seniority": record.get("seniority", "norm"),
        }
        for member_id, record in raw.get("disp_qual", {}).items()
    }

    output = {
        "volunteer": volunteers,
        "date": dates,
        "base": bases,
        "role": roles,
        "availability": availability,
        "qual": qual,
        "demand": demand,
        "pairing_requests": raw.get("pairing_requests", []),
        "weekend_map": dict(sorted(raw.get("weekend_map", {}).items())),
        "weeks_to_weekends": dict(sorted(raw.get("weeks_to_weekends", {}).items())),
        "base_schedule": _normalise_base_schedule(raw.get("base_schedule", {})),
        **heli_data,
        "ctrl_volunteer": ctrl_volunteers,
        "ctrl_week": ctrl_weeks,
        "ctrl_availability": _simplify_window_availability(
            raw.get("ctrl_availability", {}), ctrl_volunteers, ctrl_weeks
        ),
        "ctrl_qual": ctrl_qual,
        "disp_volunteer": disp_volunteers,
        "disp_week": disp_weeks,
        "disp_availability": _simplify_window_availability(
            raw.get("disp_availability", {}), disp_volunteers, disp_weeks
        ),
        "disp_role": ["mgr", "norm", "trainee"],
        "disp_qual": disp_qual,
    }

    # These are not shown in the skeleton comment, but the solver will usually
    # still need them for control/dispatch coverage.
    if "ctrl_demand" in raw:
        output["ctrl_demand"] = raw["ctrl_demand"]
    if "disp_demand" in raw:
        output["disp_demand"] = raw["disp_demand"]

    return output


@dataclass
class ValidationReport:
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    infos: List[str] = field(default_factory=list)

    def add_error(self, msg: str) -> None:
        self.errors.append(msg)

    def add_warning(self, msg: str) -> None:
        self.warnings.append(msg)

    def add_info(self, msg: str) -> None:
        self.infos.append(msg)

    @property
    def ok(self) -> bool:
        return not self.errors

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ok": self.ok,
            "errors": self.errors,
            "warnings": self.warnings,
            "infos": self.infos,
        }


def _safe_str(value: Any) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def _clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() if c is not None else "" for c in df.columns]
    keep = [c for c in df.columns if c and not c.startswith("Unnamed:")]
    return df.loc[:, keep]


def _drop_empty(df: pd.DataFrame) -> pd.DataFrame:
    return df.dropna(axis=0, how="all").dropna(axis=1, how="all")


def _to_date_str(value: Any) -> str | None:
    if pd.isna(value):
        return None
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return None
    return ts.strftime("%Y-%m-%d")


def _to_bool01(value: Any) -> int:
    if pd.isna(value):
        return 0
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value != 0)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return 1
    if text in {"0", "false", "no", "n", ""}:
        return 0
    try:
        return int(float(text) != 0)
    except Exception:
        return 0


def _first_non_empty(*values: Any) -> str | None:
    for value in values:
        text = _safe_str(value)
        if text and text != "0":
            return text
    return None


def _unique_sorted(values: Iterable[Any]) -> List[str]:
    return sorted({_safe_str(v) for v in values if _safe_str(v)})


def read_preference_flags(workbook_path: Path) -> pd.DataFrame:
    df = pd.read_excel(workbook_path, sheet_name=EXPECTED_SHEETS["preferences"], header=1)
    df = _drop_empty(_clean_columns(df))
    if "Member_ID" not in df.columns:
        raise ValueError("Preference_Flags_Clean is missing Member_ID after loading.")
    return df.reset_index(drop=True)


def read_cleaned_workbook(workbook_path: str | Path) -> Dict[str, pd.DataFrame]:
    workbook_path = Path(workbook_path)
    xls = pd.ExcelFile(workbook_path)

    missing = [sheet for sheet in EXPECTED_SHEETS.values() if sheet not in xls.sheet_names]
    if missing:
        raise ValueError(f"Workbook is missing expected sheets: {missing}")

    frames: Dict[str, pd.DataFrame] = {}
    for key, sheet_name in EXPECTED_SHEETS.items():
        if key == "preferences":
            df = read_preference_flags(workbook_path)
        else:
            df = pd.read_excel(workbook_path, sheet_name=sheet_name)
            df = _drop_empty(_clean_columns(df))

        if "Member ID" in df.columns:
            df = df.rename(columns={"Member ID": "Member_ID"})
        if "Role " in df.columns:
            df = df.rename(columns={"Role ": "Role"})

        frames[key] = df.reset_index(drop=True)

    return frames


def require_columns(df: pd.DataFrame, expected: Iterable[str], name: str, report: ValidationReport) -> None:
    missing = [col for col in expected if col not in df.columns]
    if missing:
        report.add_error(f"{name}: missing required columns {missing}")


def check_no_duplicates(df: pd.DataFrame, subset: List[str], name: str, report: ValidationReport) -> None:
    if any(col not in df.columns for col in subset):
        return
    dup_count = int(df.duplicated(subset=subset, keep=False).sum())
    if dup_count:
        report.add_error(f"{name}: found {dup_count} duplicate rows on key {subset}")


def check_binary(df: pd.DataFrame, cols: Iterable[str], name: str, report: ValidationReport) -> None:
    for col in cols:
        if col not in df.columns:
            continue
        values = {_to_bool01(v) for v in df[col].dropna().tolist()}
        bad = values - {0, 1}
        if bad:
            report.add_error(f"{name}: column {col} has non-binary values {sorted(bad)}")


def check_not_null(df: pd.DataFrame, cols: Iterable[str], name: str, report: ValidationReport) -> None:
    for col in cols:
        if col not in df.columns:
            continue
        count = int(df[col].isna().sum())
        if count:
            report.add_error(f"{name}: column {col} has {count} null values")


def check_allowed_values(df: pd.DataFrame, col: str, allowed: Iterable[str], name: str, report: ValidationReport) -> None:
    if col not in df.columns:
        return
    bad = sorted({_safe_str(v) for v in df[col].dropna().tolist()} - set(allowed))
    if bad:
        report.add_warning(f"{name}: unexpected values in {col}: {bad}")


def validate_cleaned_sheets(frames: Dict[str, pd.DataFrame]) -> ValidationReport:
    report = ValidationReport()

    for key, cols in REQUIRED_COLUMNS.items():
        require_columns(frames[key], cols, key, report)

    members = frames["members"]
    quals = frames["quals"]
    availability = frames["availability"]
    demand = frames["demand"]
    helitack = frames["helitack"]
    ctrl_av = frames["ctrl_disp_availability"]
    ctrl_dem = frames["ctrl_disp_demand"]
    preferences = frames["preferences"]

    check_not_null(members, ["Member_ID", "Home_Base"], "members", report)
    check_no_duplicates(members, ["Member_ID"], "members", report)

    check_not_null(quals, ["Member_ID", "Role", "Qualified", "Trainee", "Senior", "Status"], "quals", report)
    check_binary(quals, ["Qualified", "Trainee", "Senior"], "quals", report)
    check_no_duplicates(quals, ["Member_ID", "Role", "Status"], "quals", report)

    check_not_null(availability, ["Member_ID", "Date", "Available", "Sat_Available", "Sun_Available", "Missing_Response"], "availability", report)
    check_binary(availability, ["Available", "Sat_Available", "Sun_Available", "Wants_Extra", "Missing_Response"], "availability", report)
    check_no_duplicates(availability, ["Member_ID", "Date"], "availability", report)
    check_allowed_values(availability, "Day_Type", {"", "Both", "Sat only", "Sun only", "None"}, "availability", report)

    missing_responders = availability.loc[availability["Missing_Response"].map(_to_bool01) == 1, "Member_ID"].nunique()
    if missing_responders == 6:
        report.add_info("availability: 6 missing responders correctly flagged")
    else:
        report.add_warning(f"availability: expected 6 missing responders, found {missing_responders}")

    bad_missing_rows = availability[
        (availability["Missing_Response"].map(_to_bool01) == 1) &
        (
            availability["Available"].map(_to_bool01).eq(1) |
            availability["Sat_Available"].map(_to_bool01).eq(1) |
            availability["Sun_Available"].map(_to_bool01).eq(1)
        )
    ]
    if not bad_missing_rows.empty:
        report.add_error("availability: some Missing_Response rows are still marked available")

    check_not_null(demand, ["Date", "Base", "Role", "Required_Count", "Roster_Type"], "demand", report)
    check_no_duplicates(demand, ["Date", "Base", "Role"], "demand", report)
    if "Required_Count" in demand.columns:
        bad_counts = pd.to_numeric(demand["Required_Count"], errors="coerce")
        if bad_counts.isna().any():
            report.add_error("demand: some Required_Count values are non-numeric")
        elif (bad_counts <= 0).any():
            report.add_error("demand: found non-positive Required_Count values")

    check_not_null(helitack, ["Member_ID", "Helitack_Role", "Station", "Qualified"], "helitack", report)
    check_binary(helitack, ["Qualified"], "helitack", report)
    check_no_duplicates(helitack, ["Member_ID"], "helitack", report)

    check_not_null(ctrl_av, ["Member_ID", "Intended_Role", "Window_Start", "Window_End", "Window_Type", "Available", "Dual_Role_OK"], "ctrl_disp_availability", report)
    check_binary(ctrl_av, ["Available", "Dual_Role_OK"], "ctrl_disp_availability", report)
    check_no_duplicates(ctrl_av, ["Member_ID", "Window_Start", "Window_End", "Window_Type"], "ctrl_disp_availability", report)

    check_not_null(ctrl_dem, ["Window_Start", "Window_End", "Role", "Required_Count", "Roster_Type"], "ctrl_disp_demand", report)
    check_no_duplicates(ctrl_dem, ["Window_Start", "Window_End", "Role"], "ctrl_disp_demand", report)

    member_ids = set(members["Member_ID"].dropna().astype(str))
    for name in ["quals", "availability", "preferences"]:
        bad = sorted(set(frames[name]["Member_ID"].dropna().astype(str)) - member_ids)
        if bad:
            report.add_error(f"{name}: found {len(bad)} member IDs not present in Members Clean Draft")

    qual_roles = set(quals["Role"].dropna().astype(str))
    demand_roles = set(demand["Role"].dropna().astype(str))
    unmapped = {r for r in demand_roles if r not in qual_roles and ROLE_ALIASES.get(r) not in qual_roles}
    if unmapped:
        report.add_warning(f"demand: some roles are not directly present in qualifications and may need alias handling: {sorted(unmapped)}")

    report.add_info(f"members: {len(member_ids)} unique members")
    report.add_info(f"quals: {len(quals)} rows")
    report.add_info(f"availability: {len(availability)} rows / {availability['Date'].nunique()} weekend dates")
    report.add_info(f"demand: {len(demand)} rows / {demand['Date'].nunique()} daily dates")
    report.add_info(f"helitack: {len(helitack)} rows")
    report.add_info(f"control-dispatch availability: {len(ctrl_av)} rows")
    report.add_info(f"control-dispatch demand: {len(ctrl_dem)} rows")

    return report


def _build_preference_lookup(preferences: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    lookup: Dict[str, Dict[str, Any]] = {}
    for _, row in preferences.iterrows():
        member_id = _safe_str(row.get("Member_ID"))
        if not member_id:
            continue
        lookup[member_id] = {
            # Keep all three preference cells in their original positions.
            # Empty cells are kept as None so secondary/tertiary do not shift
            # left and change meaning.
            "preferred_bases": [
                _first_non_empty(row.get("Preferred_Base_1"), row.get("Preferred_Base_Primary")),
                _first_non_empty(row.get("Preferred_Base_2"), row.get("Preferred_Base_Secondary")),
                _first_non_empty(row.get("Preferred_Base_Tertiary")),
            ],
            "wants_extra": bool(_to_bool01(row.get("Wants_Extra"))),
            "extra_as_ff": bool(_to_bool01(row.get("Extra_As_FF"))),
            "extra_secondary": bool(_to_bool01(row.get("Extra_Secondary"))),
            "pairing_request": bool(_to_bool01(row.get("Pairing_Request"))),
            "raw_note": _first_non_empty(row.get("Raw_Note"), row.get("Raw_Notes_Comments")),
        }
    return lookup


def _extract_pairing_requests(preferences: pd.DataFrame) -> List[Tuple[str, str]]:
    pairs: List[Tuple[str, str]] = []
    uuid_pattern = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.IGNORECASE)

    for _, row in preferences.iterrows():
        member_id = _safe_str(row.get("Member_ID"))
        if not member_id or not _to_bool01(row.get("Pairing_Request")):
            continue
        note = _first_non_empty(row.get("Raw_Note"), row.get("Raw_Notes_Comments")) or ""
        for target in uuid_pattern.findall(note):
            if target != member_id:
                pairs.append((member_id, target))
    return sorted(set(pairs))


def build_main_availability(frames: Dict[str, pd.DataFrame]) -> Tuple[Dict[str, Dict[str, Dict[str, Any]]], Dict[str, str]]:
    availability_df = frames["availability"].copy()
    demand_df = frames["demand"].copy()

    availability_df["Date"] = pd.to_datetime(availability_df["Date"], errors="coerce")
    demand_df["Date"] = pd.to_datetime(demand_df["Date"], errors="coerce")

    demand_dates = set(demand_df["Date"].dropna().dt.strftime("%Y-%m-%d"))
    saturday_starts = sorted(availability_df["Date"].dropna().dt.normalize().unique().tolist())

    weekend_map: Dict[str, str] = {}
    for idx, sat_dt in enumerate(saturday_starts, start=1):
        sat_key = pd.Timestamp(sat_dt).strftime("%Y-%m-%d")
        sun_key = (pd.Timestamp(sat_dt) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        weekend_id = f"weekend_{idx:02d}"
        # Only include dates that actually appear in the demand sheet —
        # this drops pre-season training weekends (e.g. Nov 8-9) that
        # appear in the availability form but have no roster slots.
        if sat_key in demand_dates:
            weekend_map[sat_key] = weekend_id
        if sun_key in demand_dates:
            weekend_map[sun_key] = weekend_id

    availability: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(dict)

    for _, row in availability_df.iterrows():
        member_id = _safe_str(row["Member_ID"])
        weekend_start = pd.to_datetime(row["Date"], errors="coerce")
        if not member_id or pd.isna(weekend_start):
            continue

        sat_date = weekend_start.strftime("%Y-%m-%d")
        sun_date = (weekend_start + pd.Timedelta(days=1)).strftime("%Y-%m-%d")

        shared = {
            "weekend_start": sat_date,
            "day_type": _first_non_empty(row.get("Day_Type")),
            "preferred_base_1": _first_non_empty(row.get("Preferred_Base_1")),
            "preferred_base_2": _first_non_empty(row.get("Preferred_Base_2")),
            "wants_extra": bool(_to_bool01(row.get("Wants_Extra"))),
            "missing_response": bool(_to_bool01(row.get("Missing_Response"))),
            "raw_note": _first_non_empty(row.get("Notes")),
        }

        if sat_date in demand_dates:
            availability[member_id][sat_date] = {
                "available": bool(_to_bool01(row.get("Sat_Available"))),
                "sat": bool(_to_bool01(row.get("Sat_Available"))),
                "sun": False,
                "overall_available": bool(_to_bool01(row.get("Available"))),
                **shared,
            }
        if sun_date in demand_dates:
            availability[member_id][sun_date] = {
                "available": bool(_to_bool01(row.get("Sun_Available"))),
                "sat": False,
                "sun": bool(_to_bool01(row.get("Sun_Available"))),
                "overall_available": bool(_to_bool01(row.get("Available"))),
                **shared,
            }

    return {k: dict(v) for k, v in availability.items()}, weekend_map


def build_main_quals(frames: Dict[str, pd.DataFrame]) -> Dict[str, Dict[str, Any]]:
    members = frames["members"].copy()
    quals = frames["quals"].copy()
    preferences = frames["preferences"].copy()

    home_base = {
        _safe_str(r["Member_ID"]): _safe_str(r["Home_Base"])
        for _, r in members.iterrows()
        if _safe_str(r["Member_ID"])
    }
    pref_lookup = _build_preference_lookup(preferences)

    result: Dict[str, Dict[str, Any]] = {}
    for member_id, grp in quals.groupby("Member_ID"):
        member_id = _safe_str(member_id)
        qualified_roles: List[str] = []
        trainee_roles: List[str] = []
        role_status: Dict[str, str] = {}

        for _, row in grp.iterrows():
            if _to_bool01(row.get("Qualified")) != 1:
                continue
            role = _safe_str(row.get("Role"))
            status = (_safe_str(row.get("Status")) or "active").lower()
            role_status[role] = status
            if _to_bool01(row.get("Trainee")) == 1 or status == "trainee":
                trainee_roles.append(role)
            else:
                qualified_roles.append(role)

        prefs = pref_lookup.get(member_id, {})
        result[member_id] = {
            "roles": sorted(set(qualified_roles)),
            "trainee_roles": sorted(set(trainee_roles)),
            "role_status": role_status,
            "home_base": home_base.get(member_id),
            "preferred_bases": prefs.get("preferred_bases", []),
            "wants_extra": prefs.get("wants_extra", False),
            "extra_as_ff": prefs.get("extra_as_ff", False),
            "extra_secondary": prefs.get("extra_secondary", False),
            "raw_note": prefs.get("raw_note"),
        }

    return result


def build_main_demand(frames: Dict[str, pd.DataFrame]) -> Dict[str, Dict[str, Dict[str, int]]]:
    demand_df = frames["demand"].copy()
    demand_df["Date"] = pd.to_datetime(demand_df["Date"], errors="coerce")

    result: Dict[str, Dict[str, Dict[str, int]]] = defaultdict(lambda: defaultdict(dict))
    for _, row in demand_df.iterrows():
        date_key = _to_date_str(row.get("Date"))
        base = _safe_str(row.get("Base"))
        role = _safe_str(row.get("Role"))
        count = int(row.get("Required_Count"))
        if not date_key or not base or not role:
            continue
        result[date_key][base][role] = count
    return {k: dict(v) for k, v in result.items()}


def build_base_schedule(demand: Dict[str, Dict[str, Dict[str, int]]], weekend_map: Dict[str, str]) -> Dict[str, List[str]]:
    schedule: Dict[str, set[str]] = defaultdict(set)
    for date_key, base_map in demand.items():
        weekend_id = weekend_map.get(date_key)
        if not weekend_id:
            continue
        for base in base_map:
            if base != "Helitack STB":
                schedule[weekend_id].add(base)
    return {k: sorted(v) for k, v in schedule.items()}


def build_weeks_to_weekends(frames: Dict[str, pd.DataFrame]) -> Dict[str, List[str]]:
    ctrl_dem = frames["ctrl_disp_demand"].copy()
    demand = frames["demand"].copy()

    ctrl_dem["Window_Start"] = pd.to_datetime(ctrl_dem["Window_Start"], errors="coerce")
    ctrl_dem["Window_End"] = pd.to_datetime(ctrl_dem["Window_End"], errors="coerce")
    demand["Date"] = pd.to_datetime(demand["Date"], errors="coerce")

    # Demand_Clean has many rows per standby date because each base/role/count is
    # a separate row.  For weeks_to_weekends we only want each actual weekend
    # date once, otherwise the JSON repeats the same Saturday/Sunday many times.
    demand_dates = sorted(set(demand["Date"].dropna().dt.normalize().tolist()))
    mapping_sets: Dict[str, set[str]] = defaultdict(set)

    for _, row in ctrl_dem.iterrows():
        start = pd.to_datetime(row.get("Window_Start"), errors="coerce")
        end = pd.to_datetime(row.get("Window_End"), errors="coerce")
        if pd.isna(start) or pd.isna(end):
            continue

        start = start.normalize()
        end = end.normalize()
        start_key = start.strftime("%Y-%m-%d")

        for dt in demand_dates:
            dt = pd.Timestamp(dt).normalize()
            if start <= dt <= end:
                mapping_sets[start_key].add(dt.strftime("%Y-%m-%d"))

    return {week: sorted(days) for week, days in sorted(mapping_sets.items())}


def build_helitack_data(frames: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    heli = frames["helitack"].copy()
    demand_df = frames["demand"].copy()
    demand_df["Date"] = pd.to_datetime(demand_df["Date"], errors="coerce")

    heli_rows = demand_df.loc[demand_df["Base"].astype(str).str.strip() == "Helitack STB"].copy()
    heli_demand: Dict[str, Dict[str, int]] = defaultdict(dict)
    for _, row in heli_rows.iterrows():
        date_key = _to_date_str(row.get("Date"))
        role = _safe_str(row.get("Role"))
        if date_key and role:
            heli_demand[date_key][role] = int(row.get("Required_Count"))

    heli_qual: Dict[str, Dict[str, Any]] = {}
    for _, row in heli.iterrows():
        member_id = _safe_str(row.get("Member_ID"))
        if not member_id:
            continue
        heli_qual[member_id] = {
            "role": _safe_str(row.get("Helitack_Role")),
            "station": _safe_str(row.get("Station")),
            "qualified": bool(_to_bool01(row.get("Qualified"))),
            "note": _first_non_empty(row.get("Notes")),
        }

    return {
        "heli_volunteer": sorted(heli_qual.keys()),
        "heli_week": sorted(heli_demand.keys()),
        "heli_demand": {k: dict(v) for k, v in heli_demand.items()},
        "heli_qual": heli_qual,
    }


def build_control_dispatch_data(frames: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    av = frames["ctrl_disp_availability"].copy()
    dem = frames["ctrl_disp_demand"].copy()

    av["Window_Start"] = pd.to_datetime(av["Window_Start"], errors="coerce")
    av["Window_End"] = pd.to_datetime(av["Window_End"], errors="coerce")
    dem["Window_Start"] = pd.to_datetime(dem["Window_Start"], errors="coerce")
    dem["Window_End"] = pd.to_datetime(dem["Window_End"], errors="coerce")

    ctrl_availability: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(dict)
    disp_availability: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(dict)
    ctrl_qual: Dict[str, Dict[str, Any]] = {}
    disp_qual: Dict[str, Dict[str, Any]] = {}
    ctrl_volunteers, disp_volunteers = set(), set()
    ctrl_weeks, disp_weeks = set(), set()

    for _, row in av.iterrows():
        member_id = _safe_str(row.get("Member_ID"))
        role_text = _safe_str(row.get("Intended_Role")).lower()
        start_key = _to_date_str(row.get("Window_Start"))
        end_key = _to_date_str(row.get("Window_End"))
        window_type = _safe_str(row.get("Window_Type"))
        raw_note = _first_non_empty(row.get("Raw_Notes"), row.get("Notes"))
        available = bool(_to_bool01(row.get("Available")))
        dual_role_ok = bool(_to_bool01(row.get("Dual_Role_OK")))
        if not member_id or not start_key or not end_key:
            continue

        window_key = start_key  # plain date string, e.g. "2025-11-26"
        payload = {
            "available": available,
            "window_start": start_key,
            "window_end": end_key,
            "window_type": window_type,
            "raw_note": raw_note,
        }

        is_control = any(k in role_text for k in CONTROL_ROLE_KEYWORDS)
        is_dispatch = any(k in role_text for k in DISPATCH_ROLE_KEYWORDS)

        if is_control:
            ctrl_volunteers.add(member_id)
            ctrl_weeks.add(window_key)
            ctrl_availability[member_id][window_key] = payload
            ctrl_qual[member_id] = {
                "role": "Ctrl",
                "disp_control": dual_role_ok or ("dispatch" in role_text),
            }

        if is_dispatch:
            disp_volunteers.add(member_id)
            disp_weeks.add(window_key)
            disp_availability[member_id][window_key] = payload
            disp_qual[member_id] = {
                "role": "Disp",
                "seniority": DISPATCH_ROLE_MAP.get(role_text, "norm"),
                "disp_control": dual_role_ok or ("control" in role_text),
            }

    ctrl_demand: Dict[str, Dict[str, int]] = {}
    disp_demand: Dict[str, Dict[str, int]] = {}

    for _, row in dem.iterrows():
        start_key = _to_date_str(row.get("Window_Start"))
        end_key = _to_date_str(row.get("Window_End"))
        role = _safe_str(row.get("Role"))
        if not start_key or not end_key or not role:
            continue
        key = start_key  # plain start-date string
        count = int(row.get("Required_Count"))
        if role == "Control":
            ctrl_demand[key] = {"Control": count}
        else:
            disp_demand.setdefault(key, {})
            disp_demand[key][role] = count

    return {
        "ctrl_volunteer": sorted(ctrl_volunteers),
        "ctrl_week": sorted(ctrl_weeks),
        "ctrl_availability": {k: dict(v) for k, v in ctrl_availability.items()},
        "ctrl_qual": ctrl_qual,
        "disp_volunteer": sorted(disp_volunteers),
        "disp_week": sorted(disp_weeks),
        "disp_availability": {k: dict(v) for k, v in disp_availability.items()},
        "disp_role": ["mgr", "norm", "trainee"],
        "disp_qual": disp_qual,
        "ctrl_demand": ctrl_demand,
        "disp_demand": disp_demand,
    }


def build_data_structures(frames: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    """
    Build and return the final solver-ready data structure.

    The loader first builds a richer internal representation because that is
    easier to validate and debug.  It then converts that representation into
    the flatter DataCleaning(1).py-style JSON requested by the group.
    """
    members = frames["members"]
    preferences = frames["preferences"]
    demand_df = frames["demand"]

    availability, weekend_map = build_main_availability(frames)
    quals = build_main_quals(frames)
    demand = build_main_demand(frames)
    base_schedule = build_base_schedule(demand, weekend_map)
    weeks_to_weekends = build_weeks_to_weekends(frames)
    pairing_requests = _extract_pairing_requests(preferences)
    heli_data = build_helitack_data(frames)
    ctrl_disp_data = build_control_dispatch_data(frames)

    demand_df["Date"] = pd.to_datetime(demand_df["Date"], errors="coerce")

    detailed = {
        "volunteer": _unique_sorted(members["Member_ID"].tolist()),
        "date": sorted(demand.keys()),
        "base": _unique_sorted(demand_df["Base"].tolist()),
        "role": _unique_sorted(demand_df["Role"].tolist()),
        "availability": availability,
        "quals": quals,
        "demand": demand,
        "pairing_requests": pairing_requests,
        "weekend_map": weekend_map,
        "weeks_to_weekends": weeks_to_weekends,
        "base_schedule": base_schedule,
        **heli_data,
        **ctrl_disp_data,
    }

    return convert_to_datacleaning_schema(detailed)


def load_and_validate(workbook_path: str | Path) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    frames = read_cleaned_workbook(workbook_path)
    report = validate_cleaned_sheets(frames)
    data = build_data_structures(frames)
    return data, report.to_dict()


def main(argv: List[str]) -> int:
    if len(argv) < 2:
        print("Usage: python vws_data_loader.py /path/to/workbook.xlsx [optional_output_json]")
        return 1

    workbook_path = Path(argv[1])
    if not workbook_path.exists():
        print(f"Workbook not found: {workbook_path}")
        return 1

    try:
        data, report = load_and_validate(workbook_path)
    except Exception as exc:
        print(f"Failed to load workbook: {exc}")
        return 1

    # print(json.dumps(report, indent=2))

    if len(argv) >= 3:
        out_path = Path(argv[2])
        with out_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        print(f"Saved structured data to: {out_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))